# src/cap/main.py
import logging
import uvloop
import asyncio
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles

from starlette.responses import FileResponse  # <- add
from opentelemetry import trace
from sqlalchemy import text

from cap.api.router import router as api_router
from cap.telemetry import setup_telemetry, instrument_app
from cap.data.virtuoso import VirtuosoClient
from cap.config import settings
from cap.etl.cdb.service import etl_service

from cap.database.session import engine
from cap.database.model import Base
from cap.api.auth import router as auth_router
from cap.api.waitlist import router as wait_router

from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

# Configure ETL logging
etl_logger = logging.getLogger('cap.etl')
etl_logger.setLevel(getattr(logging, settings.LOG_LEVEL))

# Set uvloop as the event loop policy
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

async def initialize_graph(client: VirtuosoClient, graph_uri: str, ontology_path: str) -> bool:
    """Initialize a graph with ontology data if it doesn't exist."""
    with tracer.start_as_current_span("initialize_graph") as span:
        span.set_attribute("graph_uri", graph_uri)
        span.set_attribute("ontology_path", ontology_path)

        try:
            exists = await client.check_graph_exists(graph_uri)

            if not exists:
                span.set_attribute("creating_new_graph", True)

                if ontology_path != "":
                    with open(ontology_path, "r") as f:
                        turtle_data = f.read()
                else:
                    turtle_data = ""

                await client.create_graph(graph_uri, turtle_data)
                exists = await client.check_graph_exists(graph_uri)
                if exists:
                    logger.info(f"Successfully initialized graph: {graph_uri}")
                    return True

                else:
                    logger.error(f"Could not create graph: {graph_uri}")
                    return False

            logger.info(f"Graph already exists: {graph_uri}")
            return False

        except Exception as e:
            span.set_attribute("error", str(e))
            logger.error(f"Failed to initialize graph {graph_uri}: {e}")
            raise RuntimeError(f"Failed to initialize graph {graph_uri}: {e}")

async def initialize_required_graphs(client: VirtuosoClient) -> None:
    """Initialize all required graphs for the application."""
    with tracer.start_as_current_span("initialize_required_graphs") as span:
        required_graphs = [
            (settings.CARDANO_GRAPH, "src/ontologies/cardano.ttl"),
            (f"{settings.CARDANO_GRAPH}/metadata", "")
        ]

        initialization_results = []
        for graph_uri, ontology_path in required_graphs:
            try:
                if ontology_path:
                    result = await initialize_graph(client, graph_uri, ontology_path)

                else:
                    # Create empty graph for data
                    exists = await client.check_graph_exists(graph_uri)
                    if not exists:
                        await client.create_graph(graph_uri, "")
                        logger.info(f"Created empty graph: {graph_uri}")
                        result = True
                    else:
                        result = False

                initialization_results.append((graph_uri, result))
            except Exception as e:
                logger.error(f"Failed to initialize graph {graph_uri}: {e}")
                raise RuntimeError(f"Application startup failed: {e}")

        span.set_attribute("initialization_results", str(initialization_results))
        logger.info("Graph initialization completed successfully")

async def start_etl_service():
    """Start the ETL service if configured to auto-start."""
    if settings.ETL_AUTO_START:
        try:
            logger.info("Auto-starting ETL service...")
            # Run ETL startup in a separate task to avoid blocking
            asyncio.create_task(
                etl_service.start_etl(
                    batch_size=settings.ETL_BATCH_SIZE,
                    sync_interval=settings.ETL_SYNC_INTERVAL,
                    continuous=settings.ETL_CONTINUOUS
                )
            )
            logger.info("ETL service auto-start task scheduled")
        except Exception as e:
            logger.error(f"Failed to auto-start ETL service: {e}")
    else:
        logger.info("ETL auto-start disabled. ETL service can be started manually.")

async def stop_etl_service():
    """Stop the ETL service gracefully."""
    try:
        logger.info("Stopping ETL service...")
        await etl_service.stop_etl()
        logger.info("ETL service stopped successfully")
    except Exception as e:
        logger.error(f"Error stopping ETL service: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager with ETL integration."""
    with tracer.start_as_current_span("application_startup") as span:
        client = VirtuosoClient()

        try:
            # Initialize graphs
            await initialize_required_graphs(client)
            logger.info("Application startup completed successfully")

            # Start ETL service
            await start_etl_service()

        except Exception as e:
            span.set_attribute("startup_error", str(e))
            logger.error(f"Application startup failed: {e}")
            raise RuntimeError(f"Application startup failed: {e}")

    try:
        yield
    finally:
        # Shutdown
        await stop_etl_service()
        logger.info("Application shutdown completed")

def setup_tracing():
    # Only set up tracing if explicitly enabled
    if settings.ENABLE_TRACING:
        setup_telemetry()

    else:
        # Set a no-op tracer provider to disable tracing
        trace.set_tracer_provider(trace.NoOpTracerProvider())

def create_application() -> FastAPI:
    setup_tracing()
    app = FastAPI(
        title="CAP",
        description="Cardano Analytics Platform with ETL Pipeline",
        version="0.1.0",
        lifespan=lifespan,
    )

    instrument_app(app)

    # Place all backend routes under /api
    app.include_router(api_router, prefix="/api")
    app.include_router(auth_router, prefix="/api")
    app.include_router(wait_router, prefix="/api")

    return app

app = create_application()

# DB init
Base.metadata.create_all(bind=engine)
with engine.begin() as conn:
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS waiting_list (
      id SERIAL PRIMARY KEY,
      email TEXT UNIQUE NOT NULL,
      ref TEXT,
      language TEXT
    )
    """))

FRONTEND_DIST = os.getenv(
    "FRONTEND_DIST",
    os.path.join(os.path.dirname(__file__), "static")  # must contain index.html
)

# 1) Serve built assets (Vite puts them under 'assets/')
assets_dir = os.path.join(FRONTEND_DIST, "assets")
if os.path.isdir(assets_dir):
    app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

# 2) Root -> index.html
@app.get("/", include_in_schema=False)
async def index():
    return FileResponse(os.path.join(FRONTEND_DIST, "index.html"))

# 3) Catch-all SPA fallback (except API routes). Also serve real files if they exist.
@app.get("/{full_path:path}", include_in_schema=False)
async def spa_fallback(full_path: str):
    if full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="Not Found")

    candidate = os.path.join(FRONTEND_DIST, full_path)
    if os.path.isfile(candidate):
        return FileResponse(candidate)

    return FileResponse(os.path.join(FRONTEND_DIST, "index.html"))