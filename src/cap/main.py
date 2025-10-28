import logging
import uvloop
import asyncio
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from starlette.responses import FileResponse
from opentelemetry import trace
from sqlalchemy import text

from cap.api.router import router as api_router
from cap.api.nl_query import router as nl_router
from cap.telemetry import setup_telemetry, instrument_app
from cap.data.virtuoso import VirtuosoClient
from cap.config import settings
from cap.etl.cdb.service import etl_service
from cap.services.ollama_client import cleanup_ollama_client
from cap.services.redis_client import cleanup_redis_client

from cap.database.session import engine
from cap.database.model import Base
from cap.api.auth import router as auth_router
from cap.api.waitlist import router as wait_router
from cap.api.cache_admin import router as cache_router
from cap.api.etl_admin import router as etl_router

from dotenv import load_dotenv
load_dotenv()

# Allowed frontend origins (comma-separated env optional)
DEFAULT_CORS = [
    "http://localhost:5173",   # Vite dev
    "http://localhost:4173",   # Vite preview
    "http://0.0.0.0:8000",     # your current UI origin (from the screenshot)
    "http://localhost:8000",
    "https://cap.mobr.ai",     # production
]
ENV_CORS = os.getenv("CORS_ORIGINS", "")
ALLOWED_ORIGINS = [o.strip() for o in ENV_CORS.split(",") if o.strip()] or DEFAULT_CORS

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
    if settings.ETL_AUTO_START:
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
        await cleanup_ollama_client()
        await cleanup_redis_client()
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
        description="Cardano Analytics Platform with ETL Pipeline and Natural Language Queries",
        version="0.1.0",
        lifespan=lifespan,
    )

    instrument_app(app)

    # CORS (handles preflight + normal responses)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOWED_ORIGINS,            # no "*" when using credentials/Authorization
        allow_credentials=True,                   # you send Authorization / may send cookies
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "Accept", "X-Requested-With"],
        expose_headers=["Content-Disposition"],   # add more if client needs to read them
    )

    # Place all backend routes under /api
    app.include_router(api_router)
    app.include_router(nl_router)
    app.include_router(auth_router)
    app.include_router(wait_router)
    app.include_router(cache_router)
    app.include_router(etl_router)

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
    os.path.join(os.path.dirname(__file__), "static")
)

# 1) Serve built assets
assets_dir = os.path.join(FRONTEND_DIST, "assets")
if os.path.isdir(assets_dir):
    app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

# 2) LLM interface route (must come before catch-all)
# @app.get("/llm", include_in_schema=False)
# async def llm_interface():
#     """Serve the LLM natural language query interface."""
#     llm_page = os.path.join(os.path.dirname(__file__), "templates", "llm.html")
#     if os.path.isfile(llm_page):
#         return FileResponse(llm_page)
#     raise HTTPException(status_code=404, detail="LLM interface not found")

# 3) Root -> index.html
@app.get("/", include_in_schema=False)
async def index():
    return FileResponse(os.path.join(FRONTEND_DIST, "index.html"))

# 4) Catch-all SPA fallback (must be last)
@app.get("/{full_path:path}", include_in_schema=False)
async def spa_fallback(full_path: str):
    if full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="Not Found")

    candidate = os.path.join(FRONTEND_DIST, full_path)
    if os.path.isfile(candidate):
        return FileResponse(candidate)

    return FileResponse(os.path.join(FRONTEND_DIST, "index.html"))