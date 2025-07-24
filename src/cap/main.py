import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from opentelemetry import trace

from cap.api.router import router
from cap.telemetry import setup_telemetry, instrument_app
from cap.data.virtuoso import VirtuosoClient
from cap.config import settings
from cap.etl.cdb.service import etl_service

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)
setup_telemetry()

# Configure ETL logging
etl_logger = logging.getLogger('cap.etl')
etl_logger.setLevel(getattr(logging, settings.ETL_LOG_LEVEL))

async def initialize_graph(client: VirtuosoClient, graph_uri: str, ontology_path: str) -> bool:
    """Initialize a graph with ontology data if it doesn't exist."""
    with tracer.start_as_current_span("initialize_graph") as span:
        span.set_attribute("graph_uri", graph_uri)
        span.set_attribute("ontology_path", ontology_path)

        try:
            exists = await client.check_graph_exists(graph_uri)

            if not exists:
                span.set_attribute("creating_new_graph", True)

                with open("src/ontologies/cardano.ttl", "r") as f:
                    turtle_data = f.read()

                await client.create_graph(graph_uri, turtle_data)
                logger.info(f"Successfully initialized graph: {graph_uri}")
                return True

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
            (settings.CARDANO_GRAPH, "src/ontologies/cardano.ttl")
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
            await etl_service.start_etl(
                batch_size=settings.ETL_BATCH_SIZE,
                sync_interval=settings.ETL_SYNC_INTERVAL,
                continuous=settings.ETL_CONTINUOUS
            )
            logger.info("ETL service auto-started successfully")
        except Exception as e:
            logger.error(f"Failed to auto-start ETL service: {e}")
            # Don't fail application startup if ETL fails
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

    yield

    # Shutdown
    await stop_etl_service()
    logger.info("Application shutdown completed")

def create_application() -> FastAPI:
    """Create and configure the FastAPI application with ETL integration."""
    app = FastAPI(
        title="CAP",
        description="Cardano Analytics Platform with ETL Pipeline",
        version="0.1.0",
        lifespan=lifespan
    )
    
    instrument_app(app)
    app.include_router(router)
    
    return app

app = create_application()
