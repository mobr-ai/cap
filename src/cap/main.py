import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from opentelemetry import trace

from cap.api.router import router
from cap.telemetry import setup_telemetry, instrument_app
from cap.virtuoso import VirtuosoClient
from cap.config import settings

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)
setup_telemetry()

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
            (settings.CARDANO_GRAPH, "src/ontologies/cardano.ttl"),
            # Other required graphs in the future
        ]

        initialization_results = []
        for graph_uri, ontology_path in required_graphs:
            try:
                result = await initialize_graph(client, graph_uri, ontology_path)
                initialization_results.append((graph_uri, result))
            except Exception as e:
                logger.error(f"Failed to initialize graph {graph_uri}: {e}")
                raise RuntimeError(f"Application startup failed: {e}")

        span.set_attribute("initialization_results", str(initialization_results))
        logger.info("Graph initialization completed successfully")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    with tracer.start_as_current_span("application_startup") as span:
        client = VirtuosoClient()

        try:
            await initialize_required_graphs(client)
            logger.info("Application startup completed successfully")
        except Exception as e:
            span.set_attribute("startup_error", str(e))
            logger.error(f"Application startup failed: {e}")
            raise RuntimeError(f"Application startup failed: {e}")

    yield

    logger.info("Application shutdown completed")

def create_application() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="CAP",
        description="Cardano Analytics Platform",
        version="0.1.0",
        lifespan=lifespan
    )
    
    instrument_app(app)
    app.include_router(router)
    
    return app

app = create_application()