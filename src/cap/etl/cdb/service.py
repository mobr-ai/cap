"""
ETL pipeline service for syncing cardano-db-sync data to Virtuoso triplestore.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor

import logging
from typing import Optional
from datetime import datetime
from enum import Enum
from dataclasses import dataclass

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from opentelemetry import trace

from cap.data.virtuoso import VirtuosoClient
from cap.config import settings
from cap.etl.cdb.extractor_factory import ExtractorFactory
from cap.etl.cdb.transformer_factory import TransformerFactory
from cap.etl.cdb.loaders.loader import CDBLoader
from cap.data.cdb_model import Block

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class ETLStatus(Enum):
    RUNNING = "running"
    PAUSED = "paused"
    ERROR = "error"
    COMPLETED = "completed"

@dataclass
class ETLProgress:
    """Tracks ETL progress for each entity type."""
    entity_type: str
    last_processed_id: Optional[int] = None
    total_records: int = 0
    processed_records: int = 0
    status: ETLStatus = ETLStatus.RUNNING
    last_updated: datetime = None
    error_message: Optional[str] = None

class ETLPipeline:
    """ETL pipeline for complete cardano-db-sync to Virtuoso synchronization."""

    def __init__(self, batch_size: int = 1000, sync_interval: int = 300):
        """
        Initialize ETL pipeline.

        Args:
            batch_size: Number of records to process in each batch
            sync_interval: Sync interval in seconds
        """
        self.batch_size = batch_size
        self.sync_interval = sync_interval
        self.running = False
        self.progress: dict[str, ETLProgress] = {}

        # Database connections
        try:
            self.pg_engine = create_engine(
                f"postgresql://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}@"
                f"{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}",
                pool_pre_ping=True,
                pool_recycle=3600,
                pool_size=20,
                max_overflow=40,
                pool_timeout=30,
                connect_args={
                    "connect_timeout": 10,
                    "options": "-c statement_timeout=30000"
                }
            )

            self.pg_session_factory = sessionmaker(bind=self.pg_engine)
        except Exception as e:
            logger.error(f"Failed to create database engine: {e}")
            raise RuntimeError(f"Database connection failed: {e}")

        # Virtuoso client and loader
        self.virtuoso_client = VirtuosoClient()
        self.loader = CDBLoader()

        # ETL entity types
        self.entity_types = [
            # Foundation entities first
            'account',
            'epoch',
            'multi_asset',
            'script',
            'datum',

            # Block and transaction entities
            'block',
            'transaction',

            # Staking entities
            'stake_address',
            'stake_pool',
            'delegation',
            'pool_registration',
            'pool_retirement',

            # Reward entities
            'reward',
            'withdrawal',
            'instantaneous_reward',

            # Treasury and governance entities
            'treasury',
            'reserve',
            'pot_transfer',
            'governance_action',
            'voting_procedure',
            'drep_registration',
            'drep_update',
            'committee_registration',
            'committee_deregistration',

            # Asset and metadata entities
            'asset_mint',
            'certificate',
            'voting_anchor',
            'pool_metadata',

            # Protocol parameter entities
            'protocol_parameters',
            'epoch_parameters'
        ]

        # Initialize progress tracking
        self._initialize_progress()

    def _initialize_progress(self):
        """Initialize progress tracking for all entity types."""
        for entity_type in self.entity_types:
            self.progress[entity_type] = ETLProgress(
                entity_type=entity_type,
                last_updated=datetime.now()
            )

    async def start_sync(self, continuous: bool = True):
        """Start the ETL sync process."""
        with tracer.start_as_current_span("etl_start_sync") as span:
            span.set_attribute("continuous", continuous)

            logger.info("Starting ETL pipeline sync...")
            self.running = True

            try:
                # Load existing progress from Virtuoso
                await self._load_existing_progress()

                while self.running:
                    await self._sync_all_entities()

                    if not continuous:
                        break

                    logger.info(f"Sync cycle completed. Waiting {self.sync_interval} seconds...")
                    await asyncio.sleep(self.sync_interval)

            except Exception as e:
                logger.error(f"ETL pipeline error: {e}", exc_info=True)
                for progress in self.progress.values():
                    progress.status = ETLStatus.ERROR
                    progress.error_message = str(e)
                    progress.last_updated = datetime.now()

                raise
            finally:
                self.running = False
                logger.info("ETL pipeline stopped")

    async def stop_sync(self):
        """Stop the ETL sync process."""
        logger.info("Stopping ETL pipeline...")
        self.running = False

    async def _load_existing_progress(self, metadata_graph: str=settings.ETL_PROGRESS_GRAPH):
        """Load existing ETL progress from Virtuoso metadata."""
        with tracer.start_as_current_span("load_existing_progress") as span:
            for entity_type in self.entity_types:
                try:
                    existing_progress = await self._load_progress_metadata(entity_type, metadata_graph)
                    if existing_progress:
                        self.progress[entity_type] = existing_progress
                        logger.info(f"Loaded existing progress for {entity_type}: "
                                  f"last_id={existing_progress.last_processed_id}, "
                                  f"processed={existing_progress.processed_records}")
                except Exception as e:
                    logger.warning(f"Could not load existing progress for {entity_type}: {e}")

    async def _load_progress_metadata(self, entity_type: str, graph_uri: str) -> Optional[ETLProgress]:
        """Load ETL progress metadata from Virtuoso."""
        with tracer.start_as_current_span("load_etl_progress") as span:
            span.set_attribute("entity_type", entity_type)

            try:
                query = f"""
                PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

                SELECT ?lastId ?totalRecords ?processedRecords ?status ?lastUpdated ?errorMessage
                WHERE {{
                    GRAPH <{graph_uri}> {{
                        <{settings.CARDANO_GRAPH}/etl/progress/{entity_type}>
                            cardano:hasLastProcessedId ?lastId ;
                            cardano:hasTotalRecords ?totalRecords ;
                            cardano:hasProcessedRecords ?processedRecords ;
                            cardano:hasStatus ?status ;
                            cardano:hasLastUpdated ?lastUpdated .
                        OPTIONAL {{
                            <{settings.CARDANO_GRAPH}/etl/progress/{entity_type}>
                                cardano:hasErrorMessage ?errorMessage .
                        }}
                    }}
                }}
                """

                results = await self.virtuoso_client.execute_query(query)

                if results.get('results', {}).get('bindings'):
                    binding = results['results']['bindings'][0]

                    # Parse the datetime
                    last_updated_str = binding.get('lastUpdated', {}).get('value')
                    last_updated = None
                    if last_updated_str:
                        try:
                            last_updated = datetime.fromisoformat(last_updated_str.replace('Z', '+00:00'))
                        except ValueError:
                            logger.warning(f"Could not parse datetime: {last_updated_str}")

                    progress = ETLProgress(
                        entity_type=entity_type,
                        last_processed_id=int(binding.get('lastId', {}).get('value', 0)),
                        total_records=int(binding.get('totalRecords', {}).get('value', 0)),
                        processed_records=int(binding.get('processedRecords', {}).get('value', 0)),
                        status=ETLStatus(binding.get('status', {}).get('value', 'running')),
                        last_updated=last_updated or datetime.now(),
                        error_message=binding.get('errorMessage', {}).get('value')
                    )

                    span.set_attribute("found_progress", True)
                    return progress

                span.set_attribute("found_progress", False)
                return None

            except Exception as e:
                logger.warning(f"Could not load progress metadata for {entity_type}: {e}")
                span.set_attribute("error", str(e))
                return None

    async def _sync_all_entities(self):
        """Sync all entity types in dependency order with parallel processing."""
        with tracer.start_as_current_span("etl_sync_all_entities") as span:
            # Group entities by dependency level
            # Foundation entities (no dependencies)
            foundation_entities = ['account', 'epoch', 'multi_asset', 'script', 'datum']
            # Entities that depend on foundation
            level1 = ['block', 'stake_address', 'stake_pool']
            # Entities that depend on previous level
            level2 = ['transaction', 'delegation', 'pool_registration', 'pool_retirement']
            final_level = list(set(self.entity_types) - set(level1) - set(level2))
            dependency_groups = [
                foundation_entities,
                level1,
                level2,
                final_level
            ]

            with ThreadPoolExecutor(max_workers=settings.ETL_PARALLEL_WORKERS) as executor:
                for group in dependency_groups:
                    if not self.running:
                        break

                    # Process entities in parallel within each dependency group
                    futures = []
                    for entity_type in group:
                        if entity_type in self.entity_types:
                            future = asyncio.get_event_loop().run_in_executor(
                                executor,
                                self._sync_entity_type_threadsafe,
                                entity_type
                            )
                            futures.append(future)

                    # Wait for all entities in the group to complete
                    if futures:
                        await asyncio.gather(*futures, return_exceptions=True)

    def _sync_entity_type_threadsafe(self, entity_type: str):
        """Thread-safe wrapper for entity synchronization."""
        # Create a new database session for this thread
        with self.pg_session_factory() as db_session:
            try:
                # Run the async method in a new event loop for thread safety
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(self._sync_entity_type_async(entity_type, db_session))
                finally:
                    loop.close()
            except Exception as e:
                logger.error(f"Error in threadsafe sync for {entity_type}: {e}", exc_info=True)
                raise

    async def _sync_entity_type_async(self, entity_type: str, db_session):
        """Async entity synchronization with provided session."""
        with tracer.start_as_current_span("etl_sync_entity_async") as span:
            span.set_attribute("entity_type", entity_type)

            progress = self.progress[entity_type]
            progress.status = ETLStatus.RUNNING
            progress.last_updated = datetime.now()
            progress.error_message = None

            logger.info(f"Starting sync for {entity_type}")

            try:
                # Create extractor and transformer
                try:
                    extractor = ExtractorFactory.create_extractor(
                        entity_type, db_session, self.batch_size
                    )
                    transformer = TransformerFactory.create_transformer(entity_type)
                except ValueError as e:
                    logger.warning(f"Extractor/Transformer not available for {entity_type}: {e}")
                    progress.status = ETLStatus.COMPLETED
                    return

                # Get total count for progress tracking
                try:
                    progress.total_records = extractor.get_total_count()
                    span.set_attribute("total_records", progress.total_records)
                except Exception as e:
                    logger.warning(f"Could not get total count for {entity_type}: {e}")
                    progress.total_records = 0

                # Process in batches
                batch_count = 0
                processed_in_session = 0

                for batch in extractor.extract_batch(progress.last_processed_id):
                    if not self.running:
                        break

                    batch_count += 1
                    span.set_attribute("batch_count", batch_count)
                    span.set_attribute("batch_size", len(batch))

                    if not batch:
                        logger.debug(f"Empty batch received for {entity_type}, stopping")
                        break

                    try:
                        # Transform to RDF
                        turtle_data = transformer.transform(batch)

                        # Load to Virtuoso
                        graph_uri = settings.CARDANO_GRAPH
                        batch_info = {
                            "entity_type": entity_type,
                            "size": len(batch),
                            "batch_number": batch_count
                        }

                        await self.loader.load_batch(graph_uri, turtle_data, batch_info)

                        # Update progress
                        max_id = None
                        for item in batch:
                            if isinstance(item, dict) and 'id' in item:
                                current_id = item['id']
                                # Handle composite IDs (rewards use strings)
                                if isinstance(current_id, str) and '_composite_key' in item:
                                    if max_id is None:
                                        max_id = item['_composite_key']
                                    else:
                                        # Compare composite keys
                                        if (current_id > str(max_id.get('addr_id', 0)) + '_' +
                                            str(max_id.get('type', '')) + '_' +
                                            str(max_id.get('earned_epoch', 0))):
                                            max_id = item['_composite_key']
                                elif isinstance(current_id, (int, float)):
                                    # Normal numeric ID
                                    if max_id is None or (isinstance(max_id, (int, float)) and current_id > max_id):
                                        max_id = current_id

                        if max_id is not None:
                            progress.last_processed_id = max_id
                        else:
                            logger.debug(f"Batch {batch_count} for {entity_type} has an id issue, stopping")
                            break

                        progress.processed_records += len(batch)
                        processed_in_session += len(batch)
                        progress.last_updated = datetime.now()

                        # Save progress periodically (every 50 batches for better performance)
                        if batch_count % 50 == 0:
                            await self.loader.save_progress_metadata(
                                entity_type,
                                progress,
                                f"{settings.CARDANO_GRAPH}/metadata"
                            )

                        logger.debug(f"Processed batch {batch_count} for {entity_type} "
                                f"({len(batch)} records, total: {progress.processed_records})")

                    except Exception as e:
                        logger.error(f"Error processing batch {batch_count} for {entity_type}: {e}")
                        raise

                # Final progress save
                logger.info(f"Finishing sync for {entity_type}. ")
                progress.status = ETLStatus.COMPLETED
                await self.loader.save_progress_metadata(
                    entity_type,
                    progress,
                    f"{settings.CARDANO_GRAPH}/metadata"
                )

                logger.info(f"Completed sync for {entity_type}. "
                        f"Processed {processed_in_session} new records "
                        f"(total: {progress.processed_records})")

            except Exception as e:
                err_msg = f"Error syncing {entity_type}"
                logger.error(err_msg, exc_info=True)
                progress.status = ETLStatus.ERROR
                progress.error_message = str(e)
                progress.last_updated = datetime.now()

                # Save error state
                try:
                    await self.loader.save_progress_metadata(
                        entity_type,
                        progress,
                        f"{settings.CARDANO_GRAPH}/metadata"
                    )
                except Exception as save_error:
                    logger.error(f"Could not save error state for {entity_type}: {save_error}")

                raise

    async def get_sync_status(self) -> dict[str, any]:
        """Get current sync status for all entity types."""
        status = {
            'running': self.running,
            'entity_progress': {}
        }

        for entity_type, progress in self.progress.items():
            progress_pct = 0
            if progress.total_records > 0:
                progress_pct = (progress.processed_records / progress.total_records) * 100

            status['entity_progress'][entity_type] = {
                'status': progress.status.value,
                'last_processed_id': progress.last_processed_id,
                'total_records': progress.total_records,
                'processed_records': progress.processed_records,
                'progress_percentage': round(progress_pct, 2),
                'last_updated': progress.last_updated.isoformat() if progress.last_updated else None,
                'error_message': progress.error_message
            }

        return status

    async def reset_sync_progress(self, entity_types: Optional[list[str]] = None):
        """Reset sync progress for specified entity types."""
        with tracer.start_as_current_span("etl_reset_progress") as span:
            entity_types_to_reset = entity_types or self.entity_types
            span.set_attribute("entity_types", entity_types_to_reset)

            for entity_type in entity_types_to_reset:
                if entity_type in self.progress:
                    # Reset progress tracking
                    self.progress[entity_type] = ETLProgress(
                        entity_type=entity_type,
                        last_updated=datetime.now()
                    )

                    # Clear metadata in Virtuoso (data stays in main graph)
                    try:
                        metadata_uri = f"{settings.CARDANO_GRAPH}/etl/progress/{entity_type}"
                        delete_query = f"<{metadata_uri}> ?p ?o ."

                        await self.virtuoso_client.update_graph(
                            f"{settings.CARDANO_GRAPH}/metadata",
                            delete_data=delete_query
                        )

                    except Exception as e:
                        logger.warning(f"Could not clear metadata for {entity_type}: {e}")

            logger.info(f"Reset sync progress for: {entity_types_to_reset}")

    async def sync_latest_blocks(self, limit: int = 100):
        """Sync only the latest blocks for real-time updates."""
        with tracer.start_as_current_span("etl_sync_latest_blocks") as span:
            span.set_attribute("limit", limit)

            logger.info(f"Syncing latest {limit} blocks...")

            try:
                with self.pg_session_factory() as db_session:
                    # Get latest blocks
                    latest_blocks = (
                        db_session.query(Block)
                        .order_by(Block.id.desc())
                        .limit(limit)
                        .all()
                    )

                    if latest_blocks:
                        # Transform and load
                        extractor = ExtractorFactory.create_extractor('block', db_session, self.batch_size)
                        transformer = TransformerFactory.create_transformer('block')

                        # Serialize blocks
                        block_data = [extractor._serialize_block(block) for block in latest_blocks]

                        # Transform to RDF
                        turtle_data = transformer.transform(block_data)

                        # Load to Virtuoso - use main graph
                        batch_info = {
                            "entity_type": "block",
                            "size": len(latest_blocks),
                            "sync_type": "latest"
                        }

                        await self.loader.load_batch(settings.CARDANO_GRAPH, turtle_data, batch_info)

                        logger.info(f"Synced {len(latest_blocks)} latest blocks")
                        return len(latest_blocks)
                    else:
                        logger.info("No new blocks to sync")
                        return 0

            except Exception as e:
                logger.error(f"Error syncing latest blocks: {e}", exc_info=True)
                raise

class ETLService:
    """Service wrapper for ETL pipeline management."""

    def __init__(self):
        self.pipeline: Optional[ETLPipeline] = None
        self.sync_task: Optional[asyncio.Task] = None

    async def start_etl(self, batch_size: int = 1000, sync_interval: int = 300, continuous: bool = True):
        """Start the ETL pipeline."""
        if self.pipeline and self.pipeline.running:
            raise RuntimeError("ETL pipeline is already running")

        try:
            self.pipeline = ETLPipeline(batch_size=batch_size, sync_interval=sync_interval)

            # Start sync task
            self.sync_task = asyncio.create_task(
                self.pipeline.start_sync(continuous=continuous)
            )

            logger.info("ETL service started")

        except Exception as e:
            logger.error(f"Failed to start ETL service: {e}")
            self.pipeline = None
            self.sync_task = None
            raise

    async def stop_etl(self):
        """Stop the ETL pipeline."""
        if self.pipeline:
            await self.pipeline.stop_sync()

        if self.sync_task:
            self.sync_task.cancel()
            try:
                await self.sync_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"Error during ETL task cancellation: {e}")

        self.pipeline = None
        self.sync_task = None

        logger.info("ETL service stopped")

    async def get_status(self) -> dict[str, any]:
        """Get ETL status."""
        if not self.pipeline:
            return {'running': False, 'message': 'ETL pipeline not initialized'}

        return await self.pipeline.get_sync_status()

    async def reset_progress(self, entity_types: Optional[list[str]] = None):
        """Reset ETL progress."""
        if not self.pipeline:
            raise RuntimeError("ETL pipeline not initialized")

        await self.pipeline.reset_sync_progress(entity_types)

    async def sync_latest(self, limit: int = 100):
        """Sync latest blocks."""
        if not self.pipeline:
            self.pipeline = ETLPipeline()

        return await self.pipeline.sync_latest_blocks(limit)

# Global ETL service instance
etl_service = ETLService()