"""
Data loader for cardano-db-sync ETL pipeline.
Handles loading transformed RDF data into Virtuoso triplestore.
"""

import logging
from typing import Optional
from datetime import datetime

from opentelemetry import trace

from cap.data.virtuoso import VirtuosoClient
from cap.config import settings

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class CDBLoader:
    """Data loader for Cardano blockchain data to Virtuoso triplestore."""
    
    def __init__(self):
        """Initialize the CDB loader with Virtuoso client."""
        self.virtuoso_client = VirtuosoClient()
        
    async def load_batch(self, graph_uri: str, turtle_data: str, batch_info: dict = None) -> bool:
        """
        Load a batch of RDF data to Virtuoso triplestore.
        
        Args:
            graph_uri: URI of the target graph
            turtle_data: RDF data in Turtle format
            batch_info: Additional information about the batch
            
        Returns:
            bool: Success status
        """
        with tracer.start_as_current_span("etl_load_batch") as span:
            span.set_attribute("graph_uri", graph_uri)
            if batch_info:
                span.set_attribute("batch_size", batch_info.get("size", 0))
                span.set_attribute("entity_type", batch_info.get("entity_type", "unknown"))
            
            try:
                # Validate turtle data
                if not turtle_data or not turtle_data.strip():
                    logger.warning("Empty turtle data provided for loading")
                    return True
                
                # Load data to Virtuoso
                await self._load_to_virtuoso(graph_uri, turtle_data)
                
                logger.debug(f"Successfully loaded batch to graph: {graph_uri}")
                return True
                
            except Exception as e:
                logger.error(f"Error loading batch to Virtuoso: {e}", exc_info=True)
                span.set_attribute("error", str(e))
                raise
    
    async def _load_to_virtuoso(self, graph_uri: str, turtle_data: str):
        """Load RDF data to Virtuoso triplestore."""
        with tracer.start_as_current_span("etl_load_virtuoso") as span:
            span.set_attribute("graph_uri", graph_uri)
            span.set_attribute("data_size", len(turtle_data))
            
            try:
                # Check if graph exists, create if not
                exists = await self.virtuoso_client.check_graph_exists(graph_uri)
                if not exists:
                    logger.info(f"Creating new graph: {graph_uri}")
                    await self.virtuoso_client.create_graph(graph_uri, turtle_data)
                else:
                    # Insert data into existing graph
                    logger.debug(f"Inserting data into existing graph: {graph_uri}")
                    await self.virtuoso_client.update_graph(
                        graph_uri, insert_data=turtle_data
                    )
                
                span.set_attribute("operation", "create" if not exists else "update")
                    
            except Exception as e:
                logger.error(f"Error loading data to Virtuoso: {e}")
                span.set_attribute("error", str(e))
                raise

    async def clear_graph_data(self, graph_uri: str) -> bool:
        """Clear all data from a specific graph."""
        with tracer.start_as_current_span("clear_graph_data") as span:
            span.set_attribute("graph_uri", graph_uri)
            
            try:
                exists = await self.virtuoso_client.check_graph_exists(graph_uri)
                if exists:
                    await self.virtuoso_client.delete_graph(graph_uri)
                    logger.info(f"Cleared data from graph: {graph_uri}")
                    return True
                else:
                    logger.warning(f"Graph does not exist: {graph_uri}")
                    return False
                    
            except Exception as e:
                logger.error(f"Error clearing graph {graph_uri}: {e}")
                span.set_attribute("error", str(e))
                raise
    
    async def save_progress_metadata(self, entity_type: str, progress, metadata_graph_uri: str):
        """Save ETL progress metadata to Virtuoso."""
        with tracer.start_as_current_span("save_progress_metadata") as span:
            span.set_attribute("entity_type", entity_type)
            span.set_attribute("metadata_graph_uri", metadata_graph_uri)
            
            try:
                # Create progress URI
                progress_uri = f"{settings.CARDANO_GRAPH}/etl/progress/{entity_type}"
                
                # Build RDF data for progress
                turtle_data = f"""
                @prefix cardano: <http://www.mobr.ai/ontologies/cardano#> .
                @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
                
                <{progress_uri}> a cardano:ETLProgress ;
                    cardano:hasEntityType "{entity_type}" ;
                    cardano:hasLastProcessedId {progress.last_processed_id or 0} ;
                    cardano:hasTotalRecords {progress.total_records} ;
                    cardano:hasProcessedRecords {progress.processed_records} ;
                    cardano:hasStatus "{progress.status.value}" ;
                    cardano:hasLastUpdated "{progress.last_updated.isoformat() if progress.last_updated else datetime.now().isoformat()}"^^xsd:dateTime"""
                
                if progress.error_message:
                    escaped_error = progress.error_message.replace('"', '\\"').replace('\n', '\\n')
                    turtle_data += f' ;\n    cardano:hasErrorMessage "{escaped_error}"'
                
                turtle_data += " .\n"
                
                # First delete existing progress metadata
                delete_query = f"<{progress_uri}> ?p ?o ."
                
                # Update the metadata graph
                await self.virtuoso_client.update_graph(
                    metadata_graph_uri,
                    delete_data=delete_query,
                    insert_data=turtle_data
                )
                
                logger.debug(f"Saved progress metadata for {entity_type}")
                span.set_attribute("success", True)
                
            except Exception as e:
                logger.error(f"Error saving progress metadata for {entity_type}: {e}")
                span.set_attribute("error", str(e))
                raise
    
    async def validate_data_integrity(self, graph_uri: str, expected_count: int = None) -> dict:
        """Validate the integrity of loaded data."""
        with tracer.start_as_current_span("validate_data_integrity") as span:
            span.set_attribute("graph_uri", graph_uri)
            
            try:
                # Count total triples in graph
                count_query = f"""
                SELECT (COUNT(*) AS ?count)
                WHERE {{
                    GRAPH <{graph_uri}> {{
                        ?s ?p ?o
                    }}
                }}
                """
                
                results = await self.virtuoso_client.execute_query(count_query)
                
                actual_count = 0
                if results.get('results', {}).get('bindings'):
                    actual_count = int(results['results']['bindings'][0]['count']['value'])
                
                validation_result = {
                    "graph_uri": graph_uri,
                    "actual_count": actual_count,
                    "expected_count": expected_count,
                    "valid": True if expected_count is None else actual_count == expected_count
                }
                
                span.set_attribute("actual_count", actual_count)
                span.set_attribute("valid", validation_result["valid"])
                
                logger.debug(f"Data integrity validation for {graph_uri}: {validation_result}")
                
                return validation_result
                
            except Exception as e:
                logger.error(f"Error validating data integrity for {graph_uri}: {e}")
                span.set_attribute("error", str(e))
                raise
    
    async def get_graph_statistics(self, graph_uri: str) -> dict:
        """Get statistics about the data in a graph."""
        with tracer.start_as_current_span("get_graph_statistics") as span:
            span.set_attribute("graph_uri", graph_uri)
            
            try:
                stats_query = f"""
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>
                PREFIX blockchain: <http://www.mobr.ai/ontologies/blockchain#>
                
                SELECT 
                    (COUNT(DISTINCT ?s) AS ?subjects)
                    (COUNT(DISTINCT ?p) AS ?predicates)
                    (COUNT(DISTINCT ?o) AS ?objects)
                    (COUNT(*) AS ?triples)
                WHERE {{
                    GRAPH <{graph_uri}> {{
                        ?s ?p ?o
                    }}
                }}
                """
                
                results = await self.virtuoso_client.execute_query(stats_query)
                
                stats = {
                    "graph_uri": graph_uri,
                    "subjects": 0,
                    "predicates": 0,
                    "objects": 0,
                    "triples": 0
                }
                
                if results.get('results', {}).get('bindings'):
                    binding = results['results']['bindings'][0]
                    stats.update({
                        "subjects": int(binding.get('subjects', {}).get('value', 0)),
                        "predicates": int(binding.get('predicates', {}).get('value', 0)),
                        "objects": int(binding.get('objects', {}).get('value', 0)),
                        "triples": int(binding.get('triples', {}).get('value', 0))
                    })
                
                span.set_attribute("triples_count", stats["triples"])
                
                logger.debug(f"Graph statistics for {graph_uri}: {stats}")
                
                return stats
                
            except Exception as e:
                logger.error(f"Error getting graph statistics for {graph_uri}: {e}")
                span.set_attribute("error", str(e))
                raise