"""
Post-processing service for ETL pipeline to calculate and add aggregate data
needed for complex statements and query optimization.
"""

import logging
from datetime import datetime
from opentelemetry import trace

from cap.data.virtuoso import VirtuosoClient
from cap.config import settings

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class ETLPostProcessor:
    """Handles post-processing of ETL data to add computed relationships and aggregates."""

    def __init__(self):
        self.virtuoso_client = VirtuosoClient()

    async def process_stake_aggregates(self):
        """Calculate and add stake amount aggregates for pools - supports Query 9."""
        with tracer.start_as_current_span("process_stake_aggregates") as span:
            try:
                # Query to calculate total stake per pool
                query = f"""
                PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>
                PREFIX blockchain: <http://www.mobr.ai/ontologies/blockchain#>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

                SELECT ?pool (SUM(?amount) as ?totalStake)
                FROM <{settings.CARDANO_GRAPH}>
                WHERE {{
                    ?account cardano:delegatesTo ?pool ;
                             cardano:hasStakeAmount ?amount .
                    ?pool a cardano:StakePool .
                }}
                GROUP BY ?pool
                """

                results = await self.virtuoso_client.execute_query(query)

                if results.get('results', {}).get('bindings'):
                    # Create turtle data for pool stake amounts
                    turtle_lines = [
                        "@prefix cardano: <http://www.mobr.ai/ontologies/cardano#> .",
                        "@prefix blockchain: <http://www.mobr.ai/ontologies/blockchain#> .",
                        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .",
                        ""
                    ]

                    for binding in results['results']['bindings']:
                        pool_uri = binding['pool']['value']
                        total_stake = binding['totalStake']['value']

                        # Create a stake amount for the pool - supports Query 9
                        turtle_lines.extend([
                            f"<{pool_uri}> cardano:hasStakeAmount {total_stake}^^xsd:integer .",
                            ""
                        ])

                    # Update the graph with aggregated data
                    turtle_data = '\n'.join(turtle_lines)
                    await self.virtuoso_client.update_graph(
                        settings.CARDANO_GRAPH,
                        insert_data=turtle_data
                    )

                    logger.info(f"Added stake aggregates for {len(results['results']['bindings'])} pools")
                    span.set_attribute("pools_processed", len(results['results']['bindings']))

            except Exception as e:
                logger.error(f"Error processing stake aggregates: {e}")
                span.set_attribute("error", str(e))
                raise

    async def link_governance_votes(self):
        """Link votes to their corresponding governance proposals - supports Query 10."""
        with tracer.start_as_current_span("link_governance_votes") as span:
            try:
                # Find governance proposals from transaction metadata
                query = f"""
                PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>
                PREFIX blockchain: <http://www.mobr.ai/ontologies/blockchain#>

                SELECT DISTINCT ?proposal ?tx ?block ?timestamp
                FROM <{settings.CARDANO_GRAPH}>
                WHERE {{
                    ?tx cardano:hasTransactionMetadata ?metadata .
                    ?metadata cardano:hasGovernanceProposal ?proposal .
                    ?block blockchain:hasTransaction ?tx ;
                           blockchain:hasTimestamp ?timestamp .
                }}
                ORDER BY DESC(?timestamp)
                LIMIT 1
                """

                results = await self.virtuoso_client.execute_query(query)

                if results.get('results', {}).get('bindings'):
                    # Store the latest proposal info
                    binding = results['results']['bindings'][0]
                    proposal_uri = binding['proposal']['value']
                    tx_uri = binding['tx']['value']
                    timestamp = binding['timestamp']['value']

                    # Create metadata entry for latest proposal
                    turtle_data = f"""
                    @prefix cardano: <http://www.mobr.ai/ontologies/cardano#> .
                    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

                    <{settings.CARDANO_GRAPH}/metadata/latest_proposal> a cardano:NetworkMetric ;
                        cardano:hasMetricType "latest_proposal" ;
                        cardano:hasMetricValue <{proposal_uri}> ;
                        cardano:hasLastUpdated "{datetime.now().isoformat()}"^^xsd:dateTime .
                    """

                    await self.virtuoso_client.update_graph(
                        f"{settings.CARDANO_GRAPH}/metadata",
                        insert_data=turtle_data
                    )

                    logger.info(f"Indexed latest proposal: {proposal_uri}")

            except Exception as e:
                logger.error(f"Error linking governance votes: {e}")
                span.set_attribute("error", str(e))
                raise

    async def calculate_total_staked(self):
        """Calculate and store the total ADA staked in the system - supports Query 9."""
        with tracer.start_as_current_span("calculate_total_staked") as span:
            try:
                query = f"""
                PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>
                PREFIX blockchain: <http://www.mobr.ai/ontologies/blockchain#>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

                SELECT (SUM(?amount) as ?totalStaked)
                FROM <{settings.CARDANO_GRAPH}>
                WHERE {{
                    ?account cardano:hasStakeAmount ?amount .
                }}
                """

                results = await self.virtuoso_client.execute_query(query)

                if results.get('results', {}).get('bindings'):
                    total_staked = results['results']['bindings'][0]['totalStaked']['value']

                    # Store as metadata
                    turtle_data = f"""
                    @prefix cardano: <http://www.mobr.ai/ontologies/cardano#> .
                    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

                    <{settings.CARDANO_GRAPH}/metadata/total_staked> a cardano:NetworkMetric ;
                        cardano:hasMetricType "total_staked" ;
                        cardano:hasMetricValue {total_staked}^^xsd:integer ;
                        cardano:hasLastUpdated "{datetime.now().isoformat()}"^^xsd:dateTime .
                    """

                    await self.virtuoso_client.update_graph(
                        f"{settings.CARDANO_GRAPH}/metadata",
                        insert_data=turtle_data
                    )

                    logger.info(f"Total staked ADA: {total_staked}")

            except Exception as e:
                logger.error(f"Error calculating total staked: {e}")
                span.set_attribute("error", str(e))
                raise

    async def ensure_block_transaction_links(self):
        """Ensure all transactions are properly linked to blocks with timestamps - supports Queries 1, 2, 5, 6, 7, 8."""
        with tracer.start_as_current_span("ensure_block_tx_links") as span:
            try:
                # Find transactions without proper block links
                query = f"""
                PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>
                PREFIX blockchain: <http://www.mobr.ai/ontologies/blockchain#>

                SELECT ?tx ?block
                FROM <{settings.CARDANO_GRAPH}>
                WHERE {{
                    ?tx a blockchain:Transaction ;
                        cardano:hasAnchor ?block .
                    FILTER NOT EXISTS {{
                        ?block blockchain:hasTransaction ?tx .
                    }}
                }}
                LIMIT 10000
                """

                results = await self.virtuoso_client.execute_query(query)

                if results.get('results', {}).get('bindings'):
                    turtle_lines = [
                        "@prefix blockchain: <http://www.mobr.ai/ontologies/blockchain#> .",
                        ""
                    ]

                    for binding in results['results']['bindings']:
                        tx_uri = binding['tx']['value']
                        block_uri = binding['block']['value']

                        turtle_lines.append(f"<{block_uri}> blockchain:hasTransaction <{tx_uri}> .")

                    turtle_data = '\n'.join(turtle_lines)
                    await self.virtuoso_client.update_graph(
                        settings.CARDANO_GRAPH,
                        insert_data=turtle_data
                    )

                    logger.info(f"Fixed {len(results['results']['bindings'])} block-transaction links")

            except Exception as e:
                logger.error(f"Error ensuring block-transaction links: {e}")
                span.set_attribute("error", str(e))
                raise



    async def ensure_token_transfer_tracking(self):
        """Ensure token transfers are properly tracked."""
        with tracer.start_as_current_span("ensure_token_transfers") as span:
            try:
                # Create token transfer events from transaction outputs
                query = f"""
                PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>
                PREFIX blockchain: <http://www.mobr.ai/ontologies/blockchain#>

                SELECT DISTINCT ?tx ?token
                FROM <{settings.CARDANO_GRAPH}>
                WHERE {{
                    ?tx cardano:hasOutput ?output .
                    ?output blockchain:hasTokenAmount ?tokenAmount .
                    ?tokenAmount blockchain:hasCurrency ?token ;
                                blockchain:hasAmountValue ?amount .
                    ?token a cardano:CNT .
                    FILTER(?amount > 0)
                    FILTER NOT EXISTS {{
                        ?token cardano:transferredIn ?tx .
                    }}
                }}
                LIMIT 10000
                """

                results = await self.virtuoso_client.execute_query(query)

                if results.get('results', {}).get('bindings'):
                    turtle_lines = [
                        "@prefix cardano: <http://www.mobr.ai/ontologies/cardano#> .",
                        ""
                    ]

                    for binding in results['results']['bindings']:
                        tx_uri = binding['tx']['value']
                        token_uri = binding['token']['value']

                        # Create a transfer event linking the token to the transaction
                        turtle_lines.append(f"<{token_uri}> cardano:transferredIn <{tx_uri}> .")

                    turtle_data = '\n'.join(turtle_lines)
                    await self.virtuoso_client.update_graph(
                        settings.CARDANO_GRAPH,
                        insert_data=turtle_data
                    )

                    logger.info(f"Added {len(results['results']['bindings'])} token transfer links")

            except Exception as e:
                logger.error(f"Error ensuring token transfers: {e}")
                span.set_attribute("error", str(e))
                raise

    async def calculate_nft_statistics(self):
        """Calculate NFT statistics."""
        with tracer.start_as_current_span("calculate_nft_stats") as span:
            try:
                # Count NFTs minted in different time periods
                query = f"""
                PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>
                PREFIX blockchain: <http://www.mobr.ai/ontologies/blockchain#>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

                SELECT (COUNT(DISTINCT ?nft) as ?nftCount)
                FROM <{settings.CARDANO_GRAPH}>
                WHERE {{
                    ?block blockchain:hasTransaction ?tx ;
                           blockchain:hasTimestamp ?ts .
                    ?tx cardano:hasMintedAsset ?nft .
                    ?nft a blockchain:NFT .
                }}
                """

                results = await self.virtuoso_client.execute_query(query)

                if results.get('results', {}).get('bindings'):
                    nft_count = results['results']['bindings'][0]['nftCount']['value']

                    # Store as metadata
                    turtle_data = f"""
                    @prefix cardano: <http://www.mobr.ai/ontologies/cardano#> .
                    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

                    <{settings.CARDANO_GRAPH}/metadata/total_nfts> a cardano:NetworkMetric ;
                        cardano:hasMetricType "total_nfts" ;
                        cardano:hasMetricValue {nft_count}^^xsd:integer ;
                        cardano:hasLastUpdated "{datetime.now().isoformat()}"^^xsd:dateTime .
                    """

                    await self.virtuoso_client.update_graph(
                        f"{settings.CARDANO_GRAPH}/metadata",
                        insert_data=turtle_data
                    )

                    logger.info(f"Total NFTs minted: {nft_count}")

            except Exception as e:
                logger.error(f"Error calculating NFT statistics: {e}")
                span.set_attribute("error", str(e))
                raise

    async def ensure_account_timestamps(self):
        """Ensure accounts have proper timestamps."""
        with tracer.start_as_current_span("ensure_account_timestamps") as span:
            try:
                # Find accounts with first transaction but missing timestamp link
                query = f"""
                PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>
                PREFIX blockchain: <http://www.mobr.ai/ontologies/blockchain#>

                SELECT ?account ?firstTx
                FROM <{settings.CARDANO_GRAPH}>
                WHERE {{
                    ?account a blockchain:Account ;
                            blockchain:firstAppearedInTransaction ?firstTx .
                    FILTER NOT EXISTS {{
                        ?block blockchain:hasTransaction ?firstTx ;
                               blockchain:hasTimestamp ?ts .
                    }}
                }}
                LIMIT 5000
                """

                results = await self.virtuoso_client.execute_query(query)

                if results.get('results', {}).get('bindings'):
                    # For each account, we need to ensure the transaction is linked to a block
                    # This should have been done in the transaction transformer, but we'll fix it here
                    logger.warning(f"Found {len(results['results']['bindings'])} accounts with missing timestamp links")

                    # Get transaction details to find their blocks
                    for binding in results['results']['bindings']:
                        tx_uri = binding['firstTx']['value']

                        # Find the block for this transaction
                        block_query = f"""
                        PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>
                        PREFIX blockchain: <http://www.mobr.ai/ontologies/blockchain#>

                        SELECT ?block ?timestamp
                        FROM <{settings.CARDANO_GRAPH}>
                        WHERE {{
                            <{tx_uri}> cardano:hasAnchor ?block .
                            ?block blockchain:hasTimestamp ?timestamp .
                        }}
                        """

                        block_results = await self.virtuoso_client.execute_query(block_query)

                        if block_results.get('results', {}).get('bindings'):
                            block_binding = block_results['results']['bindings'][0]
                            block_uri = block_binding['block']['value']

                            # Create the missing link
                            turtle_data = f"""
                            @prefix blockchain: <http://www.mobr.ai/ontologies/blockchain#> .
                            <{block_uri}> blockchain:hasTransaction <{tx_uri}> .
                            """

                            await self.virtuoso_client.update_graph(
                                settings.CARDANO_GRAPH,
                                insert_data=turtle_data
                            )

                    logger.info("Fixed account timestamp links")

            except Exception as e:
                logger.error(f"Error ensuring account timestamps: {e}")
                span.set_attribute("error", str(e))
                raise

    async def create_latest_proposal_index(self):
        """Create an index of the latest proposal."""
        with tracer.start_as_current_span("create_latest_proposal_index") as span:
            try:
                # Find the latest proposal
                query = f"""
                PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>
                PREFIX blockchain: <http://www.mobr.ai/ontologies/blockchain#>

                SELECT ?proposal ?proposalId (COUNT(?vote) as ?voteCount)
                FROM <{settings.CARDANO_GRAPH}>
                WHERE {{
                    ?proposal a cardano:GovernanceProposal ;
                             cardano:hasProposalId ?proposalId .
                    OPTIONAL {{
                        ?proposal cardano:hasVote ?vote .
                    }}
                }}
                GROUP BY ?proposal ?proposalId
                ORDER BY DESC(?proposalId)
                LIMIT 1
                """

                results = await self.virtuoso_client.execute_query(query)

                if results.get('results', {}).get('bindings'):
                    binding = results['results']['bindings'][0]
                    proposal_uri = binding['proposal']['value']
                    vote_count = binding['voteCount']['value']

                    # Store as metadata for quick access
                    turtle_data = f"""
                    @prefix cardano: <http://www.mobr.ai/ontologies/cardano#> .
                    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

                    <{settings.CARDANO_GRAPH}/metadata/latest_proposal> a cardano:NetworkMetric ;
                        cardano:hasMetricType "latest_proposal" ;
                        cardano:hasMetricValue {vote_count}^^xsd:integer ;
                        cardano:hasLastUpdated "{datetime.now().isoformat()}"^^xsd:dateTime .

                    <{proposal_uri}> cardano:hasMetricType "latest" .
                    """

                    await self.virtuoso_client.update_graph(
                        f"{settings.CARDANO_GRAPH}/metadata",
                        insert_data=turtle_data
                    )

                    logger.info(f"Indexed latest proposal with {vote_count} votes")

            except Exception as e:
                logger.error(f"Error creating latest proposal index: {e}")
                span.set_attribute("error", str(e))
                raise

    async def ensure_token_amounts_for_outputs(self):
        """Ensure all transaction outputs have proper token amounts for Query 1."""
        with tracer.start_as_current_span("ensure_output_token_amounts") as span:
            try:
                query = f"""
                PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>
                PREFIX blockchain: <http://www.mobr.ai/ontologies/blockchain#>

                SELECT ?output ?value
                FROM <{settings.CARDANO_GRAPH}>
                WHERE {{
                    ?tx cardano:hasOutput ?output .
                    ?output a cardano:RegularOutput .
                    FILTER NOT EXISTS {{
                        ?output blockchain:hasTokenAmount ?amt .
                    }}
                }}
                LIMIT 10000
                """

                results = await self.virtuoso_client.execute_query(query)

                if results.get('results', {}).get('bindings'):
                    logger.warning(f"Found {len(results['results']['bindings'])} outputs without token amounts")
                    # Log this issue - the data should have been created during transformation

            except Exception as e:
                logger.error(f"Error checking output token amounts: {e}")
                span.set_attribute("error", str(e))
                raise

    async def create_stake_pool_aggregates(self):
        """Create aggregated stake amounts for pools to support Query 9."""
        with tracer.start_as_current_span("create_pool_aggregates") as span:
            try:
                # First get all delegations with amounts
                query = f"""
                PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>
                PREFIX blockchain: <http://www.mobr.ai/ontologies/blockchain#>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

                SELECT ?pool (SUM(?amount) as ?totalStake)
                FROM <{settings.CARDANO_GRAPH}>
                WHERE {{
                    ?account cardano:delegatesTo ?pool ;
                            blockchain:hasTokenAmount ?tokenAmt .
                    ?tokenAmt blockchain:hasCurrency cardano:ADA ;
                            blockchain:hasAmountValue ?amount .
                    ?pool a cardano:StakePool .
                }}
                GROUP BY ?pool
                """

                results = await self.virtuoso_client.execute_query(query)

                if results.get('results', {}).get('bindings'):
                    turtle_lines = [
                        "@prefix cardano: <http://www.mobr.ai/ontologies/cardano#> .",
                        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .",
                        ""
                    ]

                    for binding in results['results']['bindings']:
                        pool_uri = binding['pool']['value']
                        total_stake = binding['totalStake']['value']

                        turtle_lines.append(f"<{pool_uri}> cardano:hasStakeAmount {total_stake}^^xsd:integer .")

                    turtle_data = '\n'.join(turtle_lines)
                    await self.virtuoso_client.update_graph(
                        settings.CARDANO_GRAPH,
                        insert_data=turtle_data
                    )

                    logger.info(f"Created stake amounts for {len(results['results']['bindings'])} pools")

            except Exception as e:
                logger.error(f"Error creating pool aggregates: {e}")
                span.set_attribute("error", str(e))
                raise

    async def ensure_governance_vote_links(self):
        """Ensure governance votes are properly linked for Query 10."""
        with tracer.start_as_current_span("ensure_governance_links") as span:
            try:
                # Find votes without proper account links
                query = f"""
                PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>
                PREFIX blockchain: <http://www.mobr.ai/ontologies/blockchain#>

                SELECT ?vote ?proposal
                FROM <{settings.CARDANO_GRAPH}>
                WHERE {{
                    ?proposal a cardano:GovernanceProposal ;
                            cardano:hasVote ?vote .
                    ?vote a cardano:Vote .
                    FILTER NOT EXISTS {{
                        ?account cardano:castsVote ?vote .
                    }}
                }}
                LIMIT 10000
                """

                results = await self.virtuoso_client.execute_query(query)

                if results.get('results', {}).get('bindings'):
                    logger.warning(f"Found {len(results['results']['bindings'])} votes without account links")

            except Exception as e:
                logger.error(f"Error checking governance links: {e}")
                span.set_attribute("error", str(e))
                raise

    async def fix_timestamp_formatting(self):
        """Ensure all timestamps are properly formatted for time-based queries."""
        with tracer.start_as_current_span("fix_timestamps") as span:
            try:
                # Check for blocks with missing or improperly formatted timestamps
                query = f"""
                PREFIX blockchain: <http://www.mobr.ai/ontologies/blockchain#>

                SELECT ?block
                FROM <{settings.CARDANO_GRAPH}>
                WHERE {{
                    ?block a blockchain:Block .
                    FILTER NOT EXISTS {{
                        ?block blockchain:hasTimestamp ?ts .
                    }}
                }}
                LIMIT 10
                """

                results = await self.virtuoso_client.execute_query(query)

                if results.get('results', {}).get('bindings'):
                    logger.warning(f"Found {len(results['results']['bindings'])} blocks without timestamps")

            except Exception as e:
                logger.error(f"Error checking timestamps: {e}")
                span.set_attribute("error", str(e))
                raise

    async def create_total_stake_metric(self):
        """Create total stake metric for Query 9."""
        with tracer.start_as_current_span("create_total_stake") as span:
            try:
                query = f"""
                PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>
                PREFIX blockchain: <http://www.mobr.ai/ontologies/blockchain#>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

                SELECT (SUM(?amount) as ?totalStaked)
                FROM <{settings.CARDANO_GRAPH}>
                WHERE {{
                    ?account blockchain:hasTokenAmount ?tokenAmt .
                    ?tokenAmt blockchain:hasCurrency cardano:ADA ;
                            blockchain:hasAmountValue ?amount .
                    ?account cardano:delegatesTo ?pool .
                    ?pool a cardano:StakePool .
                }}
                """

                results = await self.virtuoso_client.execute_query(query)

                if results.get('results', {}).get('bindings'):
                    total_staked = results['results']['bindings'][0]['totalStaked']['value']

                    turtle_data = f"""
                    @prefix cardano: <http://www.mobr.ai/ontologies/cardano#> .
                    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

                    <{settings.CARDANO_GRAPH}/metadata/total_staked> a cardano:NetworkMetric ;
                        cardano:hasMetricType "total_staked" ;
                        cardano:hasMetricValue {total_staked}^^xsd:integer ;
                        cardano:hasLastUpdated "{datetime.now().isoformat()}"^^xsd:dateTime .
                    """

                    await self.virtuoso_client.update_graph(
                        f"{settings.CARDANO_GRAPH}/metadata",
                        insert_data=turtle_data
                    )

                    logger.info(f"Total staked ADA: {total_staked}")

            except Exception as e:
                logger.error(f"Error calculating total stake: {e}")
                span.set_attribute("error", str(e))
                raise

    async def run_all_post_processing(self):
        """Run all post-processing tasks."""
        logger.info("Starting ETL post-processing...")

        try:
            # Ensure basic relationships for time-based queries
            await self.ensure_block_transaction_links()
            await self.fix_timestamp_formatting()

            # Ensure account timestamps for new accounts query
            await self.ensure_account_timestamps()

            # Ensure output token amounts for transaction queries
            await self.ensure_token_amounts_for_outputs()

            # Calculate aggregates for pool and staking queries
            await self.create_stake_pool_aggregates()
            await self.create_total_stake_metric()

            # Token and NFT tracking for asset queries
            await self.ensure_token_transfer_tracking()
            await self.calculate_nft_statistics()

            # Link governance data for voting queries
            await self.ensure_governance_vote_links()
            await self.create_latest_proposal_index()

            logger.info("ETL post-processing completed successfully")

        except Exception as e:
            logger.error(f"Error in ETL post-processing: {e}")
            raise

# Global post-processor instance
etl_post_processor = ETLPostProcessor()