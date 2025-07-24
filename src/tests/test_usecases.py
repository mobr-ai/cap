import pytest
from datetime import datetime, timedelta
import logging
from httpx import AsyncClient
from cap.data.virtuoso import VirtuosoClient

PREFIXES = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX blockchain: <http://www.mobr.ai/ontologies/blockchain#>
    PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
     
"""
TEST_GRAPH = "http://test.cardano.queries"
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

@pytest.fixture(autouse=True)
async def cleanup(virtuoso_client: VirtuosoClient):
    """Cleanup test graph before and after each test."""
    try:
        exists = await virtuoso_client.check_graph_exists(TEST_GRAPH)
        if exists:
            await virtuoso_client.delete_graph(TEST_GRAPH)
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
    
    yield
    
    try:
        exists = await virtuoso_client.check_graph_exists(TEST_GRAPH)
        if exists:
            await virtuoso_client.delete_graph(TEST_GRAPH)
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")

def generate_test_data():
    """Generate turtle data for testing queries."""
    now = datetime.now()

    # Generate timestamps for different time periods
    timestamps = {
        'recent': now.isoformat() + 'Z',
        'yesterday': (now - timedelta(days=1)).isoformat() + 'Z',
        'last_week': (now - timedelta(days=7)).isoformat() + 'Z',
        'last_month': (now - timedelta(days=30)).isoformat() + 'Z',
        'last_year': (now - timedelta(days=365)).isoformat() + 'Z'
    }

    turtle_data = f"""
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix blockchain: <http://www.mobr.ai/ontologies/blockchain#> .
        @prefix cardano: <http://www.mobr.ai/ontologies/cardano#> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

        # ADA token definition
        cardano:ADA rdf:type cardano:CNT ;
            blockchain:hasTokenName "Cardano ADA" ;
            blockchain:hasTokenSymbol "ADA" ;
            blockchain:hasMaxSupply 45000000000000000 ;
            blockchain:hasConversionRate 1000000 ;
            blockchain:hasDenominationName "Lovelace" .

        # Test tokens
        <http://test/token1> rdf:type cardano:CNT ;
            blockchain:hasTokenName "Test Token 1" .
        <http://test/token2> rdf:type cardano:CNT ;
            blockchain:hasTokenName "Test Token 2" .

        # Blocks with timestamps
        <http://test/block1> rdf:type blockchain:Block ;
            blockchain:hasTimestamp "{timestamps['recent']}"^^xsd:dateTime ;
            blockchain:hasTransaction <http://test/tx1>, <http://test/tx2> .

        <http://test/block2> rdf:type blockchain:Block ;
            blockchain:hasTimestamp "{timestamps['yesterday']}"^^xsd:dateTime ;
            blockchain:hasTransaction <http://test/tx3>, <http://test/tx4> .

        <http://test/block3> rdf:type blockchain:Block ;
            blockchain:hasTimestamp "{timestamps['last_week']}"^^xsd:dateTime ;
            blockchain:hasTransaction <http://test/tx5> .

        <http://test/block4> rdf:type blockchain:Block ;
            blockchain:hasTimestamp "{timestamps['last_month']}"^^xsd:dateTime ;
            blockchain:hasTransaction <http://test/tx6> .

        # Transactions
        <http://test/tx1> cardano:hasFee "1000000"^^xsd:decimal ;
            cardano:hasOutput <http://test/output1>, <http://test/output2> .

        <http://test/tx2> cardano:hasFee "900000"^^xsd:decimal ;
            cardano:hasOutput <http://test/output3> .

        <http://test/tx3> cardano:hasFee "950000"^^xsd:decimal ;
            cardano:hasOutput <http://test/output4> .

        <http://test/tx4> cardano:hasFee "980000"^^xsd:decimal ;
            cardano:hasOutput <http://test/output5> .

        # Transaction outputs
        <http://test/output1> blockchain:hasTokenAmount <http://test/amount1> .
        <http://test/amount1> blockchain:hasCurrency cardano:ADA ;
            blockchain:hasAmountValue "5000000000"^^xsd:integer .

        <http://test/output2> blockchain:hasTokenAmount <http://test/amount2> .
        <http://test/amount2> blockchain:hasCurrency <http://test/token1> ;
            blockchain:hasAmountValue "1000"^^xsd:integer .

        <http://test/output3> blockchain:hasTokenAmount <http://test/amount3> .
        <http://test/amount3> blockchain:hasCurrency cardano:ADA ;
            blockchain:hasAmountValue "3000000000"^^xsd:integer .

        # Accounts
        <http://test/account1> rdf:type blockchain:Account ;
            blockchain:hasTokenAmount <http://test/holding1> ;
            cardano:hasReward <http://test/reward1> ;
            blockchain:firstAppearedInTransaction <http://test/tx1> .

        <http://test/account2> rdf:type blockchain:Account ;
            blockchain:hasTokenAmount <http://test/holding2> ;
            cardano:hasReward <http://test/reward2> ;
            blockchain:firstAppearedInTransaction <http://test/tx2> .

        <http://test/account3> rdf:type blockchain:Account ;
            blockchain:hasTokenAmount <http://test/holding3> ;
            blockchain:firstAppearedInTransaction <http://test/tx3> .

        # Account holdings
        <http://test/holding1> blockchain:hasCurrency cardano:ADA ;
            blockchain:hasAmountValue "250000000000000"^^xsd:integer .

        <http://test/holding2> blockchain:hasCurrency cardano:ADA ;
            blockchain:hasAmountValue "150000000000"^^xsd:integer .

        <http://test/holding3> blockchain:hasCurrency cardano:ADA ;
            blockchain:hasAmountValue "180000000000"^^xsd:integer .

        # Rewards
        <http://test/reward1> cardano:hasRewardAmount <http://test/rewardAmount1> .
        <http://test/rewardAmount1> blockchain:hasAmountValue "1000000"^^xsd:integer .

        <http://test/reward2> cardano:hasRewardAmount <http://test/rewardAmount2> .
        <http://test/rewardAmount2> blockchain:hasAmountValue "1500000"^^xsd:integer .

        # Smart Contracts
        <http://test/contract1> rdf:type blockchain:SmartContract ;
            cardano:embeddedIn <http://test/tx1> .

        <http://test/contract2> rdf:type blockchain:SmartContract ;
            cardano:embeddedIn <http://test/tx2> .

        # NFTs
        <http://test/nft1> rdf:type blockchain:NFT .
        <http://test/nft2> rdf:type blockchain:NFT .
        <http://test/tx1> cardano:hasMintedAsset <http://test/nft1> .
        <http://test/tx2> cardano:hasMintedAsset <http://test/nft2> .

        # Stake Pools
        <http://test/pool1> rdf:type cardano:StakePool .
        <http://test/pool2> rdf:type cardano:StakePool .

        # Staking
        <http://test/account1> cardano:delegatesTo <http://test/pool1> ;
            cardano:hasStakeAmount "100000000000"^^xsd:integer .
        <http://test/account2> cardano:delegatesTo <http://test/pool1> ;
            cardano:hasStakeAmount "80000000000"^^xsd:integer .
        <http://test/account3> cardano:delegatesTo <http://test/pool2> ;
            cardano:hasStakeAmount "90000000000"^^xsd:integer .

        # Governance
        <http://test/proposal1> rdf:type cardano:GovernanceProposal .
        <http://test/metadata1> cardano:hasGovernanceProposal <http://test/proposal1> .
        <http://test/tx1> cardano:hasTransactionMetadata <http://test/metadata1> .

        # Voting
        <http://test/vote1> rdf:type cardano:Vote .
        <http://test/vote2> rdf:type cardano:Vote .
        <http://test/account1> cardano:castsVote <http://test/vote1> .
        <http://test/account2> cardano:castsVote <http://test/vote2> .
        <http://test/proposal1> cardano:hasVote <http://test/vote1>, <http://test/vote2> .
    """

    return turtle_data

@pytest.mark.asyncio
async def test_sparql_queries(async_client: AsyncClient):
    """Test all SPARQL queries with sample data."""

    now = datetime.now()
    current_time = now.isoformat() + 'Z'

    # Create test graph with sample data
    verification_query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX blockchain: <http://www.mobr.ai/ontologies/blockchain#>
        PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>

        SELECT (COUNT(?s) as ?count) 
        FROM <{graph}>
        WHERE {{ ?s ?p ?o }}
    """

    response = await async_client.post(
        "/api/v1/graphs",
        json={
            "graph_uri": TEST_GRAPH,
            "turtle_data": generate_test_data()
        }
    )
    assert response.status_code == 200

    # Verify if the test graph was injected
    response = await async_client.post(
        "/api/v1/query",
        json={
            "query": verification_query.format(graph=TEST_GRAPH),
            "type": "SELECT"
        }
    )
    assert response.status_code == 200
    results = response.json()["results"]["results"]
    logger.debug(f"Verification results: {results}")
    assert int(results["bindings"][0]["count"]["value"]) > 0

    # debugging timestamp
    debug_query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX blockchain: <http://www.mobr.ai/ontologies/blockchain#>
        PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        # First, check what timestamps we actually have in the graph
        SELECT ?timestamp
        FROM <{graph}>
        WHERE {{
            ?block a blockchain:Block ;
                blockchain:hasTimestamp ?timestamp .
        }}
    """

    response = await async_client.post(
        "/api/v1/query",
        json={
            "query": debug_query.format(graph=TEST_GRAPH),
            "type": "SELECT"
        }
    )
    assert response.status_code == 200
    debug_results = response.json()["results"]["results"]
    logger.debug(f"Timestamp debug results: {debug_results}")

    debug_query2 = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX blockchain: <http://www.mobr.ai/ontologies/blockchain#>
        PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        # Check the full path exists without time filter
        SELECT ?tx ?amount ?timestamp
        FROM <{graph}>
        WHERE {{
            ?block a blockchain:Block ;
                blockchain:hasTransaction ?tx ;
                blockchain:hasTimestamp ?timestamp .
            ?tx cardano:hasOutput ?output .
            ?output blockchain:hasTokenAmount ?tokenAmount .
            ?tokenAmount blockchain:hasCurrency cardano:ADA ;
                blockchain:hasAmountValue ?amount .
        }}
        ORDER BY DESC(?amount)
        LIMIT 10
    """
    response = await async_client.post(
        "/api/v1/query",
        json={
            "query": debug_query2.format(graph=TEST_GRAPH),
            "type": "SELECT"
        }
    )
    assert response.status_code == 200
    debug_results2 = response.json()["results"]["results"]
    logger.debug(f"Full path debug results: {debug_results2}")

    # debuggin NOW()
    timestamp_query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX blockchain: <http://www.mobr.ai/ontologies/blockchain#>
        PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        SELECT DISTINCT ?timestamp (now() as ?current) (now() - "P1D"^^xsd:duration as ?oneDayAgo)
            (?timestamp >= now() - "P1D"^^xsd:duration as ?withinDay)
        FROM <{graph}>
        WHERE {{
            ?block a blockchain:Block ;
                blockchain:hasTimestamp ?timestamp .
        }}
        ORDER BY DESC(?timestamp)
    """

    response = await async_client.post(
        "/api/v1/query",
        json={
            "query": timestamp_query.format(graph=TEST_GRAPH),
            "type": "SELECT"
        }
    )
    
    assert response.status_code == 200
    timestamp_results = response.json()["results"]["results"]
    logger.debug(f"Timestamp debugging now - comparison results: {timestamp_results}")

    # debugging query 1
    query1 = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX blockchain: <http://www.mobr.ai/ontologies/blockchain#>
        PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        SELECT ?tx ?amount ?timestamp (xsd:dateTime(NOW() - "P1D"^^xsd:duration) as ?oneDayAgo)
        FROM <{graph}>
        WHERE {{
            ?block a blockchain:Block ;
                blockchain:hasTransaction ?tx ;
                blockchain:hasTimestamp ?timestamp .
            ?tx cardano:hasOutput ?output .
            ?output blockchain:hasTokenAmount ?tokenAmount .
            ?tokenAmount blockchain:hasCurrency cardano:ADA ;
                blockchain:hasAmountValue ?amount .
        }}
        ORDER BY DESC(?amount)
        LIMIT 10
    """

    response = await async_client.post(
        "/api/v1/query",
        json={
            "query": query1.format(graph=TEST_GRAPH),
            "type": "SELECT"
        }
    )

    assert response.status_code == 200
    query1_results = response.json()["results"]["results"]
    logger.debug(f"Debugging Query1 without a filter results: {query1_results}")

    # and then the use case queries
    queries = [
        # Query 1: What were the 10 largest ADA transactions in the last 24 hours?
        """
        SELECT ?tx ?amount ?timestamp
        FROM <{graph}>
        WHERE {{
            ?block a blockchain:Block ;
                blockchain:hasTransaction ?tx ;
                blockchain:hasTimestamp ?timestamp .
            ?tx cardano:hasOutput ?output .
            ?output blockchain:hasTokenAmount ?tokenAmount .
            ?tokenAmount blockchain:hasCurrency cardano:ADA ;
                blockchain:hasAmountValue ?amount .
            BIND (NOW() - "P1D"^^xsd:dayTimeDuration as ?oneDayAgo)
            FILTER (?timestamp >= ?oneDayAgo)
        }}
        ORDER BY DESC(?amount)
        LIMIT 10
        """,
        
        # Query 2: What were the average transaction fees last week?
        """
        SELECT (AVG(?fee) as ?avgFee)
        FROM <{graph}>
        WHERE {{
            ?block a blockchain:Block ;
                blockchain:hasTransaction ?tx ;
                blockchain:hasTimestamp ?ts .
            ?tx cardano:hasFee ?fee .
            BIND (NOW() - "P7D"^^xsd:dayTimeDuration as ?oneWeekAgo)
            FILTER (?ts >= ?oneWeekAgo)
        }}
        """,

        # Query 3: How many accounts hold over 200,000 ADA?
        """
        SELECT (COUNT(DISTINCT ?account) as ?numAccounts)
        FROM <{graph}>
        WHERE {{
            ?account a blockchain:Account ;
                blockchain:hasTokenAmount ?tokenAmount .
            ?tokenAmount blockchain:hasCurrency cardano:ADA ;
                blockchain:hasAmountValue ?amount .
            FILTER(?amount > 200000000000)
        }}
        """,

        # Query 4: What are the average staking rewards for accounts holding more than 1 million ADA?
        """
        SELECT (AVG(?rewardValue) as ?avgReward)
        FROM <{graph}>
        WHERE {{
            ?account blockchain:hasTokenAmount ?holding .
            ?holding blockchain:hasCurrency cardano:ADA ;
                blockchain:hasAmountValue ?adaAmount .
            ?account cardano:hasReward ?reward .
            ?reward cardano:hasRewardAmount ?rewardAmt .
            ?rewardAmt blockchain:hasAmountValue ?rewardValue .
            FILTER (?adaAmount > 1000000000000)
        }}
        """,

        # Query 5: Monthly smart contract deployments
        """
        SELECT (MONTH(?ts) as ?month) (COUNT(DISTINCT ?contract) as ?deployments)
        FROM <{graph}>
        WHERE {{
            ?block a blockchain:Block ;
                blockchain:hasTransaction ?tx ;
                blockchain:hasTimestamp ?ts .
            ?contract a blockchain:SmartContract ;
                cardano:embeddedIn ?tx .
            BIND (NOW() - "P365D"^^xsd:dayTimeDuration as ?oneYearAgo)
            FILTER (?ts >= ?oneYearAgo)
        }}
        GROUP BY (MONTH(?ts))
        ORDER BY ?month
        """,

        # Query 6: NFT mints in last month
        """
        SELECT (COUNT(DISTINCT ?nft) as ?nftCount)
        FROM <{graph}>
        WHERE {{
            ?block a blockchain:Block ;
                blockchain:hasTransaction ?tx ;
                blockchain:hasTimestamp ?ts .
            ?tx cardano:hasMintedAsset ?nft .
            ?nft a blockchain:NFT .
            BIND (NOW() - "P30D"^^xsd:dayTimeDuration as ?oneMonthAgo)
            FILTER (?ts >= ?oneMonthAgo)
        }}
        """,

        # Query 7: Top 10 tokens by transfer count
        """
        SELECT ?token (COUNT(DISTINCT ?output) as ?transfers)
        FROM <{graph}>
        WHERE {{
            ?block a blockchain:Block ;
                blockchain:hasTransaction ?tx ;
                blockchain:hasTimestamp ?ts .
            ?tx cardano:hasOutput ?output .
            ?output blockchain:hasTokenAmount ?tokenAmount .
            ?tokenAmount blockchain:hasCurrency ?token .
            BIND (NOW() - "P7D"^^xsd:dayTimeDuration as ?oneWeekAgo)
            FILTER (?ts >= ?oneWeekAgo)
        }}
        GROUP BY ?token
        ORDER BY DESC(?transfers)
        LIMIT 10
        """,

        # Query 8: New accounts per day in last 30 days
        """
        SELECT (xsd:date(?timestamp) as ?date) (COUNT(DISTINCT ?account) as ?new_accounts)
        FROM <{graph}>
        WHERE {{
            ?account blockchain:firstAppearedInTransaction ?tx .
            ?block blockchain:hasTransaction ?tx .
            ?block blockchain:hasTimestamp ?timestamp .
            BIND (NOW() - "P30D"^^xsd:dayTimeDuration as ?oneMonthAgo)
            FILTER (?timestamp >= ?oneMonthAgo)
        }}
        GROUP BY (xsd:date(?timestamp))
        ORDER BY ?date
        """,

        # Query 9: Top 10 pools stake percentage
        """
        SELECT ?pool ?stakeAmount ?totalStaked
        FROM <{graph}>
        WHERE {{
            {{
                SELECT ?pool (SUM(?amount) as ?stakeAmount)
                WHERE {{
                    ?account cardano:delegatesTo ?pool ;
                        cardano:hasStakeAmount ?amount .
                    ?pool a cardano:StakePool .
                }}
                GROUP BY ?pool
                ORDER BY DESC(?stakeAmount)
                LIMIT 10
            }}
            {{
                SELECT (SUM(?amount) as ?totalStaked)
                WHERE {{
                    ?account cardano:hasStakeAmount ?amount .
                }}
            }}
        }}
        """,

        # Query 10: Vote count on latest proposal
        """
        SELECT (COUNT(DISTINCT ?account) as ?voteCount)
        FROM <{graph}>
        WHERE {{
            {{
                SELECT ?proposal
                WHERE {{
                    ?block blockchain:hasTransaction ?tx .
                    ?tx cardano:hasTransactionMetadata ?metadata .
                    ?metadata cardano:hasGovernanceProposal ?proposal .
                }}
                ORDER BY DESC(?timestamp)
                LIMIT 1
            }}
            ?account cardano:castsVote ?vote .
            ?proposal cardano:hasVote ?vote .
        }}
        """
    ]

    for i, query in enumerate(queries, 1):
        full_query = PREFIXES + query

        formatted_query = full_query.format(
            graph=TEST_GRAPH,
            current_time=current_time
        )

        response = await async_client.post(
            "/api/v1/query",
            json={
                "query": formatted_query,
                "type": "SELECT"
            }
        )

        logger.debug(f"Query {i} response status: {response.status_code}")
        logger.debug(f"Query {i} response body: {response.text}")
        assert response.status_code == 200

        results = response.json()["results"]["results"]

        # Verify results based on query
        if i == 1:  # Top ADA outputs
            assert len(results["bindings"]) > 0
            # Verify amounts are in descending order
            amounts = [float(r["amount"]["value"]) for r in results["bindings"]]
            assert amounts == sorted(amounts, reverse=True)
            assert amounts[0] == 5000000000  # Highest amount from sample data

        elif i == 2:  # Average fee
            assert "avgFee" in results["bindings"][0]
            avg_fee = float(results["bindings"][0]["avgFee"]["value"])
            # Expected average from sample data (1000000 + 900000 + 950000 + 980000) / 4
            expected_avg = 957500
            assert abs(avg_fee - expected_avg) < 1  # Allow for small floating-point differences
            
        elif i == 3:  # Accounts with > 200K ADA
            assert "numAccounts" in results["bindings"][0]
            num_accounts = int(results["bindings"][0]["numAccounts"]["value"])
            assert num_accounts == 1  # Only account1 has > 200K ADA
            
        elif i == 4:  # Average rewards
            assert "avgReward" in results["bindings"][0]
            avg_reward = float(results["bindings"][0]["avgReward"]["value"])
            # Expected average from sample data - only account1 meets criteria
            expected_avg = 1000000
            assert abs(avg_reward - expected_avg) < 1

        elif i == 5:  # Monthly smart contract deployments
            # Verify we get contract deployments grouped by month
            assert len(results["bindings"]) > 0
            for binding in results["bindings"]:
                assert "month" in binding
                assert "deployments" in binding
                month = int(binding["month"]["value"])
                assert 1 <= month <= 12
                
        elif i == 6:  # NFT mints in last month
            assert "nftCount" in results["bindings"][0]
            nft_count = int(results["bindings"][0]["nftCount"]["value"])
            assert nft_count == 2  # Two NFTs in sample data
            
        elif i == 7:  # Top tokens by transfer
            assert len(results["bindings"]) > 0
            # Verify transfers are in descending order
            transfers = [int(r["transfers"]["value"]) for r in results["bindings"]]
            assert transfers == sorted(transfers, reverse=True)
            
        elif i == 8:  # New accounts per day
            assert len(results["bindings"]) > 0
            for binding in results["bindings"]:
                assert "date" in binding
                assert "new_accounts" in binding
                new_accounts = int(binding["new_accounts"]["value"])
                assert new_accounts > 0
                
        elif i == 9:  # Top pools stake percentage
            assert len(results["bindings"]) > 0
            # Verify stake amounts
            for binding in results["bindings"]:
                stake_amount = int(binding["stakeAmount"]["value"])
                total_staked = int(binding["totalStaked"]["value"])
                # Total staked should be sum of all accounts (270B)
                assert total_staked == 270000000000
                # Stake percentage should be calculable
                percentage = (stake_amount * 100) / total_staked
                assert 0 <= percentage <= 100
                
        elif i == 10:  # Vote count on latest proposal
            assert "voteCount" in results["bindings"][0]
            vote_count = int(results["bindings"][0]["voteCount"]["value"])
            assert vote_count == 2  # Two accounts voted in sample data

        logger.info(f"Query {i} verified successfully")