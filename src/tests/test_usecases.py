import pytest
from datetime import datetime, timedelta
import logging
from httpx import AsyncClient
from cap.rdf.triplestore import TriplestoreClient

PREFIXES = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX b: <https://mobr.ai/ont/blockchain#>
    PREFIX c: <https://mobr.ai/ont/cardano#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

"""
TEST_GRAPH = "http://test.cardano.queries"
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

@pytest.fixture(autouse=True)
async def cleanup(virtuoso_client: TriplestoreClient):
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
        @prefix b: <https://mobr.ai/ont/blockchain#> .
        @prefix c: <https://mobr.ai/ont/cardano#> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

        # ADA token definition
        c:ADA rdf:type c:CNT ;
            b:hasTokenName "Cardano ADA" ;
            b:hasTokenSymbol "ADA" ;
            b:hasMaxSupply 45000000000000000 ;
            b:hasConversionRate 1000000 ;
            b:hasDenominationName "Lovelace" .

        # Test tokens
        <http://test/token1> rdf:type c:CNT ;
            b:hasTokenName "Test Token 1" .
        <http://test/token2> rdf:type c:CNT ;
            b:hasTokenName "Test Token 2" .

        # Blocks with timestamps
        <http://test/block1> rdf:type b:Block ;
            b:hasTimestamp "{timestamps['recent']}"^^xsd:dateTime ;
            b:hasTx <http://test/tx1>, <http://test/tx2> .

        <http://test/block2> rdf:type b:Block ;
            b:hasTimestamp "{timestamps['yesterday']}"^^xsd:dateTime ;
            b:hasTx <http://test/tx3>, <http://test/tx4> .

        <http://test/block3> rdf:type b:Block ;
            b:hasTimestamp "{timestamps['last_week']}"^^xsd:dateTime ;
            b:hasTx <http://test/tx5> .

        <http://test/block4> rdf:type b:Block ;
            b:hasTimestamp "{timestamps['last_month']}"^^xsd:dateTime ;
            b:hasTx <http://test/tx6> .

        # Transactions
        <http://test/tx1> c:hasFee "1000000"^^xsd:decimal ;
            c:hasOutput <http://test/output1>, <http://test/output2> .

        <http://test/tx2> c:hasFee "900000"^^xsd:decimal ;
            c:hasOutput <http://test/output3> .

        <http://test/tx3> c:hasFee "950000"^^xsd:decimal ;
            c:hasOutput <http://test/output4> .

        <http://test/tx4> c:hasFee "980000"^^xsd:decimal ;
            c:hasOutput <http://test/output5> .

        # Transaction outputs
        <http://test/output1> b:hasTokenAmount <http://test/amount1> .
        <http://test/amount1> b:hasCurrency c:ADA ;
            b:hasAmountValue "5000000000"^^xsd:decimal .

        <http://test/output2> b:hasTokenAmount <http://test/amount2> .
        <http://test/amount2> b:hasCurrency <http://test/token1> ;
            b:hasAmountValue "1000"^^xsd:decimal .

        <http://test/output3> b:hasTokenAmount <http://test/amount3> .
        <http://test/amount3> b:hasCurrency c:ADA ;
            b:hasAmountValue "3000000000"^^xsd:decimal .

        # Accounts
        <http://test/account1> rdf:type b:Account ;
            b:hasTokenAmount <http://test/holding1> ;
            c:hasReward <http://test/reward1> ;
            b:firstAppearedInTransaction <http://test/tx1> .

        <http://test/account2> rdf:type b:Account ;
            b:hasTokenAmount <http://test/holding2> ;
            c:hasReward <http://test/reward2> ;
            b:firstAppearedInTransaction <http://test/tx2> .

        <http://test/account3> rdf:type b:Account ;
            b:hasTokenAmount <http://test/holding3> ;
            b:firstAppearedInTransaction <http://test/tx3> .

        # Account holdings
        <http://test/holding1> b:hasCurrency c:ADA ;
            b:hasAmountValue "27670109999999999998"^^xsd:decimal .

        <http://test/holding2> b:hasCurrency c:ADA ;
            b:hasAmountValue "150000000000"^^xsd:decimal .

        <http://test/holding3> b:hasCurrency c:ADA ;
            b:hasAmountValue "180000000000"^^xsd:decimal .

        # Rewards
        <http://test/reward1> c:hasRewardAmount <http://test/rewardAmount1> .
        <http://test/rewardAmount1> b:hasAmountValue "1000000"^^xsd:decimal .

        <http://test/reward2> c:hasRewardAmount <http://test/rewardAmount2> .
        <http://test/rewardAmount2> b:hasAmountValue "1500000"^^xsd:decimal .

        # Smart Contracts
        <http://test/contract1> rdf:type b:SmartContract ;
            c:embeddedIn <http://test/tx1> .

        <http://test/contract2> rdf:type b:SmartContract ;
            c:embeddedIn <http://test/tx2> .

        # NFTs
        <http://test/nft1> rdf:type b:NFT .
        <http://test/nft2> rdf:type b:NFT .
        <http://test/tx1> c:hasMintedAsset <http://test/nft1> .
        <http://test/tx2> c:hasMintedAsset <http://test/nft2> .

        # Stake Pools
        <http://test/pool1> rdf:type c:StakePool .
        <http://test/pool2> rdf:type c:StakePool .

        # Staking
        <http://test/account1> c:delegatesTo <http://test/pool1> ;
            c:hasStakeAmount "100000000000"^^xsd:decimal .
        <http://test/account2> c:delegatesTo <http://test/pool1> ;
            c:hasStakeAmount "80000000000"^^xsd:decimal .
        <http://test/account3> c:delegatesTo <http://test/pool2> ;
            c:hasStakeAmount "90000000000"^^xsd:decimal .

        # Governance
        <http://test/proposal1> rdf:type c:GovernanceProposal .
        <http://test/metadata1> c:hasGovernanceProposal <http://test/proposal1> .
        <http://test/tx1> c:hasTxMetadata <http://test/metadata1> .

        # Voting
        <http://test/vote1> rdf:type c:Vote .
        <http://test/vote2> rdf:type c:Vote .
        <http://test/account1> c:castsVote <http://test/vote1> .
        <http://test/account2> c:castsVote <http://test/vote2> .
        <http://test/proposal1> c:hasVote <http://test/vote1>, <http://test/vote2> .
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
        PREFIX b: <https://mobr.ai/ont/blockchain#>
        PREFIX c: <https://mobr.ai/ont/cardano#>

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
        PREFIX b: <https://mobr.ai/ont/blockchain#>
        PREFIX c: <https://mobr.ai/ont/cardano#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        # First, check what timestamps we actually have in the graph
        SELECT ?timestamp
        FROM <{graph}>
        WHERE {{
            ?block a b:Block ;
                b:hasTimestamp ?timestamp .
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
        PREFIX b: <https://mobr.ai/ont/blockchain#>
        PREFIX c: <https://mobr.ai/ont/cardano#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        # Check the full path exists without time filter
        SELECT ?tx ?amount ?timestamp
        FROM <{graph}>
        WHERE {{
            ?block a b:Block ;
                b:hasTx ?tx ;
                b:hasTimestamp ?timestamp .
            ?tx c:hasOutput ?output .
            ?output b:hasTokenAmount ?tokenAmount .
            ?tokenAmount b:hasCurrency c:ADA ;
                b:hasAmountValue ?amount .
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
        PREFIX b: <https://mobr.ai/ont/blockchain#>
        PREFIX c: <https://mobr.ai/ont/cardano#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        SELECT DISTINCT ?timestamp (now() as ?current) (now() - "P1D"^^xsd:duration as ?oneDayAgo)
            (?timestamp >= now() - "P1D"^^xsd:duration as ?withinDay)
        FROM <{graph}>
        WHERE {{
            ?block a b:Block ;
                b:hasTimestamp ?timestamp .
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
        PREFIX b: <https://mobr.ai/ont/blockchain#>
        PREFIX c: <https://mobr.ai/ont/cardano#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        SELECT ?tx ?amount ?timestamp (xsd:dateTime(NOW() - "P1D"^^xsd:duration) as ?oneDayAgo)
        FROM <{graph}>
        WHERE {{
            ?block a b:Block ;
                b:hasTx ?tx ;
                b:hasTimestamp ?timestamp .
            ?tx c:hasOutput ?output .
            ?output b:hasTokenAmount ?tokenAmount .
            ?tokenAmount b:hasCurrency c:ADA ;
                b:hasAmountValue ?amount .
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
            ?block a b:Block ;
                b:hasTx ?tx ;
                b:hasTimestamp ?timestamp .
            ?tx c:hasOutput ?output .
            ?output b:hasTokenAmount ?tokenAmount .
            ?tokenAmount b:hasCurrency c:ADA ;
                b:hasAmountValue ?amount .
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
            ?block a b:Block ;
                b:hasTx ?tx ;
                b:hasTimestamp ?ts .
            ?tx c:hasFee ?fee .
            BIND (NOW() - "P7D"^^xsd:dayTimeDuration as ?oneWeekAgo)
            FILTER (?ts >= ?oneWeekAgo)
        }}
        """,

        # Query 3: How many accounts hold over 200,000 ADA?
        """
        SELECT (COUNT(DISTINCT ?account) as ?numAccounts)
        FROM <{graph}>
        WHERE {{
            ?account a b:Account ;
                b:hasTokenAmount ?tokenAmount .
            ?tokenAmount b:hasCurrency c:ADA ;
                b:hasAmountValue ?amount .
            FILTER(?amount > 200000000000)
        }}
        """,

        # Query 4: What are the average staking rewards for accounts holding more than 1 million ADA?
        """
        SELECT (AVG(?rewardValue) as ?avgReward)
        FROM <{graph}>
        WHERE {{
            ?account b:hasTokenAmount ?holding .
            ?holding b:hasCurrency c:ADA ;
                b:hasAmountValue ?adaAmount .
            ?account c:hasReward ?reward .
            ?reward c:hasRewardAmount ?rewardAmt .
            ?rewardAmt b:hasAmountValue ?rewardValue .
            FILTER (?adaAmount > 1000000000000)
        }}
        """,

        # Query 5: Monthly smart contract deployments
        """
        SELECT (MONTH(?ts) as ?month) (COUNT(DISTINCT ?contract) as ?deployments)
        FROM <{graph}>
        WHERE {{
            ?block a b:Block ;
                b:hasTx ?tx ;
                b:hasTimestamp ?ts .
            ?contract a b:SmartContract ;
                c:embeddedIn ?tx .
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
            ?block a b:Block ;
                b:hasTx ?tx ;
                b:hasTimestamp ?ts .
            ?tx c:hasMintedAsset ?nft .
            ?nft a b:NFT .
            BIND (NOW() - "P30D"^^xsd:dayTimeDuration as ?oneMonthAgo)
            FILTER (?ts >= ?oneMonthAgo)
        }}
        """,

        # Query 7: Top 10 tokens by transfer count
        """
        SELECT ?token (COUNT(DISTINCT ?output) as ?transfers)
        FROM <{graph}>
        WHERE {{
            ?block a b:Block ;
                b:hasTx ?tx ;
                b:hasTimestamp ?ts .
            ?tx c:hasOutput ?output .
            ?output b:hasTokenAmount ?tokenAmount .
            ?tokenAmount b:hasCurrency ?token .
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
            ?account b:firstAppearedInTransaction ?tx .
            ?block b:hasTx ?tx .
            ?block b:hasTimestamp ?timestamp .
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
                    ?account c:delegatesTo ?pool ;
                        c:hasStakeAmount ?amount .
                    ?pool a c:StakePool .
                }}
                GROUP BY ?pool
                ORDER BY DESC(?stakeAmount)
                LIMIT 10
            }}
            {{
                SELECT (SUM(?amount) as ?totalStaked)
                WHERE {{
                    ?account c:hasStakeAmount ?amount .
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
                    ?block b:hasTx ?tx .
                    ?tx c:hasTxMetadata ?metadata .
                    ?metadata c:hasGovernanceProposal ?proposal .
                }}
                ORDER BY DESC(?timestamp)
                LIMIT 1
            }}
            ?account c:castsVote ?vote .
            ?proposal c:hasVote ?vote .
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