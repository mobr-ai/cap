from datetime import datetime, timedelta
import asyncio

from cap.rdf.triplestore import TriplestoreClient

PREFIXES = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX b: <https://mobr.ai/ont/blockchain#>
    PREFIX c: <https://mobr.ai/ont/cardano#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

"""
TEST_GRAPH = "http://test.cardano.queries"

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
            b:hasAmountValue "250000000000"^^xsd:decimal .

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

async def inject_test():
    vc = TriplestoreClient()
    success = await vc.create_graph(TEST_GRAPH, generate_test_data())
    assert success

if __name__ == "__main__":
    asyncio.run(inject_test())