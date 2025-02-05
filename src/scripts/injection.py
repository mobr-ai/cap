from datetime import datetime, timedelta
import asyncio

from cap.virtuoso import VirtuosoClient

PREFIXES = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX blockchain: <http://www.mobr.ai/ontologies/blockchain#>
    PREFIX cardano: <http://www.mobr.ai/ontologies/cardano#>
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
            blockchain:hasAmountValue "250000000000"^^xsd:integer .

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

async def inject_test():
    vc = VirtuosoClient()
    success = await vc.create_graph(TEST_GRAPH, generate_test_data())
    assert success

if __name__ == "__main__":
    asyncio.run(inject_test())