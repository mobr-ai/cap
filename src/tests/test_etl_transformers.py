import pytest

from cap.etl.cdb.transformer_factory import TransformerFactory
from cap.config import settings

@pytest.mark.asyncio
async def test_account_transformer():
    """Test account data transformation to RDF."""
    transformer = TransformerFactory.create_transformer('account')

    test_accounts = [{
        'id': 1,
        'stake_address': 'stake1ux...',
        'stake_address_hash': 'abcd1234',
        'ada_balance': 1000000000,
        'token_balances': [
            {
                'fingerprint': 'asset1...',
                'policy': 'policy123',
                'name': 'token123',
                'quantity': 100
            }
        ],
        'first_tx_hash': 'tx123',
        'first_tx_timestamp': '2024-01-01T00:00:00Z',
        'first_block_hash': 'block123',
        'first_block_timestamp': '2024-01-01T00:00:00Z'
    }]

    turtle_data = transformer.transform(test_accounts)

    assert turtle_data
    assert 'blockchain:Account' in turtle_data
    assert 'stake1ux...' in turtle_data
    assert 'blockchain:hasTokenAmount' in turtle_data
    assert 'cardano:ADA' in turtle_data

@pytest.mark.asyncio
async def test_block_transformer():
    """Test block data transformation to RDF."""
    transformer = TransformerFactory.create_transformer('block')

    test_blocks = [{
        'id': 1,
        'hash': 'blockhash123',
        'time': '2024-01-01T00:00:00Z',
        'slot_no': 12345,
        'epoch_no': 100,
        'epoch_slot_no': 1000,
        'block_no': 1000000,
        'size': 2048,
        'tx_count': 5,
        'previous_id': 0,
        'slot_leader_hash': 'leader123',
        'pool_hash': 'pool123',
        'proto_major': 8,
        'proto_minor': 0,
        'transactions': [
            {'hash': 'tx1', 'epoch_no': 100},
            {'hash': 'tx2', 'epoch_no': 100}
        ]
    }]

    turtle_data = transformer.transform(test_blocks)

    assert turtle_data
    assert 'blockchain:Block' in turtle_data
    assert 'blockchain:hasTimestamp' in turtle_data
    assert 'blockchain:hasTransaction' in turtle_data
    assert 'cardano:hasSlotNumber' in turtle_data

@pytest.mark.asyncio
async def test_transaction_transformer():
    """Test transaction data transformation to RDF."""
    transformer = TransformerFactory.create_transformer('transaction')

    test_transactions = [{
        'id': 1,
        'hash': 'txhash123',
        'block_hash': 'blockhash123',
        'block_timestamp': '2024-01-01T00:00:00Z',
        'block_epoch_no': 100,
        'fee': '1000000',
        'inputs': [],
        'outputs': [
            {
                'id': 1,
                'index': 0,
                'address': 'addr1...',
                'value': '5000000',
                'multi_assets': []
            }
        ],
        'metadata': [],
        'minted_assets': [],
        'certificates': [],
        'withdrawals': []
    }]

    turtle_data = transformer.transform(test_transactions)

    assert turtle_data
    assert 'blockchain:Transaction' in turtle_data
    assert 'cardano:hasFee' in turtle_data
    assert 'cardano:hasOutput' in turtle_data
    assert 'blockchain:hasTokenAmount' in turtle_data

@pytest.mark.asyncio
async def test_stake_pool_transformer():
    """Test stake pool data transformation to RDF."""
    transformer = TransformerFactory.create_transformer('stake_pool')

    test_pools = [{
        'id': 1,
        'pool_hash': 'pool123',
        'pledge': '1000000000',
        'margin': 0.05,
        'fixed_cost': '340000000',
        'reward_addr': 'stake1...',
        'metadata_url': 'https://pool.com/metadata.json'
    }]

    turtle_data = transformer.transform(test_pools)

    assert turtle_data
    assert 'cardano:StakePool' in turtle_data
    assert 'cardano:hasPoolPledge' in turtle_data
    assert 'cardano:hasMargin' in turtle_data
    assert 'cardano:hasFixedCost' in turtle_data

@pytest.mark.asyncio
async def test_transformer_factory_all_types():
    """Test that all transformer types can be created."""
    transformer_types = [
        'account', 'epoch', 'block', 'transaction',
        'multi_asset', 'script', 'datum',
        'stake_address', 'stake_pool', 'delegation',
        'reward', 'withdrawal', 'governance_action',
        'drep_registration', 'treasury'
    ]

    for transformer_type in transformer_types:
        transformer = TransformerFactory.create_transformer(transformer_type)
        assert transformer is not None

@pytest.mark.asyncio
async def test_transformer_invalid_type():
    """Test transformer factory with invalid type."""
    with pytest.raises(ValueError) as exc_info:
        TransformerFactory.create_transformer('invalid_type')

    assert "Unknown transformer type" in str(exc_info.value)

@pytest.mark.asyncio
async def test_governance_transformer():
    """Test governance data transformation to RDF."""
    transformer = TransformerFactory.create_transformer('governance_action')

    test_actions = [{
        'id': 1,
        'tx_hash': 'tx123',
        'type': 'ParameterChange',
        'voting_procedures': [
            {
                'id': 1,
                'vote': 'Yes',
                'voter_role': 'DRep'
            }
        ]
    }]

    turtle_data = transformer.transform(test_actions)

    assert turtle_data
    assert 'cardano:GovernanceAction' in turtle_data
    assert 'cardano:Vote' in turtle_data
    assert 'cardano:hasVotingResult' in turtle_data

@pytest.mark.asyncio
async def test_multi_asset_transformer():
    """Test multi-asset data transformation to RDF."""
    transformer = TransformerFactory.create_transformer('multi_asset')

    test_assets = [{
        'id': 1,
        'fingerprint': 'asset123',
        'policy': 'policy456',
        'name': 'MyToken'
    }]

    turtle_data = transformer.transform(test_assets)

    assert turtle_data
    assert 'cardano:CNT' in turtle_data
    assert 'blockchain:hasHash' in turtle_data
    assert 'cardano:hasPolicyId' in turtle_data
    assert 'blockchain:hasTokenName' in turtle_data