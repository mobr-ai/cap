import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from cap.config import settings
from cap.etl.cdb.extractor_factory import ExtractorFactory
from cap.data.cdb_model import Epoch

@pytest.fixture
def db_session():
    """Create a database session for testing."""
    engine = create_engine(
        f"postgresql://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}@"
        f"{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}",
        pool_pre_ping=True
    )
    SessionFactory = sessionmaker(bind=engine)
    session = SessionFactory()
    yield session
    session.close()

@pytest.mark.asyncio
async def test_account_extractor_creation(db_session):
    """Test account extractor creation and basic functionality."""
    extractor = ExtractorFactory.create_extractor('account', db_session, batch_size=10)

    assert extractor is not None
    assert extractor.batch_size == 10
    assert extractor.db_session == db_session

@pytest.mark.asyncio
async def test_multi_asset_extractor_batch(db_session):
    """Test multi-asset extractor batch extraction."""
    extractor = ExtractorFactory.create_extractor('multi_asset', db_session, batch_size=10)

    # Get total count
    total = extractor.get_total_count()
    assert isinstance(total, int)
    assert total >= 0

    # Get last ID
    last_id = extractor.get_last_id()
    assert last_id is None or isinstance(last_id, int)

@pytest.mark.asyncio
async def test_epoch_extractor_serialization(db_session):
    """Test epoch extractor data serialization."""
    extractor = ExtractorFactory.create_extractor('epoch', db_session, batch_size=10)

    # Query for a single epoch to test serialization
    epoch = db_session.query(Epoch).first()

    if epoch:
        serialized = extractor._serialize_epoch(epoch)

        assert 'id' in serialized
        assert 'no' in serialized
        assert 'start_time' in serialized
        assert 'end_time' in serialized

        if epoch.no is not None:
            assert serialized['no'] == epoch.no

@pytest.mark.asyncio
async def test_extractor_factory_all_types():
    """Test that all extractor types can be created."""
    extractor_types = [
        'account', 'epoch', 'block', 'transaction',
        'multi_asset', 'script', 'datum',
        'stake_address', 'stake_pool', 'delegation',
        'reward', 'withdrawal', 'governance_action',
        'drep_registration', 'treasury'
    ]

    engine = create_engine(
        f"postgresql://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}@"
        f"{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}"
    )
    SessionFactory = sessionmaker(bind=engine)

    with SessionFactory() as session:
        for extractor_type in extractor_types:
            extractor = ExtractorFactory.create_extractor(extractor_type, session, 100)
            assert extractor is not None

@pytest.mark.asyncio
async def test_extractor_invalid_type(db_session):
    """Test extractor factory with invalid type."""
    with pytest.raises(ValueError) as exc_info:
        ExtractorFactory.create_extractor('invalid_type', db_session, 100)

    assert "Unknown extractor type" in str(exc_info.value)