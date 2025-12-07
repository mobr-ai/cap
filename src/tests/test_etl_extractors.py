import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from cap.config import settings
from cap.etl.cdb.extractor_factory import ExtractorFactory
from cap.rdf.cdb_model import Epoch

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
