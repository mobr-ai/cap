from logging.config import fileConfig
import os
from dotenv import load_dotenv

from sqlalchemy import engine_from_config, pool
from alembic import context

# --- Load .env from project root ---
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../../"))
dotenv_path = os.path.join(BASE_DIR, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
else:
    print(f"Warning: .env not found at {dotenv_path}")

# Alembic Config object
config = context.config

# Configure SQLAlchemy URL from env
database_url = os.getenv("DATABASE_URL")
if not database_url:
    raise RuntimeError("DATABASE_URL not set. Check your .env or environment variables.")
config.set_main_option("sqlalchemy.url", database_url)

# Logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# --- Import model metadata for autogenerate ---
# Only manage these tables with Alembic
MANAGED_TABLES = {"user"}

def include_object(object, name, type_, reflected, compare_to):
    # Ignore all tables not in MANAGED_TABLES
    if type_ == "table" and name not in MANAGED_TABLES:
        return False
    # Also ignore FKs that point to unmanaged tables
    if type_ == "foreign_key_constraint":
        # if any remote table isn’t managed, skip this FK
        try:
            remote_table = list(object.elements)[0].column.table.name
            if remote_table not in MANAGED_TABLES:
                return False
        except Exception:
            pass
    return True


from cap.database.model import Base  # ensure this import loads all models
target_metadata = Base.metadata

def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,      # helpful for type diffs
        compare_server_default=True,
        include_object=include_object,
    )

    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online() -> None:
    """Run migrations in 'online' mode'."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
            include_object=include_object,
        )

        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
