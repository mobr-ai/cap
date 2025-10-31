from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, LargeBinary

Base = declarative_base()

class User(Base):
    __tablename__ = "user"
    user_id        = Column(Integer, primary_key=True)
    email          = Column(String, unique=True, index=True, nullable=True)
    password_hash  = Column(String, nullable=True)
    google_id      = Column(String, unique=True, nullable=True)
    wallet_address = Column(String(128), index=True, nullable=True)
    username       = Column(String(30), unique=True, index=True, nullable=True)
    display_name   = Column(String(30), nullable=True)

    settings       = Column(String, nullable=True)
    refer_id       = Column(Integer)
    is_confirmed   = Column(Boolean, default=False)
    confirmation_token = Column(String(128), nullable=True)

    # on-prem avatar storage
    avatar_blob    = Column(LargeBinary, nullable=True)      # BYTEA
    avatar_mime    = Column(String(64), nullable=True)       # e.g., "image/png"
    avatar_etag    = Column(String(64), nullable=True)       # md5/sha1 for cache/If-None-Match

    # URL kept for compatibility
    avatar         = Column(String, nullable=True)
