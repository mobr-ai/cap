# cap/src/cap/database/model.py
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Boolean
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
    avatar         = Column(String, nullable=True)
    settings       = Column(String, nullable=True)
    refer_id       = Column(Integer)
    is_confirmed   = Column(Boolean, default=False)
    confirmation_token = Column(String(128), nullable=True)
