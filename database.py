import os
import uuid
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine, Column, String, Integer, DateTime, Boolean, Index, UniqueConstraint, event
from sqlalchemy.orm import declarative_base, sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:////Users/prudhvisailingineni/Desktop/LinkPlease/linkplease.db")

# Detect database dialect
is_sqlite = DATABASE_URL.startswith("sqlite")

# Configure database engine
if is_sqlite:
    # Use check_same_thread=False for SQLite with multi-threading / async workers
    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False}
    )
    
    # Enable WAL mode and performance pragmas for SQLite
    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA busy_timeout=5000")
        cursor.close()
else:
    # For PostgreSQL or other engines, use standard connection pooling
    engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=10, max_overflow=20)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Rule(Base):
    __tablename__ = "rules"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    keyword = Column(String, nullable=False, index=True)  # Normalized to lower-case
    dm_message = Column(String, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)


class ProcessedEvent(Base):
    __tablename__ = "processed_events"
    
    event_id = Column(String, primary_key=True)
    received_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)


class UserRuleInteraction(Base):
    __tablename__ = "user_rule_interactions"
    
    user_id = Column(String, primary_key=True)
    rule_id = Column(String, primary_key=True)


class DeletedComment(Base):
    __tablename__ = "deleted_comments"
    
    comment_id = Column(String, primary_key=True)
    deleted_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)


class DMJob(Base):
    __tablename__ = "dm_jobs"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    comment_id = Column(String, nullable=False, index=True)
    recipient_user_id = Column(String, nullable=False)
    rule_id = Column(String, nullable=False)
    message = Column(String, nullable=False)
    status = Column(String, default="queued", nullable=False)  # queued, sent, failed, duplicate_blocked
    retry_count = Column(Integer, default=0, nullable=False)
    next_retry_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    dm_id = Column(String, nullable=True, index=True)
    is_deleted = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)


def init_db():
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
