"""
db.py - Database connection layer for Yelhao backend.

Reads DATABASE_URL from environment, normalises postgres:// → postgresql://,
creates a SQLAlchemy engine and a sessionmaker, and exposes two thin helpers:

    get_db_session()   - context manager that yields a session and handles
                          commit / rollback / close automatically.
    test_connection()  - quick connectivity check; returns (True, None) or
                          (False, error_string).

Nothing in this file imports from app.py, so it is safe to import anywhere.
"""

import os
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# ---------------------------------------------------------------------------
# 1. Read + normalise DATABASE_URL
# ---------------------------------------------------------------------------

_raw_url = os.environ.get("DATABASE_URL", "")

if _raw_url.startswith("postgres://"):
    # Render (and Heroku) still issue postgres:// URLs; SQLAlchemy ≥ 1.4
    # requires postgresql://.
    DATABASE_URL = _raw_url.replace("postgres://", "postgresql://", 1)
else:
    DATABASE_URL = _raw_url

# ---------------------------------------------------------------------------
# 2. Engine - created lazily so import doesn't blow up if URL is missing
# ---------------------------------------------------------------------------

_engine = None


def _get_engine():
    global _engine
    if _engine is None:
        if not DATABASE_URL:
            raise RuntimeError(
                "DATABASE_URL environment variable is not set. "
                "Add it to your Render environment and redeploy."
            )
        _engine = create_engine(
            DATABASE_URL,
            pool_pre_ping=True,   # detect stale connections
            pool_size=5,
            max_overflow=10,
            connect_args={"connect_timeout": 10},
        )
    return _engine


# ---------------------------------------------------------------------------
# 3. SessionLocal factory
# ---------------------------------------------------------------------------

_SessionLocal = None


def _get_session_factory():
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(
            bind=_get_engine(),
            autocommit=False,
            autoflush=False,
        )
    return _SessionLocal


# ---------------------------------------------------------------------------
# 4. Public helpers
# ---------------------------------------------------------------------------

@contextmanager
def get_db_session():
    """
    Yield a SQLAlchemy Session.  Commits on clean exit, rolls back on error,
    always closes.

    Usage:
        with get_db_session() as session:
            session.execute(text("SELECT 1"))
    """
    factory = _get_session_factory()
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def test_connection():
    """
    Run a trivial query to verify the database is reachable.

    Returns:
        (True,  None)          - connected OK
        (False, error_string)  - connection failed
    """
    try:
        with get_db_session() as session:
            session.execute(text("SELECT 1"))
        return True, None
    except Exception as exc:
        return False, str(exc)
