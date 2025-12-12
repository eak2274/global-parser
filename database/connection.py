"""
PostgreSQL connection management module using psycopg.

Provides:
- Connection pool with lazy initialization
- Context managers for working with connections and cursors
- Automatic search_path (schema) configuration
- Simple connection without pool for one-off scripts

Usage examples:

    # Option 1: Working with cursor (autocommit)
    with get_cursor() as cur:
        cur.execute("SELECT * FROM matches WHERE id = %s", (match_id,))
        row = cur.fetchone()

    # Option 2: Working with connection (manual transaction control)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO ...")
            cur.execute("UPDATE ...")
        conn.commit()

    # Option 3: Simple connection without pool
    with get_simple_connection() as conn:
        ...

    # On application shutdown:
    close_pool()
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Generator

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from config import settings

if TYPE_CHECKING:
    from psycopg import Connection, Cursor

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Global connection pool (lazy initialization)
# -----------------------------------------------------------------------------

_pool: ConnectionPool | None = None


def _configure_connection(conn: Connection) -> None:
    """
    Configure connection after obtaining from pool.
    
    Called automatically for each new connection in the pool.
    Sets search_path to the specified schema.
    """
    schema = settings.pg.db_schema
    conn.execute(f"SET search_path TO {schema}")
    logger.debug(f"Connection configured: search_path set to '{schema}'")


def get_pool() -> ConnectionPool:
    """
    Get or create connection pool.
    
    Pool is created on first call (lazy initialization).
    All subsequent calls return the same pool instance.
    
    Returns:
        ConnectionPool: psycopg connection pool.
    """
    global _pool
    
    if _pool is None:
        logger.info(
            f"Initializing connection pool: "
            f"{settings.pg.host}:{settings.pg.port}/{settings.pg.db} "
            f"(min={settings.pg.pool_min_size}, max={settings.pg.pool_max_size})"
        )
        
        _pool = ConnectionPool(
            conninfo=settings.pg.connection_url,
            min_size=settings.pg.pool_min_size,
            max_size=settings.pg.pool_max_size,
            configure=_configure_connection,
            kwargs={
                "row_factory": dict_row,
                "connect_timeout": settings.pg.connect_timeout,
            },
        )
        
        # Wait for pool to be ready (optional)
        _pool.wait()
        logger.info("Connection pool initialized successfully")
    
    return _pool


def close_pool() -> None:
    """
    Close connection pool.
    
    Call on application shutdown to properly release resources.
    """
    global _pool
    
    if _pool is not None:
        logger.info("Closing connection pool...")
        _pool.close()
        _pool = None
        logger.info("Connection pool closed")


def get_pool_stats() -> dict | None:
    """
    Get connection pool statistics.
    
    Returns:
        dict with pool information or None if pool is not initialized.
    """
    if _pool is None:
        return None
    
    return {
        "min_size": _pool.min_size,
        "max_size": _pool.max_size,
        "size": _pool.get_stats().get("pool_size", 0),
        "available": _pool.get_stats().get("pool_available", 0),
        "requests_waiting": _pool.get_stats().get("requests_waiting", 0),
    }


# -----------------------------------------------------------------------------
# Context managers for pool-based connections
# -----------------------------------------------------------------------------

@contextmanager
def get_connection() -> Generator[Connection, None, None]:
    """
    Context manager to obtain a connection from the pool.
    
    Connection is automatically returned to the pool on context exit.
    Commit/rollback must be handled manually.
    
    Yields:
        Connection: psycopg connection with row_factory=dict_row.
    
    Example:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM users")
                users = cur.fetchall()
            conn.commit()
    """
    pool = get_pool()
    with pool.connection() as conn:
        yield conn


@contextmanager
def get_cursor(*, autocommit: bool = True) -> Generator[Cursor, None, None]:
    """
    Context manager providing connection and cursor.
    
    Args:
        autocommit: If True (default), commits on successful context exit.
                    On exception, performs rollback.
    
    Yields:
        Cursor: psycopg cursor.
    
    Example:
        with get_cursor() as cur:
            cur.execute("INSERT INTO matches (name) VALUES (%s)", ("Test",))
            # automatic commit
        
        with get_cursor(autocommit=False) as cur:
            cur.execute("SELECT * FROM matches")
            rows = cur.fetchall()
            # no commit (read-only)
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            try:
                yield cur
                if autocommit:
                    conn.commit()
            except Exception:
                conn.rollback()
                raise


@contextmanager
def transaction() -> Generator[Cursor, None, None]:
    """
    Context manager for explicit transaction handling.
    
    Commit is performed only on successful exit.
    Any exception triggers rollback.
    
    Yields:
        Cursor: psycopg cursor within a transaction.
    
    Example:
        with transaction() as cur:
            cur.execute("UPDATE accounts SET balance = balance - %s WHERE id = %s", (100, 1))
            cur.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s", (100, 2))
            # commit only if both operations succeed
    """
    with get_connection() as conn:
        with conn.transaction():  # BEGIN ... COMMIT/ROLLBACK
            with conn.cursor() as cur:
                yield cur


# -----------------------------------------------------------------------------
# Simple connection without pool (for scripts and migrations)
# -----------------------------------------------------------------------------

@contextmanager
def get_simple_connection(
    *, 
    autocommit: bool = False,
    use_dict_row: bool = True,
) -> Generator[Connection, None, None]:
    """
    Simple database connection without using the pool.
    
    Suitable for:
    - One-off scripts
    - Migrations
    - Cases where pool is overkill
    
    Args:
        autocommit: If True, each statement is committed automatically.
        use_dict_row: If True, results are returned as dictionaries.
    
    Yields:
        Connection: psycopg connection.
    
    Example:
        with get_simple_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                print(cur.fetchone())
            conn.commit()
    """
    row_factory = dict_row if use_dict_row else None
    
    conn = psycopg.connect(
        host=settings.pg.host,
        port=settings.pg.port,
        dbname=settings.pg.db,
        user=settings.pg.user,
        password=settings.pg.password,
        connect_timeout=settings.pg.connect_timeout,
        options=f"-c search_path={settings.pg.db_schema}",
        row_factory=row_factory,
        autocommit=autocommit,
    )
    
    logger.debug(f"Simple connection opened to {settings.pg.host}:{settings.pg.port}/{settings.pg.db}")
    
    try:
        yield conn
        if not autocommit:
            conn.commit()
    except Exception:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()
        logger.debug("Simple connection closed")


# -----------------------------------------------------------------------------
# Utility functions
# -----------------------------------------------------------------------------

def check_connection() -> bool:
    """
    Check database availability.
    
    Returns:
        True if connection is successful, False on error.
    """
    try:
        with get_simple_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                return cur.fetchone() is not None
    except Exception as e:
        logger.error(f"Database connection check failed: {e}")
        return False


def get_server_version() -> str | None:
    """
    Get PostgreSQL server version.
    
    Returns:
        Version string or None on error.
    """
    try:
        with get_simple_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                result = cur.fetchone()
                return result["version"] if result else None
    except Exception as e:
        logger.error(f"Failed to get server version: {e}")
        return None