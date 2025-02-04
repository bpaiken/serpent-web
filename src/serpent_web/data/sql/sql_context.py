import logging
from typing import AsyncGenerator, Generator

import xxhash

from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncSession
from sqlalchemy.orm import sessionmaker as async_sessionmaker

from serpent_web.data.sql.sql_database_type import DatabaseType

_logger = logging.getLogger(__name__)


class InternalError(Exception):
    pass


class EngineFactory:
    """
    A unified engine factory that handles both synchronous and asynchronous engines.
    Engines are cached based on a key that incorporates the database type, sync/async flag,
    and a unique name (generated from the URL if not provided).
    """
    _engine_instances = {}
    echo = False

    @classmethod
    def get_async_engine(
        cls,
        database_type: DatabaseType,
        database_url: str,
        database_name: str = None,
    ):
        return cls.get_engine(
            database_type=database_type,
            database_url=database_url,
            database_name=database_name,
            is_async=True,
        )

    @classmethod
    def get_engine(
        cls,
        database_type: DatabaseType,
        database_url: str,
        database_name: str = None,
        is_async: bool = False,
    ):
        if database_name is None:
            # Generate a unique key if no explicit name is provided
            database_name = xxhash.xxh64(database_url).hexdigest()
        key = f"{database_type}-{is_async}-{database_name}"
        if key in cls._engine_instances:
            return cls._engine_instances[key]

        # Choose the correct strategy map based on sync vs. async
        strategy_map = cls._engine_strategy_map(is_async)
        engine_creator = strategy_map.get(database_type)
        if engine_creator is None:
            raise ValueError(
                f"Database type {database_type} not supported for "
                f"{'async' if is_async else 'sync'} engines"
            )
        engine = engine_creator(database_url)
        cls._engine_instances[key] = engine
        return engine

    @classmethod
    def _engine_strategy_map(cls, is_async: bool):
        if is_async:
            return {
                # For SQLite, the URL must use the async driver (e.g. sqlite+aiosqlite://...)
                DatabaseType.SQLITE: cls.create_async_sqlite_engine,
                DatabaseType.DATABRICKS: cls.create_async_sql_engine,
                DatabaseType.POSTGRES: cls.create_async_sql_engine,
                DatabaseType.AZURESQL: cls.create_async_sql_engine,
            }
        else:
            return {
                DatabaseType.SQLITE: cls.create_sqlite_engine,
                DatabaseType.DATABRICKS: cls.create_sql_engine,
                DatabaseType.POSTGRES: cls.create_sql_engine,
                DatabaseType.AZURESQL: cls.create_sql_engine,
            }

    @classmethod
    def create_sqlite_engine(cls, database_url: str) -> Engine:
        return create_engine(
            database_url,
            connect_args={"check_same_thread": False},
            echo=cls.echo,
        )

    @classmethod
    def create_sql_engine(cls, database_url: str) -> Engine:
        return create_engine(
            database_url,
            echo=cls.echo,
            pool_pre_ping=True,
            pool_size=200,  # maximum pool size
            pool_recycle=3600,
        )

    @classmethod
    def create_async_sqlite_engine(cls, database_url: str) -> AsyncEngine:
        return create_async_engine(
            database_url,
            connect_args={"check_same_thread": False},
            echo=cls.echo,
        )

    @classmethod
    def create_async_sql_engine(cls, database_url: str) -> AsyncEngine:
        return create_async_engine(
            database_url,
            echo=cls.echo,
            pool_pre_ping=True,
            pool_size=200,
            pool_recycle=3600,
        )

    @classmethod
    def get_session(
            cls,
            database_type: DatabaseType,
            database_url: str,
            database_name: str = None,
            is_async: bool = False,
            **kwargs):
        engine = cls.get_engine(
            database_type=database_type,
            database_url=database_url,
            database_name=database_name,
            is_async=is_async
        )

        if is_async:
            session = async_sessionmaker(
                bind=engine,
                expire_on_commit=False,
                class_=AsyncSession,
                autocommit=False,
                autoflush=False,
                **kwargs
            )
        else:
            session = sessionmaker(
                bind=engine,
                expire_on_commit=False,
                autocommit=False,
                autoflush=False,
                **kwargs
            )

        return session


def get_session_maker(
    database_type: DatabaseType, database_url: str, database_name: str = None, **kwargs
):
    """
    Returns a synchronous session.
    """
    return EngineFactory.get_session(
        database_type=database_type, database_url=database_url, database_name=database_name, **kwargs
    )

def get_async_session_maker(database_type: DatabaseType, database_url: str, database_name: str = None, **kwargs):
    """
    Returns an asynchronous session.
    """
    return EngineFactory.get_session(
        database_type=database_type, database_url=database_url, database_name=database_name, is_async=True, **kwargs
    )

def db_dependency(
        database_type: DatabaseType, database_url: str, database_name: str = None, **kwargs
) -> Generator[Session, None, None]:
    session = get_session_maker(database_type=database_type, database_url=database_url, database_name=database_name, **kwargs)

    with session() as session:
        try:
            yield session
            session.commit()
        except InternalError as e:
            _logger.exception(f'Database session rollback due to exception: {e}')
            session.rollback()
            raise e
        finally:
            session.close()


async def async_db_dependency(
    database_type: DatabaseType, database_url: str, database_name: str = None, **kwargs
) -> AsyncGenerator[AsyncSession, None]:
    """
    An async dependency generator that yields an AsyncSession.
    (For example, to be used with FastAPI's dependency injection.)
    """
    async_session_local = get_async_session_maker(
        database_type=database_type, database_url=database_url, database_name=database_name, **kwargs
    )

    async with async_session_local() as session:
        try:
            yield session
            await session.commit()
        except InternalError as e:
            _logger.exception(f"Async database session rollback due to exception: {e}")
            await session.rollback()
            raise
        finally:
            await session.close()



