import logging

import xxhash
from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker
from pydantic_settings import BaseSettings

from serpent_web.data.sql.sql_database_type import DatabaseType

_logger = logging.getLogger(__name__)

# todo: consolidate settings pattern
class SqliteSettings(BaseSettings):
    sqlite_url: str


class EngineFactory:
    _engine_instances = {}
    echo = False

    # todo:
    # echo = True if logger_settings.log_level == "DEBUG" else False

    @classmethod
    def get_engine(cls, database_type: DatabaseType, database_url: str, database_name: str = None) -> Engine:
        database_unique_id = f"{database_type}-{database_name}"
        if database_unique_id in cls._engine_instances:
            return cls._engine_instances[database_unique_id]

        else:
            engine = cls._engine_strategy_map().get(database_type, None)(database_url)

            if engine is None:
                raise ValueError(f"database type {database_type} not supported")

            cls._engine_instances[database_unique_id] = engine

        return cls.get_engine(
            database_type=database_type,
            database_url=database_url,
            database_name=database_name
        )

    @classmethod
    def _engine_strategy_map(cls):
        return {
            DatabaseType.SQLITE: cls.create_sql_lite_engine,
            DatabaseType.DATABRICKS: cls.create_sql_engine
        }

    # database url not used for sqlite - local development only
    @classmethod
    def create_sql_lite_engine(cls, database_url: str) -> Engine:
        return create_engine(
            database_url, connect_args={"check_same_thread": False}, echo=cls.echo
        )

    @classmethod
    def create_sql_engine(cls, database_url: str) -> Engine:
        return create_engine(
            database_url,
            echo=cls.echo,
            pool_pre_ping=True,
            pool_size=200,  # 200 is max possible pool size
            pool_recycle=3600
        )


def get_session(**kwargs):
    return sessionmaker(
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
        **kwargs
    )()


class InternalError(Exception):
    pass


def db_dependency(database_type: DatabaseType, database_url: str, database_name: str = None) -> get_session:
    if database_name is None:
        # unique key for engine instances dict
        database_name = xxhash.xxh64(database_url).hexdigest()

    def get_db():
        engine = EngineFactory.get_engine(database_type=database_type, database_url=database_url,
                                          database_name=database_name)
        db = get_session(bind=engine)
        try:
            yield db
            db.commit()
        except InternalError as e:
            _logger.exception(f'Database session rollback due to exception: {e}')
            db.rollback()
            raise e
        finally:
            db.close()

    return get_db
