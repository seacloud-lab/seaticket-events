# -*- coding: utf-8 -*-
import logging
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    """SQLAlchemy declarative base for ORM models."""
    pass


def create_engine_from_conf(configs):
    """Create database engine from configuration."""
    host = configs.get('MYSQL_DB_HOST', 'localhost')
    port = configs.get('MYSQL_DB_PORT', 3306)
    username = configs.get('MYSQL_DB_USER', 'seaticket')
    password = configs.get('MYSQL_DB_PASSWORD', '')
    db_name = configs.get('MYSQL_DB_NAME', 'seaticket_db')

    db_url = "mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8" % \
                (username, quote_plus(password), host, port, db_name)
    logger.debug('[seaqa_events] database: mysql, name: %s', db_name)

    # Add pool recycle, or mysql connection will be closed
    # by mysql daemon if idle for too long.
    """MySQL has gone away
    https://docs.sqlalchemy.org/en/20/faq/connections.html#mysql-server-has-gone-away
    https://docs.sqlalchemy.org/en/20/core/pooling.html#pool-disconnects
    """
    kwargs = dict(pool_recycle=300, pool_pre_ping=True, echo=False, echo_pool=False)

    engine = create_engine(db_url, **kwargs)

    return engine


def init_db_session_class(configs):
    """Configure session class for mysql according to the configs file."""
    try:
        engine = create_engine_from_conf(configs)
    except Exception as e:
        logger.error("Init db session class error: %s" % e)
        raise RuntimeError("Init db session class error: %s" % e)

    session = sessionmaker(bind=engine)
    return session


def create_db_tables(configs):
    """Create events tables if not exists."""
    try:
        engine = create_engine_from_conf(configs)
    except Exception as e:
        logger.error("Create tables error: %s" % e)
        raise RuntimeError("Create tables error: %s" % e)

    Base.metadata.create_all(engine)
    engine.dispose()
