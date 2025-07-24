from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from .config import DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER


SYNC_DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )


ASYNC_DATABASE_URL = (
    f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


Base = declarative_base()

sync_engine = create_engine(SYNC_DATABASE_URL)
sync_session = sessionmaker(bind=sync_engine)

async_engine = create_async_engine(ASYNC_DATABASE_URL)
async_session = async_sessionmaker(bind=async_engine)
