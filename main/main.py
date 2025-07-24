from config.database import Base, sync_engine
from models.models import SpimexTradingResults


def create_tables():
    Base.metadata.create_all(sync_engine)


def delete_tables():
    Base.metadata.drop_all(sync_engine)


def main():
    delete_tables()
    create_tables()


main()
