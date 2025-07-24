import time
import asyncio
from config.database import Base, sync_engine
from models.models import SpimexTradingResults
from parser import sync_parser, async_parser


def create_tables():
    Base.metadata.create_all(sync_engine)


def delete_tables():
    Base.metadata.drop_all(sync_engine)


async def main():
    delete_tables()
    create_tables()

    print('Запущен синхронный парсер')
    start_time = time.time()
    sync_parser.db_load_data()
    sync_parser_worktime = time.time() - start_time
    print('Синхронный парсер завершил свою работу')
    
    delete_tables()
    create_tables()

    print('Запущен асинхронный парсер')
    start_time = time.time()
    task = asyncio.create_task(async_parser.db_load_data())
    await task
    async_parser_worktime = time.time() - start_time
    print('Асинхронный парсер завершил свою работу')

    print(f'Время работы синхронного парсера: {round(sync_parser_worktime, 2)}')
    print(f'Время работы асинхронного парсера: {round(async_parser_worktime, 2)}')

asyncio.run(main())
