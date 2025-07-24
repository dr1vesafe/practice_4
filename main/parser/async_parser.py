import asyncio
import datetime
import pandas as pd
from io import BytesIO
from urllib.parse import urljoin
from config.database import async_session
from models.models import SpimexTradingResults
from .base_parser import async_client, BASE_URL, LOAD_URL


async def get_excel(date, semaphore):
    date_str = date.strftime('%Y%m%d')
    url = urljoin(BASE_URL, LOAD_URL.format(date_str))
    async with semaphore:
        try:
            response = await async_client.get(url)
            if response.status_code == 200:
                print(f'{date}: {response.status_code}')
                excel_content = response.content
                df = pd.read_excel(BytesIO(excel_content))
                return (df, date)
            else:
                print(f'{date}: {response.status_code}')
        except Exception as e:
            print(f'Ошибка при загрузке: {e}')
    return None


async def get_all_excels():
    semaphore = asyncio.Semaphore(25)
    start_date = datetime.date(year=2025, month=1, day=1)
    end_date = datetime.date.today()
    delta = datetime.timedelta(days=1)
    date = start_date
    dates = []

    while date <= end_date:
        dates.append(date)
        date += delta

    tasks = [asyncio.create_task(get_excel(task_date, semaphore)) for task_date in dates]
    results = await asyncio.gather(*tasks)

    for result in results:
        if result:
            yield result


async def parse_excel():
    columns = {
        'Код Инструмента': 'exchange_product_id',
        'Наименование Инструмента': 'exchange_product_name',
        'Базис поставки': 'delivery_basis_name',
        'Объем Договоров в единицах измерения': 'volume',
        'Обьем Договоров, руб.': 'total',
        'Количество Договоров, шт.': 'count',
    }

    async for df, date in get_all_excels():
        df_slice = df.iloc[5:, 1:].dropna(how='all')
        df_slice.columns = (
            df_slice.iloc[0]
            .astype(str)
            .str.replace('\n', ' ', regex=False)
            .str.strip()
        )
        df_slice = df_slice[1:]

        first_column = df_slice.columns[0]
        df_slice = df_slice[
            ~df_slice[first_column].astype(str).str.contains(
                'Итого',
                case=False,
                na=False
                )
        ]

        try:
            df_slice = df_slice[list(columns.keys())]
            df_slice = df_slice.rename(columns=columns)

            filter_column_name = 'count'
            df_slice[filter_column_name] = pd.to_numeric(
                df_slice[filter_column_name],
                errors='coerce',
            )

            df_slice = df_slice[df_slice[filter_column_name] > 0]
            df_slice = df_slice.reset_index(drop=True)
        except KeyError as e:
            print(f'Ошибка при парсинге: {e}')
            continue

        yield (df_slice, date)


async def db_load_data():
    async with async_session() as session:
        async for df, date in parse_excel():
            try:
                for _, row in df.iterrows():
                    session.add(
                        SpimexTradingResults(
                            exchange_product_id=str(
                                row.get('exchange_product_id')
                                ),
                            exchange_product_name=str(
                                row.get('exchange_product_name')
                                ),
                            oil_id=str(row.get('exchange_product_id'))[:4],
                            delivery_basis_id=str(
                                row.get('exchange_product_id')
                                )[4:7],
                            delivery_basis_name=str(
                                row.get('delivery_basis_name')
                                ),
                            delivery_type_id=str(
                                row.get('exchange_product_id')
                                )[-1],
                            volume=int(row.get('volume', 0)),
                            total=int(row.get('total', 0)),
                            count=int(row.get('count')),
                            date=date
                        )
                    )
                await session.commit()
            except Exception as e:
                print(f'Ошибка при загрузке данных: {e}')
                await session.rollback()
