import datetime
import pandas as pd
from io import BytesIO
from httpx import Client
from urllib.parse import urljoin
from config.database import sync_session
from models.models import SpimexTradingResults


client = Client()
BASE_URL = 'https://spimex.com'
LOAD_URL = '/upload/reports/oil_xls/oil_xls_{}162000.xls'


def get_excel():
    start_date = datetime.date(year=2022, month=1, day=1)
    end_date = datetime.date.today()
    delta = datetime.timedelta(days=1)
    date = start_date

    while date <= end_date:
        date_str = date.strftime('%Y%m%d')
        url = urljoin(BASE_URL, LOAD_URL.format(date_str))

        try:
            response = client.get(url)

            if response.status_code == 200:
                print(f'{date}: {response.status_code}')
                excel_content = response.content
                df = pd.read_excel(BytesIO(excel_content))
                yield (df, date)
            else:
                print(f'{date}: {response.status_code}')

        except Exception as e:
            print(f'Ошибка при загрузке: {e}')

        date += delta


def parse_excel():
    columns = {
        'Код Инструмента': 'exchange_product_id',
        'Наименование Инструмента': 'exchange_product_name',
        'Базис поставки': 'delivery_basis_name',
        'Объем Договоров в единицах измерения': 'volume',
        'Обьем Договоров, руб.': 'total',
        'Количество Договоров, шт.': 'count',
    }

    for df, date in get_excel():
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


def db_load_data():
    with sync_session() as session:
        for df, date in parse_excel():
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
                session.commit()
            except Exception as e:
                print(f'Ошибка при загрузке данных: {e}')
                session.rollback()
