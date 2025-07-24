from httpx import Client, AsyncClient


sync_client = Client()
async_client = AsyncClient()
BASE_URL = 'https://spimex.com'
LOAD_URL = '/upload/reports/oil_xls/oil_xls_{}162000.xls'