import csv
import json
import os
import ssl
from io import StringIO

from aiohttp.client import ClientSession
from aiohttp.client_exceptions import ClientError
from fastapi import FastAPI, Request, Response

CH_HOST = os.getenv('LOGBROKER_CH_HOST', 'localhost')
PG_HOST = os.getenv('LOGBROKER_PG_HOST', 'localhost')
CH_USER = os.getenv('LOGBROKER_CH_USER')
CH_PASSWORD = os.getenv('LOGBROKER_CH_PASSWORD')
CH_PORT = int(os.getenv('LOGBROKER_CH_PORT', 8123))
CH_CERT_PATH = os.getenv('LOGBROKER_CH_CERT_PATH')


async def execute_query(query, data=None):
    url = f'http://{CH_HOST}:{CH_PORT}/'
    params = {
        'query': query.strip()
    }
    headers = {}
    if CH_USER is not None:
        headers['X-ClickHouse-User'] = CH_USER
        if CH_PASSWORD is not None:
            headers['X-ClickHouse-Key'] = CH_PASSWORD
    ssl_context = ssl.create_default_context(cafile=CH_CERT_PATH) if CH_CERT_PATH is not None else None

    async with ClientSession() as session:
        async with session.post(url, params=params, data=data, headers=headers, ssl=ssl_context) as resp:
            await resp.read()
            try:
                resp.raise_for_status()
                return resp, None
            except ClientError as e:
                return resp, {'error': str(e)}


app = FastAPI()


async def query_wrapper(query, data=None):
    res, err = await execute_query(query, data)
    if err is not None:
        return err
    return await res.text()


@app.get('/show_create_table')
async def show_create_table(table_name: str):
    resp = await query_wrapper(f'SHOW CREATE TABLE "{table_name}";')
    if isinstance(resp, str):
        return Response(content=resp.replace('\\n', '\n'), media_type='text/plain; charset=utf-8')
    return resp


async def send_csv(table_name, rows):
    data = StringIO()
    cwr = csv.writer(data, quoting=csv.QUOTE_ALL)
    cwr.writerows(rows)
    return await query_wrapper(f'INSERT INTO \"{table_name}\" FORMAT CSV', data)


async def send_json_each_row(table_name, rows):
    data = StringIO()
    for row in rows:
        assert isinstance(row, dict)
        data.write(json.dumps(row))
        data.write('\n')
    return await query_wrapper(f'INSERT INTO \"{table_name}\" FORMAT JSONEachRow', data)


@app.post('/write_log')
async def write_log(request: Request):
    body = await request.json()
    res = []
    for log_entry in body:
        table_name = log_entry['table_name']
        rows = log_entry['rows']
        if log_entry.get('format') == 'list':
            res.append(await send_csv(table_name, rows))
        elif log_entry.get('format') == 'json':
            res.append(await send_json_each_row(table_name, rows))
        else:
            res.append({'error': f'unknown format {log_entry.get("format")}, you must use list or json'})
    return res


@app.get('/healthcheck')
async def healthcheck():
    return Response(content='Ok', media_type='text/plain')
