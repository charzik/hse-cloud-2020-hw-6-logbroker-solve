import csv
import json
import os
import ssl
from io import StringIO
import psycopg2
from psycopg2.extras import execute_values

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


@app.post('/write_log')
async def write_log(request: Request):
    body = await request.json()
    rows_to_inesrt = []
    for log_entry in body:
        rows_to_inesrt.append(
            dict (
                ch_table=log_entry['table_name'],
                rows=json.dumps(log_entry['rows']),
                format=log_entry.get('format')
            )
        )
        
    conn = psycopg2.connect(
        host=PG_HOST,
        database="logs",
        user="postgres",
        password="password1"
    )
    salesorder_write = """
        INSERT INTO logs (
            ch_table, rows, format
        ) values %s
        RETURNING id
    """
    cursor = conn.cursor()
    execute_values(
        cursor,
        salesorder_write,
        rows_to_inesrt,
        template = """(
            %(ch_table)s,
            %(rows)s,
            %(format)s
        )""",
        page_size = 1000
    )
    ids = cursor.fetchall()
    conn.commit()

    response = []
    for _id in ids:
        response.append(_id[0])

    return response


@app.post('/write_log/status')
async def write_log_status(request: Request):
    body = await request.json()
    conn = psycopg2.connect(
        host=PG_HOST,
        database="logs",
        user="postgres",
        password="password1"
    )
    cur = conn.cursor()
    cur.execute(
            f"""
        SELECT count(*), '' FROM logs WHERE id = {body['op_id']}
        UNION ALL
        SELECT count(*), error FROM errors WHERE id = {body['op_id']} GROUP BY error;
        """
    )
    rows = cur.fetchall()
    if rows[0][0] == 1:
        return {'status': 'processing'}
    elif len(rows) == 2:
        return {'status': 'error', 'error': rows[1][1]}
    else:
        return {'status': 'ok'}


@app.get('/healthcheck')
async def healthcheck():
    return Response(content='Ok', media_type='text/plain')
