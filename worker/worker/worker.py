import csv
import json
import sys
import os
import asyncio
import ssl
import time
import psycopg2
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

async def query_wrapper(query, data=None):
    res, err = await execute_query(query, data)
    if err is not None:
        return err
    return await res.text()


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


async def process(rows):
    res = []
    for row in rows:
        table_name = row[1]
        ch_rows = row[2]
        if row[3] == 'list':
            res.append(await send_csv(table_name, ch_rows))
        elif row[3] == 'json':
            res.append(await send_json_each_row(table_name, ch_rows))
        else:
            res.append({'error': f'unknown format {row[3]}, you must use list or json'})
    return res


async def main():
    conn = psycopg2.connect(
        host=PG_HOST,
        database="logs",
        user="postgres",
        password="password1"
    )
    cur = conn.cursor()

    CHUNK_SIZE = 100
    SLEEP_TIME = 1

    while True:
        cur.execute(
            f"""
        SELECT id, ch_table, rows, format
        FROM logs
        ORDER BY id
        LIMIT {CHUNK_SIZE};
            """
        )
        rows = cur.fetchall()

        res = await process(rows)
        print(res)

        ids = []
        for row in rows:
            ids.append(row[0])
        cur = conn.cursor()
        cur.execute(
            """
        DELETE FROM logs
        WHERE id = ANY(%s)
            """, (ids,)
        )
        conn.commit()

        sys.stdout.flush()
        time.sleep(SLEEP_TIME)
    
loop = asyncio.get_event_loop()
loop.run_until_complete(main())
