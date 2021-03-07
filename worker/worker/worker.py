import csv
import json
import sys
import os
import asyncio
import ssl
import time
import psycopg2
from io import StringIO
from psycopg2.extras import execute_values

from aiohttp.client import ClientSession
from aiohttp.client_exceptions import ClientError
from fastapi import FastAPI, Request, Response

CH_HOST = os.getenv('LOGBROKER_CH_HOST', 'localhost')
PG_HOST = os.getenv('LOGBROKER_PG_HOST', 'localhost')
CH_USER = os.getenv('LOGBROKER_CH_USER', None)
CH_PASSWORD = os.getenv('LOGBROKER_CH_PASSWORD', None)
CH_PORT = int(os.getenv('LOGBROKER_CH_PORT', 8123))
CH_CERT_PATH = os.getenv('LOGBROKER_CH_CERT_PATH', None)


async def execute_query(query, data=None, json=None):
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
        async with session.post(url, params=params, data=data, json=json, headers=headers, ssl=ssl_context) as resp:
            await resp.read()
            try:
                resp.raise_for_status()
                return resp, None
            except ClientError as e:
                return resp, str(e)


async def query_wrapper(query, data=None, json=None):
    res, err = await execute_query(query, data, json)
    return err


async def send_csv(table_name, rows):
    csv_rows = []
    for row in rows:
        csv_rows.append(','.join(map(str, row)))
    data = '\n'.join(csv_rows)

    return await query_wrapper(f'INSERT INTO \"{table_name}\" FORMAT CSV', data=data)


async def send_json_each_row(table_name, rows):
    return await query_wrapper(f'INSERT INTO \"{table_name}\" FORMAT JSONEachRow', json=rows)


async def process(rows):
    errors = []
    for row in rows:
        error = None
        table_name = row[1]
        ch_rows = row[2]
        if row[3] == 'list':
            error = await send_csv(table_name, ch_rows)
        elif row[3] == 'json':
            error = await send_json_each_row(table_name, ch_rows)
        else:
            error = f'unknown format {row[3]}, you must use list or json'

        if error:
            errors.append(dict(id=row[0], error=error))
    return errors


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

        errors = await process(rows)

        if errors:
            salesorder_write = """
                INSERT INTO errors (
                    id, error
                ) values %s
            """
            cursor = conn.cursor()
            execute_values(
                cursor,
                salesorder_write,
                errors,
                template = """(
                    %(id)s,
                    %(error)s
                )""",
                page_size=1000
            )
            conn.commit()

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
