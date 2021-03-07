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
CH_USER = os.getenv('LOGBROKER_CH_USER', None)
CH_PASSWORD = os.getenv('LOGBROKER_CH_PASSWORD', None)
CH_PORT = int(os.getenv('LOGBROKER_CH_PORT', 8123))
CH_CERT_PATH = os.getenv('LOGBROKER_CH_CERT_PATH', None)


async def send_csv(table_name, rows):
    url = f'http://{CH_HOST}:{CH_PORT}/'
    params = {
        'query': f'INSERT INTO \"{table_name}\" FORMAT CSV'.strip()
    }

    csv_rows = []
    for row in rows:
        csv_rows.append(','.join(map(str, row)))
    data = '\n'.join(csv_rows)

    async with ClientSession() as session:
        async with session.post(url, params=params, data=data) as resp:
            await resp.read()
            try:
                resp.raise_for_status()
                return resp, None
            except ClientError as e:
                return resp, {'error': str(e)}


async def send_json_each_row(table_name, rows):
    url = f'http://{CH_HOST}:{CH_PORT}/'
    params = {
        'query': f'INSERT INTO \"{table_name}\" FORMAT JSONEachRow'.strip()
    }

    async with ClientSession() as session:
        async with session.post(url, params=params, json=rows) as resp:
            await resp.read()
            try:
                resp.raise_for_status()
                return resp, None
            except ClientError as e:
                return resp, {'error': str(e)}


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
