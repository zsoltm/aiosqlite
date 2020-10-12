import asyncio
import sqlite3
import time
from concurrent.futures.thread import ThreadPoolExecutor

import aiosqlite

# http://2016.padjo.org/files/data/starterpack/publicpay/peninsula_publicpay.sqlite
DBNAME = 'test.db.peninsula_publicpay.sqlite'
QUERY = 'SELECT * FROM salaries'
CHUNK_SIZE = 250


def bench(label, func, db):
    t1 = time.time()
    func(db)
    t2 = time.time()
    print(label, t2 - t1)


async def abench(label, func, db):
    t1 = time.time()
    await func(db)
    t2 = time.time()
    print(label, t2 - t1)


def sqlite_iter(db):
    cursor = db.execute(QUERY)
    for row in cursor:
        pass
    cursor.close()


def sqlite_fetchone(db):
    cursor = db.execute(QUERY)
    while True:
        row = cursor.fetchone()
        if not row:
            break
    cursor.close()


def sqlite_fetchmany(db):
    cursor = db.execute(QUERY)
    while True:
        rows = cursor.fetchmany(CHUNK_SIZE)
        if not rows:
            break
        for row in rows:
            pass
    cursor.close()


async def aio_aiter(db):
    async with db.execute(QUERY) as cursor:
        async for row in cursor:
            pass


async def aio_fetchone(db):
    async with db.execute(QUERY) as cursor:
        while True:
            row = await cursor.fetchone()
            if not row:
                break


async def aio_fetchmany(db):
    async with db.execute(QUERY) as cursor:
        while True:
            rows = await cursor.fetchmany(CHUNK_SIZE)
            if not rows:
                break
            for row in rows:
                pass


async def amain():
    with sqlite3.connect(DBNAME) as db:
        bench('sqlite3 __iter__', sqlite_iter, db)
        bench('sqlite3 fetchone', sqlite_fetchone, db)
        bench('sqlite3 fetchmany', sqlite_fetchmany, db)

    async with aiosqlite.connect(DBNAME) as db:
        await abench('aiosqlite __aiter__', aio_aiter, db)
        await abench('aiosqlite fetchone', aio_fetchone, db)
        await abench('aiosqlite fetchmany', aio_fetchmany, db)


if __name__ == '__main__':
    asyncio.run(amain())
