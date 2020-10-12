# Copyright 2018 John Reese
# Licensed under the MIT license

import sqlite3
from typing import TYPE_CHECKING, Any, Iterable, Iterator, Optional, Tuple

from .w_thread import execute_blocking

if TYPE_CHECKING:
    from .core import Connection


class Cursor:
    def __init__(self, cursor: sqlite3.Cursor) -> None:
        self._cursor = cursor
        self.__chunk: Optional[Iterator[sqlite3.Row]]
        self.iter_chunk_size = 1024

    def __aiter__(self) -> "Cursor":
        """The cursor proxy is also an async iterator."""
        self.__chunk = None
        return self

    async def __anext__(self) -> sqlite3.Row:
        """Use `cursor.fetchone()` to provide an async iterable."""
        if self.__chunk is None:
            self.__chunk = await self.__fetch_anext_chunk()

        try:
            return self.__chunk.__next__()
        except StopIteration:
            self.__chunk = await self.__fetch_anext_chunk()
            return self.__chunk.__next__()

    async def __fetch_anext_chunk(self) -> Iterator[sqlite3.Row]:
        chunk = await self.fetchmany(self.iter_chunk_size)
        if not chunk:
            raise StopAsyncIteration
        return iter(chunk)

    async def execute(self, sql: str, parameters: Iterable[Any] = None) -> "Cursor":
        """Execute the given query."""
        if parameters is None:
            parameters = []
        await execute_blocking(self._cursor.execute, sql, parameters)
        return self

    async def executemany(
        self, sql: str, parameters: Iterable[Iterable[Any]]
    ) -> "Cursor":
        """Execute the given multiquery."""
        await execute_blocking(self._cursor.executemany, sql, parameters)
        return self

    async def executescript(self, sql_script: str) -> "Cursor":
        """Execute a user script."""
        await execute_blocking(self._cursor.executescript, sql_script)
        return self

    async def fetchone(self) -> Optional[sqlite3.Row]:
        """Fetch a single row."""
        return await execute_blocking(self._cursor.fetchone)

    async def fetchmany(self, size: int = None) -> Iterable[sqlite3.Row]:
        """Fetch up to `cursor.arraysize` number of rows."""
        args: Tuple[int, ...] = ()
        if size is not None:
            args = (size,)
        return await execute_blocking(self._cursor.fetchmany, *args)

    async def fetchall(self) -> Iterable[sqlite3.Row]:
        """Fetch all remaining rows."""
        return await execute_blocking(self._cursor.fetchall)

    async def close(self) -> None:
        """Close the cursor."""
        await execute_blocking(self._cursor.close)

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    @property
    def lastrowid(self) -> int:
        return self._cursor.lastrowid

    @property
    def arraysize(self) -> int:
        return self._cursor.arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        self._cursor.arraysize = value

    @property
    def description(self) -> Tuple[Tuple]:
        return self._cursor.description

    @property
    def connection(self) -> sqlite3.Connection:
        return self._cursor.connection

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
