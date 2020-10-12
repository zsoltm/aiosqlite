import asyncio
import logging
from asyncio import Future
from asyncio.events import AbstractEventLoop
from functools import partial
from queue import Empty, SimpleQueue
from threading import Lock, Thread, local as thread_local
from typing import Callable, Optional, Tuple, TypeVar, Final

TaskTuple = Tuple[Callable, AbstractEventLoop, Future]

_TL = thread_local()
_L: Final[Lock] = Lock()
_LOG = logging.getLogger("aiosqlite.worker")


class _WorkerThread(Thread):
    def __init__(
        self, q: SimpleQueue, lock: Lock, timeout: Optional[float] = None
    ) -> None:
        super().__init__(name="AIOSQLite", daemon=True)
        self.__lock: Final[Lock] = lock
        self.__q = q
        self.__t = timeout

    def run(self) -> None:
        _LOG.debug("Start thread")
        while True:
            try:
                task: Optional[TaskTuple] = self.__q.get(timeout=self.__t)
                if task is None:  # force quit signalled with a None task
                    break
                fn, loop, future = task
                try:
                    _LOG.debug("Executing %r", fn)
                    result = fn()
                    _LOG.debug("Returning %r", result)
                    loop.call_soon_threadsafe(future.set_result, result)
                except BaseException as be:
                    _LOG.error("Exception in worker thread", exc_info=be)
                    loop.call_soon_threadsafe(future.set_exception, be)
            except Empty:
                with self.__lock:
                    if self.__q.empty():
                        break
        _LOG.debug("Stop thread")


class WorkerThreadExecutor:
    def __init__(self, lazy_thread_grace: Optional[float] = None):
        self.__lazy_thread_grace = lazy_thread_grace
        self.__thread: Optional[_WorkerThread] = None
        self.__q: SimpleQueue = SimpleQueue()
        self.__lock: Final[Lock] = Lock()
        if self.__lazy_thread_grace is None:
            self.__ensure_thread_running()
            self.submit = self.__submit
        else:
            self.submit = self.__submit_grace

    def __submit(self, fn: Callable, loop: AbstractEventLoop, future: Future) -> None:
        if self.__lazy_thread_grace is not None:
            with self.__lock:
                self.__ensure_thread_running()
                self.__q.put((fn, loop, future))
        else:
            self.__q.put((fn, loop, future))

    def __submit_grace(
        self, fn: Callable, loop: AbstractEventLoop, future: Future
    ) -> None:
        with self.__lock:
            self.__ensure_thread_running()
            self.__q.put((fn, loop, future))

    def close(self):
        self.__q.put(None)

    def __ensure_thread_running(self):
        if not self.__thread or not self.__thread.is_alive():
            self.__thread = _WorkerThread(self.__q, self.__lock, self.__lazy_thread_grace)
            self.__thread.start()


T = TypeVar('T')
_GE: Optional[WorkerThreadExecutor] = None


def tl_executor():
    with _L:
        executor = getattr(_TL, 'aiosqlite_exec', None)
        if not executor:
            executor = WorkerThreadExecutor()
            setattr(_TL, 'aiosqlite_exec', executor)
    return executor


def g_executor():
    global _GE
    with _L:
        if _GE is None:
            _GE = WorkerThreadExecutor()
    return _GE


get_executor = g_executor


async def execute_blocking(fn: Callable[[], T], *args, **kwargs) -> T:
    """Queue a function with the given arguments for execution."""

    loop = asyncio.get_event_loop()
    future = loop.create_future()
    executor = get_executor()
    executor.submit(partial(fn, *args, **kwargs), loop, future)
    return await future
