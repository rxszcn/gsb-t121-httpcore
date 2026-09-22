"""三条代理路径下，后端 connect_tcp 收到哪些参数、被调用几次。

跑法：.venv/bin/python repro/proxy-options.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import os
import typing

import anyio
import httpcore


GOOD = [
    b"HTTP/1.1 200 OK\r\n",
    b"Content-Type: plain/text\r\n",
    b"Content-Length: 13\r\n",
    b"\r\n",
    b"Hello, world!",
]
BUFFER = GOOD * 3


class FlakyBackend(httpcore.AsyncMockBackend):
    """Fails the first `fail_first` connect_tcp calls, then succeeds."""

    def __init__(self, fail_first: int = 2) -> None:
        super().__init__(BUFFER)
        self.calls: typing.List[dict] = []
        self.fail_first = fail_first

    async def connect_tcp(self, host, port, timeout=None, local_address=None,
                          socket_options=None):
        self.calls.append(
            {"host": host, "port": port, "local_address": local_address,
             "socket_options": None if socket_options is None else list(socket_options)}
        )
        if len(self.calls) <= self.fail_first:
            raise httpcore.ConnectError("nope")
        return httpcore.AsyncMockStream(list(self._buffer), http2=self._http2)


SOCKET_OPTIONS = [(6, 1, 1)]


async def run(label: str, url: str,
              proxy: typing.Optional[httpcore.Proxy]) -> None:
    backend = FlakyBackend(fail_first=2)
    pool = httpcore.AsyncConnectionPool(
        network_backend=backend,
        proxy=proxy,
        retries=3,
        local_address="127.0.0.1",
        socket_options=SOCKET_OPTIONS,
    )
    async with pool:
        try:
            response = await pool.request("GET", url)
            outcome = f"status={response.status}"
        except BaseException as exc:  # noqa: BLE001
            outcome = f"{type(exc).__name__}: {exc}"
    print(f"{label} {url}: {outcome} | connect_tcp calls={len(backend.calls)}")
    for call in backend.calls:
        print("   ", call)


async def main() -> None:
    proxy = httpcore.Proxy("http://127.0.0.1:8080")
    await run("direct ", "http://example.com/", None)
    await run("forward", "http://example.com/", proxy)
    await run("tunnel ", "https://example.com/", proxy)


anyio.run(main)
