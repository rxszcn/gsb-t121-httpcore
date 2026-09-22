"""retries 与退避在直连和代理下的实际次数。

跑法：.venv/bin/python repro/retry-ledger.py
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


class Ledger(httpcore.AsyncMockBackend):
    def __init__(self, fail_first: int) -> None:
        super().__init__(GOOD)
        self.fail_first = fail_first
        self.connects = 0
        self.sleeps: typing.List[float] = []

    async def connect_tcp(self, host, port, timeout=None, local_address=None,
                          socket_options=None):
        self.connects += 1
        if self.connects <= self.fail_first:
            raise httpcore.ConnectError("refused")
        return httpcore.AsyncMockStream(list(self._buffer))

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)


async def case(retries: int, fail_first: int, proxy=None) -> None:
    backend = Ledger(fail_first)
    pool = httpcore.AsyncConnectionPool(
        network_backend=backend, retries=retries, proxy=proxy
    )
    async with pool:
        try:
            response = await pool.request("GET", "https://example.com/")
            outcome = f"status={response.status}"
        except BaseException as exc:  # noqa: BLE001
            outcome = f"{type(exc).__name__}: {exc}"
    print(
        f"retries={retries} failures={fail_first} "
        f"proxy={'-' if proxy is None else proxy.url.scheme.decode()} -> {outcome}"
        f" | attempts={backend.connects} backoff={backend.sleeps}"
    )


async def main() -> None:
    await case(retries=0, fail_first=0)
    await case(retries=1, fail_first=1)
    await case(retries=3, fail_first=3)
    await case(retries=3, fail_first=4)
    await case(retries=3, fail_first=1,
               proxy=httpcore.Proxy("http://localhost:8080/"))


anyio.run(main)
