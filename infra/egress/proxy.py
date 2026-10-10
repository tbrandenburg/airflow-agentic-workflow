"""Deployment-owned CONNECT-only allowlist; opaque TLS, no request/credential logging."""

import asyncio
import ipaddress
import json
import re
import socket
from pathlib import Path

POLICY = Path(__file__).with_name("policy.json")


def approved(authority: str) -> str:
    match = re.fullmatch(r"([a-z0-9.-]+):443", authority)
    if match is None:
        raise ValueError("Invalid destination")
    host = match[1]
    policy = json.loads(POLICY.read_bytes())
    if host in policy["exact"]:
        return host
    if any(host.endswith("." + suffix) for suffix in policy["subdomains"]):
        if re.fullmatch(r"[a-z0-9]+(?:[a-z0-9.-]*[a-z0-9])?", host):
            return host
    raise ValueError("Destination denied")


async def connect(host: str) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    addresses = await asyncio.get_running_loop().getaddrinfo(host, 443, type=socket.SOCK_STREAM)
    ips = list(dict.fromkeys(entry[4][0] for entry in addresses))
    if not ips or any(not ipaddress.ip_address(ip).is_global for ip in ips):
        raise ValueError("Nonpublic destination denied")
    # Connect to the validated IP, avoiding a second DNS lookup/rebinding window.
    for ip in ips:
        try:
            return await asyncio.wait_for(asyncio.open_connection(ip, 443), 5)
        except OSError:
            continue
    raise ConnectionError("Approved destination unavailable")


async def relay(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    while data := await asyncio.wait_for(reader.read(65536), 45):
        writer.write(data)
        await writer.drain()


class Proxy:
    def __init__(self) -> None:
        self.slots = asyncio.Semaphore(16)
        self.accepted = 0
        self.denied = 0
        self.errors = 0

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        remote: asyncio.StreamWriter | None = None
        established = False
        try:
            async with asyncio.timeout(180), self.slots:
                header = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 5)
                if len(header) > 8192:
                    raise ValueError("Header bound exceeded")
                line = header.split(b"\r\n", 1)[0]
                if line == b"GET /health HTTP/1.1":
                    body = json.dumps(
                        {"accepted": self.accepted, "denied": self.denied, "errors": self.errors}
                    ).encode()
                    writer.write(
                        b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: "
                        + str(len(body)).encode()
                        + b"\r\n\r\n"
                        + body
                    )
                    await writer.drain()
                    return
                parts = line.decode("ascii").split(" ")
                if len(parts) != 3 or parts[0] != "CONNECT" or parts[2] != "HTTP/1.1":
                    raise ValueError("CONNECT required")
                host = approved(parts[1])
                upstream, remote = await connect(host)
                self.accepted += 1
                writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                await writer.drain()
                established = True
                tasks = [
                    asyncio.create_task(relay(reader, remote)),
                    asyncio.create_task(relay(upstream, writer)),
                ]
                try:
                    done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        task.result()
                finally:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
        except (ValueError, UnicodeError, asyncio.LimitOverrunError):
            self.denied += 1
            writer.write(
                b"HTTP/1.1 403 Forbidden\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
            )
        except (OSError, TimeoutError, asyncio.IncompleteReadError):
            self.errors += 1
            if not established:
                writer.write(
                    b"HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
                )
        finally:
            if remote is not None:
                remote.close()
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                self.errors += 1


async def main() -> None:
    proxy = Proxy()
    server = await asyncio.start_server(proxy.handle, "0.0.0.0", 8080, limit=8192)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
