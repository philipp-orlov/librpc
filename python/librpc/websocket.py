# Copyright (c) 2026 Philipp Orlov

"""A minimal WebSocket client (RFC 6455) over asyncio streams: binary messages only, which is all librpc speaks.
No dependencies. `ws://` and `wss://` (pass an ssl.SSLContext, or the default context is used)."""

import asyncio
import base64
import hashlib
import os
import ssl as _ssl
import struct
from urllib.parse import urlsplit

_GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class HandshakeError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


class WebSocket:
    def __init__(self, reader, writer, max_message):
        self._reader = reader
        self._writer = writer
        self._max_message = max_message
        self.close_code = None
        self.close_reason = ""
        self.closed = False

    @classmethod
    async def connect(cls, url, subprotocol, headers=None, ssl=None, max_message=16 << 20):
        parts = urlsplit(url)
        if parts.scheme not in ("ws", "wss"):
            raise ValueError(f"not a WebSocket URL: {url}")
        secure = parts.scheme == "wss"
        host = parts.hostname
        port = parts.port or (443 if secure else 80)
        context = (ssl if ssl is not None else _ssl.create_default_context()) if secure else None
        reader, writer = await asyncio.open_connection(host, port, ssl=context, limit=max(1 << 16, max_message))
        key = base64.b64encode(os.urandom(16))
        target = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
        lines = [f"GET {target} HTTP/1.1", f"Host: {parts.netloc}", "Upgrade: websocket", "Connection: Upgrade",
                 f"Sec-WebSocket-Key: {key.decode()}", "Sec-WebSocket-Version: 13", f"Sec-WebSocket-Protocol: {subprotocol}"]
        for name, value in (headers or {}).items():
            lines.append(f"{name}: {value}")
        writer.write(("\r\n".join(lines) + "\r\n\r\n").encode())
        await writer.drain()
        try:
            head = await reader.readuntil(b"\r\n\r\n")
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError) as e:
            writer.close()
            raise HandshakeError(0, "the server closed the connection during the handshake") from e
        text = head.decode("latin-1").split("\r\n")
        status = int(text[0].split(" ", 2)[1]) if text and text[0].startswith("HTTP/") else 0
        response = {}
        for line in text[1:]:
            if ":" in line:
                name, value = line.split(":", 1)
                response[name.strip().lower()] = value.strip()
        expected = base64.b64encode(hashlib.sha1(key + _GUID).digest()).decode()
        if status != 101 or response.get("sec-websocket-accept") != expected:
            writer.close()
            raise HandshakeError(status, f"the WebSocket handshake failed (HTTP {status})")
        return cls(reader, writer, max_message)

    def _frame(self, opcode, payload):
        mask = os.urandom(4)
        n = len(payload)
        header = bytes([0x80 | opcode])
        if n < 126:
            header += bytes([0x80 | n])
        elif n < 1 << 16:
            header += bytes([0x80 | 126]) + struct.pack(">H", n)
        else:
            header += bytes([0x80 | 127]) + struct.pack(">Q", n)
        # XOR with the repeating mask at C speed: big-int arithmetic on the whole payload
        if n:
            stream = (mask * (n // 4 + 1))[:n]
            payload = (int.from_bytes(payload, "big") ^ int.from_bytes(stream, "big")).to_bytes(n, "big")
        return header + mask + payload

    def send_nowait(self, data):
        """Queues one binary message. Call drain() to wait for the transport to take it."""
        if self.closed:
            raise ConnectionError("the WebSocket is closed")
        self._writer.write(self._frame(0x2, bytes(data)))

    async def drain(self):
        await self._writer.drain()

    async def recv(self):
        """The next binary message, or None when the connection closed."""
        message = bytearray()
        while True:
            try:
                head = await self._reader.readexactly(2)
                opcode = head[0] & 0x0F
                final = bool(head[0] & 0x80)
                n = head[1] & 0x7F
                if n == 126:
                    n = struct.unpack(">H", await self._reader.readexactly(2))[0]
                elif n == 127:
                    n = struct.unpack(">Q", await self._reader.readexactly(8))[0]
                if head[1] & 0x80:  # servers do not mask, but be liberal
                    mask = await self._reader.readexactly(4)
                    payload = bytearray(await self._reader.readexactly(n))
                    for i in range(n):
                        payload[i] ^= mask[i & 3]
                    payload = bytes(payload)
                else:
                    if n > self._max_message + (1 << 16):
                        await self.close(1009, "message too big")
                        return None
                    payload = await self._reader.readexactly(n)
            except (asyncio.IncompleteReadError, ConnectionError, OSError):
                self.closed = True
                return None
            if opcode == 0x8:
                self.closed = True
                if len(payload) >= 2:
                    self.close_code = struct.unpack(">H", payload[:2])[0]
                    self.close_reason = payload[2:].decode("utf-8", "replace")
                try:
                    self._writer.write(self._frame(0x8, payload[:2]))
                    self._writer.close()
                except Exception:
                    pass
                return None
            if opcode == 0x9:
                try:
                    self._writer.write(self._frame(0xA, payload))
                except Exception:
                    pass
                continue
            if opcode == 0xA:
                continue
            if opcode == 0x1:
                await self.close(1003, "text frames are not supported")
                return None
            message += payload
            if len(message) > self._max_message + (1 << 16):
                await self.close(1009, "message too big")
                return None
            if final:
                return bytes(message)

    async def close(self, code=1000, reason=""):
        if self.closed and self._writer.is_closing():
            return
        self.closed = True
        try:
            self._writer.write(self._frame(0x8, struct.pack(">H", code) + reason.encode()))
            await self._writer.drain()
        except Exception:
            pass
        try:
            self._writer.close()
            await self._writer.wait_closed()
        except Exception:
            pass
