from __future__ import annotations

import json
import logging
import socket
import time
from collections.abc import Callable
from typing import Any

LOGGER = logging.getLogger(__name__)


class TcpJsonSender:
    def __init__(self, host: str, port: int, reconnect_delay: float = 2.0):
        self.host = host
        self.port = port
        self.reconnect_delay = reconnect_delay
        self._sock: socket.socket | None = None

    def _connect(self) -> socket.socket:
        while True:
            try:
                sock = socket.create_connection((self.host, self.port), timeout=10)
                LOGGER.info("Connected to TCP receiver %s:%s", self.host, self.port)
                return sock
            except OSError as exc:
                LOGGER.warning("TCP connect failed; retrying: %s", exc)
                time.sleep(self.reconnect_delay)

    def send(self, payload: dict[str, Any]) -> None:
        line = (json.dumps(payload) + "\n").encode("utf-8")
        while True:
            if self._sock is None:
                self._sock = self._connect()
            try:
                self._sock.sendall(line)
                return
            except OSError as exc:
                LOGGER.warning("TCP send failed; reconnecting: %s", exc)
                self.close()
                time.sleep(self.reconnect_delay)

    def close(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            finally:
                self._sock = None


def serve_json_lines(host: str, port: int, handler: Callable[[dict[str, Any]], None]) -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(5)
    LOGGER.info("Listening for TCP JSON lines on %s:%s", host, port)
    with server:
        while True:
            conn, addr = server.accept()
            LOGGER.info("Accepted TCP connection from %s", addr)
            with conn:
                buffer = b""
                while True:
                    chunk = conn.recv(65536)
                    if not chunk:
                        break
                    buffer += chunk
                    while b"\n" in buffer:
                        line, buffer = buffer.split(b"\n", 1)
                        if not line.strip():
                            continue
                        try:
                            payload = json.loads(line.decode("utf-8"))
                            handler(payload)
                        except Exception as exc:
                            LOGGER.exception("Skipping malformed TCP payload: %s", exc)
