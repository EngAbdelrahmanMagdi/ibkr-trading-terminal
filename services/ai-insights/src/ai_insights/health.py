import socket
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from typing import Any

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest


class Health:
    def __init__(self, port: int) -> None:
        self.ready = False
        health = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path == "/metrics":
                    body, code, content = generate_latest(), 200, CONTENT_TYPE_LATEST
                elif self.path in {"/liveness", "/readiness"}:
                    code = 200 if self.path == "/liveness" or health.ready else 503
                    body, content = (
                        b'{"status":"UP"}' if code == 200 else b'{"status":"DOWN"}',
                        "application/json",
                    )
                else:
                    body, code, content = b"", 404, "text/plain"
                self.send_response(code)
                self.send_header("Content-Type", content)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: object) -> None:
                return

        class Server(HTTPServer):
            def get_request(self) -> tuple[socket.socket, Any]:
                connection, address = super().get_request()
                connection.settimeout(3)
                return connection, address

        self.server = Server(("0.0.0.0", port), Handler)
        Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
