"""
mock_fakestore_server.py
-------------------------
A tiny local stand-in for https://fakestoreapi.com/products.

Why this exists: fakestoreapi.com sits behind Cloudflare and has started
returning 403 Forbidden to requests coming from shared/cloud IP ranges,
including GitHub Actions runners — even though the same request succeeds
from a normal residential connection. That makes CI's "integration test"
step flaky for reasons entirely outside this repo's control.

This script serves the same JSON shape the real API returns (see
scripts/fixtures/fakestore_products.json) from 127.0.0.1, so CI can point
FAKESTORE_API_URL at it and still exercise the real
extract -> validate -> transform code path end-to-end, without depending
on a third-party service being reachable/whitelisted.

Usage:
    python scripts/mock_fakestore_server.py [port]   # default port 8000

Serves the fixture list at GET /products (and, for convenience, at GET /).
"""

import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "fakestore_products.json"


class FakeStoreHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/products", "/"):
            payload = FIXTURE_PATH.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):  # noqa: A002 - matches BaseHTTPRequestHandler signature
        # Keep CI logs quiet; uncomment for debugging.
        pass


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    server = ThreadingHTTPServer(("127.0.0.1", port), FakeStoreHandler)
    print(f"Mock FakeStore API serving {FIXTURE_PATH.name} on http://127.0.0.1:{port}/products")
    server.serve_forever()


if __name__ == "__main__":
    main()
