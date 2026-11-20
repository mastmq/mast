"""The policy service the conformance run points mast at.

usage: authz.py [port]

The Paho suite's subscribe-failure test needs a broker configured to refuse
one topic filter, test/nosubscribe, and expects SUBACK reason 0x80 for it.
mast has no static ACL: what a client may use is the policy service's call,
so this stands in for one. Every connection is admitted into the default
tenant and every topic is allowed except that one, which also exercises the
http backend end to end on every run rather than only in unit tests.
"""

import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DENIED = "test/nosubscribe"


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path == "/authn":
            reply = {"allow": True, "tenant": "default"}
        else:
            reply = {"allow": body.get("topic") != DENIED}
        data = json.dumps(reply).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format: str, *args: object) -> None:
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
