import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("webhook-receiver")


class WebhookHandler(BaseHTTPRequestHandler):
    def _respond(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            body = raw.decode("utf-8", errors="replace")

        if self.path == "/webhook/ok":
            log.info("webhook received: body=%s", json.dumps(body, ensure_ascii=False))
            self._respond(200, {"received": True})
        elif self.path == "/webhook/fail":
            log.info(
                "webhook FAIL requested (returning 500): body=%s",
                json.dumps(body, ensure_ascii=False),
            )
            self._respond(500, {"error": "intentional failure for retry/DLQ demo"})
        else:
            self._respond(404, {"error": "unknown path"})

    def do_GET(self) -> None:
        self._respond(200, {"status": "ok"})

    def log_message(self, format: str, *args) -> None:  # noqa: A002
        # Стандартные access-логи отключены: payload логируем сами.
        return


if __name__ == "__main__":
    log.info("webhook receiver listening on :8000")
    ThreadingHTTPServer(("0.0.0.0", 8000), WebhookHandler).serve_forever()
