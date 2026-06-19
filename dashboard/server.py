"""
Lightweight dashboard for the agent_metrics output topic.

Browsers can't talk to Kafka directly, so this service:
  * reads agent_metrics from Kafka in the background and keeps recent aggregates in memory;
  * serves them over HTTP at /data (JSON) and the page at / (index.html with a chart).

Run:  python dashboard/server.py          (reads from Kafka)
      python dashboard/server.py --demo   (no Kafka, fills with demo data)

Open: http://localhost:8088
Deps: kafka-python (already installed via generator/requirements.txt), rest is stdlib.
"""

import json
import os
import sys
import threading
from collections import deque
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC = os.environ.get("TOPIC_OUT", "agent_metrics")
PORT = int(os.environ.get("DASHBOARD_PORT", "8088"))
MAXLEN = int(os.environ.get("DASHBOARD_MAXLEN", "5000"))

_lock = threading.Lock()
_data = deque(maxlen=MAXLEN)
_status = {"connected": False, "topic": TOPIC, "broker": BROKER, "demo": False}

HTML_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")


def consume_loop():
    from kafka import KafkaConsumer

    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[BROKER],
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="dashboard",
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                consumer_timeout_ms=0,
            )
            with _lock:
                _status["connected"] = True
            for msg in consumer:
                with _lock:
                    _data.append(msg.value)
        except Exception as exc:  # reconnect if Kafka isn't ready yet
            with _lock:
                _status["connected"] = False
            print(f"[dashboard] kafka error: {exc}; retry in 3s")
            threading.Event().wait(3)


def seed_demo():
    """Fill with demo aggregates so the page can be viewed without Kafka."""
    types = [
        "RAG answer", "Email assistant", "Calendar planner", "Code assistant",
        "Task prioritizer", "Document summarizer",
    ]
    base = {
        "RAG answer": (4000, 2), "Email assistant": (2000, 1),
        "Calendar planner": (1500, 2), "Code assistant": (5500, 5),
        "Task prioritizer": (1800, 3), "Document summarizer": (7000, 1),
    }
    import random

    t0 = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=15)
    for m in range(15):
        ts = (t0 + timedelta(minutes=m)).strftime("%H:%M")
        for name in types:
            lat, tc = base[name]
            _data.append({
                "time": ts,
                "type_name": name,
                "avg_latency_ms": round(lat * random.uniform(0.85, 1.15), 1),
                "median_tool_calls_count": float(max(0, tc + random.randint(-1, 1))),
            })
    _status["demo"] = True
    _status["connected"] = True


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/data"):
            with _lock:
                payload = json.dumps({"status": _status, "rows": list(_data)}).encode("utf-8")
            self._send(200, payload, "application/json")
        elif self.path == "/" or self.path.startswith("/index"):
            with open(HTML_PATH, "rb") as f:
                self._send(200, f.read(), "text/html; charset=utf-8")
        else:
            self._send(404, b"not found", "text/plain")

    def log_message(self, *args):
        pass  # quiet log


def main():
    if "--demo" in sys.argv:
        seed_demo()
        print("[dashboard] DEMO mode (no Kafka)")
    else:
        threading.Thread(target=consume_loop, daemon=True).start()

    print(f"[dashboard] http://localhost:{PORT}  (topic={TOPIC}, broker={BROKER})")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
