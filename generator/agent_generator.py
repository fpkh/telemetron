"""
AI agent telemetry generator.

Every 1/GEN_RATE_PER_SEC seconds it publishes an "agent processed a request" event
to Kafka:
    { type_id, event_time, latency_ms, tool_calls_count }

To exercise Flink's event-time processing the generator deliberately distorts data:
  * event_time is shifted into the past relative to "now":
        85% by 0..3 s, 10% by 5..20 s, 5% by 30..90 s;
  * the send order is slightly shuffled, so events do NOT arrive in event_time order
    — exactly what watermarks / bounded out-of-orderness are meant to handle.

Per-type metric ranges live in AGENT_PROFILES. The "metric A / metric B" pair
(latency_ms = average, tool_calls_count = median) is set by the profile and is easy
to swap.
"""

import json
import os
import random
import time
from collections import deque
from datetime import datetime, timezone

# Agent profiles: per-event metric ranges.
#   latency_ms       -> metric A (Flink computes its AVERAGE per minute)
#   tool_calls       -> metric B (Flink computes its MEDIAN per minute)
AGENT_PROFILES = {
    1: {"name": "RAG answer",              "latency_ms": (1800, 6500),  "tool_calls": (1, 4)},
    2: {"name": "Email assistant",         "latency_ms": (900, 3500),   "tool_calls": (0, 2)},
    3: {"name": "Calendar planner",        "latency_ms": (700, 2500),   "tool_calls": (1, 3)},
    4: {"name": "Resume helper",           "latency_ms": (1500, 5000),  "tool_calls": (0, 2)},
    5: {"name": "Code assistant",          "latency_ms": (2500, 9000),  "tool_calls": (2, 8)},
    6: {"name": "Telegram channel writer", "latency_ms": (1200, 4500),  "tool_calls": (0, 2)},
    7: {"name": "Task prioritizer",        "latency_ms": (800, 3000),   "tool_calls": (1, 5)},
    8: {"name": "Document summarizer",     "latency_ms": (3000, 12000), "tool_calls": (0, 3)},
}


def env(key, default):
    return os.environ.get(key, default)


def sample_lateness_sec():
    """How far back to shift event_time from 'now', in seconds (85/10/5 model)."""
    r = random.random()
    if r < 0.85:
        return random.uniform(0, 3)
    if r < 0.95:
        return random.uniform(5, 20)
    return random.uniform(30, 90)


def make_event(now_epoch):
    type_id = random.choice(list(AGENT_PROFILES.keys()))
    p = AGENT_PROFILES[type_id]
    event_epoch = now_epoch - sample_lateness_sec()
    event_time = datetime.fromtimestamp(event_epoch, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%f"
    )[:-3] + "Z"
    return {
        "type_id": type_id,
        "event_time": event_time,
        "latency_ms": random.randint(*p["latency_ms"]),
        "tool_calls_count": random.randint(*p["tool_calls"]),
    }


def build_producer(broker):
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=[broker],
        key_serializer=lambda k: str(k).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def main():
    broker = env("KAFKA_BROKER", "localhost:9092")
    topic = env("TOPIC_IN", "agent_events")
    rate = float(env("GEN_RATE_PER_SEC", "1"))
    dry_run = env("GEN_DRY_RUN", "0") == "1"
    interval = 1.0 / rate if rate > 0 else 1.0

    producer = None if dry_run else build_producer(broker)

    # Small buffer to shuffle send order: collect events and occasionally send a
    # newer one before an older one.
    buf = deque()
    sent = 0
    mode = "DRY-RUN (stdout)" if dry_run else f"kafka://{broker} -> {topic}"
    print(f"[generator] start: {mode}, rate={rate}/s. Press Ctrl+C to stop.")

    try:
        while True:
            buf.append(make_event(time.time()))
            # 30% chance to swap the last two — light out-of-order
            if len(buf) >= 2 and random.random() < 0.3:
                buf[-1], buf[-2] = buf[-2], buf[-1]

            ev = buf.popleft()
            key = ev["type_id"]
            if dry_run:
                print(json.dumps(ev, ensure_ascii=False))
            else:
                producer.send(topic, key=key, value=ev)

            sent += 1
            if sent % 20 == 0 and not dry_run:
                producer.flush()
                print(f"[generator] sent {sent} events")
            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\n[generator] stop, total sent: {sent}")
    finally:
        if producer is not None:
            producer.flush()
            producer.close()


if __name__ == "__main__":
    main()
