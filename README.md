# Telemetron

Streaming telemetry for AI agents — **Kafka + Flink + Postgres**.

Imagine a fleet of AI agents (RAG answers, an email assistant, a code assistant, and
so on) where every processed request is an event. Events stream into Kafka, Flink
enriches each one on the fly with the agent type name from a Postgres dimension table,
and once per minute it computes two metrics per type: **average response latency** and
**median number of tool calls**. Results are written back to Kafka, and a small live
dashboard visualizes them.

It's the same shape as the classic IoT homework (device type → metrics), but in a
domain closer to real work with LLMs and agents.

> Built as the capstone project for a Big Data course. Stack and idioms follow the
> course materials: PyFlink, Kafka 3.9, Postgres 14, a Python generator, JSON messages,
> event-time windows.

## How it works

```
generator (1 event/sec)          Flink DataStream API, event time
        │                ┌───────────────────────────────────────────────┐
        ▼                │  read from Kafka                                │
  topic: agent_events ───┼─► parse JSON ─► assign watermark ─► enrich      │
                         │                          from Postgres (lookup)  │
  Postgres ─────────────┼─► agent_types dimension   │                      │
  (type → name)          │     loaded once          ▼                      │
                         │              key_by(type) ─► 1-min window ─►     │
                         │              avg(latency) + median(tool_calls)   │
                         │                          │                       │
                         │                          ▼                       │
                         │                   write to Kafka                 │
                         └──────────────────────► topic: agent_metrics ─────┘
```

## Quick start

You only need Docker (with `docker compose`) and Python 3.

```bash
./run.sh
```

This one command does everything: starts Kafka and Postgres, waits for them to become
healthy, downloads the Flink Kafka connector, creates the topics, builds a virtual
environment, and launches the generator together with the Flink job. The first
per-minute aggregates show up in the output after 1–2 minutes (the window closes on a
watermark). Stop with `Ctrl+C`.

To only prepare the environment without running: `./run.sh --setup`.

Want to watch the output stream separately, in another terminal:

```bash
make consume
```

The same steps are available individually via `make` (see `make help`).

## Dashboard

A live web dashboard for the output metrics:

```bash
make dashboard      # then open http://localhost:8088
```

Browsers can't talk to Kafka directly, so `dashboard/server.py` reads the
`agent_metrics` topic in the background, keeps recent aggregates in memory, and serves
them to a page that refreshes every 3 seconds with two charts (average latency and
median tool calls by agent type) plus a table of the latest values. No dependencies
beyond `kafka-python`.

To preview the dashboard without bringing up the whole pipeline:

```bash
python dashboard/server.py --demo
```

## Reading the data

**Input event** (`agent_events`), message key — `type_id`:

```json
{ "type_id": 7, "event_time": "2026-06-19T03:25:12.413Z", "latency_ms": 2840, "tool_calls_count": 3 }
```

**Output aggregate** (`agent_metrics`) — one record per agent type per minute:

```json
{ "time": "03:25", "type_name": "Task prioritizer", "avg_latency_ms": 3120.5, "median_tool_calls_count": 2.0 }
```

Agent types (the `agent_types` table in Postgres):

| id | type_name | latency_ms | tool_calls_count |
|--:|-----------|-----------:|-----------------:|
| 1 | RAG answer | 1800–6500 | 1–4 |
| 2 | Email assistant | 900–3500 | 0–2 |
| 3 | Calendar planner | 700–2500 | 1–3 |
| 4 | Resume helper | 1500–5000 | 0–2 |
| 5 | Code assistant | 2500–9000 | 2–8 |
| 6 | Telegram channel writer | 1200–4500 | 0–2 |
| 7 | Task prioritizer | 800–3000 | 1–5 |
| 8 | Document summarizer | 3000–12000 | 0–3 |

The ranges on the right are realistic per-event metric values the generator emits for
each type.

## A few design decisions worth knowing

**Event-time processing.** The generator deliberately distorts data the way real
systems do: 85% of events arrive 0–3 seconds late, 10% are 5–20 seconds late, 5% are
30–90 seconds late, and the order is slightly shuffled. Flink relies not on arrival
time but on the `event_time` field plus a watermark with
`bounded_out_of_orderness(90s)`, so late events still land in their correct minute.

**Dimension join — in-memory lookup, not a query per event.** The dimension is small
and static (8 rows), so Flink loads it once when the operator starts (in `open()` via
`psycopg2`) and keeps it in a dict. That's faster and simpler than a JDBC lookup per
record. If the dimension ever becomes mutable, the logic moves to a broadcast stream or
CDC.

**Exact median.** At one event per second there aren't many values per minute, so the
median is computed exactly (buffer the window, sort, take the middle). For large
volumes an approximate percentile (t-digest) would fit here, but the exact median is
clearer for a learning project.

**Reproducibility.** The whole infrastructure comes up from a single
`docker-compose.yml`, and the dimension is seeded automatically on first Postgres start.
No manual Kafka/Postgres installs.

## Project layout

```
telemetron/
├── run.sh                  run everything with one command
├── docker-compose.yml      Kafka 3.9 (KRaft) + Postgres 14
├── Makefile                individual steps (make help)
├── .env.example            settings (copy to .env)
├── sql/
│   ├── ddl.sql             agent_types table
│   └── dml.sql             dimension seed
├── generator/
│   └── agent_generator.py  event generator
├── flink/
│   ├── job.py              Flink job (DataStream API)
│   ├── metrics.py          pure aggregation logic (unit-tested)
│   └── jars/               the Kafka connector lands here
├── dashboard/
│   ├── server.py           consumer + HTTP metrics server
│   └── index.html          web page with charts (Chart.js)
├── tests/
│   └── test_metrics.py     unit tests for avg/median and time labels
└── scripts/                helper scripts (topics, seed, consume, jars)
```

## Configuration

Everything is in `.env` (created from `.env.example`): Kafka address, topic names,
Postgres credentials, generation rate, window size, and the watermark out-of-orderness
bound. `GEN_DRY_RUN=1` makes the generator print events to the console instead of
sending them to Kafka — handy for a quick check without any infrastructure.

The A/B metrics (what to average, what to take the median of) are defined in
`generator/agent_generator.py` under `AGENT_PROFILES`. You can change the pair (for
example to `cost_rub` / `output_tokens`) right there without touching the Flink job.

## Tests

```bash
make test
```

These cover the pure aggregation logic in `flink/metrics.py`: average, median (for both
odd and even counts), rounding, and the window time label. They don't require PyFlink —
they run with plain Python.

## Troubleshooting

The `apache-flink` and Kafka connector (`flink-sql-connector-kafka-4.0.1-2.0.jar`)
versions come from the course materials (Flink 2.0). If your course build pins
different ones, update `flink/requirements.txt` and `scripts/download_jars.sh`;
depending on the PyFlink version, the connector import names in `flink/job.py` may
differ slightly.

`run.sh` waits for the containers' own healthchecks. If startup ever stalls, check
status with `docker compose ps` and logs with `docker compose logs --tail=40 kafka`.

Empty dimension in Postgres? The volume already existed on first start and
auto-seeding didn't run — seed it manually: `make seed`.

Tear down the infrastructure and delete data: `docker compose down -v`.

## Course mapping

Homework L04 (source → Kafka, window functions) and L05 (stream join, write results)
are combined here into one end-to-end pipeline from the capstone slide — only the domain
is AI-agent monitoring instead of IoT telemetry.
