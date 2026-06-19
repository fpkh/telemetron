"""
Flink DataStream API (PyFlink), event-time processing.

Pipeline:
    KafkaSource(agent_events)
        -> parse JSON
        -> assign event-time watermarks (bounded out-of-orderness)
        -> enrich: join with the static agent_types dimension from Postgres
                   (loaded once in open(), in-memory lookup)
        -> key_by(type_id)
        -> window(Tumbling, 1 minute, event time)
        -> ProcessWindowFunction: avg(latency_ms), median(tool_calls_count)
        -> KafkaSink(agent_metrics)

Runs as a plain Python process — PyFlink spins up an embedded mini-cluster
(like in the course lectures). Flink Web UI: http://localhost:8081
"""

import json
import os

from metrics import compute_window_metrics, iso_to_epoch_ms

from pyflink.common import Configuration, Duration, Types, WatermarkStrategy, Time, Row
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee,
)
from pyflink.datastream.functions import (
    MapFunction,
    FilterFunction,
    ProcessWindowFunction,
    RuntimeContext,
)
from pyflink.datastream.window import TumblingEventTimeWindows


def env(key, default):
    return os.environ.get(key, default)


BROKER = env("KAFKA_BROKER", "localhost:9092")
TOPIC_IN = env("TOPIC_IN", "agent_events")
TOPIC_OUT = env("TOPIC_OUT", "agent_metrics")
WINDOW_MINUTES = int(env("WINDOW_MINUTES", "1"))
MAX_OOO_SEC = int(env("MAX_OUT_OF_ORDERNESS_SEC", "90"))
KAFKA_JAR = env("FLINK_KAFKA_JAR", "flink/jars/flink-sql-connector-kafka-4.0.1-2.0.jar")

PG = dict(
    host=env("PG_HOST", "localhost"),
    port=env("PG_PORT", "5432"),
    dbname=env("PG_DB", "agentsdb"),
    user=env("PG_USER", "postgres"),
    password=env("PG_PASSWORD", "postgres"),
)

# Row type after parsing: type_id, event_ts(ms), latency_ms, tool_calls
PARSED_TYPE = Types.ROW_NAMED(
    ["type_id", "event_ts", "latency_ms", "tool_calls"],
    [Types.INT(), Types.LONG(), Types.INT(), Types.INT()],
)
# Row type after enrichment with the type name from the dimension
ENRICHED_TYPE = Types.ROW_NAMED(
    ["type_id", "type_name", "event_ts", "latency_ms", "tool_calls"],
    [Types.INT(), Types.STRING(), Types.LONG(), Types.INT(), Types.INT()],
)


class ParseJson(MapFunction):
    def map(self, value):
        try:
            d = json.loads(value)
            # Must be a Row (not a tuple): output_type is ROW_NAMED, and PyFlink's
            # row coder calls get_fields_by_names() on the emitted value.
            return Row(
                int(d["type_id"]),
                iso_to_epoch_ms(d["event_time"]),
                int(d["latency_ms"]),
                int(d["tool_calls_count"]),
            )
        except Exception:
            # mark malformed messages with type_id=-1, filtered out below
            return Row(-1, 0, -1, -1)


class EventTimeAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value[1]  # event_ts (ms)


class DropInvalid(FilterFunction):
    def filter(self, value):
        # drop garbage and non-positive latency
        return value[0] > 0 and value[2] > 0


class EnrichWithAgentType(MapFunction):
    """Join with the dimension: load agent_types once in open() into a dict."""

    def open(self, runtime_context: RuntimeContext):
        import psycopg2

        conn = psycopg2.connect(**PG)
        cur = conn.cursor()
        cur.execute("SELECT id, type_name FROM agent_types;")
        self.lookup = {row[0]: row[1] for row in cur.fetchall()}
        cur.close()
        conn.close()

    def map(self, value):
        type_id, event_ts, latency_ms, tool_calls = value
        type_name = self.lookup.get(type_id)  # None -> filtered out
        return Row(type_id, type_name, event_ts, latency_ms, tool_calls)


class DropUnknownType(FilterFunction):
    def filter(self, value):
        return value[1] is not None


class AggregateWindow(ProcessWindowFunction):
    """avg(latency_ms) and exact median(tool_calls) per window."""

    def process(self, key, context, elements):
        latencies = []
        tool_calls = []
        type_name = None
        for e in elements:
            type_name = e[1]
            latencies.append(e[3])
            tool_calls.append(e[4])

        metrics = compute_window_metrics(
            type_name, latencies, tool_calls, context.window().start
        )
        yield json.dumps(metrics, ensure_ascii=False)


def build_source():
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(BROKER)
        .set_topics(TOPIC_IN)
        .set_group_id("agent-metrics-job")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def build_sink():
    record_serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(TOPIC_OUT)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(BROKER)
        .set_record_serializer(record_serializer)
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


def main():
    conf = Configuration()
    conf.set_integer("rest.port", int(env("FLINK_REST_PORT", "8081")))
    e = StreamExecutionEnvironment.get_execution_environment(conf)

    jar_path = os.path.abspath(KAFKA_JAR)
    e.add_jars(f"file://{jar_path}")

    # Parallelism 1: at ~1 event/sec spread over 24 partitions, parallel subtasks
    # go idle and the keyed-window watermark (the MIN across inputs) freezes, so
    # windows never fire. Single parallelism keeps the watermark advancing. The
    # idleness guard is belt-and-suspenders for momentary gaps in the stream.
    e.set_parallelism(int(env("FLINK_PARALLELISM", "1")))

    watermark = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(MAX_OOO_SEC))
        .with_idleness(Duration.of_seconds(5))
        .with_timestamp_assigner(EventTimeAssigner())
    )

    raw = e.from_source(build_source(), WatermarkStrategy.no_watermarks(), "kafka-source")

    parsed = (
        raw.map(ParseJson(), output_type=PARSED_TYPE)
        .filter(DropInvalid())
        .assign_timestamps_and_watermarks(watermark)
    )

    enriched = (
        parsed.map(EnrichWithAgentType(), output_type=ENRICHED_TYPE)
        .filter(DropUnknownType())
    )

    result = (
        enriched.key_by(lambda row: row[0])
        .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_MINUTES)))
        .process(AggregateWindow(), output_type=Types.STRING())
    )

    result.sink_to(build_sink())
    result.print()  # also echo to the console for visibility

    e.execute("agent-telemetry-metrics")


if __name__ == "__main__":
    main()
