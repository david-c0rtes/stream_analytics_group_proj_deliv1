from __future__ import annotations

import json
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Windows: set HADOOP_HOME so Spark can find winutils.exe + hadoop.dll ──────
import sys
if sys.platform == "win32":
    _hadoop = Path("C:/hadoop")
    if not (_hadoop / "bin" / "winutils.exe").exists():
        print(
            "ERROR: winutils.exe not found at C:/hadoop/bin/winutils.exe\n"
            "Install Hadoop winutils at C:/hadoop/bin before starting Spark."
        )
        sys.exit(1)
    _hadoop_bin = str(_hadoop / "bin")
    os.environ["HADOOP_HOME"]      = str(_hadoop)
    os.environ["hadoop.home.dir"]  = str(_hadoop)
    # Prepend to PATH so Java's native loader finds hadoop.dll
    os.environ["PATH"] = _hadoop_bin + os.pathsep + os.environ.get("PATH", "")
    # Python 3.8+ Windows DLL search path
    try:
        os.add_dll_directory(_hadoop_bin)
    except AttributeError:
        pass

# ── Credentials ───────────────────────────────────────────────────────────────
EH_CONN_STR  = os.getenv("EVENTHUB_CONN_STR", "").strip()
ORDERS_HUB   = os.getenv("EVENTHUB_ORDERS",   "").strip()
COURIERS_HUB = os.getenv("EVENTHUB_COURIERS", "").strip()
STORAGE_STR  = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "").strip()
CONTAINER    = os.getenv("AZURE_STORAGE_CONTAINER", "dashboard-data").strip()

if not all([EH_CONN_STR, ORDERS_HUB, COURIERS_HUB, STORAGE_STR]):
    raise EnvironmentError(
        "Missing credentials — check .env has EVENTHUB_CONN_STR, "
        "EVENTHUB_ORDERS, EVENTHUB_COURIERS, AZURE_STORAGE_CONNECTION_STRING"
    )

# ── Event Hub Kafka endpoint ───────────────────────────────────────────────────
# Event Hub exposes a Kafka-compatible endpoint on port 9093
# Extract namespace from the connection string
_NAMESPACE = EH_CONN_STR.split("//")[1].split(".")[0]  # e.g. iesstsabdbaa-grp-06-10
KAFKA_BOOTSTRAP = f"{_NAMESPACE}.servicebus.windows.net:9093"
KAFKA_SASL = (
    'org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="$ConnectionString" password="{EH_CONN_STR}";'
)

# ── Blob Storage ───────────────────────────────────────────────────────────────
SCHEMA_DIR = Path(__file__).parent / "src" / "delivery_simulation" / "schemas"


def _parse_storage_account(conn_str: str) -> tuple[str, str]:
    """
    Parse account name/key from Azure Storage connection string.
    Expected keys: AccountName, AccountKey.
    """
    parts = {}
    for token in conn_str.split(";"):
        token = token.strip()
        if not token or "=" not in token:
            continue
        k, v = token.split("=", 1)
        parts[k.strip()] = v.strip()
    account = parts.get("AccountName", "")
    key = parts.get("AccountKey", "")
    if not account or not key:
        raise EnvironmentError(
            "AZURE_STORAGE_CONNECTION_STRING must include AccountName and AccountKey "
            "for Spark native Blob sink (wasbs)."
        )
    return account, key


# ── Event schemas ─────────────────────────────────────────────────────────────
def _order_schema():
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, LongType, BooleanType
    )
    return StructType([
        StructField("event_id",                StringType(),  True),
        StructField("event_type",              StringType(),  True),
        StructField("event_version",           StringType(),  True),
        StructField("event_time",              StringType(),  True),
        StructField("ingestion_time",          StringType(),  True),
        StructField("order_id",                StringType(),  True),
        StructField("user_id",                 StringType(),  True),
        StructField("restaurant_id",           StringType(),  True),
        StructField("courier_id",              StringType(),  True),
        StructField("user_zone_id",            StringType(),  True),
        StructField("restaurant_zone_id",      StringType(),  True),
        StructField("estimated_prep_time",     DoubleType(),  True),
        StructField("actual_prep_time",        DoubleType(),  True),
        StructField("estimated_delivery_time", DoubleType(),  True),
        StructField("actual_delivery_time",    DoubleType(),  True),
        StructField("promo_applied",           StringType(),  True),
        StructField("restaurant_category",     StringType(),  True),
        StructField("match_day_flag",          BooleanType(), True),
        StructField("holiday_flag",            BooleanType(), True),
        StructField("anomaly_flag",            BooleanType(), True),
        StructField("is_late_event",           BooleanType(), True),
        StructField("event_delay_seconds",     DoubleType(),  True),
        StructField("event_time_epoch_ms",     LongType(),    True),
        StructField("ingestion_time_epoch_ms", LongType(),    True),
    ])


def _courier_schema():
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, LongType, BooleanType
    )
    return StructType([
        StructField("event_id",                StringType(),  True),
        StructField("event_type",              StringType(),  True),
        StructField("event_version",           StringType(),  True),
        StructField("event_time",              StringType(),  True),
        StructField("ingestion_time",          StringType(),  True),
        StructField("courier_id",              StringType(),  True),
        StructField("courier_zone_id",         StringType(),  True),
        StructField("order_id",                StringType(),  True),
        StructField("restaurant_id",           StringType(),  True),
        StructField("restaurant_zone_id",      StringType(),  True),
        StructField("user_zone_id",            StringType(),  True),
        StructField("shift_id",                StringType(),  True),
        StructField("courier_status",          StringType(),  True),
        StructField("zone_hops_to_restaurant", DoubleType(),  True),
        StructField("zone_hops_to_user",       DoubleType(),  True),
        StructField("estimated_travel_time",   DoubleType(),  True),
        StructField("actual_travel_time",      DoubleType(),  True),
        StructField("offline_reason",          StringType(),  True),
        StructField("is_late_event",           BooleanType(), True),
        StructField("event_delay_seconds",     DoubleType(),  True),
        StructField("event_time_epoch_ms",     LongType(),    True),
        StructField("ingestion_time_epoch_ms", LongType(),    True),
    ])


def _load_avro_schema(schema_file: str) -> str:
    with open(SCHEMA_DIR / schema_file, "r", encoding="utf-8") as fh:
        return json.dumps(json.load(fh))


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql.avro.functions import from_avro
    from pyspark.sql.functions import (
        avg,
        coalesce,
        col,
        count,
        from_json,
        from_unixtime,
        lit,
        to_timestamp,
        to_utc_timestamp,
        when,
        window,
    )

    account_name, account_key = _parse_storage_account(STORAGE_STR)
    output_base = f"wasbs://{CONTAINER}@{account_name}.blob.core.windows.net/stream-output"
    checkpoint_base = f"wasbs://{CONTAINER}@{account_name}.blob.core.windows.net/checkpoint"

    _java_lib = "C:/hadoop/bin" if sys.platform == "win32" else ""
    builder = (
        SparkSession.builder
        .appName("MadridDeliveryStreaming")
        .master("local[2]")   # 2 local threads — one per hub
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.apache.spark:spark-avro_2.12:3.5.1,"
            "org.apache.hadoop:hadoop-azure:3.3.4,"
            "com.microsoft.azure:azure-storage:8.6.6",
        )
        .config(f"fs.azure.account.key.{account_name}.blob.core.windows.net", account_key)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
    )
    if _java_lib:
        builder = builder.config(
            "spark.driver.extraJavaOptions",
            f"-Djava.library.path={_java_lib}",
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("  Madrid Delivery — Spark Structured Streaming")
    print(f"  Event Hub  : {KAFKA_BOOTSTRAP}")
    print(f"  Blob output: container '{CONTAINER}'")
    print(f"  Blob sink  : {output_base}")
    print("  Decode mode: AVRO (preferred) with JSON fallback")
    print("=" * 60)

    # Common Kafka options for Event Hub
    kafka_opts = {
        "kafka.bootstrap.servers":        KAFKA_BOOTSTRAP,
        "kafka.security.protocol":        "SASL_SSL",
        "kafka.sasl.mechanism":           "PLAIN",
        "kafka.sasl.jaas.config":         KAFKA_SASL,
        "kafka.request.timeout.ms":       "60000",
        "kafka.session.timeout.ms":       "30000",
        "startingOffsets":                "earliest",  # read all history
        "failOnDataLoss":                 "false",
    }

    order_avro_schema = _load_avro_schema("order_lifecycle_schema.avsc")
    courier_avro_schema = _load_avro_schema("courier_operations_schema.avsc")

    def _read_topic(topic: str, schema, avro_schema: str):
        raw = (
            spark.readStream
            .format("kafka")
            .options(**kafka_opts)
            .option("subscribe", topic)
            .load()
        )

        json_df = (
            raw
            .select(from_json(col("value").cast("string"), schema).alias("d"))
            .select("d.*")
            .filter(col("event_id").isNotNull())
        )

        avro_df = (
            raw
            .select(from_avro(col("value"), avro_schema, {"mode": "PERMISSIVE"}).alias("d"))
            .select("d.*")
            .filter(col("event_id").isNotNull())
            .withColumn("event_time_epoch_ms", col("event_time").cast("long"))
            .withColumn("ingestion_time_epoch_ms", col("ingestion_time").cast("long"))
            .withColumn(
                "event_time",
                to_utc_timestamp(from_unixtime(col("event_time_epoch_ms") / 1000.0), "UTC").cast("string"),
            )
            .withColumn(
                "ingestion_time",
                to_utc_timestamp(from_unixtime(col("ingestion_time_epoch_ms") / 1000.0), "UTC").cast("string"),
            )
        )

        def _align_to_schema(df):
            cols = []
            present = set(df.columns)
            for field in schema.fields:
                dt = field.dataType.simpleString()
                if field.name in present:
                    cols.append(col(field.name).cast(dt).alias(field.name))
                else:
                    cols.append(lit(None).cast(dt).alias(field.name))
            return df.select(*cols)

        unified = _align_to_schema(json_df).unionByName(_align_to_schema(avro_df))
        return unified.dropDuplicates(["event_id"])

    orders_stream = _read_topic(ORDERS_HUB, _order_schema(), order_avro_schema)
    couriers_stream = _read_topic(COURIERS_HUB, _courier_schema(), courier_avro_schema)

    orders_event_time = orders_stream.withColumn(
        "event_time_ts", to_timestamp(col("event_time"))
    )
    couriers_event_time = couriers_stream.withColumn(
        "event_time_ts", to_timestamp(col("event_time"))
    )

    # ── UC1a: Tumbling window — 5-min, global order throughput + avg delivery ──
    orders_tumbling_kpi = (
        orders_event_time
        .withWatermark("event_time_ts", "15 minutes")
        .groupBy(window(col("event_time_ts"), "5 minutes"))
        .agg(
            count("*").alias("event_count"),
            count(when(col("event_type") == "order_created", 1)).alias("orders_created"),
            count(when(col("event_type") == "order_delivered", 1)).alias("orders_delivered"),
            avg(col("actual_delivery_time")).alias("avg_delivery_time_min"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("event_count"),
            col("orders_created"),
            col("orders_delivered"),
            col("avg_delivery_time_min"),
        )
    )

    # ── UC1b: Hopping window — 10-min window sliding every 5 min, per zone ─────
    # Overlapping buckets smooth short-lived spikes and reveal zone-level trends
    # that a non-overlapping tumbling window would split across boundaries.
    orders_hopping_kpi = (
        orders_event_time
        .withWatermark("event_time_ts", "20 minutes")
        .groupBy(
            window(col("event_time_ts"), "10 minutes", "5 minutes"),
            col("user_zone_id").alias("zone_id"),
        )
        .agg(
            count("*").alias("event_count"),
            count(when(col("event_type") == "order_created", 1)).alias("orders_created"),
            count(when(col("event_type") == "order_delivered", 1)).alias("orders_delivered"),
            avg(col("actual_delivery_time")).alias("avg_delivery_time_min"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("zone_id"),
            col("event_count"),
            col("orders_created"),
            col("orders_delivered"),
            col("avg_delivery_time_min"),
        )
    )

    # ── UC2: Zone demand-supply health (stateful stream-stream join) ────────────
    # Demand side: orders that have reached ready_for_pickup (waiting for a courier).
    orders_waiting = (
        orders_event_time
        .filter(col("event_type") == "order_ready_for_pickup")
        .withWatermark("event_time_ts", "15 minutes")
        .groupBy(
            window(col("event_time_ts"), "5 minutes"),
            col("restaurant_zone_id").alias("zone_id"),
        )
        .agg(count("*").alias("orders_awaiting_pickup"))
    )

    # Supply side: couriers dispatched (assigned_order) per zone in the same window.
    couriers_dispatched = (
        couriers_event_time
        .filter(col("event_type") == "courier_assigned_order")
        .withWatermark("event_time_ts", "15 minutes")
        .groupBy(
            window(col("event_time_ts"), "5 minutes"),
            col("courier_zone_id").alias("zone_id"),
        )
        .agg(count("*").alias("couriers_dispatched"))
    )

    # Stream-stream left join on matching window + zone.
    # supply_gap > 0 means more orders waiting than couriers dispatched → stress.
    zone_health = (
        orders_waiting.alias("o")
        .join(
            couriers_dispatched.alias("c"),
            (col("o.window") == col("c.window")) &
            (col("o.zone_id") == col("c.zone_id")),
            "left",
        )
        .select(
            col("o.window.start").alias("window_start"),
            col("o.window.end").alias("window_end"),
            col("o.zone_id").alias("zone_id"),
            col("o.orders_awaiting_pickup"),
            coalesce(col("c.couriers_dispatched"), lit(0)).alias("couriers_dispatched"),
            (
                col("o.orders_awaiting_pickup")
                - coalesce(col("c.couriers_dispatched"), lit(0))
            ).alias("supply_gap"),
        )
    )

    # ── UC3: Anomaly detection — delivery SLA breaches + injected anomaly flags ─
    # Flags a zone-window as anomalous if:
    #   (a) avg actual delivery time exceeds SLA_MINUTES, OR
    #   (b) more than ANOMALY_RATE_THRESHOLD of events carry anomaly_flag=true.
    # Late events are handled by the 15-min watermark — they still fall into the
    # correct window as long as they arrive within the tolerance.
    _SLA_MINUTES = 45.0
    _ANOMALY_RATE_THRESHOLD = 0.05  # 5 % of events flagged → zone alert

    orders_anomaly = (
        orders_event_time
        .withWatermark("event_time_ts", "15 minutes")
        .groupBy(
            window(col("event_time_ts"), "5 minutes"),
            col("user_zone_id").alias("zone_id"),
        )
        .agg(
            count("*").alias("total_events"),
            count(when(col("anomaly_flag") == True, 1)).alias("anomaly_count"),
            avg(col("actual_delivery_time")).alias("avg_delivery_min"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("zone_id"),
            col("total_events"),
            col("anomaly_count"),
            col("avg_delivery_min"),
            (col("anomaly_count") / col("total_events")).alias("anomaly_rate"),
            (
                (col("avg_delivery_min") > lit(_SLA_MINUTES)) |
                (col("anomaly_count") / col("total_events") > lit(_ANOMALY_RATE_THRESHOLD))
            ).alias("zone_flagged"),
        )
    )

    # ── Sink: raw events to Parquet (data at rest) ─────────────────────────────
    q_orders = (
        orders_stream.writeStream
        .format("parquet")
        .option("path", f"{output_base}/orders")
        .option("checkpointLocation", f"{checkpoint_base}/orders")
        .trigger(processingTime="10 seconds")
        .start()
    )

    q_couriers = (
        couriers_stream.writeStream
        .format("parquet")
        .option("path", f"{output_base}/couriers")
        .option("checkpointLocation", f"{checkpoint_base}/couriers")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # ── Sink: UC1a tumbling KPIs ───────────────────────────────────────────────
    q_tumbling_kpi = (
        orders_tumbling_kpi.writeStream
        .format("parquet")
        .option("path", f"{output_base}/kpi_orders_5min_tumbling")
        .option("checkpointLocation", f"{checkpoint_base}/kpi_orders_5min_tumbling")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # ── Sink: UC1b hopping KPIs per zone ──────────────────────────────────────
    q_hopping_kpi = (
        orders_hopping_kpi.writeStream
        .format("parquet")
        .option("path", f"{output_base}/kpi_orders_10min_hop5")
        .option("checkpointLocation", f"{checkpoint_base}/kpi_orders_10min_hop5")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # ── Sink: UC2 zone demand-supply health ────────────────────────────────────
    q_zone_health = (
        zone_health.writeStream
        .format("parquet")
        .option("path", f"{output_base}/kpi_zone_health")
        .option("checkpointLocation", f"{checkpoint_base}/kpi_zone_health")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # ── Sink: UC3 anomaly detection per zone ───────────────────────────────────
    q_anomaly = (
        orders_anomaly.writeStream
        .format("parquet")
        .option("path", f"{output_base}/kpi_anomaly_zones")
        .option("checkpointLocation", f"{checkpoint_base}/kpi_anomaly_zones")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("\n[Spark] 7 streams running:")
    print("  raw      → /orders, /couriers")
    print("  UC1a     → /kpi_orders_5min_tumbling      (tumbling window, global)")
    print("  UC1b     → /kpi_orders_10min_hop5         (hopping window 10min/5min, per zone)")
    print("  UC2      → /kpi_zone_health               (demand-supply gap per zone)")
    print("  UC3      → /kpi_anomaly_zones             (SLA breach + anomaly flag detection)")
    print("[Spark] Parquet written to Blob every 10 seconds. Ctrl+C to stop.\n")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n[Spark] shutting down...")
        for q in [q_orders, q_couriers, q_tumbling_kpi, q_hopping_kpi, q_zone_health, q_anomaly]:
            q.stop()
        spark.stop()
        print("[Spark] stopped.")


if __name__ == "__main__":
    main()
