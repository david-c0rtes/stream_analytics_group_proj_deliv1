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
        col,
        count,
        from_json,
        from_unixtime,
        lit,
        to_timestamp,
        to_utc_timestamp,
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

    # Event-time analytics stream: 5-minute tumbling window with watermark.
    orders_event_time = orders_stream.withColumn(
        "event_time_ts",
        to_timestamp(col("event_time")),
    )
    orders_windowed_kpi = (
        orders_event_time
        .withWatermark("event_time_ts", "15 minutes")
        .groupBy(window(col("event_time_ts"), "5 minutes"))
        .agg(
            count("*").alias("orders_events"),
            avg(col("actual_delivery_time")).alias("avg_delivery_time_min"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("orders_events"),
            col("avg_delivery_time_min"),
        )
    )

    # Write each micro-batch to Azure Blob as Parquet (native Spark sink).
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

    q_orders_kpi = (
        orders_windowed_kpi.writeStream
        .format("parquet")
        .option("path", f"{output_base}/kpi_orders_5min")
        .option("checkpointLocation", f"{checkpoint_base}/kpi_orders_5min")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("\n[Spark] streams running — raw + windowed Parquet written to Blob every 10 seconds")
    print("[Spark] dashboard will refresh automatically")
    print("[Spark] press Ctrl+C to stop\n")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n[Spark] shutting down...")
        q_orders.stop()
        q_couriers.stop()
        q_orders_kpi.stop()
        spark.stop()
        print("[Spark] stopped.")


if __name__ == "__main__":
    main()
