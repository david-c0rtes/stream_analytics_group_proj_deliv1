"""Data loading utilities — local NDJSON (live) + Azure Blob Parquet (Spark output)."""
from __future__ import annotations

import json
import os
from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st

_PROJECT_ROOT   = Path(__file__).parent.parent.parent
_LOCAL_DATA_DIR = _PROJECT_ROOT / "output" / "json"


# ── Local NDJSON (primary — instant, used during live simulation) ─────────────

def _load_local_ndjson(glob_pattern: str) -> pd.DataFrame:
    records: list[dict] = []
    for path in sorted(_LOCAL_DATA_DIR.glob(glob_pattern)):
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    return pd.DataFrame(records) if records else pd.DataFrame()


# ── Azure Blob Parquet (Spark output — polled every 10s to avoid freezing) ────

@st.cache_data(ttl=10, show_spinner=False)
def _load_blob_parquet(blob_prefix: str) -> pd.DataFrame | None:
    """
    Download Parquet files written by spark_streaming_job.py from Azure Blob.
    Cached for 10 seconds — Blob reads are slow (network), not suitable for 1s refresh.
    Returns None if Azure not configured or no Parquet files found.
    """
    conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "").strip()
    if not conn_str:
        return None
    container = os.environ.get("AZURE_STORAGE_CONTAINER", "dashboard-data")
    try:
        from azure.storage.blob import ContainerClient
        client = ContainerClient.from_connection_string(conn_str, container)
        frames: list[pd.DataFrame] = []
        for blob in client.list_blobs(name_starts_with=blob_prefix):
            if not blob.name.endswith(".parquet"):
                continue
            try:
                data = client.get_blob_client(blob.name).download_blob().readall()
                df = pd.read_parquet(BytesIO(data), engine="pyarrow")
                if not df.empty:
                    frames.append(df)
            except Exception:
                continue
        return pd.concat(frames, ignore_index=True) if frames else None
    except Exception:
        return None


# ── Shared post-processing ────────────────────────────────────────────────────

def _finalise(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if "event_id" in df.columns:
        df = df.drop_duplicates(subset=["event_id"])
    if "event_time" in df.columns:
        df["event_time"] = pd.to_datetime(df["event_time"], utc=True, errors="coerce")
    return df.reset_index(drop=True)


# ── Public loaders ────────────────────────────────────────────────────────────

@st.cache_data(ttl=1, show_spinner=False)
def load_order_events() -> pd.DataFrame:
    """
    Load order events. Priority:
      1. Local NDJSON (instant — used during live simulation)
      2. Azure Blob Parquet (Spark output — used when no local files present)
    """
    blob_df = _load_blob_parquet("order_lifecycle_events_")
    if blob_df is not None and not blob_df.empty:
        return _finalise(blob_df)
    local_df = _load_local_ndjson("order_lifecycle_events_*.ndjson")
    return _finalise(local_df)


@st.cache_data(ttl=1, show_spinner=False)
def load_courier_events() -> pd.DataFrame:
    """
    Load courier events. Priority:
      1. Azure Blob Parquet (Spark output)
      2. Local NDJSON fallback
    """
    blob_df = _load_blob_parquet("courier_operations_events_")
    if blob_df is not None and not blob_df.empty:
        return _finalise(blob_df)
    local_df = _load_local_ndjson("courier_operations_events_*.ndjson")
    return _finalise(local_df)


def data_freshness(df_orders: pd.DataFrame, df_couriers: pd.DataFrame) -> dict:
    result = {"order_count": 0, "courier_count": 0, "latest_event": None, "earliest_event": None}
    if not df_orders.empty:
        result["order_count"] = len(df_orders)
        if "event_time" in df_orders.columns:
            result["earliest_event"] = df_orders["event_time"].min()
            result["latest_event"]   = df_orders["event_time"].max()
    if not df_couriers.empty:
        result["courier_count"] = len(df_couriers)
    return result
