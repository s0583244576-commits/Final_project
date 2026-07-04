"""Background replay loop + MinIO publication state.

Holds `simulated_now`, advances it at REPLAY_SECONDS_PER_MONTH pace, and PUTs
each window-month's HV-FHV parquet from /cache into the MinIO bucket as its
simulated publication date arrives. Persists state to
<CACHE_DIR>/_publisher_state.json every 5 wall-seconds.

The zone-lookup CSV is uploaded once at startup (it's reference data, not
part of the replay).

Late-correction chaos lives in `chaos.py`.
"""
from __future__ import annotations

import asyncio
import json
import os
import random
import time as _time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import boto3
from botocore.client import Config as BotoConfig

from . import chaos


CACHE_DIR = Path(os.environ.get("CACHE_DIR", "/cache"))
STATE_PATH = CACHE_DIR / "_publisher_state.json"

SECONDS_PER_MONTH = float(os.environ.get("REPLAY_SECONDS_PER_MONTH", "75"))
WINDOW_START_YM = os.environ.get("REPLAY_WINDOW_START", "2024-09")
WINDOW_END_YM = os.environ.get("REPLAY_WINDOW_END", "2024-12")
# How often (in simulated days) to roll for a late-correction chaos event.
# Decoupled from publication boundaries so chaos keeps firing after every
# month is in the bucket. Default 5 sim-days ≈ 12 wall-seconds at the
# default replay speed.
CHAOS_CHECK_SIM_DAYS = float(os.environ.get("VENDOR_CHAOS_CHECK_SIM_DAYS", "5"))

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "tlc-drops")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "tessera")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "tessera-secret")


def _log(msg: str) -> None:
    print(f"[publisher] {msg}", flush=True)


def _ym_to_date(ym: str) -> date:
    return date.fromisoformat(ym + "-01")


def _add_month(d: date) -> date:
    y, m = d.year, d.month + 1
    if m == 13:
        m = 1
        y += 1
    return date(y, m, 1)


def _months_in_window() -> list[str]:
    start = _ym_to_date(WINDOW_START_YM)
    end = _ym_to_date(WINDOW_END_YM)
    months: list[str] = []
    cur = start
    while cur <= end:
        months.append(f"{cur.year:04d}-{cur.month:02d}")
        cur = _add_month(cur)
    return months


def _parquet_filename(ym: str) -> str:
    return f"fhvhv_tripdata_{ym}.parquet"


@dataclass
class PublisherState:
    simulated_now: datetime = field(
        default_factory=lambda: datetime.combine(
            _ym_to_date(WINDOW_START_YM), datetime.min.time()
        ).replace(tzinfo=timezone.utc)
    )
    published_months: list[str] = field(default_factory=list)
    chaos_rounds: int = 0
    chaos_mutations: list[dict[str, Any]] = field(default_factory=list)
    puts_total: int = 0
    rng_seed: int = 0xC0FFEE
    # Timestamp of the last chaos roll attempt (sim-time), so we can fire on
    # a regular sim-day cadence independent of publication boundaries.
    last_chaos_check: datetime | None = None

    def snapshot(self) -> dict[str, Any]:
        return {
            "simulated_now": self.simulated_now.isoformat(),
            "published_months": list(self.published_months),
            "chaos_rounds": self.chaos_rounds,
            "chaos_mutations": list(self.chaos_mutations),
            "puts_total": self.puts_total,
            "rng_seed": self.rng_seed,
            "last_chaos_check": self.last_chaos_check.isoformat() if self.last_chaos_check else None,
        }

    def restore(self, payload: dict[str, Any]) -> None:
        try:
            sn = datetime.fromisoformat(payload["simulated_now"])
            if sn.tzinfo is None:
                sn = sn.replace(tzinfo=timezone.utc)
            self.simulated_now = sn
            self.published_months = list(payload.get("published_months", []))
            self.chaos_rounds = int(payload.get("chaos_rounds", 0))
            self.chaos_mutations = list(payload.get("chaos_mutations", []))
            self.puts_total = int(payload.get("puts_total", 0))
            self.rng_seed = int(payload.get("rng_seed", 0xC0FFEE))
            lcc = payload.get("last_chaos_check")
            if lcc:
                dt = datetime.fromisoformat(lcc)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                self.last_chaos_check = dt
        except Exception as e:
            _log(f"restore failed: {e}; starting fresh")


def load_persisted_state() -> dict[str, Any] | None:
    if not STATE_PATH.exists():
        return None
    try:
        return json.loads(STATE_PATH.read_text())
    except Exception:
        return None


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4", retries={"max_attempts": 5}),
        region_name="us-east-1",
    )


def ensure_bucket(s3) -> None:
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
        _log(f"bucket {MINIO_BUCKET} exists")
    except Exception:
        _log(f"creating bucket {MINIO_BUCKET}")
        s3.create_bucket(Bucket=MINIO_BUCKET)


def upload_zone_lookup(s3) -> None:
    src = CACHE_DIR / "taxi_zone_lookup.csv"
    if not src.exists():
        _log(f"WARN: {src} not found; zone lookup not uploaded")
        return
    key = "taxi_zone_lookup.csv"
    _log(f"PUT s3://{MINIO_BUCKET}/{key} ({src.stat().st_size/1e3:,.1f} KB)")
    with src.open("rb") as fh:
        s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=fh)


def put_month_file(s3, ym: str) -> str | None:
    """Upload the parquet file for the given month. Returns the resulting ETag."""
    src = CACHE_DIR / _parquet_filename(ym)
    if not src.exists():
        _log(f"WARN: {src} not found; cannot publish {ym}")
        return None
    key = _parquet_filename(ym)
    size_mb = src.stat().st_size / 1e6
    _log(f"PUT s3://{MINIO_BUCKET}/{key} ({size_mb:,.1f} MB)")
    with src.open("rb") as fh:
        resp = s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=fh)
    etag = (resp.get("ETag") or "").strip('"')
    _log(f"  → etag {etag}")
    return etag


async def _persist_loop(state: PublisherState) -> None:
    while True:
        await asyncio.sleep(5)
        try:
            STATE_PATH.write_text(json.dumps(state.snapshot(), indent=2))
        except Exception as e:
            _log(f"persist failed: {e}")


async def replay_loop(state: PublisherState) -> None:
    """Advance simulated_now, publish month files, and roll chaos.

    Wall-time anchored: every tick computes how much simulated time should
    have elapsed since loop start (or since restored origin), advances
    simulated_now to match, and publishes any month whose first-of-next-month
    trigger has been reached.

    Chaos is on an INDEPENDENT cadence: every CHAOS_CHECK_SIM_DAYS of
    simulated time, roll once. If the roll succeeds (probability
    LATE_CORRECTION_RATE), mutate a previously-published month's parquet
    and re-PUT it. This means `make vendor-chaos` keeps producing
    corrections even after every file is in the bucket.
    """
    try:
        _log(
            f"starting: window={WINDOW_START_YM}..{WINDOW_END_YM} "
            f"seconds_per_month={SECONDS_PER_MONTH} "
            f"chaos_check_sim_days={CHAOS_CHECK_SIM_DAYS} "
            f"late_correction_rate={chaos.LATE_CORRECTION_RATE} "
            f"simulated_now={state.simulated_now.isoformat()}"
        )

        s3 = _s3_client()
        ensure_bucket(s3)
        upload_zone_lookup(s3)

        rng = random.Random(state.rng_seed)

        months = _months_in_window()
        window_start_dt = datetime.combine(
            _ym_to_date(WINDOW_START_YM), datetime.min.time()
        ).replace(tzinfo=timezone.utc)

        # Simulated seconds per wall second. One simulated month = ~30.44 days.
        SIM_SEC_PER_MONTH = 30.44 * 86400.0
        sim_per_wall_s = SIM_SEC_PER_MONTH / SECONDS_PER_MONTH

        if state.simulated_now < window_start_dt:
            state.simulated_now = window_start_dt
        if state.last_chaos_check is None:
            state.last_chaos_check = state.simulated_now

        wall_origin = _time.monotonic()
        sim_origin = state.simulated_now

        TICK_S = 0.5
        chaos_interval = timedelta(days=CHAOS_CHECK_SIM_DAYS)

        while True:
            try:
                wall_elapsed = _time.monotonic() - wall_origin
                target = sim_origin + timedelta(seconds=wall_elapsed * sim_per_wall_s)
                state.simulated_now = target

                # Publish any month whose trigger is reached.
                for ym in months:
                    if ym in state.published_months:
                        continue
                    trigger = _add_month(_ym_to_date(ym))
                    trigger_dt = datetime.combine(
                        trigger, datetime.min.time()
                    ).replace(tzinfo=timezone.utc)
                    if state.simulated_now >= trigger_dt:
                        put_month_file(s3, ym)
                        state.published_months.append(ym)
                        state.puts_total += 1

                # Chaos cadence: roll every CHAOS_CHECK_SIM_DAYS of sim-time.
                # The check itself is cheap; the roll inside may be a no-op.
                while (
                    state.last_chaos_check is not None
                    and state.simulated_now - state.last_chaos_check >= chaos_interval
                ):
                    state.last_chaos_check = state.last_chaos_check + chaos_interval
                    if (
                        chaos.LATE_CORRECTION_RATE > 0
                        and len(state.published_months) >= 1
                        and rng.random() < chaos.LATE_CORRECTION_RATE
                    ):
                        _log(
                            f"chaos roll fired at sim_time={state.simulated_now.isoformat()}"
                        )
                        # Run in a thread — the parquet read/write + S3 upload
                        # blocks for ~10-30s on a ~500MB file; keeping it off
                        # the event loop keeps /healthz responsive.
                        mutation = await asyncio.to_thread(
                            chaos.apply_late_correction,
                            s3=s3,
                            bucket=MINIO_BUCKET,
                            cache_dir=CACHE_DIR,
                            published_months=list(state.published_months),
                            rng=rng,
                            put_month_file=put_month_file,
                        )
                        if mutation is not None:
                            state.chaos_rounds += 1
                            state.chaos_mutations.append(mutation)

                await asyncio.sleep(TICK_S)
            except Exception as e:
                _log(f"loop iter error (continuing): {type(e).__name__}: {e}")
                await asyncio.sleep(TICK_S)
    except asyncio.CancelledError:
        _log("replay_loop cancelled")
        raise
    except Exception as e:
        _log(f"FATAL replay_loop crashed: {type(e).__name__}: {e}")
        raise
