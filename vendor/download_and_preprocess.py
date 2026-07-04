#!/usr/bin/env python3
"""data-init sidecar entrypoint.

Downloads the four NYC TLC HV-FHV monthly parquet files plus the taxi-zone
lookup CSV from CloudFront into the tlc-cache volume:

  <cache_dir>/fhvhv_tripdata_<YYYY-MM>.parquet   (one per month in window)
  <cache_dir>/taxi_zone_lookup.csv
  <cache_dir>/_READY                              sentinel — last

Idempotent: if <cache_dir>/_READY exists, exits 0 immediately. Individual
files that already exist with non-zero size are also skipped.

Host-cache passthrough: if a directory is bind-mounted at /host-cache and it
contains a file with the same name, that file is copied into the cache
instead of being downloaded. Set HVFHV_CACHE_DIR=/path/on/host in .env to
enable. Useful for iterating without paying the ~2 GB CloudFront download on
every `make reset`.
"""
from __future__ import annotations

import os
import shutil
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import requests


BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
HVFHV_URL_TEMPLATE = f"{BASE_URL}/trip-data/fhvhv_tripdata_{{ym}}.parquet"
ZONE_LOOKUP_URL = f"{BASE_URL}/misc/taxi_zone_lookup.csv"
HOST_CACHE_DIR = Path("/host-cache")


def log(msg: str) -> None:
    print(f"[data-init] {msg}", flush=True)


def parse_window_env() -> list[str]:
    """Return YYYY-MM strings for every month in [start, end] inclusive."""
    start_raw = os.environ.get("REPLAY_WINDOW_START", "2024-09")
    end_raw = os.environ.get("REPLAY_WINDOW_END", "2024-12")
    start = date.fromisoformat(start_raw + "-01")
    end = date.fromisoformat(end_raw + "-01")
    if end < start:
        raise SystemExit(f"REPLAY_WINDOW_END ({end_raw}) is before REPLAY_WINDOW_START ({start_raw})")
    months: list[str] = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            m = 1
            y += 1
    return months


def download_with_progress(url: str, dest: Path) -> None:
    log(f"GET {url}")
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        written = 0
        last_log = time.monotonic()
        with tmp.open("wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 20):
                if not chunk:
                    continue
                fh.write(chunk)
                written += len(chunk)
                now = time.monotonic()
                if now - last_log >= 5:
                    pct = (written / total * 100) if total else 0.0
                    log(f"  downloaded {written/1e6:,.1f} MB"
                        + (f" / {total/1e6:,.1f} MB ({pct:.1f}%)" if total else ""))
                    last_log = now
    tmp.replace(dest)
    log(f"  done ({dest.stat().st_size/1e6:,.1f} MB) → {dest.name}")


def copy_from_host_cache(dest: Path, min_size_mb: float) -> bool:
    """If /host-cache/<dest.name> exists and is large enough, copy it. Returns True if copied."""
    src = HOST_CACHE_DIR / dest.name
    if not src.exists() or not src.is_file():
        return False
    size = src.stat().st_size
    if size <= min_size_mb * 1e6:
        log(f"  /host-cache/{src.name} present but too small ({size/1e6:,.1f} MB) — ignoring")
        return False
    log(f"  copying /host-cache/{src.name} ({size/1e6:,.1f} MB) → {dest.name}")
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    shutil.copyfile(src, tmp)
    tmp.replace(dest)
    return True


def fetch_if_missing(url: str, dest: Path, min_size_mb: float = 0.0) -> None:
    if dest.exists() and dest.stat().st_size > min_size_mb * 1e6:
        log(f"  {dest.name} already present ({dest.stat().st_size/1e6:,.1f} MB) — skipping")
        return
    if copy_from_host_cache(dest, min_size_mb):
        return
    download_with_progress(url, dest)


def main(cache_dir: Path) -> int:
    cache_dir.mkdir(parents=True, exist_ok=True)
    ready = cache_dir / "_READY"
    if ready.exists():
        log(f"{ready} exists — skipping (idempotent re-run)")
        return 0

    months = parse_window_env()
    log(f"window: {months[0]} .. {months[-1]} ({len(months)} months)")

    if HOST_CACHE_DIR.is_dir() and any(HOST_CACHE_DIR.iterdir()):
        log(f"host-cache mounted at {HOST_CACHE_DIR} — will reuse matching files")

    # Clean up any stale .tmp from a SIGKILL'd previous run.
    for stale in cache_dir.glob("*.tmp"):
        log(f"removing stale {stale.name}")
        stale.unlink(missing_ok=True)

    # 1. The zone lookup CSV.
    fetch_if_missing(ZONE_LOOKUP_URL, cache_dir / "taxi_zone_lookup.csv", min_size_mb=0.001)

    # 2. The HV-FHV monthly parquet files.
    for ym in months:
        url = HVFHV_URL_TEMPLATE.format(ym=ym)
        dest = cache_dir / f"fhvhv_tripdata_{ym}.parquet"
        fetch_if_missing(url, dest, min_size_mb=50.0)

    ready.write_text(datetime.now(timezone.utc).isoformat() + "\n")
    log(f"_READY at {ready}")
    return 0


if __name__ == "__main__":
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("/cache")
    sys.exit(main(target))
