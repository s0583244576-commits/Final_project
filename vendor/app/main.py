"""FastAPI app for the tlc-publisher mock.

Routes:
  GET  /healthz
  GET  /stats
  GET  /simulated_now
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from fastapi import FastAPI

from . import chaos
from . import publisher as publisher_mod


state: publisher_mod.PublisherState = publisher_mod.PublisherState()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Restore prior state (simulated_now, published_months) if present.
    persisted = publisher_mod.load_persisted_state()
    if persisted:
        state.restore(persisted)
        print(
            f"[main] restored state: simulated_now={state.simulated_now.isoformat()}, "
            f"published_months={state.published_months}",
            flush=True,
        )

    replay_task = asyncio.create_task(publisher_mod.replay_loop(state))
    persist_task = asyncio.create_task(publisher_mod._persist_loop(state))
    try:
        yield
    finally:
        replay_task.cancel()
        persist_task.cancel()


app = FastAPI(title="Tessera tlc-publisher mock", version="0.1.0", lifespan=lifespan)


def _iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


@app.get("/healthz")
def healthz() -> dict[str, Any]:
    return {
        "ready": True,
        "simulated_now": _iso(state.simulated_now),
        "published_months": list(state.published_months),
    }


@app.get("/simulated_now")
def simulated_now() -> dict[str, Any]:
    return {"simulated_now": _iso(state.simulated_now)}


@app.get("/stats")
def stats() -> dict[str, Any]:
    return {
        "simulated_now": _iso(state.simulated_now),
        "published_months": list(state.published_months),
        "puts_total": state.puts_total,
        "chaos_rounds": state.chaos_rounds,
        "chaos_mutations": list(state.chaos_mutations),
        "chaos": {
            "late_correction_rate": chaos.LATE_CORRECTION_RATE,
        },
    }
