from __future__ import annotations

import logging

from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse

from config import Settings, get_settings
from metrics.daily_metrics import compute_daily_metrics
from metrics.host_metrics import compute_host_daily_metrics
from metrics import utils

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

app = FastAPI(title="Jogo Metrics Worker", version="1.0.0")


def run_metrics_job():
    utils.log_job_start("metrics")
    total = 0
    total += compute_daily_metrics()
    total += compute_host_daily_metrics()
    utils.log_job_end("metrics", total)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/run")
def run_metrics(
    background_tasks: BackgroundTasks,
    x_metrics_secret: str = Header(..., alias="X-Metrics-Secret"),
    settings: Settings = Depends(get_settings),
):
    if x_metrics_secret != settings.metrics_secret:
        raise HTTPException(status_code=401, detail="Unauthorized")

    background_tasks.add_task(run_metrics_job)
    return JSONResponse({"status": "metrics job started"})

