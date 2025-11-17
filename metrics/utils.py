from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, Iterable, Iterator

logger = logging.getLogger(__name__)


def daterange(start: date, end: date) -> Iterator[date]:
    delta = (end - start).days
    for idx in range(delta + 1):
        yield start + timedelta(days=idx)


def clamp_start_to_lookback(start: date, lookback_days: int) -> date:
    cutoff = date.today() - timedelta(days=lookback_days)
    return max(start, cutoff)


def decimal_to_float(value: Decimal | None) -> float:
    if value is None:
        return 0.0
    return float(round(value, 2))


def json_dumps(data: Dict) -> str:
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def coerce_date(value) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    raise ValueError(f"Unsupported date type: {type(value)}")


def log_job_start(job_name: str):
    logger.info("Starting %s job at %s", job_name, datetime.utcnow().isoformat())


def log_job_end(job_name: str, processed: int):
    logger.info("Finished %s job. %s records processed.", job_name, processed)

