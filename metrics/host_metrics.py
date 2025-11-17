from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal

from config import get_settings
from db import execute_many, fetch_all, fetch_one
from metrics import utils

logger = logging.getLogger(__name__)
settings = get_settings()

BOOKING_STATUSES = ("confirmed", "refunded")

BOOKING_SQL = """
    SELECT
        DATE(b.booking_time AT TIME ZONE 'UTC') AS day,
        e.game_owner_id AS host_id,
        eh.name AS host_name,
        COALESCE(g.display_name, g.city, 'Unknown') AS city_name,
        COUNT(*) AS total_bookings,
        SUM(b.total_price) AS host_gmv
    FROM events_booking b
    JOIN events_eventpage e ON b.event_page_id = e.page_ptr_id
    LEFT JOIN events_eventhost eh ON e.game_owner_id = eh.id
    LEFT JOIN utils_geolocation g ON e.geolocation_id = g.id
    WHERE DATE(b.booking_time AT TIME ZONE 'UTC') BETWEEN %s AND %s
      AND e.game_owner_id IS NOT NULL
      AND b.status = ANY(%s)
    GROUP BY day, host_id, host_name, city_name
"""

CANCEL_SQL = """
    SELECT
        DATE(b.cancelled_at AT TIME ZONE 'UTC') AS day,
        e.game_owner_id AS host_id,
        COUNT(*) AS cancels
    FROM events_booking b
    JOIN events_eventpage e ON b.event_page_id = e.page_ptr_id
    WHERE b.cancelled_at IS NOT NULL
      AND e.game_owner_id IS NOT NULL
      AND DATE(b.cancelled_at AT TIME ZONE 'UTC') BETWEEN %s AND %s
    GROUP BY day, host_id
"""

UPSERT_SQL = """
    INSERT INTO metrics_dashboard_hostdailymetrics (
        host_id,
        host_name,
        date,
        host_bookings,
        host_gmv,
        host_payouts,
        cancels,
        city,
        last_updated
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, TIMEZONE('utc', NOW()))
    ON CONFLICT (host_id, date) DO UPDATE SET
        host_name = EXCLUDED.host_name,
        host_bookings = EXCLUDED.host_bookings,
        host_gmv = EXCLUDED.host_gmv,
        host_payouts = EXCLUDED.host_payouts,
        cancels = EXCLUDED.cancels,
        city = EXCLUDED.city,
        last_updated = TIMEZONE('utc', NOW());
"""


def _last_host_metrics_date() -> date | None:
    row = fetch_one("SELECT MAX(date) AS max_date FROM metrics_dashboard_hostdailymetrics;")
    return row["max_date"] if row and row["max_date"] else None


def _determine_range() -> tuple[date, date] | None:
    today = date.today()
    last_date = _last_host_metrics_date()
    if last_date:
        start = last_date + timedelta(days=1)
    else:
        start = today - timedelta(days=settings.max_lookback_days)

    start = utils.clamp_start_to_lookback(start, settings.max_lookback_days)
    end = today
    if start > end:
        return None
    return start, end


def compute_host_daily_metrics() -> int:
    range_result = _determine_range()
    if not range_result:
        logger.info("Host metrics already up to date.")
        return 0

    start, end = range_result
    logger.info("Computing host metrics from %s to %s", start, end)

    booking_rows = fetch_all(BOOKING_SQL, (start, end, list(BOOKING_STATUSES)))
    if not booking_rows:
        logger.info("No host activity detected in range.")
        return 0

    cancel_rows = fetch_all(CANCEL_SQL, (start, end))
    cancel_map: dict[tuple[int, date], int] = {}
    for row in cancel_rows:
        day = utils.coerce_date(row["day"])
        cancel_map[(row["host_id"], day)] = int(row["cancels"] or 0)

    upsert_rows = []
    for row in booking_rows:
        day = utils.coerce_date(row["day"])
        host_id = row["host_id"]
        if host_id is None:
            continue

        key = (host_id, day)
        cancels = cancel_map.get(key, 0)
        upsert_rows.append(
            (
                host_id,
                row["host_name"] or "Host",
                day,
                int(row["total_bookings"] or 0),
                row["host_gmv"] or Decimal("0"),
                None,
                cancels,
                row["city_name"] or "Unknown",
            )
        )

    execute_many(UPSERT_SQL, upsert_rows)
    logger.info("Inserted/updated %s host metric rows.", len(upsert_rows))
    return len(upsert_rows)

