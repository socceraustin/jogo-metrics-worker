from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal

from config import get_settings
from db import execute_many, fetch_all, fetch_one
from metrics import utils
from stripe_client import get_daily_revenue

logger = logging.getLogger(__name__)
settings = get_settings()

BOOKING_STATUSES = ("confirmed", "refunded")
DAILY_SELECT_SQL = """
    SELECT
        DATE(b.booking_time AT TIME ZONE 'UTC') AS day,
        SUM(b.total_price) AS total_gmv,
        COUNT(*) AS total_bookings,
        COUNT(DISTINCT b.attendee_id) AS total_unique_players,
        SUM(
            CASE
                WHEN b.payment_method = 'online' AND b.payment_completed THEN b.total_price
                ELSE 0
            END
        ) AS total_revenue_stripe,
        SUM(
            CASE
                WHEN b.status = 'refunded' THEN b.total_price
                ELSE 0
            END
        ) AS total_refunds
    FROM events_booking b
    WHERE DATE(b.booking_time AT TIME ZONE 'UTC') BETWEEN %s AND %s
      AND b.status = ANY(%s)
    GROUP BY day
"""

CITY_BREAKDOWN_SQL = """
    SELECT
        DATE(b.booking_time AT TIME ZONE 'UTC') AS day,
        COALESCE(g.display_name, g.city, 'Unknown') AS city_name,
        SUM(b.total_price) AS total_gmv,
        COUNT(*) AS total_bookings
    FROM events_booking b
    JOIN events_eventpage e ON b.event_page_id = e.page_ptr_id
    LEFT JOIN utils_geolocation g ON e.geolocation_id = g.id
    WHERE DATE(b.booking_time AT TIME ZONE 'UTC') BETWEEN %s AND %s
      AND b.status = ANY(%s)
    GROUP BY day, city_name
"""

UPSERT_SQL = """
    INSERT INTO metrics_dashboard_dailymetrics (
        date,
        total_gmv,
        total_bookings,
        total_unique_players,
        total_revenue_stripe,
        total_refunds,
        city_breakdown,
        last_updated
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, TIMEZONE('utc', NOW()))
    ON CONFLICT (date) DO UPDATE SET
        total_gmv = EXCLUDED.total_gmv,
        total_bookings = EXCLUDED.total_bookings,
        total_unique_players = EXCLUDED.total_unique_players,
        total_revenue_stripe = EXCLUDED.total_revenue_stripe,
        total_refunds = EXCLUDED.total_refunds,
        city_breakdown = EXCLUDED.city_breakdown,
        last_updated = TIMEZONE('utc', NOW());
"""


def _get_last_processed_date() -> date | None:
    row = fetch_one("SELECT MAX(date) AS max_date FROM metrics_dashboard_dailymetrics;")
    return row["max_date"] if row and row["max_date"] else None


def _determine_range() -> tuple[date, date] | None:
    today = date.today()
    last_date = _get_last_processed_date()
    if last_date:
        start = last_date + timedelta(days=1)
    else:
        start = today - timedelta(days=settings.max_lookback_days)

    start = utils.clamp_start_to_lookback(start, settings.max_lookback_days)
    end = today
    if start > end:
        return None
    return start, end


def _build_city_breakdown(rows):
    breakdown: dict[date, dict[str, dict[str, float | int]]] = defaultdict(dict)
    for row in rows:
        day = utils.coerce_date(row["day"])
        city_name = row["city_name"] or "Unknown"
        breakdown[day][city_name] = {
            "gmv": utils.decimal_to_float(row["total_gmv"]),
            "bookings": int(row["total_bookings"] or 0),
        }
    return breakdown


def compute_daily_metrics() -> int:
    range_result = _determine_range()
    if not range_result:
        logger.info("Daily metrics already up to date.")
        return 0

    start, end = range_result
    logger.info("Computing daily metrics from %s to %s", start, end)

    metric_rows = fetch_all(DAILY_SELECT_SQL, (start, end, list(BOOKING_STATUSES)))
    if not metric_rows:
        logger.info("No new bookings between %s and %s", start, end)
        return 0

    city_rows = fetch_all(CITY_BREAKDOWN_SQL, (start, end, list(BOOKING_STATUSES)))
    city_map = _build_city_breakdown(city_rows)

    upsert_rows = []
    for row in metric_rows:
        day = utils.coerce_date(row["day"])
        revenue_from_db = row["total_revenue_stripe"] or Decimal("0")
        refunds_from_db = row["total_refunds"] or Decimal("0")
        stripe_revenue, stripe_refunds = get_daily_revenue(datetime.combine(day, datetime.min.time()))
        total_revenue = stripe_revenue if stripe_revenue is not None else revenue_from_db
        total_refunds = stripe_refunds if stripe_refunds is not None else refunds_from_db

        upsert_rows.append(
            (
                day,
                row["total_gmv"] or Decimal("0"),
                int(row["total_bookings"] or 0),
                int(row["total_unique_players"] or 0),
                total_revenue,
                total_refunds,
                utils.json_dumps(city_map.get(day, {})),
            )
        )

    execute_many(UPSERT_SQL, upsert_rows)
    logger.info("Inserted/updated %s daily metric rows.", len(upsert_rows))
    return len(upsert_rows)

