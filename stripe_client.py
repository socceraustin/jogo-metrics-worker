from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import stripe

from config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

if settings.stripe_secret_key:
    stripe.api_key = settings.stripe_secret_key
else:
    logger.warning("STRIPE_SECRET_KEY not set; Stripe revenue data will be 0.")


def _iso_range_for_day(day: datetime) -> tuple[int, int]:
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return int(start.timestamp()), int(end.timestamp())


def get_daily_revenue(day: datetime) -> tuple[Decimal | None, Decimal | None]:
    """
    Returns (captured_revenue, refunds) for the given UTC day using Stripe.
    If the Stripe key is missing or a request fails, returns Decimal(0) for both values.
    """
    if not settings.stripe_secret_key:
        return None, None

    start_ts, end_ts = _iso_range_for_day(day)
    revenue = Decimal("0")
    refunds = Decimal("0")

    try:
        charges = stripe.Charge.list(
            created={"gte": start_ts, "lt": end_ts},
            limit=100,
        )
        for charge in charges.auto_paging_iter():
            if charge.get("paid") and not charge.get("refunded"):
                revenue += Decimal(charge["amount"]) / Decimal("100")
            if charge.get("refunded"):
                refunds += Decimal(charge["amount_refunded"]) / Decimal("100")

        refund_list = stripe.Refund.list(
            created={"gte": start_ts, "lt": end_ts},
            limit=100,
        )
        for refund in refund_list.auto_paging_iter():
            refunds += Decimal(refund["amount"]) / Decimal("100")
    except Exception as exc:
        logger.warning("Stripe API error for %s: %s", day.date(), exc)
        return None, None

    return revenue, refunds

