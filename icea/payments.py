"""Payment and tier logic for ICEA (Stripe Tier 1, request forms Tier 2/3)."""
import os
from typing import Optional

from icea.store import create_pending_report as _store_create
from icea.store import consume_pending_report as _store_consume


def create_pending_report(request_dict: dict) -> str:
    """Store request for Tier 1 in persistent store; return one-time token."""
    return _store_create(request_dict)


def consume_pending_report(token: str) -> Optional[dict]:
    """Return and remove request for token if valid and not expired."""
    return _store_consume(token)


def get_stripe_secret_key() -> Optional[str]:
    return os.environ.get("STRIPE_SECRET_KEY") or os.environ.get("STRIPE_SECRET_KEY_TEST")


def create_checkout_session(
    token: str,
    amount_cents: int,
    success_url: str,
    cancel_url: str,
    description: str = "ICEA Tier 1 — Automated Report",
) -> Optional[str]:
    """Create Stripe Checkout Session; return checkout URL or None if Stripe not configured."""
    try:
        import stripe
    except ImportError:
        return None
    key = get_stripe_secret_key()
    if not key:
        return None
    stripe.api_key = key
    session = stripe.checkout.Session.create(
        mode="payment",
        line_items=[
            {
                "price_data": {
                    "currency": "usd",
                    "product_data": {
                        "name": "ICEA — Automated Report",
                        "description": description,
                        "images": [],
                    },
                    "unit_amount": amount_cents,
                },
                "quantity": 1,
            }
        ],
        success_url=success_url,
        cancel_url=cancel_url,
        metadata={"icea_token": token, "tier": "1"},
        expires_in=1800,
    )
    return session.url
