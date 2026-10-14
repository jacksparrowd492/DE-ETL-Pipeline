"""
conftest.py
-----------
Shared pytest fixtures. `pythonpath = .` in pytest.ini already puts the
project root on sys.path, so plain imports like `from transform.transformer
import ...` work from any test module.
"""

import pytest

from observability import alerts, logging_config, metrics


@pytest.fixture(autouse=True)
def isolated_observability(tmp_path, monkeypatch):
    """
    Every test gets its own LOG_DIR, so logs/metrics/alerts/lineage events
    written by code under test never land in the real logs/ folder, and
    external alert channels are off unless a test turns them on.
    """
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("ALERTS_ENABLED", "false")
    monkeypatch.setattr(logging_config, "_default_component", "pipeline")
    for var in (
        "SLACK_WEBHOOK_URL",
        "SMTP_HOST",
        "ALERT_EMAIL_TO",
        "OPENLINEAGE_URL",
        "PUSHGATEWAY_URL",
        "LOG_FORMAT",
    ):
        monkeypatch.delenv(var, raising=False)
    alerts.reset_cooldowns()
    metrics.reset()
    yield tmp_path / "logs"
    alerts.reset_cooldowns()
    metrics.reset()


@pytest.fixture
def raw_product():
    """One raw product exactly as the FakeStore API returns it."""
    return {
        "id": 1,
        "title": "  Fjallraven Backpack  ",
        "price": 109.95,
        "category": "men's clothing",
        "description": "A backpack for everyday use.",
        "image": "https://fakestoreapi.com/img/backpack.jpg",
        "rating": {"rate": 4.6, "count": 259},
    }


@pytest.fixture
def raw_products(raw_product):
    """A small batch: one valid product, one missing a required field."""
    invalid = {
        "id": 2,
        "title": "",
        "price": 19.99,
        "category": "electronics",
    }
    return [raw_product, invalid]
