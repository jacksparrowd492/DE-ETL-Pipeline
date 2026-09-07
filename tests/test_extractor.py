"""
test_extractor.py
------------------
Mock tests for extract/extractor.py — no real HTTP call is ever made;
requests.get is patched so the tests are fast and deterministic.
"""

import requests

from extract.extractor import extract_products


class _FakeResponse:
    """Minimal stand-in for requests.Response."""

    def __init__(self, payload, status_ok=True):
        self._payload = payload
        self._status_ok = status_ok

    def raise_for_status(self):
        if not self._status_ok:
            raise requests.exceptions.HTTPError("boom")

    def json(self):
        return self._payload


def test_extract_products_returns_api_payload(mocker):
    payload = [{"id": 1, "title": "Item", "price": 9.99, "category": "misc"}]
    mock_get = mocker.patch(
        "extract.extractor.requests.get",
        return_value=_FakeResponse(payload),
    )

    result = extract_products()

    assert result == payload
    mock_get.assert_called_once()


def test_extract_products_calls_configured_url_with_timeout(mocker):
    from config import config

    mock_get = mocker.patch(
        "extract.extractor.requests.get",
        return_value=_FakeResponse([]),
    )

    extract_products()

    args, kwargs = mock_get.call_args
    assert args[0] == config.FAKESTORE_API_URL
    assert kwargs.get("timeout") == 10


def test_extract_products_raises_on_http_error(mocker):
    mocker.patch(
        "extract.extractor.requests.get",
        return_value=_FakeResponse(None, status_ok=False),
    )

    try:
        extract_products()
        assert False, "expected HTTPError to propagate"
    except requests.exceptions.HTTPError:
        pass


def test_extract_products_raises_on_connection_error(mocker):
    mocker.patch(
        "extract.extractor.requests.get",
        side_effect=requests.exceptions.ConnectionError("no network"),
    )

    try:
        extract_products()
        assert False, "expected ConnectionError to propagate"
    except requests.exceptions.ConnectionError:
        pass
