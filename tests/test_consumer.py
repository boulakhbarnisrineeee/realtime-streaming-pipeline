import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from decimal import Decimal
from datetime import datetime, timezone
from consumer.consumer import transform


def test_transform_converts_string_prices_to_decimal():
    raw = {
        "trade_id": 12345,
        "symbol": "BTCUSDT",
        "price": "77289.09000000",
        "quantity": "0.02929000",
        "quote_qty": "2263.79744610",
        "trade_time": 1789267497801,
        "is_buyer_maker": False,
    }

    result = transform(raw)

    assert result["price"] == Decimal("77289.09000000")
    assert result["quantity"] == Decimal("0.02929000")
    assert result["quote_qty"] == Decimal("2263.79744610")


def test_transform_converts_milliseconds_timestamp_to_utc_datetime():
    raw = {
        "trade_id": 12345,
        "symbol": "BTCUSDT",
        "price": "77289.09000000",
        "quantity": "0.02929000",
        "quote_qty": "2263.79744610",
        "trade_time": 1789267497801,
        "is_buyer_maker": False,
    }

    result = transform(raw)

    expected = datetime.fromtimestamp(1789267497801 / 1000, tz=timezone.utc)
    assert result["trade_time"] == expected
    assert result["trade_time"].tzinfo == timezone.utc


def test_transform_preserves_trade_id_symbol_and_buyer_maker_flag():
    raw = {
        "trade_id": 999,
        "symbol": "ETHUSDT",
        "price": "3000.00",
        "quantity": "1.0",
        "quote_qty": "3000.00",
        "trade_time": 1700000000000,
        "is_buyer_maker": True,
    }

    result = transform(raw)

    assert result["trade_id"] == 999
    assert result["symbol"] == "ETHUSDT"
    assert result["is_buyer_maker"] is True