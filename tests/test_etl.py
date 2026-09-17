import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from datetime import datetime, timezone
from decimal import Decimal
from etl.etl import floor_to_interval, transform


def test_floor_to_interval_rounds_down_to_nearest_5_minutes():
    dt = datetime(2026, 9, 14, 14, 37, 22)
    result = floor_to_interval(dt, 5)
    assert result == datetime(2026, 9, 14, 14, 35, 0)


def test_floor_to_interval_exact_boundary_stays_unchanged():
    dt = datetime(2026, 9, 14, 14, 35, 0)
    result = floor_to_interval(dt, 5)
    assert result == datetime(2026, 9, 14, 14, 35, 0)


def test_floor_to_interval_discards_seconds_and_microseconds():
    dt = datetime(2026, 9, 14, 14, 39, 59, 999999)
    result = floor_to_interval(dt, 5)
    assert result == datetime(2026, 9, 14, 14, 35, 0)


class FakeRow:
    """Simule une ligne Cassandra (namedtuple-like) pour les tests."""
    def __init__(self, trade_time, price, quantity):
        self.trade_time = trade_time
        self.price = price
        self.quantity = quantity


def test_transform_computes_correct_ohlc_for_single_candle():
    rows = [
        FakeRow(datetime(2026, 9, 14, 14, 35, 0, tzinfo=timezone.utc), Decimal("100.00"), Decimal("1.0")),
        FakeRow(datetime(2026, 9, 14, 14, 36, 0, tzinfo=timezone.utc), Decimal("105.00"), Decimal("2.0")),
        FakeRow(datetime(2026, 9, 14, 14, 37, 0, tzinfo=timezone.utc), Decimal("95.00"), Decimal("0.5")),
        FakeRow(datetime(2026, 9, 14, 14, 38, 0, tzinfo=timezone.utc), Decimal("102.00"), Decimal("1.5")),
    ]

    candles = transform(rows, symbol="BTCUSDT")

    assert len(candles) == 1
    candle = candles[0]
    assert candle["symbol"] == "BTCUSDT"
    assert candle["open_price"] == Decimal("100.00")
    assert candle["high_price"] == Decimal("105.00")
    assert candle["low_price"] == Decimal("95.00")
    assert candle["close_price"] == Decimal("102.00")
    assert candle["volume"] == Decimal("5.0")
    assert candle["trade_count"] == 4


def test_transform_splits_into_multiple_candles_across_intervals():
    rows = [
        FakeRow(datetime(2026, 9, 14, 14, 35, 0, tzinfo=timezone.utc), Decimal("100.00"), Decimal("1.0")),
        FakeRow(datetime(2026, 9, 14, 14, 40, 0, tzinfo=timezone.utc), Decimal("110.00"), Decimal("1.0")),
    ]

    candles = transform(rows, symbol="BTCUSDT")

    assert len(candles) == 2


def test_transform_empty_rows_returns_empty_list():
    candles = transform([], symbol="BTCUSDT")
    assert candles == []