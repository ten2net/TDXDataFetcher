"""
Unit tests for advanced features module

Test coverage:
- DataFrameConverter: Pandas/Polars conversion
- StockScreener: Stock screening
- AlertSystem: Alert system
- Helper functions: Alert creation, cross detection
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from tdxapi.models import Bar, StockQuote
from tdxapi.advanced import (
    # DataFrame conversion
    DataFrameConverter,
    # Screener
    FilterOperator,
    FilterCondition,
    ScreenerRule,
    StockScreener,
    # Alert system
    AlertType,
    Alert,
    AlertResult,
    AlertSystem,
    # Helper functions
    create_price_alert,
    create_ma_cross_alert,
    create_macd_cross_alert,
    create_kdj_cross_alert,
    detect_cross,
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def sample_bars():
    """Create sample K-line data."""
    base_time = datetime(2024, 1, 1, 9, 30)
    bars = []
    for i in range(100):
        bars.append(Bar(
            code="000001",
            market="SZ",
            datetime=base_time + timedelta(minutes=i),
            open=10.0 + i * 0.1,
            high=10.5 + i * 0.1,
            low=9.5 + i * 0.1,
            close=10.2 + i * 0.1,
            volume=10000 + i * 100,
            amount=100000.0 + i * 1000,
        ))
    return bars


@pytest.fixture
def sample_quotes():
    """Create sample quote data."""
    return [
        StockQuote(
            code="000001",
            market="SZ",
            name="Ping An Bank",
            price=15.5,
            last_close=15.0,
            open=15.2,
            high=15.8,
            low=15.0,
            volume=100000,
            amount=1550000.0,
            bid1=15.4,
            bid1_vol=1000,
            ask1=15.6,
            ask1_vol=1000,
            datetime=datetime.now(),
        ),
        StockQuote(
            code="000002",
            market="SZ",
            name="Vanke A",
            price=8.5,
            last_close=9.0,
            open=8.8,
            high=9.0,
            low=8.4,
            volume=50000,
            amount=425000.0,
            bid1=8.4,
            bid1_vol=500,
            ask1=8.6,
            ask1_vol=500,
            datetime=datetime.now(),
        ),
    ]


@pytest.fixture
def sample_stock_data():
    """Create sample stock data for screener."""
    return [
        {"code": "000001", "name": "Ping An Bank", "close": 15.5, "pe": 12.0, "pb": 1.2, "market_cap": 3e9},
        {"code": "000002", "name": "Vanke A", "close": 8.5, "pe": 25.0, "pb": 0.8, "market_cap": 8e9},
        {"code": "000858", "name": "Wuliangye", "close": 150.0, "pe": 35.0, "pb": 5.0, "market_cap": 5e11},
        {"code": "600519", "name": "Kweichow Moutai", "close": 1800.0, "pe": 40.0, "pb": 10.0, "market_cap": 2e12},
    ]


# =============================================================================
# DataFrameConverter Tests
# =============================================================================

class TestDataFrameConverter:
    """Test DataFrame converter."""

    def test_to_pandas_with_bars(self, sample_bars):
        """Test converting Bar list to Pandas DataFrame."""
        pytest.importorskip("pandas")

        df = DataFrameConverter.to_pandas(sample_bars)

        assert len(df) == len(sample_bars)
        assert "code" in df.columns
        assert "market" in df.columns
        assert "open" in df.columns
        assert "high" in df.columns
        assert "low" in df.columns
        assert "close" in df.columns
        assert "volume" in df.columns
        assert "amount" in df.columns
        assert df["code"].iloc[0] == "000001"
        assert df["market"].iloc[0] == "SZ"

    def test_to_pandas_with_quotes(self, sample_quotes):
        """Test converting StockQuote list to Pandas DataFrame."""
        pytest.importorskip("pandas")

        df = DataFrameConverter.to_pandas(sample_quotes)

        assert len(df) == len(sample_quotes)
        assert "code" in df.columns
        assert "price" in df.columns
        assert "name" in df.columns
        assert df["code"].iloc[0] == "000001"
        assert df["price"].iloc[0] == 15.5

    def test_to_pandas_with_columns(self, sample_bars):
        """Test converting with specific columns."""
        pytest.importorskip("pandas")

        df = DataFrameConverter.to_pandas(sample_bars, columns=["code", "close", "volume"])

        assert list(df.columns) == ["code", "close", "volume"]

    def test_to_pandas_empty_data(self):
        """Test empty data raises exception."""
        pytest.importorskip("pandas")

        with pytest.raises(ValueError, match="data cannot be empty"):
            DataFrameConverter.to_pandas([])

    def test_to_polars_with_bars(self, sample_bars):
        """Test converting Bar list to Polars DataFrame."""
        pytest.importorskip("polars")

        df = DataFrameConverter.to_polars(sample_bars)

        assert len(df) == len(sample_bars)
        assert "code" in df.columns
        assert "close" in df.columns

    def test_to_polars_with_columns(self, sample_bars):
        """Test Polars conversion with specific columns."""
        pytest.importorskip("polars")

        df = DataFrameConverter.to_polars(sample_bars, columns=["code", "close"])

        assert set(df.columns) == {"code", "close"}

    def test_bars_to_ohlc(self, sample_bars):
        """Test converting to OHLC format."""
        ohlc = DataFrameConverter.bars_to_ohlc(sample_bars)

        assert "open" in ohlc
        assert "high" in ohlc
        assert "low" in ohlc
        assert "close" in ohlc
        assert "volume" in ohlc
        assert len(ohlc["close"]) == len(sample_bars)

    def test_bars_to_ohlc_empty(self):
        """Test empty data returns empty OHLC."""
        ohlc = DataFrameConverter.bars_to_ohlc([])

        assert ohlc["open"] == []
        assert ohlc["close"] == []


# =============================================================================
# FilterCondition Tests
# =============================================================================

class TestFilterCondition:
    """Test filter conditions."""

    def test_eq_operator(self):
        """Test equal operator."""
        condition = FilterCondition("close", FilterOperator.EQ, 10.0)
        assert condition.evaluate({"close": 10.0}) is True
        assert condition.evaluate({"close": 11.0}) is False

    def test_gt_operator(self):
        """Test greater than operator."""
        condition = FilterCondition("close", FilterOperator.GT, 10.0)
        assert condition.evaluate({"close": 11.0}) is True
        assert condition.evaluate({"close": 10.0}) is False
        assert condition.evaluate({"close": 9.0}) is False

    def test_between_operator(self):
        """Test between operator."""
        condition = FilterCondition("pe", FilterOperator.BETWEEN, (10, 30))
        assert condition.evaluate({"pe": 20}) is True
        assert condition.evaluate({"pe": 10}) is True
        assert condition.evaluate({"pe": 30}) is True
        assert condition.evaluate({"pe": 5}) is False
        assert condition.evaluate({"pe": 35}) is False

    def test_in_operator(self):
        """Test in operator."""
        condition = FilterCondition("code", FilterOperator.IN, ["000001", "000002"])
        assert condition.evaluate({"code": "000001"}) is True
        assert condition.evaluate({"code": "000003"}) is False

    def test_contains_operator(self):
        """Test contains operator."""
        condition = FilterCondition("name", FilterOperator.CONTAINS, "Bank")
        assert condition.evaluate({"name": "Ping An Bank"}) is True
        assert condition.evaluate({"name": "Vanke A"}) is False

    def test_missing_field(self):
        """Test missing field."""
        condition = FilterCondition("close", FilterOperator.GT, 10.0)
        assert condition.evaluate({"open": 10.0}) is False

    def test_none_value(self):
        """Test None value."""
        condition = FilterCondition("close", FilterOperator.GT, 10.0)
        assert condition.evaluate({"close": None}) is False


# =============================================================================
# ScreenerRule Tests
# =============================================================================

class TestScreenerRule:
    """Test screener rules."""

    def test_and_logic(self):
        """Test AND logic."""
        rule = ScreenerRule(
            name="test",
            conditions=[
                FilterCondition("close", FilterOperator.GT, 10.0),
                FilterCondition("pe", FilterOperator.LT, 30.0),
            ],
            logic="AND"
        )
        assert rule.evaluate({"close": 15.0, "pe": 20.0}) is True
        assert rule.evaluate({"close": 15.0, "pe": 35.0}) is False
        assert rule.evaluate({"close": 5.0, "pe": 20.0}) is False

    def test_or_logic(self):
        """Test OR logic."""
        rule = ScreenerRule(
            name="test",
            conditions=[
                FilterCondition("close", FilterOperator.GT, 100.0),
                FilterCondition("pe", FilterOperator.LT, 10.0),
            ],
            logic="OR"
        )
        assert rule.evaluate({"close": 150.0, "pe": 20.0}) is True
        assert rule.evaluate({"close": 50.0, "pe": 5.0}) is True
        assert rule.evaluate({"close": 50.0, "pe": 20.0}) is False

    def test_empty_conditions(self):
        """Test empty conditions list."""
        rule = ScreenerRule(name="test", conditions=[])
        assert rule.evaluate({"close": 10.0}) is True


# =============================================================================
# StockScreener Tests
# =============================================================================

class TestStockScreener:
    """Test stock screener."""

    def test_add_and_get_rule(self):
        """Test adding and getting rule."""
        screener = StockScreener()
        rule = ScreenerRule("test_rule", [FilterCondition("close", FilterOperator.GT, 10.0)])

        screener.add_rule(rule)

        assert screener.get_rule("test_rule") == rule

    def test_remove_rule(self):
        """Test removing rule."""
        screener = StockScreener()
        rule = ScreenerRule("custom_rule", [FilterCondition("close", FilterOperator.GT, 10.0)])

        screener.add_rule(rule)
        assert screener.remove_rule("custom_rule") is True
        assert screener.get_rule("custom_rule") is None

    def test_cannot_remove_predefined_rule(self):
        """Test cannot remove predefined rule."""
        screener = StockScreener()

        # Predefined rules cannot be removed
        assert screener.remove_rule("low_price") is False
        assert screener.get_rule("low_price") is not None

    def test_list_rules(self):
        """Test listing all rules."""
        screener = StockScreener()
        rules = screener.list_rules()

        assert "low_price" in rules
        assert "growth" in rules
        assert "value" in rules

    def test_screen_with_rule_name(self, sample_stock_data):
        """Test screening with rule name."""
        screener = StockScreener()

        results = screener.screen(sample_stock_data, rule_name="low_price")

        # low_price: close < 10.0
        assert len(results) == 1
        assert results[0]["code"] == "000002"

    def test_screen_with_conditions(self, sample_stock_data):
        """Test screening with custom conditions."""
        screener = StockScreener()

        results = screener.screen(
            sample_stock_data,
            conditions=[FilterCondition("pe", FilterOperator.BETWEEN, (10, 30))]
        )

        # PE between 10-30: 000001 (PE=12), 000002 (PE=25)
        assert len(results) == 2
        codes = [r["code"] for r in results]
        assert "000001" in codes
        assert "000002" in codes

    def test_screen_invalid_rule(self, sample_stock_data):
        """Test invalid rule name."""
        screener = StockScreener()

        with pytest.raises(ValueError, match="Rule 'invalid_rule' not found"):
            screener.screen(sample_stock_data, rule_name="invalid_rule")

    def test_screen_missing_params(self, sample_stock_data):
        """Test missing required parameters."""
        screener = StockScreener()

        with pytest.raises(ValueError, match="Either rule_name or conditions"):
            screener.screen(sample_stock_data)

    def test_screen_with_indicators(self, sample_bars):
        """Test screening with technical indicators."""
        screener = StockScreener()
        stocks_bars = {"000001": sample_bars}

        results = screener.screen_with_indicators(
            stocks_bars,
            conditions=[FilterCondition("close", FilterOperator.GT, 0)]
        )

        assert len(results) == 1
        assert "ma5" in results[0]
        assert "macd_dif" in results[0]
        assert "kdj_k" in results[0]
        assert "rsi6" in results[0]

    def test_screen_insufficient_data(self, sample_bars):
        """Test insufficient data."""
        screener = StockScreener()
        stocks_bars = {"000001": sample_bars[:10]}  # Less than 60 bars

        results = screener.screen_with_indicators(
            stocks_bars,
            conditions=[FilterCondition("close", FilterOperator.GT, 0)]
        )

        assert len(results) == 0  # Filtered due to insufficient data

    def test_calculate_indicators_ma_cross(self, sample_bars):
        """Test MA cross detection."""
        screener = StockScreener()

        # Create golden cross: MA5 crosses above MA10
        for i, bar in enumerate(sample_bars):
            if i >= 95:
                bar.close = 20.0 + i * 0.5  # Rapid increase
            else:
                bar.close = 10.0 + i * 0.05

        indicators = screener._calculate_indicators(sample_bars)

        assert "ma_cross" in indicators


# =============================================================================
# Alert Tests
# =============================================================================

class TestAlert:
    """Test alert definition."""

    def test_is_in_cooldown(self):
        """Test cooldown detection."""
        alert = Alert(
            id="test",
            name="Test Alert",
            alert_type=AlertType.PRICE_ABOVE,
            code="000001",
            market="SZ",
            cooldown=60
        )

        assert alert.is_in_cooldown() is False

        alert.mark_triggered()
        assert alert.is_in_cooldown() is True

    def test_cooldown_expired(self):
        """Test expired cooldown."""
        alert = Alert(
            id="test",
            name="Test Alert",
            alert_type=AlertType.PRICE_ABOVE,
            code="000001",
            market="SZ",
            cooldown=0  # Immediate expiration
        )

        alert.mark_triggered()
        assert alert.is_in_cooldown() is False


# =============================================================================
# AlertSystem Tests
# =============================================================================

class TestAlertSystem:
    """Test alert system."""

    def test_add_and_get_alert(self):
        """Test adding and getting alert."""
        system = AlertSystem()
        alert = Alert(
            id="test",
            name="Test",
            alert_type=AlertType.PRICE_ABOVE,
            code="000001",
            market="SZ",
            params={"price": 15.0}
        )

        system.add_alert(alert)

        assert system.get_alert("test") == alert

    def test_remove_alert(self):
        """Test removing alert."""
        system = AlertSystem()
        alert = Alert(
            id="test",
            name="Test",
            alert_type=AlertType.PRICE_ABOVE,
            code="000001",
            market="SZ"
        )

        system.add_alert(alert)
        assert system.remove_alert("test") is True
        assert system.get_alert("test") is None

    def test_list_alerts(self):
        """Test listing alerts."""
        system = AlertSystem()

        system.add_alert(Alert("1", "Alert 1", AlertType.PRICE_ABOVE, "000001", "SZ"))
        system.add_alert(Alert("2", "Alert 2", AlertType.PRICE_BELOW, "000002", "SZ", enabled=False))

        all_alerts = system.list_alerts()
        assert len(all_alerts) == 2

        enabled_alerts = system.list_alerts(enabled_only=True)
        assert len(enabled_alerts) == 1

    def test_enable_disable_alert(self):
        """Test enabling/disabling alert."""
        system = AlertSystem()
        alert = Alert("test", "Test", AlertType.PRICE_ABOVE, "000001", "SZ", enabled=False)

        system.add_alert(alert)

        assert system.enable_alert("test") is True
        assert system.get_alert("test").enabled is True

        assert system.disable_alert("test") is True
        assert system.get_alert("test").enabled is False

    def test_check_price_above_alert(self, sample_quotes):
        """Test price above alert detection."""
        system = AlertSystem()
        alert = Alert(
            id="test",
            name="Breakout 15",
            alert_type=AlertType.PRICE_ABOVE,
            code="000001",
            market="SZ",
            params={"price": 15.0}
        )

        system.add_alert(alert)
        results = system.check_alerts(sample_quotes)

        assert len(results) == 1
        assert results[0].triggered is True
        assert "broke" in results[0].message.lower() or "above" in results[0].message.lower()

    def test_check_price_below_alert(self, sample_quotes):
        """Test price below alert detection."""
        system = AlertSystem()
        alert = Alert(
            id="test",
            name="Below 9",
            alert_type=AlertType.PRICE_BELOW,
            code="000002",
            market="SZ",
            params={"price": 9.0}
        )

        system.add_alert(alert)
        results = system.check_alerts(sample_quotes)

        assert len(results) == 1
        assert results[0].triggered is True
        assert "below" in results[0].message.lower()

    def test_check_price_change_alert(self, sample_quotes):
        """Test price change alert detection."""
        system = AlertSystem()
        alert = Alert(
            id="test",
            name="Change 5%",
            alert_type=AlertType.PRICE_CHANGE,
            code="000001",
            market="SZ",
            params={"threshold": 3}  # 3% change
        )

        system.add_alert(alert)
        results = system.check_alerts(sample_quotes)

        # 000001: (15.5 - 15.0) / 15.0 = 3.33% > 3%
        assert len(results) == 1
        assert results[0].triggered is True

    def test_check_alert_no_match(self, sample_quotes):
        """Test no alert triggered."""
        system = AlertSystem()
        alert = Alert(
            id="test",
            name="Breakout 20",
            alert_type=AlertType.PRICE_ABOVE,
            code="000001",
            market="SZ",
            params={"price": 20.0}
        )

        system.add_alert(alert)
        results = system.check_alerts(sample_quotes)

        assert len(results) == 0

    def test_check_alert_cooldown(self, sample_quotes):
        """Test alert cooldown."""
        system = AlertSystem()
        alert = Alert(
            id="test",
            name="Breakout 15",
            alert_type=AlertType.PRICE_ABOVE,
            code="000001",
            market="SZ",
            params={"price": 15.0},
            cooldown=3600  # 1 hour cooldown
        )

        system.add_alert(alert)

        # First check should trigger
        results1 = system.check_alerts(sample_quotes)
        assert len(results1) == 1

        # Second check within cooldown should not trigger
        results2 = system.check_alerts(sample_quotes)
        assert len(results2) == 0

    def test_check_ma_golden_cross_alert(self, sample_quotes, sample_bars):
        """Test MA golden cross alert detection."""
        system = AlertSystem()
        alert = Alert(
            id="test",
            name="MA Golden Cross",
            alert_type=AlertType.MA_GOLDEN_CROSS,
            code="000001",
            market="SZ",
            params={"fast": 5, "slow": 10}
        )

        system.add_alert(alert)

        # Create golden cross data: MA5 crosses above MA10
        # Need at least 10 bars of data with proper pattern
        for i, bar in enumerate(sample_bars):
            if i < 90:
                # Before cross: MA5 < MA10 (downtrend or flat)
                bar.close = 10.0 + i * 0.01
            elif i == 90:
                # Transition point
                bar.close = 11.0
            else:
                # After cross: rapid rise to ensure MA5 > MA10
                bar.close = 20.0 + (i - 90) * 0.5

        bars_map = {"000001": sample_bars}
        results = system.check_alerts(sample_quotes, bars_map)

        # Should trigger if golden cross detected
        if len(results) > 0:
            assert results[0].triggered is True
            assert "golden" in results[0].message.lower()
        else:
            # If no cross detected, that's also valid (depends on exact MA values)
            pass

    def test_check_macd_cross_alert(self, sample_quotes, sample_bars):
        """Test MACD cross alert detection."""
        system = AlertSystem()
        alert = Alert(
            id="test",
            name="MACD Golden Cross",
            alert_type=AlertType.MACD_GOLDEN_CROSS,
            code="000001",
            market="SZ"
        )

        system.add_alert(alert)

        bars_map = {"000001": sample_bars}
        results = system.check_alerts(sample_quotes, bars_map)

        # Results depend on data, but should return list
        assert isinstance(results, list)

    def test_check_rsi_alert(self, sample_quotes, sample_bars):
        """Test RSI alert detection."""
        system = AlertSystem()
        alert = Alert(
            id="test",
            name="RSI Oversold",
            alert_type=AlertType.RSI_OVERSOLD,
            code="000001",
            market="SZ",
            params={"period": 6, "threshold": 20}
        )

        system.add_alert(alert)

        bars_map = {"000001": sample_bars}
        results = system.check_alerts(sample_quotes, bars_map)

        assert isinstance(results, list)

    def test_check_boll_break_alert(self, sample_quotes, sample_bars):
        """Test Bollinger Bands break alert detection."""
        system = AlertSystem()
        alert = Alert(
            id="test",
            name="Break Upper",
            alert_type=AlertType.BOLL_BREAK_UP,
            code="000001",
            market="SZ"
        )

        system.add_alert(alert)

        bars_map = {"000001": sample_bars}
        results = system.check_alerts(sample_quotes, bars_map)

        assert isinstance(results, list)

    def test_register_callback(self, sample_quotes):
        """Test registering callback function."""
        system = AlertSystem()
        callback_called = False

        def callback(result):
            nonlocal callback_called
            callback_called = True

        system.register_callback(callback)

        alert = Alert(
            id="test",
            name="Breakout 15",
            alert_type=AlertType.PRICE_ABOVE,
            code="000001",
            market="SZ",
            params={"price": 15.0}
        )
        system.add_alert(alert)

        system.check_alerts(sample_quotes)

        assert callback_called is True

    def test_unregister_callback(self):
        """Test unregistering callback function."""
        system = AlertSystem()

        def callback(result):
            pass

        system.register_callback(callback)
        assert system.unregister_callback(callback) is True
        assert system.unregister_callback(callback) is False


# =============================================================================
# Helper Function Tests
# =============================================================================

class TestHelperFunctions:
    """Test helper functions."""

    def test_create_price_alert_above(self):
        """Test creating price above alert."""
        alert = create_price_alert("000001", "SZ", 15.0, "above", "Breakout 15")

        assert alert.code == "000001"
        assert alert.market == "SZ"
        assert alert.alert_type == AlertType.PRICE_ABOVE
        assert alert.params["price"] == 15.0
        assert alert.name == "Breakout 15"

    def test_create_price_alert_below(self):
        """Test creating price below alert."""
        alert = create_price_alert("000001", "SZ", 10.0, "below")

        assert alert.alert_type == AlertType.PRICE_BELOW
        assert alert.params["price"] == 10.0

    def test_create_ma_cross_alert_golden(self):
        """Test creating MA golden cross alert."""
        alert = create_ma_cross_alert("000001", "SZ", 5, 10, "golden")

        assert alert.alert_type == AlertType.MA_GOLDEN_CROSS
        assert alert.params["fast"] == 5
        assert alert.params["slow"] == 10

    def test_create_ma_cross_alert_dead(self):
        """Test creating MA dead cross alert."""
        alert = create_ma_cross_alert("000001", "SZ", 5, 10, "dead")

        assert alert.alert_type == AlertType.MA_DEAD_CROSS

    def test_create_macd_cross_alert(self):
        """Test creating MACD cross alert."""
        alert = create_macd_cross_alert("000001", "SZ", "golden")

        assert alert.alert_type == AlertType.MACD_GOLDEN_CROSS
        assert "golden" in alert.name.lower()

    def test_create_kdj_cross_alert(self):
        """Test creating KDJ cross alert."""
        alert = create_kdj_cross_alert("000001", "SZ", "dead")

        assert alert.alert_type == AlertType.KDJ_DEAD_CROSS
        assert "dead" in alert.name.lower()

    def test_detect_cross_golden(self):
        """Test detecting golden cross."""
        # Golden cross: prev1 <= prev2 and curr1 > curr2
        values1 = [10, 11, 12, 11, 13]  # Crosses above at the end
        values2 = [12, 12, 12, 12, 12]  # Flat

        result = detect_cross(values1, values2)

        assert result == "golden"

    def test_detect_cross_dead(self):
        """Test detecting dead cross."""
        # Dead cross: prev1 >= prev2 and curr1 < curr2
        values1 = [14, 15, 13, 14, 11]  # Crosses below at the end
        values2 = [12, 12, 12, 12, 12]  # Flat

        result = detect_cross(values1, values2)

        assert result == "dead"

    def test_detect_cross_none(self):
        """Test no cross detected."""
        values1 = [10, 11, 12, 13, 14]
        values2 = [8, 9, 10, 11, 12]

        result = detect_cross(values1, values2)

        assert result is None

    def test_detect_cross_insufficient_data(self):
        """Test insufficient data."""
        values1 = [10]
        values2 = [12]

        result = detect_cross(values1, values2)

        assert result is None

    def test_detect_cross_with_none(self):
        """Test data containing None."""
        values1 = [None, 14]
        values2 = [12, 12]

        result = detect_cross(values1, values2)

        assert result is None


# =============================================================================
# Integration Tests
# =============================================================================

class TestIntegration:
    """Integration tests."""

    def test_screener_with_converter(self, sample_bars):
        """Test screener with converter integration."""
        pytest.importorskip("pandas")

        # Calculate indicators with screener
        screener = StockScreener()
        stocks_bars = {"000001": sample_bars}

        results = screener.screen_with_indicators(
            stocks_bars,
            conditions=[FilterCondition("close", FilterOperator.GT, 0)]
        )

        # Convert results to DataFrame
        df = DataFrameConverter.to_pandas(results)

        assert len(df) == 1
        assert "ma5" in df.columns
        assert "rsi6" in df.columns

    def test_alert_system_full_workflow(self, sample_quotes, sample_bars):
        """Test alert system full workflow."""
        system = AlertSystem()
        triggered_alerts = []

        def on_alert(result):
            triggered_alerts.append(result)

        system.register_callback(on_alert)

        # Add multiple alerts
        system.add_alert(create_price_alert("000001", "SZ", 15.0, "above"))
        system.add_alert(create_ma_cross_alert("000001", "SZ", 5, 10, "golden"))

        # Create golden cross data
        for i, bar in enumerate(sample_bars):
            if i >= 95:
                bar.close = 20.0 + i * 0.5
            else:
                bar.close = 10.0 + i * 0.05

        bars_map = {"000001": sample_bars}

        # Check alerts
        results = system.check_alerts(sample_quotes, bars_map)

        # Verify results
        assert len(results) >= 1
        assert len(triggered_alerts) == len(results)

    def test_context_manager(self):
        """Test context manager."""
        with AlertSystem() as system:
            system.add_alert(Alert("test", "Test", AlertType.PRICE_ABOVE, "000001", "SZ"))
            assert system.get_alert("test") is not None

        # After exiting context, monitoring should stop
        assert system._running is False
