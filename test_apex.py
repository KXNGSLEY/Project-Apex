import asyncio
import os
import tempfile
import unittest
from dataclasses import replace
from unittest.mock import patch

from pathlib import Path

import pandas as pd

import apex


class ApexSafetyTests(unittest.TestCase):
    @staticmethod
    def make_opportunity(
        exchange="okx",
        symbol="BTC/USDT:USDT",
        direction=apex.TradeDirection.LONG,
    ):
        return apex.TradeOpportunity(
            exchange=exchange,
            symbol=symbol,
            direction=direction,
            status="READY",
            entry_method=apex.EntryMethod.LIMIT_RETEST,
            entry_zone_low=99,
            entry_zone_high=101,
            preferred_entry=100,
            stop_loss=apex.StopLossPlan(price=90, reason="test", atr_distance=1),
            take_profit=apex.TakeProfitPlan(tp1=120, tp2=130, tp3=140),
            risk_reward=apex.RiskRewardResult(
                entry=100, stop=90, tp1=120, tp2=130, tp3=140,
                gross_risk=10, gross_reward_tp1=20, net_risk=10.5,
                net_reward_tp1=19.5, net_rr_tp1=1.857,
            ),
            position_size=apex.PositionSizeResult(
                risk_usd=10, position_size_coins=1,
                notional_usd=100, leverage_suggested=0.1,
                margin_required=10,
            ),
            score=70,
        )

    def test_paper_is_default_and_live_requires_confirmation(self):
        adapter = apex.LiveExecutionAdapter()
        adapter.mode = "live"
        self.assertFalse(adapter.validate_configuration()[0])

        old_confirm = apex.Config.LIVE_CONFIRMATION
        old_kill = apex.Config.LIVE_KILL_SWITCH
        old_key = apex.Config.OKX_API_KEY
        old_secret = apex.Config.OKX_API_SECRET
        old_password = apex.Config.OKX_API_PASSPHRASE
        try:
            apex.Config.LIVE_CONFIRMATION = apex.Config.LIVE_CONFIRMATION_TOKEN
            apex.Config.LIVE_KILL_SWITCH = False
            apex.Config.OKX_API_KEY = "key"
            apex.Config.OKX_API_SECRET = "secret"
            apex.Config.OKX_API_PASSPHRASE = "password"
            valid, _ = adapter.validate_configuration()
            self.assertTrue(valid)
        finally:
            apex.Config.LIVE_CONFIRMATION = old_confirm
            apex.Config.LIVE_KILL_SWITCH = old_kill
            apex.Config.OKX_API_KEY = old_key
            apex.Config.OKX_API_SECRET = old_secret
            apex.Config.OKX_API_PASSPHRASE = old_password

    def test_paper_trade_close_accounting(self):
        tracker = apex.PerformanceTracker(1000)
        pos = apex.PaperPosition(
            id="test",
            exchange="okx",
            symbol="BTC/USDT:USDT",
            direction=apex.TradeDirection.LONG,
            entry_price=100,
            size=1,
            stop_loss=90,
            take_profit_1=120,
            take_profit_2=130,
            take_profit_3=140,
            opened_at=1,
            fees_paid=1,
        )
        tracker.open_position(pos)
        closed = tracker.close_position("test", 120, "take_profit_1", fee=1)
        self.assertIsNotNone(closed)
        self.assertEqual(closed.realized_pnl, 18)
        self.assertEqual(closed.r_multiple, 1.8)
        self.assertEqual(tracker.balance, 1018)

    def test_paper_funding_is_applied_once_to_equity_and_final_pnl(self):
        tracker = apex.PerformanceTracker(1000)
        now = apex.utc_ms()
        position = apex.PaperPosition(
            id="funded", exchange="okx", symbol="BTC/USDT:USDT",
            direction=apex.TradeDirection.LONG, entry_price=100, size=2,
            stop_loss=90, take_profit_1=None, take_profit_2=None,
            take_profit_3=None, opened_at=(now - 9 * 60 * 60 * 1000) / 1000,
            next_funding_time=now - 1000,
        )
        tracker.open_position(position)
        funding = apex.FundingSnapshot(
            exchange="okx", symbol=position.symbol, timestamp=now,
            funding_rate=0.001, next_funding_time=now + 8 * 60 * 60 * 1000,
            mark_price=100,
        )
        simulator = apex.PaperTradingSimulator(tracker)
        simulator.update_with_price(
            "okx", position.symbol, 100, 100, 100, funding=funding
        )
        simulator.update_with_price(
            "okx", position.symbol, 100, 100, 100, funding=funding
        )
        self.assertAlmostEqual(position.funding_paid, 0.2)
        self.assertAlmostEqual(tracker.balance, 999.8)

        closed = tracker.close_position(position.id, 110, "test")
        self.assertAlmostEqual(closed.realized_pnl, 19.8)
        self.assertAlmostEqual(tracker.balance, 1019.8)

    def test_partial_exit_updates_drawdown_and_restore_rebuilds_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = apex.MarketDatabase(Path(tmp) / "performance.db")
            db.connect()
            tracker = apex.PerformanceTracker(1000)
            partial = apex.PaperPosition(
                id="partial-loss", exchange="okx", symbol="BTC/USDT:USDT",
                direction=apex.TradeDirection.LONG, entry_price=100, size=1,
                stop_loss=90, take_profit_1=120, take_profit_2=None,
                take_profit_3=None, opened_at=1,
            )
            tracker.open_position(partial)
            tracker.close_partial(partial.id, 80, 0.5, 0)
            self.assertAlmostEqual(tracker.max_drawdown, 0.01)

            def close_trade(position_id, entry, exit_price):
                position = apex.PaperPosition(
                    id=position_id, exchange="okx", symbol=f"{position_id}/USDT:USDT",
                    direction=apex.TradeDirection.LONG, entry_price=entry, size=1,
                    stop_loss=90, take_profit_1=None, take_profit_2=None,
                    take_profit_3=None, opened_at=apex.time.time(),
                )
                tracker.open_position(position)
                tracker.close_position(position_id, exit_price, "test")
                db.save_paper_position(position)
                db.save_paper_equity(tracker.balance)

            close_trade("win", 100, 120)
            close_trade("loss-1", 100, 80)
            close_trade("loss-2", 100, 80)
            expected_drawdown = tracker.max_drawdown
            restored = apex.PerformanceTracker(1000)
            restored.restore_from_database(db)
            self.assertAlmostEqual(restored.max_drawdown, expected_drawdown)
            self.assertEqual(restored.consecutive_losses, 2)
            self.assertEqual(restored.max_consecutive_losses, 2)
            db.close()

    def test_restore_rebuilds_equity_from_closed_trades_without_samples(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = apex.MarketDatabase(Path(tmp) / "legacy-performance.db")
            db.connect()
            for position_id, pnl, closed_at in (
                ("legacy-win", 100, 10),
                ("legacy-loss", -200, 20),
            ):
                db.save_paper_position(apex.PaperPosition(
                    id=position_id, exchange="okx", symbol=f"{position_id}/USDT:USDT",
                    direction=apex.TradeDirection.LONG, entry_price=100, size=1,
                    stop_loss=90, take_profit_1=None, take_profit_2=None,
                    take_profit_3=None, opened_at=1, closed_at=closed_at,
                    status="closed", realized_pnl=pnl,
                ))
            tracker = apex.PerformanceTracker(1000)
            tracker.restore_from_database(db)
            self.assertEqual(tracker.balance, 900)
            self.assertEqual(tracker.peak_equity, 1100)
            self.assertAlmostEqual(tracker.max_drawdown, 200 / 1100)
            self.assertEqual(tracker.consecutive_losses, 1)
            self.assertEqual(tracker.max_consecutive_losses, 1)
            db.close()

    def test_adx_uses_valid_directional_movement(self):
        df = pd.DataFrame({
            "high": [10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
            "low": [9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
            "close": [10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
        })
        adx, plus_di, minus_di = apex.TechnicalEngine.adx(df, 3)
        self.assertFalse(adx.isna().all())
        self.assertTrue((plus_di.fillna(0) >= 0).all())
        self.assertTrue((minus_di.fillna(0) >= 0).all())

    def test_database_persists_opportunities_and_positions(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = apex.MarketDatabase(Path(tmp) / "test.db")
            db.connect()
            pos = apex.PaperPosition(
                id="persist-test",
                exchange="okx",
                symbol="BTC/USDT:USDT",
                direction=apex.TradeDirection.SHORT,
                entry_price=100,
                size=1,
                stop_loss=110,
                take_profit_1=80,
                take_profit_2=70,
                take_profit_3=60,
                opened_at=1,
            )
            db.save_paper_position(pos)
            row = db._conn.execute(
                "SELECT id, status, direction FROM paper_positions WHERE id='persist-test'"
            ).fetchone()
            self.assertEqual(row, ("persist-test", "open", "SHORT"))
            db.close()

    def test_paper_candle_checkpoint_persists_monotonically(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "checkpoint.db"
            db = apex.MarketDatabase(path)
            db.connect()
            db.save_paper_candle_checkpoint("okx", "BTC/USDT:USDT", "1m", 120_000)
            db.save_paper_candle_checkpoint("okx", "BTC/USDT:USDT", "1m", 60_000)
            db.close()
            restored = apex.MarketDatabase(path)
            restored.connect()
            self.assertEqual(
                restored.get_paper_candle_checkpoint("okx", "BTC/USDT:USDT", "1m"),
                120_000,
            )
            restored.close()

    def test_stage5_replays_all_available_candles_and_persists_checkpoint(self):
        class FakeConnector:
            def __init__(self, candles):
                self.candles = candles
                self.calls = []

            async def fetch_ohlcv(self, symbol, timeframe, limit, since=None):
                self.calls.append((symbol, timeframe, limit, since))
                return [bar for bar in self.candles if since is None or bar.timestamp >= since]

        with tempfile.TemporaryDirectory() as tmp:
            db = apex.MarketDatabase(Path(tmp) / "replay.db")
            db.connect()
            now_minute = apex.utc_ms() // 60_000 * 60_000
            candles = [
                apex.Candle(
                    now_minute - (8 - index) * 60_000,
                    100, 101, 99, 100, 10,
                )
                for index in range(8)
            ]
            candles[-1] = apex.Candle(
                candles[-1].timestamp, 100, 101, 89, 90, 10
            )
            tracker = apex.PerformanceTracker(1000)
            position = apex.PaperPosition(
                id="replay-stop", exchange="okx", symbol="BTC/USDT:USDT",
                direction=apex.TradeDirection.LONG, entry_price=100, size=1,
                stop_loss=90, take_profit_1=None, take_profit_2=None,
                take_profit_3=None, opened_at=(candles[0].timestamp - 60_000) / 1000,
            )
            tracker.open_position(position)
            db.save_paper_position(position)
            connector = FakeConnector(candles)
            manager = apex.MarketDataManager()
            manager.db = db
            manager.paper_tracker = tracker
            manager.paper_sim = apex.PaperTradingSimulator(tracker, db)
            manager.live_executor = apex.LiveExecutionAdapter(db)
            manager.telegram = type("Telegram", (), {"enabled": False})()
            manager.connectors = {"okx": connector}
            manager._running = True
            manager.run_opportunity_scan = lambda: []
            old_mode = apex.Config.EXECUTION_MODE
            apex.Config.EXECUTION_MODE = "paper"

            async def stop_after_cycle(_seconds):
                raise asyncio.CancelledError

            try:
                with patch.object(apex.asyncio, "sleep", stop_after_cycle):
                    with self.assertRaises(asyncio.CancelledError):
                        asyncio.run(apex._stage5_opportunity_loop(manager))
            finally:
                apex.Config.EXECUTION_MODE = old_mode

            self.assertEqual(len(tracker.closed_trades), 1)
            self.assertEqual(tracker.closed_trades[0].exit_reason, "stop_loss")
            self.assertEqual(len(connector.calls), 1)
            self.assertEqual(connector.calls[0][3], candles[0].timestamp)
            self.assertEqual(
                db.get_paper_candle_checkpoint("okx", "BTC/USDT:USDT", "1m"),
                candles[-1].timestamp,
            )
            db.close()

    def test_live_mode_runs_paper_shadow_only_when_enabled(self):
        class FakeConnector:
            def __init__(self, candle):
                self.candle = candle
                self.calls = []

            async def fetch_ohlcv(self, symbol, timeframe, limit, since=None):
                self.calls.append(since)
                return [self.candle]

        async def run_cycle(manager):
            async def stop_after_cycle(_seconds):
                raise asyncio.CancelledError

            with patch.object(apex.asyncio, "sleep", stop_after_cycle):
                with self.assertRaises(asyncio.CancelledError):
                    await apex._stage5_opportunity_loop(manager)

        with tempfile.TemporaryDirectory() as tmp:
            db = apex.MarketDatabase(Path(tmp) / "shadow.db")
            db.connect()
            candle_time = apex.utc_ms() // 60_000 * 60_000 - 60_000
            candle = apex.Candle(candle_time, 100, 101, 89, 90, 10)
            tracker = apex.PerformanceTracker(1000)
            position = apex.PaperPosition(
                id="shadow", exchange="okx", symbol="BTC/USDT:USDT",
                direction=apex.TradeDirection.LONG, entry_price=100, size=1,
                stop_loss=90, take_profit_1=None, take_profit_2=None,
                take_profit_3=None, opened_at=(candle_time - 60_000) / 1000,
            )
            tracker.open_position(position)
            db.save_paper_position(position)
            old_mode = apex.Config.EXECUTION_MODE
            old_shadow = apex.Config.PAPER_SHADOW_IN_LIVE
            try:
                apex.Config.EXECUTION_MODE = "live"
                apex.Config.PAPER_SHADOW_IN_LIVE = False
                disabled_connector = FakeConnector(candle)
                disabled_manager = apex.MarketDataManager()
                disabled_manager.db = db
                disabled_manager.paper_tracker = tracker
                disabled_manager.paper_sim = apex.PaperTradingSimulator(tracker, db)
                disabled_manager.live_executor = apex.LiveExecutionAdapter(db)
                disabled_manager.telegram = type("Telegram", (), {"enabled": False})()
                disabled_manager.connectors = {"okx": disabled_connector}
                disabled_manager._running = True
                disabled_manager.run_opportunity_scan = lambda: []
                asyncio.run(run_cycle(disabled_manager))
                self.assertEqual(disabled_connector.calls, [])
                self.assertIn(position.id, tracker.open_positions)

                apex.Config.PAPER_SHADOW_IN_LIVE = True
                enabled_connector = FakeConnector(candle)
                enabled_manager = apex.MarketDataManager()
                enabled_manager.db = db
                enabled_manager.paper_tracker = tracker
                enabled_manager.paper_sim = apex.PaperTradingSimulator(tracker, db)
                enabled_manager.live_executor = apex.LiveExecutionAdapter(db)
                enabled_manager.telegram = type("Telegram", (), {"enabled": False})()
                enabled_manager.connectors = {"okx": enabled_connector}
                enabled_manager._running = True
                enabled_manager.run_opportunity_scan = lambda: []
                asyncio.run(run_cycle(enabled_manager))
                self.assertEqual(len(enabled_connector.calls), 1)
                self.assertNotIn(position.id, tracker.open_positions)
            finally:
                apex.Config.EXECUTION_MODE = old_mode
                apex.Config.PAPER_SHADOW_IN_LIVE = old_shadow
                db.close()

    def test_backtest_rejects_invalid_inputs(self):
        engine = apex.BacktestEngine()
        result = engine.run([], apex.TradeDirection.LONG, 100, 90, 120, 1, 0)
        self.assertEqual(result["status"], "invalid")
        result = engine.run(
            [apex.Candle(1, 100, 101, 99, 100, 10)] * 3,
            apex.TradeDirection.LONG, 100, 90, 120, 1, funding_rate=float("nan"),
        )
        self.assertEqual(result["status"], "invalid")

    def test_limit_entries_wait_for_entry_zone(self):
        tracker = apex.PerformanceTracker(1000)
        simulator = apex.PaperTradingSimulator(tracker)
        opp = apex.TradeOpportunity(
            exchange="okx",
            symbol="BTC/USDT:USDT",
            direction=apex.TradeDirection.LONG,
            status="READY",
            entry_method=apex.EntryMethod.LIMIT_RETEST,
            entry_zone_low=99,
            entry_zone_high=101,
            preferred_entry=100,
            stop_loss=apex.StopLossPlan(price=90, reason="test", atr_distance=1),
            take_profit=apex.TakeProfitPlan(tp1=120),
            risk_reward=apex.RiskRewardResult(
                entry=100, stop=90, tp1=120, tp2=130, tp3=140,
                gross_risk=10,
                gross_reward_tp1=20, net_risk=10.5,
                net_reward_tp1=19.5, net_rr_tp1=1.857,
            ),
            position_size=apex.PositionSizeResult(
                risk_usd=10, position_size_coins=0.95238095,
                notional_usd=95.24, leverage_suggested=0.1,
                margin_required=9.52,
            ),
            regime="weak_bull",
            confirmations=[],
            risk_factors=[],
            invalidation="test",
            data_quality="good",
            score=70,
        )
        self.assertIsNone(simulator.consider_opportunity(opp))
        closed = simulator.update_with_price("okx", "BTC/USDT:USDT", 100, 100, 100)
        self.assertEqual(closed, [])
        self.assertEqual(len(tracker.open_positions), 1)

    def test_stale_ticker_is_not_actionable(self):
        analysis = apex.AssetAnalysis(
            exchange="okx",
            symbol="BTC/USDT:USDT",
            timeframe="1h",
            timestamp=1,
            price=100,
            data_quality="stale_ticker",
        )
        opp = apex.EntryOptimiser.evaluate(analysis)
        self.assertIsNotNone(opp)
        self.assertEqual(opp.status, "NO_TRADE")

    def test_contract_sizing_uses_contract_size_and_precision(self):
        meta = apex.MarketMeta(
            exchange="okx",
            symbol="BTC/USDT:USDT",
            base="BTC",
            quote="USDT",
            settle="USDT",
            contract_size=0.01,
            precision_amount=0.01,
            precision_mode=apex.ccxt.TICK_SIZE,
        )
        result = apex.PositionSizer.compute(
            100,
            90,
            10,
            account_balance=1000,
            risk_pct=1,
            market_meta=meta,
        )
        self.assertEqual(result.position_size_coins, 1.0)
        self.assertEqual(result.notional_usd, 100)

    def test_position_sizing_interprets_precision_mode_and_never_raises_to_minimum(self):
        decimal_meta = apex.MarketMeta(
            exchange="test", symbol="AAA/USDT", base="AAA", quote="USDT",
            settle="USDT", precision_amount=2,
            precision_mode=apex.ccxt.DECIMAL_PLACES,
        )
        decimal_size = apex.PositionSizer.compute(
            100, 90, 3, account_balance=1000, risk_pct=1,
            market_meta=decimal_meta,
        )
        self.assertEqual(decimal_size.position_size_coins, 3.33)
        self.assertLessEqual(decimal_size.position_size_coins * 3, decimal_size.risk_usd)

        tick_meta = replace(
            decimal_meta,
            precision_amount=0.25,
            precision_mode=apex.ccxt.TICK_SIZE,
            limits={"amount": {"min": 4}},
        )
        under_minimum = apex.PositionSizer.compute(
            100, 90, 3, account_balance=1000, risk_pct=1,
            market_meta=tick_meta,
        )
        self.assertEqual(under_minimum.position_size_coins, 0)
        self.assertIn("preserve risk budget", under_minimum.notes[0])

    def test_analysis_rejects_missing_ticker_and_uses_closed_higher_timeframe(self):
        now_ms = apex.utc_ms()
        candles = [
            apex.Candle(now_ms - 60 * 3_600_000 + i * 3_600_000,
                        100 + i, 102 + i, 99 + i, 101 + i, 10)
            for i in range(60)
        ]
        higher_candles = [
            apex.Candle(now_ms - 60 * 14_400_000 + i * 14_400_000,
                        100 + i, 102 + i, 99 + i, 101 + i, 10)
            for i in range(60)
        ]
        analysis = apex.AnalysisOrchestrator().analyse_symbol(
            "okx", "BTC/USDT:USDT", candles, ticker=None,
            higher_timeframe_candles=higher_candles,
        )
        self.assertEqual(analysis.data_quality, "missing_ticker+no_funding")
        self.assertEqual(analysis.price, 0)
        self.assertEqual(analysis.higher_timeframe_direction, "bullish")
        self.assertEqual(
            apex.EntryOptimiser.evaluate(analysis).direction,
            apex.TradeDirection.NO_TRADE,
        )

    def test_closed_candles_exclude_current_forming_bar(self):
        now_ms = 10_000_000
        candles = [
            apex.Candle(now_ms - 120_000, 1, 2, 1, 2, 1),
            apex.Candle(now_ms - 30_000, 2, 3, 2, 3, 1),
        ]
        self.assertEqual(
            apex.closed_candles(candles, "1m", now_ms), candles[:1]
        )

    def test_conflicting_four_hour_trend_blocks_one_hour_trade_plan(self):
        analysis = apex.AssetAnalysis(
            exchange="okx",
            symbol="BTC/USDT:USDT",
            timeframe="1h",
            timestamp=apex.utc_ms(),
            price=100,
            indicators=apex.IndicatorSet(
                ema_fast=103, ema_slow=102, ema_trend=101,
                atr=2, atr_pct=2, adx=25, plus_di=30, minus_di=10, rsi=50,
            ),
            structure=apex.StructureAnalysis(trend="bullish"),
            regime=apex.RegimeSnapshot(
                primary=apex.MarketRegime.WEAK_BULL,
                direction="bullish",
                confidence=0.8,
            ),
            higher_timeframe_direction="bearish",
            data_quality="good",
        )
        opportunity = apex.EntryOptimiser.evaluate(analysis)
        self.assertEqual(opportunity.direction, apex.TradeDirection.NO_TRADE)
        self.assertTrue(any("4h direction conflicts" in item for item in opportunity.risk_factors))
        missing_confirmation = apex.EntryOptimiser.evaluate(
            replace(analysis, higher_timeframe_direction="unknown")
        )
        self.assertEqual(missing_confirmation.direction, apex.TradeDirection.NO_TRADE)
        self.assertTrue(
            any("4h trend confirmation is unavailable" in item
                for item in missing_confirmation.risk_factors)
        )

    def test_bootstrap_and_candle_refresh_cover_every_active_symbol(self):
        class FakeDatabase:
            def insert_candles(self, *_args):
                return 0

        class FakeConnector:
            def __init__(self):
                self.calls = []

            async def fetch_ohlcv(self, symbol, timeframe, limit):
                self.calls.append((symbol, timeframe, limit))
                return []

        async def run():
            manager = apex.MarketDataManager()
            connector = FakeConnector()
            manager.db = FakeDatabase()
            manager.connectors = {"okx": connector}
            manager.active_symbols = [f"okx:ASSET{i}" for i in range(30)]

            async def controlled_sleep(seconds):
                if seconds == apex.Config.CANDLE_UPDATE_INTERVAL:
                    raise asyncio.CancelledError

            with patch.object(apex.asyncio, "sleep", controlled_sleep):
                await manager._bootstrap_history()
            manager._running = True
            with patch.object(apex.asyncio, "sleep", controlled_sleep):
                with self.assertRaises(asyncio.CancelledError):
                    await manager._candle_update_loop()
            return connector.calls

        calls = asyncio.run(run())
        self.assertEqual(len(calls), 30 * 4)
        self.assertEqual({timeframe for _, timeframe, _ in calls}, {"1h", "4h"})
        self.assertEqual({symbol for symbol, _, _ in calls}, {f"ASSET{i}" for i in range(30)})

    def test_analysis_cycle_reports_coverage_and_symbol_failures(self):
        now = apex.utc_ms()

        class FakeDatabase:
            def __init__(self):
                self.saved = []

            def get_candles(self, _exchange, symbol, timeframe, limit):
                if timeframe != apex.Config.ANALYSIS_TF:
                    return []
                count = {"SHORT": 10, "GOOD": 40, "FAIL": 40}[symbol]
                return [
                    apex.Candle(
                        now - (count - index) * 3_600_000,
                        100, 101, 99, 100, 1,
                    )
                    for index in range(count)
                ]

            def save_analysis(self, analysis):
                self.saved.append(analysis.symbol)

        class FakeOrchestrator:
            def analyse_symbol(self, **kwargs):
                if kwargs["symbol"] == "FAIL":
                    raise RuntimeError("test analysis failure")
                return apex.AssetAnalysis(
                    exchange=kwargs["exchange"], symbol=kwargs["symbol"],
                    timeframe=kwargs["timeframe"], timestamp=now,
                    price=100,
                )

        manager = apex.MarketDataManager()
        manager.db = FakeDatabase()
        manager.orchestrator = FakeOrchestrator()
        manager.active_symbols = ["okx:SHORT", "okx:GOOD", "okx:FAIL"]
        asyncio.run(manager._run_analyses())
        cycle = manager.stats["analysis_cycle"]
        self.assertEqual(
            (cycle["total"], cycle["eligible"], cycle["analysed"],
             cycle["insufficient_history"], cycle["failed"]),
            (3, 2, 1, 1, 1),
        )
        self.assertEqual(cycle["insufficient_symbols"], ["okx:SHORT (10/40 closed candles)"])
        self.assertEqual(len(cycle["failed_symbols"]), 1)

    def test_pending_opportunities_persist_and_conflict_across_directions(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = apex.MarketDatabase(Path(tmp) / "pending.db")
            db.connect()
            simulator = apex.PaperTradingSimulator(apex.PerformanceTracker(1000), db)
            opportunity = self.make_opportunity()
            self.assertIsNone(simulator.consider_opportunity(opportunity))
            restored = apex.PaperTradingSimulator(apex.PerformanceTracker(1000), db)
            self.assertEqual(len(restored.pending_opportunities), 1)
            opposite = replace(opportunity, direction=apex.TradeDirection.SHORT)
            self.assertIsNone(restored.consider_opportunity(opposite))
            self.assertEqual(len(restored.pending_opportunities), 1)
            db.close()

    def test_pending_paper_entries_reserve_portfolio_notional(self):
        old_limit = apex.Config.MAX_PORTFOLIO_NOTIONAL_PCT
        try:
            apex.Config.MAX_PORTFOLIO_NOTIONAL_PCT = 15
            simulator = apex.PaperTradingSimulator(apex.PerformanceTracker(1000))
            first = self.make_opportunity()
            second = replace(first, symbol="ETH/USDT:USDT")
            self.assertIsNone(simulator.consider_opportunity(first))
            self.assertIsNone(simulator.consider_opportunity(second))
            self.assertEqual(len(simulator.pending_opportunities), 1)
        finally:
            apex.Config.MAX_PORTFOLIO_NOTIONAL_PCT = old_limit

    def test_pending_order_ignores_pre_submission_bar_and_defers_exit_until_next_bar(self):
        tracker = apex.PerformanceTracker(1000)
        simulator = apex.PaperTradingSimulator(tracker)
        opportunity = self.make_opportunity()
        simulator.consider_opportunity(opportunity)
        current_minute = (apex.utc_ms() // 60_000) * 60_000
        simulator.update_with_price(
            "okx", opportunity.symbol, 100, 140, 80, current_minute - 60_000
        )
        self.assertEqual(len(tracker.open_positions), 0)

        opened_bar = current_minute + 60_000
        simulator.update_with_price(
            "okx", opportunity.symbol, 100, 140, 80, opened_bar
        )
        self.assertEqual(len(tracker.open_positions), 1)
        self.assertEqual(len(tracker.closed_trades), 0)

    def test_pending_fill_timestamp_allows_next_replayed_bar_exit(self):
        tracker = apex.PerformanceTracker(1000)
        simulator = apex.PaperTradingSimulator(tracker)
        opportunity = self.make_opportunity()
        simulator.consider_opportunity(opportunity)
        pending_since = simulator._pending_since[
            f"{opportunity.exchange}:{opportunity.symbol}"
        ]
        fill_bar = (
            int(pending_since * 1000 // 60_000) + 1
        ) * 60_000
        simulator.update_with_price(
            "okx", opportunity.symbol, 100, 101, 99, fill_bar
        )
        position = next(iter(tracker.open_positions.values()))
        self.assertEqual(position.opened_at, fill_bar / 1000)
        closed = simulator.update_with_price(
            "okx", opportunity.symbol, 89, 100, 89, fill_bar + 60_000
        )
        self.assertEqual(len(closed), 1)
        self.assertEqual(closed[0].exit_reason, "stop_loss")

    def test_paper_simulator_partially_exits_at_all_targets_and_stops_first(self):
        tracker = apex.PerformanceTracker(1000)
        simulator = apex.PaperTradingSimulator(tracker)
        position = apex.PaperPosition(
            id="partial",
            exchange="okx",
            symbol="BTC/USDT:USDT",
            direction=apex.TradeDirection.LONG,
            entry_price=100,
            size=4,
            stop_loss=90,
            take_profit_1=110,
            take_profit_2=120,
            take_profit_3=130,
            opened_at=1,
        )
        tracker.open_position(position)
        simulator.update_with_price("okx", position.symbol, 111, 111, 100)
        self.assertAlmostEqual(position.remaining_size, 2)
        self.assertEqual(position.targets_hit, [1])
        with tempfile.TemporaryDirectory() as tmp:
            db = apex.MarketDatabase(Path(tmp) / "partial.db")
            db.connect()
            db.save_paper_position(position)
            db.save_paper_equity(tracker.balance)
            restored_tracker = apex.PerformanceTracker(1000)
            restored_tracker.restore_from_database(db)
            simulator = apex.PaperTradingSimulator(restored_tracker, db)
            position = restored_tracker.open_positions["partial"]
            simulator.update_with_price("okx", position.symbol, 121, 121, 100)
            self.assertAlmostEqual(position.remaining_size, 1)
            closed = simulator.update_with_price("okx", position.symbol, 131, 131, 100)
            tracker = restored_tracker
            db.close()
        self.assertEqual(len(closed), 1)
        self.assertEqual(closed[0].exit_reason, "take_profit_3")
        self.assertEqual(closed[0].remaining_size, 0)
        self.assertAlmostEqual(closed[0].realized_pnl, tracker.balance - 1000)

        stop_tracker = apex.PerformanceTracker(1000)
        stop_sim = apex.PaperTradingSimulator(stop_tracker)
        stop_position = apex.PaperPosition(
            id="stop-first",
            exchange="okx",
            symbol="BTC/USDT:USDT",
            direction=apex.TradeDirection.LONG,
            entry_price=100,
            size=1,
            stop_loss=90,
            take_profit_1=110,
            take_profit_2=None,
            take_profit_3=None,
            opened_at=1,
        )
        stop_tracker.open_position(stop_position)
        result = stop_sim.update_with_price("okx", stop_position.symbol, 100, 111, 89)
        self.assertEqual(result[0].exit_reason, "stop_loss")

    def test_live_daily_loss_uses_current_day_fills_once_and_fails_closed(self):
        class FakeExchange:
            has = {"fetchMyTrades": True, "fetchFundingHistory": True}

            async def fetch_positions(self):
                return [{"symbol": "BTC/USDT:USDT", "contracts": 1, "realizedPnl": 99}]

            async def fetch_open_orders(self):
                return []

            async def fetch_balance(self, _params=None):
                return {"total": {"USDT": 1000}, "free": {"USDT": 1000}}

            async def fetch_my_trades(self, _symbol, since, limit):
                now = apex.utc_ms()
                return [
                    {"id": "fill-1", "symbol": "BTC/USDT:USDT",
                     "timestamp": now, "price": 100,
                     "fee": {"cost": 0.01, "currency": "BTC"},
                     "info": {"fillPnl": "2.5"}},
                    {"id": "fill-2", "symbol": "BTC/USDT:USDT",
                     "timestamp": now, "realizedPnl": -0.5,
                     "fee": {"cost": 0.2, "currency": "USDT"}},
                    {"id": "old-fill", "symbol": "BTC/USDT:USDT",
                     "timestamp": since - 1, "realizedPnl": -100},
                ]

            async def fetch_funding_history(self, _symbol, since, limit):
                return [{
                    "id": "funding-1", "timestamp": apex.utc_ms(),
                    "amount": -0.25, "code": "USDT",
                }]

            def market(self, _symbol):
                return {"base": "BTC", "quote": "USDT", "settle": "USDT"}

        with tempfile.TemporaryDirectory() as tmp:
            db = apex.MarketDatabase(Path(tmp) / "live.db")
            db.connect()
            adapter = apex.LiveExecutionAdapter(db)
            adapter.exchange = FakeExchange()
            state = asyncio.run(adapter.sync_live_state())
            self.assertTrue(state["daily_pnl_available"])
            self.assertAlmostEqual(state["gross_realized_pnl"], 2.0)
            self.assertAlmostEqual(state["trading_fees"], 1.2)
            self.assertAlmostEqual(state["funding_pnl"], -0.25)
            self.assertAlmostEqual(state["realized_pnl"], 0.55)
            self.assertEqual(adapter._open_positions, 1)

            class MissingPnlExchange(FakeExchange):
                async def fetch_my_trades(self, _symbol, since, limit):
                    return [{
                        "id": "missing-pnl", "symbol": "BTC/USDT:USDT",
                        "timestamp": apex.utc_ms(), "info": {},
                    }]

                async def create_order(self, *_args, **_kwargs):
                    raise AssertionError("entry must be blocked before order creation")

            adapter.exchange = MissingPnlExchange()
            adapter.mode = "live"
            adapter._hedged = True
            accepted, reason = asyncio.run(
                adapter.place_protected_market_order(self.make_opportunity())
            )
            self.assertFalse(accepted)
            self.assertIn("daily realized PnL is unavailable", reason)
            db.close()

    def test_live_portfolio_limits_include_existing_positions_and_correlation(self):
        class FakeExchange:
            def market(self, symbol):
                base = symbol.split("/", 1)[0]
                return {"base": base, "quote": "USDT", "settle": "USDT"}

        old_notional_limit = apex.Config.MAX_PORTFOLIO_NOTIONAL_PCT
        old_correlation_limit = apex.Config.MAX_CORRELATED_POSITIONS
        try:
            adapter = apex.LiveExecutionAdapter()
            adapter.exchange = FakeExchange()
            apex.Config.MAX_PORTFOLIO_NOTIONAL_PCT = 50
            apex.Config.MAX_CORRELATED_POSITIONS = 3
            allowed, reason = adapter._check_live_portfolio_limits(
                [{"symbol": "BTC/USDT:USDT", "contracts": 1, "notional": 400}],
                "ETH/USDT:USDT", 200, 1000,
            )
            self.assertFalse(allowed)
            self.assertIn("aggregate live portfolio", reason)

            apex.Config.MAX_PORTFOLIO_NOTIONAL_PCT = 100
            apex.Config.MAX_CORRELATED_POSITIONS = 1
            allowed, reason = adapter._check_live_portfolio_limits(
                [{"symbol": "BTC/USDT:USDT", "contracts": 1, "notional": 100}],
                "BTC/USDT:USDT", 100, 1000,
            )
            self.assertFalse(allowed)
            self.assertIn("correlated live position", reason)

            allowed, reason = adapter._check_live_portfolio_limits(
                [{"contracts": 1, "notional": 100}],
                "BTC/USDT:USDT", 100, 1000,
            )
            self.assertFalse(allowed)
            self.assertIn("no symbol", reason)

            apex.Config.MAX_PORTFOLIO_NOTIONAL_PCT = 40
            apex.Config.MAX_CORRELATED_POSITIONS = 3
            allowed, reason = adapter._check_live_portfolio_limits(
                [],
                "BTC/USDT:USDT",
                100,
                1000,
                [{"symbol": "ETH/USDT:USDT", "amount": 4, "price": 100}],
            )
            self.assertFalse(allowed)
            self.assertIn("aggregate live portfolio", reason)

            allowed, _ = adapter._check_live_portfolio_limits(
                [],
                "BTC/USDT:USDT",
                100,
                1000,
                [{"symbol": "ETH/USDT:USDT", "amount": 20,
                  "price": 100, "reduceOnly": True}],
            )
            self.assertTrue(allowed)

            allowed, reason = adapter._check_live_portfolio_limits(
                [],
                "BTC/USDT:USDT",
                100,
                1000,
                [{"symbol": "ETH/USDT:USDT", "amount": 1}],
            )
            self.assertFalse(allowed)
            self.assertIn("cannot value outstanding", reason)
        finally:
            apex.Config.MAX_PORTFOLIO_NOTIONAL_PCT = old_notional_limit
            apex.Config.MAX_CORRELATED_POSITIONS = old_correlation_limit

    def test_live_protection_creates_stop_and_partial_targets(self):
        class FakeExchange:
            def __init__(self):
                self.orders = []

            def market(self, _symbol):
                return {}

            def amount_to_precision(self, _symbol, amount):
                return str(amount)

            async def create_order(self, *args):
                self.orders.append(args)
                return {"id": str(len(self.orders))}

        adapter = apex.LiveExecutionAdapter()
        adapter.exchange = FakeExchange()
        adapter._hedged = True
        targets = {
            "direction": "LONG",
            "amount": 4,
            "stop": 90,
            "tp1": 110,
            "tp2": 120,
            "tp3": 130,
        }
        asyncio.run(
            adapter._ensure_protection_orders("BTC/USDT:USDT", targets, 4, [])
        )
        self.assertEqual([order[3] for order in adapter.exchange.orders], [4, 2, 1, 1])
        self.assertEqual(
            [order[5]["triggerPrice"] for order in adapter.exchange.orders],
            [90, 110, 120, 130],
        )

    def test_live_entry_protection_failure_submits_emergency_reduce_only_close(self):
        class FakeExchange:
            has = {"fetchMyTrades": True, "fetchFundingHistory": True}

            def __init__(self):
                self.orders = []

            async def fetch_positions(self):
                return []

            async def fetch_open_orders(self):
                return []

            async def fetch_balance(self, _params=None):
                return {"total": {"USDT": 1000}, "free": {"USDT": 1000}}

            async def fetch_my_trades(self, _symbol, since, limit):
                return []

            async def fetch_funding_history(self, _symbol, since, limit):
                return []

            async def fetch_ticker(self, _symbol):
                return {
                    "timestamp": apex.utc_ms(),
                    "last": 100,
                    "bid": 99.9,
                    "ask": 100.1,
                }

            def market(self, _symbol):
                return {"contractSize": 1, "limits": {"amount": {"min": 0}}}

            def amount_to_precision(self, _symbol, amount):
                return str(amount)

            async def set_leverage(self, *_args, **_kwargs):
                return {}

            async def create_order(self, *args):
                self.orders.append(args)
                if len(self.orders) == 1:
                    return {"id": "entry"}
                if args[5].get("triggerPrice"):
                    raise RuntimeError("protection unavailable")
                return {"id": "emergency-close"}

            async def cancel_all_orders(self, _symbol):
                return []

        with tempfile.TemporaryDirectory() as tmp:
            db = apex.MarketDatabase(Path(tmp) / "live-protection.db")
            db.connect()
            adapter = apex.LiveExecutionAdapter(db)
            adapter.mode = "live"
            adapter.exchange = FakeExchange()
            adapter._hedged = True
            opportunity = replace(
                self.make_opportunity(),
                entry_method=apex.EntryMethod.MARKET,
            )
            accepted, reason = asyncio.run(
                adapter.place_protected_market_order(opportunity)
            )
            self.assertTrue(accepted)
            self.assertIn("emergency close submitted", reason)
            self.assertEqual(len(adapter.exchange.orders), 3)
            self.assertTrue(adapter.exchange.orders[2][5]["reduceOnly"])
            db.close()

    def test_pending_limit_fill_uses_bar_range(self):
        tracker = apex.PerformanceTracker(1000)
        simulator = apex.PaperTradingSimulator(tracker)
        opp = apex.TradeOpportunity(
            exchange="okx",
            symbol="BTC/USDT:USDT",
            direction=apex.TradeDirection.LONG,
            status="READY",
            entry_method=apex.EntryMethod.LIMIT_RETEST,
            entry_zone_low=99,
            entry_zone_high=101,
            preferred_entry=100,
            stop_loss=apex.StopLossPlan(price=90, reason="test", atr_distance=1),
            take_profit=apex.TakeProfitPlan(tp1=120),
            risk_reward=apex.RiskRewardResult(
                entry=100, stop=90, tp1=120, tp2=None, tp3=None,
                gross_risk=10, gross_reward_tp1=20,
                net_risk=10.5, net_reward_tp1=19.5, net_rr_tp1=1.857,
            ),
            position_size=apex.PositionSizeResult(
                risk_usd=10, position_size_coins=1,
                notional_usd=100, leverage_suggested=0.1,
                margin_required=10,
            ),
            score=70,
        )
        simulator.consider_opportunity(opp)
        closed = simulator.update_with_price("okx", "BTC/USDT:USDT", 102, 103, 98)
        self.assertEqual(closed, [])
        self.assertEqual(len(tracker.open_positions), 1)
        position = next(iter(tracker.open_positions.values()))
        self.assertAlmostEqual(
            position.entry_price,
            101 * (1 + apex.Config.DEFAULT_SLIPPAGE_BPS / 10_000),
        )

    def test_backtest_models_limit_fill_slippage_and_funding(self):
        candles = [
            apex.Candle(1, 100, 102, 98, 101, 100),
            apex.Candle(2, 101, 103, 99, 102, 100),
            apex.Candle(3, 102, 121, 101, 120, 100),
        ]
        result = apex.BacktestEngine().run(
            candles,
            apex.TradeDirection.LONG,
            entry_price=100,
            stop=90,
            tp1=120,
            size=1,
            entry_idx=0,
            limit_order=True,
            slippage_bps=2,
            funding_rate=0.0001,
            funding_interval_bars=1,
        )
        self.assertEqual(result["status"], "closed")
        self.assertEqual(result["entry_index"], 1)
        self.assertIn("funding", result)
        self.assertLess(result["pnl"], 18)

        short_result = apex.BacktestEngine().run(
            candles,
            apex.TradeDirection.SHORT,
            entry_price=100,
            stop=110,
            tp1=80,
            size=1,
            entry_idx=0,
            funding_rate=-0.0001,
            funding_interval_bars=1,
        )
        self.assertGreater(short_result["funding"], 0)

    def test_backtest_reports_unfilled_limit(self):
        candles = [
            apex.Candle(1, 100, 102, 98, 101, 100),
            apex.Candle(2, 101, 103, 101, 102, 100),
            apex.Candle(3, 102, 104, 102, 103, 100),
        ]
        result = apex.BacktestEngine().run(
            candles,
            apex.TradeDirection.LONG,
            entry_price=90,
            stop=80,
            tp1=110,
            size=1,
            entry_idx=0,
            limit_order=True,
        )
        self.assertEqual(result["status"], "unfilled")


if __name__ == "__main__":
    unittest.main()
