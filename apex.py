#!/usr/bin/env python3
"""
PROJECT APEX – Advanced Multi-Exchange Cryptocurrency Futures Intelligence System
==================================================================================
Monolithic single-file implementation.

STAGE 1 (Complete): Market Data Acquisition Engine
STAGE 2 (Complete): Technical Analysis + Market Structure + Liquidity/Order-Flow
                    + Derivatives Intelligence + Volatility & Regime Detection
STAGE 3 (Complete): Entry Optimisation + Dynamic Stop-Loss + Take-Profit
                    + Net Risk/Reward + Position Sizing + Opportunity Ranking
STAGE 4 (Complete): Backtesting + Paper Trading + Performance Tracking
STAGE 5 (Current):  Streamlit Dashboard + Telegram Notifications + Monitoring

Zero-dollar budget. Public market data only. No look-ahead bias.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import signal
import sqlite3
import sys
import tempfile
import time
import traceback
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Optional .env support
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Core third-party
# ---------------------------------------------------------------------------
import ccxt
import ccxt.async_support as ccxt_async
import numpy as np
import pandas as pd

try:
    import aiosqlite
except ImportError:
    aiosqlite = None

try:
    from rich.console import Console
    from rich.table import Table
    from rich.live import Live
    from rich.panel import Panel
    from rich.text import Text
    RICH_AVAILABLE = True
    console = Console()
except ImportError:
    RICH_AVAILABLE = False
    console = None


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value (true/false)")


# ===========================================================================
# SECTION 0 – CONFIGURATION
# ===========================================================================

class Config:
    """Central configuration. Environment variables override defaults."""

    BASE_DIR = Path(__file__).resolve().parent
    DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data_store"))
    DB_PATH = DATA_DIR / "apex_market.db"
    SETTINGS_PATH = DATA_DIR / "apex_settings.json"
    LOG_DIR = BASE_DIR / "logs"

    EXCHANGE_PRIORITY = ["okx", "bybit", "binance"]
    PRIMARY_EXCHANGE = os.getenv("PRIMARY_EXCHANGE", "okx").lower()

    QUOTE = "USDT"
    CONTRACT_TYPE = "swap"
    MAX_PAIRS = int(os.getenv("MAX_PAIRS", "30"))
    MIN_VOLUME_USDT_24H = 5_000_000

    TIMEFRAMES = ["1m", "5m", "15m", "1h", "4h", "1d"]
    DEFAULT_TF = "1h"
    ANALYSIS_TF = "1h"          # primary analysis timeframe
    HIGHER_TF = "4h"
    HISTORICAL_LIMIT = 400

    FRESHNESS_TICKER = 75   # Must exceed the 50s REST polling interval
    FRESHNESS_ORDERBOOK = 5
    FRESHNESS_OHLCV = 90
    FRESHNESS_FUNDING = 300
    FRESHNESS_OI = 120

    REST_SLEEP = 0.25
    MAX_RETRIES = 4
    RECONNECT_DELAY = 5.0
    HEARTBEAT_INTERVAL = 30

    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    LOG_TO_FILE = True

    SCAN_INTERVAL = 50
    CANDLE_UPDATE_INTERVAL = 70
    ANALYSIS_INTERVAL = 90

    # Analysis parameters
    RSI_PERIOD = 14
    ATR_PERIOD = 14
    ADX_PERIOD = 14
    BB_PERIOD = 20
    BB_STD = 2.0
    EMA_FAST = 9
    EMA_SLOW = 21
    EMA_TREND = 50
    SWING_LOOKBACK = 5
    STRUCTURE_LOOKBACK = 50

    # Stage 3 – Risk & Opportunity
    ACCOUNT_BALANCE = float(os.getenv("ACCOUNT_BALANCE", "1000"))
    RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", "1.0"))  # % of equity
    MAX_RISK_PER_TRADE_USD = float(os.getenv("MAX_RISK_PER_TRADE_USD", "20"))
    MIN_NET_RR = float(os.getenv("MIN_NET_RR", "1.5"))
    TAKER_FEE_PCT = 0.0005          # 0.05% typical futures taker
    MAKER_FEE_PCT = 0.0002
    DEFAULT_SLIPPAGE_BPS = 2.0      # 2 bps estimate
    FUNDING_HORIZON_HOURS = 8       # assume hold through one funding
    MIN_CONFIDENCE = 0.35
    OPPORTUNITY_SCAN_INTERVAL = 120

    # Execution safety: paper mode is always the default.
    EXECUTION_MODE = os.getenv("EXECUTION_MODE", "paper").strip().lower()
    PAPER_SHADOW_IN_LIVE = env_bool("PAPER_SHADOW_IN_LIVE", False)
    OKX_API_KEY = os.getenv("OKX_API_KEY", "").strip()
    OKX_API_SECRET = os.getenv("OKX_API_SECRET", "").strip()
    OKX_API_PASSPHRASE = os.getenv("OKX_API_PASSPHRASE", "").strip()
    OKX_TESTNET = env_bool("OKX_TESTNET", True)
    LIVE_CONFIRMATION = os.getenv("LIVE_CONFIRMATION", "").strip()
    LIVE_CONFIRMATION_TOKEN = os.getenv("LIVE_CONFIRMATION_TOKEN", "ENABLE-LIVE-APEX").strip()
    LIVE_KILL_SWITCH = env_bool("LIVE_KILL_SWITCH", True)
    MAX_LIVE_DAILY_LOSS_USD = float(os.getenv("MAX_LIVE_DAILY_LOSS_USD", "25"))
    MAX_LIVE_ORDERS_PER_MINUTE = int(os.getenv("MAX_LIVE_ORDERS_PER_MINUTE", "6"))
    MAX_LIVE_NOTIONAL_USD = float(os.getenv("MAX_LIVE_NOTIONAL_USD", "100"))
    MAX_LIVE_POSITIONS = int(os.getenv("MAX_LIVE_POSITIONS", "2"))
    MAX_LIVE_LEVERAGE = float(os.getenv("MAX_LIVE_LEVERAGE", "3"))
    MAX_LIVE_ENTRY_DRIFT_PCT = float(os.getenv("MAX_LIVE_ENTRY_DRIFT_PCT", "0.25"))
    MAX_PORTFOLIO_NOTIONAL_PCT = float(os.getenv("MAX_PORTFOLIO_NOTIONAL_PCT", "100"))
    MAX_CORRELATED_POSITIONS = int(os.getenv("MAX_CORRELATED_POSITIONS", "2"))
    PENDING_ORDER_MAX_SECONDS = float(os.getenv("PENDING_ORDER_MAX_SECONDS", "14400"))

    @classmethod
    def ensure_dirs(cls) -> None:
        cls.DATA_DIR.mkdir(parents=True, exist_ok=True)
        cls.LOG_DIR.mkdir(parents=True, exist_ok=True)


Config.ensure_dirs()

_NON_CONFIGURABLE_SETTINGS = {
    "BASE_DIR", "DATA_DIR", "DB_PATH", "SETTINGS_PATH", "LOG_DIR",
    "EXECUTION_MODE", "OKX_API_KEY", "OKX_API_SECRET", "OKX_API_PASSPHRASE",
    "OKX_TESTNET", "LIVE_CONFIRMATION", "LIVE_CONFIRMATION_TOKEN",
    "LIVE_KILL_SWITCH",
}


def configurable_settings() -> Dict[str, Any]:
    return {
        name: getattr(Config, name)
        for name in vars(Config)
        if name.isupper()
        and name not in _NON_CONFIGURABLE_SETTINGS
        and isinstance(getattr(Config, name), (bool, int, float, str, list))
    }


def _validate_settings(updates: Dict[str, Any]) -> Dict[str, Any]:
    current = configurable_settings()
    unknown = set(updates) - current.keys()
    if unknown:
        raise ValueError(f"Unknown or non-configurable setting(s): {', '.join(sorted(unknown))}")

    candidate = {**current, **updates}
    for name, value in candidate.items():
        default = current[name]
        if isinstance(default, bool):
            valid_type = isinstance(value, bool)
        elif isinstance(default, int):
            valid_type = isinstance(value, int) and not isinstance(value, bool)
        elif isinstance(default, float):
            valid_type = isinstance(value, (int, float)) and not isinstance(value, bool)
        elif isinstance(default, str):
            valid_type = isinstance(value, str) and bool(value.strip())
        else:
            valid_type = (
                isinstance(value, list)
                and bool(value)
                and all(isinstance(item, str) and item.strip() for item in value)
                and len(value) == len(set(value))
            )
        if not valid_type:
            raise ValueError(f"{name} has an invalid value or type")
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if not math.isfinite(value):
                raise ValueError(f"{name} must be finite")

    bounds = {
        "MAX_PAIRS": (1, 200),
        "MIN_VOLUME_USDT_24H": (0, 1_000_000_000_000),
        "HISTORICAL_LIMIT": (50, 5000),
        "FRESHNESS_TICKER": (1, 3600),
        "FRESHNESS_ORDERBOOK": (1, 300),
        "FRESHNESS_OHLCV": (1, 3600),
        "FRESHNESS_FUNDING": (1, 86400),
        "FRESHNESS_OI": (1, 86400),
        "REST_SLEEP": (0.01, 30),
        "MAX_RETRIES": (1, 20),
        "RECONNECT_DELAY": (0.1, 300),
        "HEARTBEAT_INTERVAL": (1, 3600),
        "SCAN_INTERVAL": (1, 3600),
        "CANDLE_UPDATE_INTERVAL": (1, 3600),
        "ANALYSIS_INTERVAL": (1, 3600),
        "RSI_PERIOD": (2, 1000),
        "ATR_PERIOD": (2, 1000),
        "ADX_PERIOD": (2, 1000),
        "BB_PERIOD": (2, 1000),
        "BB_STD": (0.1, 10),
        "EMA_FAST": (2, 1000),
        "EMA_SLOW": (2, 2000),
        "EMA_TREND": (2, 5000),
        "SWING_LOOKBACK": (1, 100),
        "STRUCTURE_LOOKBACK": (10, 5000),
        "ACCOUNT_BALANCE": (0.01, 1_000_000_000_000),
        "RISK_PER_TRADE_PCT": (0.01, 5),
        "MAX_RISK_PER_TRADE_USD": (0.01, 1_000_000),
        "MIN_NET_RR": (0.1, 100),
        "TAKER_FEE_PCT": (0, 0.1),
        "MAKER_FEE_PCT": (0, 0.1),
        "DEFAULT_SLIPPAGE_BPS": (0, 1000),
        "FUNDING_HORIZON_HOURS": (0, 168),
        "MIN_CONFIDENCE": (0, 1),
        "OPPORTUNITY_SCAN_INTERVAL": (1, 86400),
        "MAX_LIVE_DAILY_LOSS_USD": (0.01, 1_000_000),
        "MAX_LIVE_ORDERS_PER_MINUTE": (1, 120),
        "MAX_LIVE_NOTIONAL_USD": (0.01, 1_000_000),
        "MAX_LIVE_POSITIONS": (1, 100),
        "MAX_LIVE_LEVERAGE": (0.1, 3),
        "MAX_LIVE_ENTRY_DRIFT_PCT": (0.01, 2),
        "MAX_PORTFOLIO_NOTIONAL_PCT": (0.1, 1000),
        "MAX_CORRELATED_POSITIONS": (1, 100),
        "PENDING_ORDER_MAX_SECONDS": (1, 2_592_000),
    }
    for name, (minimum, maximum) in bounds.items():
        value = candidate[name]
        if not minimum <= value <= maximum:
            raise ValueError(f"{name} must be between {minimum} and {maximum}")

    if candidate["LOG_LEVEL"] not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        raise ValueError("LOG_LEVEL must be DEBUG, INFO, WARNING, ERROR, or CRITICAL")
    exchanges = candidate["EXCHANGE_PRIORITY"]
    if not exchanges or any(exchange not in ccxt.exchanges for exchange in exchanges):
        raise ValueError("EXCHANGE_PRIORITY must contain supported CCXT exchange IDs")
    if candidate["PRIMARY_EXCHANGE"] not in exchanges:
        raise ValueError("PRIMARY_EXCHANGE must be included in EXCHANGE_PRIORITY")
    if not candidate["QUOTE"].strip():
        raise ValueError("QUOTE cannot be empty")
    for name in ("TIMEFRAMES",):
        for timeframe in candidate[name]:
            timeframe_milliseconds(timeframe)
    if not {
        candidate["DEFAULT_TF"], candidate["ANALYSIS_TF"], candidate["HIGHER_TF"]
    }.issubset(candidate["TIMEFRAMES"]):
        raise ValueError("DEFAULT_TF, ANALYSIS_TF, and HIGHER_TF must be in TIMEFRAMES")
    if not (candidate["EMA_FAST"] < candidate["EMA_SLOW"] < candidate["EMA_TREND"]):
        raise ValueError("EMA periods must satisfy EMA_FAST < EMA_SLOW < EMA_TREND")
    return candidate


def _load_settings() -> None:
    try:
        if Config.EXECUTION_MODE not in {"paper", "live"}:
            raise ValueError("EXECUTION_MODE must be paper or live")
        settings = {}
        if Config.SETTINGS_PATH.exists():
            settings = json.loads(Config.SETTINGS_PATH.read_text(encoding="utf-8"))
        if not isinstance(settings, dict):
            raise ValueError("settings file must contain a JSON object")
        candidate = _validate_settings(settings)
        effective = {
            name: getattr(Config, name) if name in os.environ else value
            for name, value in candidate.items()
        }
        _validate_settings(effective)
        for name, value in candidate.items():
            if name not in os.environ:
                setattr(Config, name, value)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        raise RuntimeError(f"Invalid APEX settings at {Config.SETTINGS_PATH}: {exc}") from exc


# ===========================================================================
# SECTION 1 – LOGGING & UTILITIES
# ===========================================================================

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("APEX")
    logger.setLevel(getattr(logging, Config.LOG_LEVEL, logging.INFO))
    logger.handlers.clear()
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    if Config.LOG_TO_FILE:
        fh = logging.FileHandler(Config.LOG_DIR / "apex.log", encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


log = setup_logging()


def _parse_config_value(name: str, raw_value: str) -> Any:
    current = configurable_settings().get(name)
    if current is None:
        raise ValueError(f"Unknown or non-configurable setting: {name}")
    if isinstance(current, bool):
        normalized = raw_value.strip().lower()
        if normalized not in {"true", "false"}:
            raise ValueError(f"{name} must be true or false")
        return normalized == "true"
    if isinstance(current, int):
        return int(raw_value)
    if isinstance(current, float):
        return float(raw_value)
    if isinstance(current, list):
        value = json.loads(raw_value)
        if not isinstance(value, list):
            raise ValueError(f"{name} must be a JSON array")
        return value
    return raw_value.strip()


def _write_settings(settings: Dict[str, Any]) -> None:
    candidate = _validate_settings(settings)
    effective = {
        name: getattr(Config, name) if name in os.environ else value
        for name, value in candidate.items()
    }
    _validate_settings(effective)

    temporary_path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=Config.SETTINGS_PATH.parent,
            prefix=".apex-settings-",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            json.dump(settings, temporary, indent=2, sort_keys=True)
            temporary.write("\n")
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_path, Config.SETTINGS_PATH)
    except OSError:
        if temporary_path is not None:
            try:
                temporary_path.unlink()
            except FileNotFoundError:
                pass
        raise


def run_config_command(arguments: List[str]) -> int:
    if not arguments or arguments[0] in {"help", "--help", "-h"}:
        print(
            "Usage:\n"
            "  python apex.py config show\n"
            "  python apex.py config set NAME=VALUE [NAME=VALUE ...]\n"
            "List settings with `python apex.py config show`. "
            "String values are unquoted; list values use JSON syntax."
        )
        return 0
    action, *operands = arguments
    if action == "show" and not operands:
        for name, value in sorted(configurable_settings().items()):
            print(f"{name}={json.dumps(value)}")
        print(f"Settings file: {Config.SETTINGS_PATH}")
        return 0
    if action != "set" or not operands:
        print("Use `python apex.py config show` or `python apex.py config set NAME=VALUE ...`.", file=sys.stderr)
        return 2

    updates: Dict[str, Any] = {}
    try:
        for operand in operands:
            name, separator, raw_value = operand.partition("=")
            if not separator or not name.strip():
                raise ValueError(f"Expected NAME=VALUE, got: {operand}")
            name = name.strip().upper()
            if name in updates:
                raise ValueError(f"{name} was specified more than once")
            updates[name] = _parse_config_value(name, raw_value)

        stored = {}
        if Config.SETTINGS_PATH.exists():
            stored = json.loads(Config.SETTINGS_PATH.read_text(encoding="utf-8"))
            if not isinstance(stored, dict):
                raise ValueError("settings file must contain a JSON object")
        stored.update(updates)
        _write_settings(stored)
    except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
        print(f"Configuration was not saved: {exc}", file=sys.stderr)
        return 2

    print(f"Saved validated settings to {Config.SETTINGS_PATH}")
    for name in sorted(updates):
        if name in os.environ:
            print(f"{name} saved; environment variable takes precedence.")
        else:
            print(f"{name}={json.dumps(updates[name])}")
    return 0


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_ms() -> int:
    return int(time.time() * 1000)


def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def ms_to_dt(ms: Optional[int]) -> Optional[datetime]:
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    except Exception:
        return None


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def timeframe_milliseconds(timeframe: str) -> int:
    units = {"m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}
    try:
        value, unit = int(timeframe[:-1]), timeframe[-1]
        if value <= 0 or unit not in units:
            raise ValueError
        return value * units[unit]
    except (TypeError, ValueError, IndexError):
        raise ValueError(f"Unsupported timeframe: {timeframe}") from None


def closed_candles(
    candles: List["Candle"], timeframe: str, now_ms: Optional[int] = None
) -> List["Candle"]:
    cutoff = (utc_ms() if now_ms is None else now_ms) - timeframe_milliseconds(timeframe)
    return [candle for candle in candles if candle.timestamp <= cutoff]


_load_settings()


# ===========================================================================
# SECTION 2 – DATA MODELS (Stage 1 + Stage 2)
# ===========================================================================

class DataFreshness(Enum):
    FRESH = "fresh"
    STALE = "stale"
    MISSING = "missing"
    CORRUPT = "corrupt"


class MarketRegime(Enum):
    STRONG_BULL = "strong_bull"
    WEAK_BULL = "weak_bull"
    STRONG_BEAR = "strong_bear"
    WEAK_BEAR = "weak_bear"
    SIDEWAYS = "sideways"
    HIGH_VOL = "high_volatility"
    LOW_VOL = "low_volatility"
    EXPANDING_VOL = "expanding_volatility"
    CONTRACTING_VOL = "contracting_volatility"
    UNSTABLE = "unstable"
    UNKNOWN = "unknown"


class StructureType(Enum):
    HIGHER_HIGH = "HH"
    HIGHER_LOW = "HL"
    LOWER_HIGH = "LH"
    LOWER_LOW = "LL"
    EQUAL_HIGH = "EH"
    EQUAL_LOW = "EL"
    BOS_BULL = "BOS_BULL"          # Break of Structure bullish
    BOS_BEAR = "BOS_BEAR"
    CHOCH_BULL = "CHOCH_BULL"      # Change of Character
    CHOCH_BEAR = "CHOCH_BEAR"
    RANGE = "RANGE"


@dataclass
class MarketMeta:
    exchange: str
    symbol: str
    base: str
    quote: str
    settle: str
    contract_size: float = 1.0
    active: bool = True
    precision_price: Optional[int] = None
    precision_amount: Optional[float] = None
    precision_mode: Optional[int] = None
    limits: Dict[str, Any] = field(default_factory=dict)
    info: Dict[str, Any] = field(default_factory=dict)

    @property
    def key(self) -> str:
        return f"{self.exchange}:{self.symbol}"


@dataclass
class TickerSnapshot:
    exchange: str
    symbol: str
    timestamp: int
    last: float
    bid: float
    ask: float
    mark: Optional[float] = None
    index: Optional[float] = None
    high_24h: Optional[float] = None
    low_24h: Optional[float] = None
    volume_24h: Optional[float] = None
    quote_volume_24h: Optional[float] = None
    change_24h_pct: Optional[float] = None
    spread: Optional[float] = None
    spread_bps: Optional[float] = None
    received_at: float = field(default_factory=time.time)

    def freshness(self) -> DataFreshness:
        age = time.time() - self.received_at
        timestamp_age = 0.0
        if self.timestamp > 0:
            timestamp_age = max(0.0, time.time() - (self.timestamp / 1000.0))
        age = max(age, timestamp_age)
        if age > Config.FRESHNESS_TICKER * 3:
            return DataFreshness.MISSING
        if age > Config.FRESHNESS_TICKER:
            return DataFreshness.STALE
        return DataFreshness.FRESH


@dataclass
class OrderBookSnapshot:
    exchange: str
    symbol: str
    timestamp: int
    bids: List[Tuple[float, float]]
    asks: List[Tuple[float, float]]
    received_at: float = field(default_factory=time.time)

    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0][0] if self.asks else None

    @property
    def mid(self) -> Optional[float]:
        if self.best_bid and self.best_ask:
            return (self.best_bid + self.best_ask) / 2
        return None

    def imbalance(self, levels: int = 10) -> float:
        b = sum(s for _, s in self.bids[:levels])
        a = sum(s for _, s in self.asks[:levels])
        total = b + a
        if total <= 0:
            return 0.0
        return (b - a) / total

    def depth_usd(self, levels: int = 10) -> Tuple[float, float]:
        bid_d = sum(p * s for p, s in self.bids[:levels])
        ask_d = sum(p * s for p, s in self.asks[:levels])
        return bid_d, ask_d

    def freshness(self) -> DataFreshness:
        age = max(
            time.time() - self.received_at,
            time.time() - (self.timestamp / 1000.0),
        )
        if age > Config.FRESHNESS_ORDERBOOK * 4:
            return DataFreshness.MISSING
        if age > Config.FRESHNESS_ORDERBOOK:
            return DataFreshness.STALE
        return DataFreshness.FRESH


@dataclass
class FundingSnapshot:
    exchange: str
    symbol: str
    timestamp: int
    funding_rate: float
    next_funding_time: Optional[int] = None
    mark_price: Optional[float] = None
    index_price: Optional[float] = None
    received_at: float = field(default_factory=time.time)

    def freshness(self) -> DataFreshness:
        age = max(
            time.time() - self.received_at,
            time.time() - (self.timestamp / 1000.0),
        )
        if age > Config.FRESHNESS_FUNDING:
            return DataFreshness.STALE
        return DataFreshness.FRESH


@dataclass
class OpenInterestSnapshot:
    exchange: str
    symbol: str
    timestamp: int
    open_interest: float
    open_interest_value: Optional[float] = None
    received_at: float = field(default_factory=time.time)

    def freshness(self) -> DataFreshness:
        age = max(
            time.time() - self.received_at,
            time.time() - (self.timestamp / 1000.0),
        )
        if age > Config.FRESHNESS_OI:
            return DataFreshness.STALE
        return DataFreshness.FRESH


@dataclass
class Candle:
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float


# ---------- Stage 2 analytical result containers ----------

@dataclass
class IndicatorSet:
    """Snapshot of classic technical indicators at the latest closed bar."""
    rsi: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_hist: Optional[float] = None
    ema_fast: Optional[float] = None
    ema_slow: Optional[float] = None
    ema_trend: Optional[float] = None
    sma_20: Optional[float] = None
    atr: Optional[float] = None
    atr_pct: Optional[float] = None
    adx: Optional[float] = None
    plus_di: Optional[float] = None
    minus_di: Optional[float] = None
    bb_upper: Optional[float] = None
    bb_middle: Optional[float] = None
    bb_lower: Optional[float] = None
    bb_width: Optional[float] = None
    stoch_k: Optional[float] = None
    stoch_d: Optional[float] = None
    obv: Optional[float] = None
    vwap: Optional[float] = None
    volume_sma: Optional[float] = None


@dataclass
class SwingPoint:
    index: int
    timestamp: int
    price: float
    kind: str          # "high" or "low"
    strength: int = 1


@dataclass
class StructureAnalysis:
    swings: List[SwingPoint] = field(default_factory=list)
    last_hh: Optional[float] = None
    last_hl: Optional[float] = None
    last_lh: Optional[float] = None
    last_ll: Optional[float] = None
    trend: str = "unknown"          # "bullish", "bearish", "range"
    bos: Optional[str] = None       # "bull" / "bear" / None
    choch: Optional[str] = None
    support: List[float] = field(default_factory=list)
    resistance: List[float] = field(default_factory=list)
    range_high: Optional[float] = None
    range_low: Optional[float] = None
    notes: List[str] = field(default_factory=list)


@dataclass
class LiquidityAnalysis:
    equal_highs: List[float] = field(default_factory=list)
    equal_lows: List[float] = field(default_factory=list)
    buy_side_liquidity: List[float] = field(default_factory=list)   # above price
    sell_side_liquidity: List[float] = field(default_factory=list)  # below price
    recent_sweep: Optional[str] = None   # "buy_side" / "sell_side" / None
    orderbook_imbalance: Optional[float] = None
    bid_depth_usd: Optional[float] = None
    ask_depth_usd: Optional[float] = None
    notes: List[str] = field(default_factory=list)


@dataclass
class OrderFlowAnalysis:
    # Proxy metrics derived from available public data (no true tick L2 tape on free tier)
    volume_delta_proxy: Optional[float] = None
    buy_pressure: Optional[float] = None
    sell_pressure: Optional[float] = None
    large_print_bias: Optional[str] = None
    absorption_hint: Optional[str] = None
    notes: List[str] = field(default_factory=list)


@dataclass
class DerivativesAnalysis:
    funding_rate: Optional[float] = None
    funding_annualised: Optional[float] = None
    funding_extreme: Optional[str] = None   # "longs_pay" / "shorts_pay" / None
    open_interest: Optional[float] = None
    open_interest_value: Optional[float] = None
    oi_change_pct: Optional[float] = None
    price_oi_regime: Optional[str] = None  # e.g. "price_up_oi_up"
    basis_proxy: Optional[float] = None
    notes: List[str] = field(default_factory=list)


@dataclass
class VolatilityAnalysis:
    atr: Optional[float] = None
    atr_pct: Optional[float] = None
    realized_vol_24h: Optional[float] = None
    bb_width: Optional[float] = None
    bb_width_percentile: Optional[float] = None
    vol_regime: MarketRegime = MarketRegime.UNKNOWN
    expanding: bool = False
    contracting: bool = False
    notes: List[str] = field(default_factory=list)


@dataclass
class RegimeSnapshot:
    primary: MarketRegime = MarketRegime.UNKNOWN
    direction: str = "neutral"          # bullish / bearish / neutral
    volatility: str = "normal"          # low / normal / high
    structure_bias: str = "unknown"
    confidence: float = 0.0             # 0-1, based on agreement of signals
    notes: List[str] = field(default_factory=list)


@dataclass
class AssetAnalysis:
    """Full Stage-2 analytical package for one symbol."""
    exchange: str
    symbol: str
    timeframe: str
    timestamp: int
    price: float
    indicators: IndicatorSet = field(default_factory=IndicatorSet)
    structure: StructureAnalysis = field(default_factory=StructureAnalysis)
    liquidity: LiquidityAnalysis = field(default_factory=LiquidityAnalysis)
    orderflow: OrderFlowAnalysis = field(default_factory=OrderFlowAnalysis)
    derivatives: DerivativesAnalysis = field(default_factory=DerivativesAnalysis)
    volatility: VolatilityAnalysis = field(default_factory=VolatilityAnalysis)
    regime: RegimeSnapshot = field(default_factory=RegimeSnapshot)
    data_quality: str = "unknown"
    higher_timeframe_direction: str = "unknown"
    computed_at: float = field(default_factory=time.time)


# ===========================================================================
# SECTION 3 – DATA VALIDATION
# ===========================================================================

class DataValidator:
    @staticmethod
    def _number(value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, bool):
            raise ValueError("boolean is not a market number")
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("market number is not finite")
        return number

    @staticmethod
    def _timestamp(value: Any, max_age_seconds: Optional[float] = None) -> int:
        if value is None:
            timestamp = utc_ms()
        else:
            if isinstance(value, bool):
                raise ValueError("timestamp is invalid")
            timestamp_value = float(value)
            if not math.isfinite(timestamp_value) or not timestamp_value.is_integer():
                raise ValueError("timestamp is invalid")
            timestamp = int(timestamp_value)
        now = utc_ms()
        if timestamp <= 0 or timestamp > now + 30_000:
            raise ValueError("timestamp is outside the acceptable range")
        if max_age_seconds is not None and now - timestamp > max_age_seconds * 1000:
            raise ValueError("market data is too old")
        return timestamp

    @staticmethod
    def validate_ticker(raw: Dict[str, Any], exchange: str, symbol: str) -> Optional[TickerSnapshot]:
        try:
            if not isinstance(raw, dict):
                return None
            last_value = raw.get("last")
            if last_value is None:
                last_value = raw.get("close")
            last = DataValidator._number(last_value)
            bid = DataValidator._number(raw.get("bid"))
            ask = DataValidator._number(raw.get("ask"))
            if (bid is None) != (ask is None):
                return None
            if bid is not None and ask is not None and (
                bid <= 0 or ask <= 0 or ask < bid
            ):
                return None
            if last is None and bid is not None and ask is not None:
                last = (bid + ask) / 2
            if last is None or last <= 0:
                return None

            info = raw.get("info") or {}
            if not isinstance(info, dict):
                return None
            mark_value = raw.get("mark")
            if mark_value is None:
                mark_value = info.get("markPx")
            index_value = raw.get("index")
            if index_value is None:
                index_value = info.get("indexPx")
            mark = DataValidator._number(mark_value)
            index = DataValidator._number(index_value)
            high = DataValidator._number(raw.get("high"))
            low = DataValidator._number(raw.get("low"))
            volume = DataValidator._number(raw.get("baseVolume"))
            quote_volume = DataValidator._number(raw.get("quoteVolume"))
            change = DataValidator._number(raw.get("percentage"))
            if any(value is not None and value <= 0 for value in (mark, index, high, low)):
                return None
            if high is not None and low is not None and high < low:
                return None
            if any(value is not None and value < 0 for value in (volume, quote_volume)):
                return None
            ts = DataValidator._timestamp(
                raw.get("timestamp"), Config.FRESHNESS_TICKER * 3
            )
            spread = spread_bps = None
            if bid is not None and ask is not None:
                spread = ask - bid
                mid = (ask + bid) / 2
                if mid > 0:
                    spread_bps = (spread / mid) * 10_000
            return TickerSnapshot(
                exchange=exchange, symbol=symbol, timestamp=ts,
                last=last,
                bid=bid, ask=ask,
                mark=mark, index=index,
                high_24h=high, low_24h=low,
                volume_24h=volume,
                quote_volume_24h=quote_volume,
                change_24h_pct=change,
                spread=spread, spread_bps=spread_bps,
            )
        except (TypeError, ValueError, OverflowError) as e:
            log.debug("Ticker validation failed %s %s: %s", exchange, symbol, e)
            return None

    @staticmethod
    def validate_orderbook(raw: Dict[str, Any], exchange: str, symbol: str) -> Optional[OrderBookSnapshot]:
        try:
            if not isinstance(raw, dict):
                return None

            def levels(name: str) -> List[Tuple[float, float]]:
                result = []
                for row in raw.get(name) or []:
                    if not isinstance(row, (list, tuple)) or len(row) < 2:
                        raise ValueError(f"invalid {name} level")
                    price = DataValidator._number(row[0])
                    size = DataValidator._number(row[1])
                    if price is None or size is None or price <= 0 or size <= 0:
                        raise ValueError(f"invalid {name} price or size")
                    result.append((price, size))
                return result

            bids = levels("bids")
            asks = levels("asks")
            bids = sorted(bids, key=lambda x: -x[0])[:50]
            asks = sorted(asks, key=lambda x: x[0])[:50]
            if not bids or not asks or bids[0][0] >= asks[0][0]:
                return None
            ts = DataValidator._timestamp(
                raw.get("timestamp"), Config.FRESHNESS_ORDERBOOK * 4
            )
            return OrderBookSnapshot(exchange=exchange, symbol=symbol, timestamp=ts, bids=bids, asks=asks)
        except (TypeError, ValueError, OverflowError) as e:
            log.debug("Orderbook validation failed %s %s: %s", exchange, symbol, e)
            return None

    @staticmethod
    def validate_funding_rate(
        raw: Dict[str, Any], exchange: str, symbol: str
    ) -> Optional[FundingSnapshot]:
        try:
            if not isinstance(raw, dict):
                return None
            rate = DataValidator._number(raw.get("fundingRate"))
            if rate is None or abs(rate) > 1:
                return None
            mark = DataValidator._number(raw.get("markPrice"))
            index = DataValidator._number(raw.get("indexPrice"))
            if any(value is not None and value <= 0 for value in (mark, index)):
                return None
            timestamp = DataValidator._timestamp(
                raw.get("timestamp"), Config.FRESHNESS_FUNDING * 2
            )
            next_time = raw.get("fundingTimestamp") or raw.get("nextFundingTimestamp")
            if next_time is not None:
                next_timestamp = float(next_time)
                if (
                    not math.isfinite(next_timestamp)
                    or not next_timestamp.is_integer()
                    or next_timestamp <= 0
                    or next_timestamp > utc_ms() + 7 * 86_400_000
                ):
                    return None
                next_time = int(next_timestamp)
            return FundingSnapshot(
                exchange=exchange,
                symbol=symbol,
                timestamp=timestamp,
                funding_rate=rate,
                next_funding_time=next_time,
                mark_price=mark,
                index_price=index,
            )
        except (TypeError, ValueError, OverflowError) as exc:
            log.debug("Funding validation failed %s %s: %s", exchange, symbol, exc)
            return None

    @staticmethod
    def validate_open_interest(
        raw: Dict[str, Any], exchange: str, symbol: str
    ) -> Optional[OpenInterestSnapshot]:
        try:
            if not isinstance(raw, dict):
                return None
            oi_value = raw.get("openInterestAmount")
            if oi_value is None:
                oi_value = raw.get("openInterest")
            open_interest = DataValidator._number(oi_value)
            value = DataValidator._number(raw.get("openInterestValue"))
            if (
                open_interest is None or open_interest <= 0
                or (value is not None and value <= 0)
            ):
                return None
            timestamp = DataValidator._timestamp(
                raw.get("timestamp"), Config.FRESHNESS_OI * 2
            )
            return OpenInterestSnapshot(
                exchange=exchange,
                symbol=symbol,
                timestamp=timestamp,
                open_interest=open_interest,
                open_interest_value=value,
            )
        except (TypeError, ValueError, OverflowError) as exc:
            log.debug("Open-interest validation failed %s %s: %s", exchange, symbol, exc)
            return None

    @staticmethod
    def validate_ohlcv(rows: List[List]) -> List[Candle]:
        candles = []
        previous_timestamp = 0
        for row in rows or []:
            try:
                if not isinstance(row, (list, tuple)) or len(row) < 6:
                    return []
                timestamp = DataValidator._timestamp(row[0])
                values = [DataValidator._number(value) for value in row[1:6]]
                if any(value is None for value in values):
                    return []
                opening, high, low, close, volume = values
                if (
                    opening <= 0 or high <= 0 or low <= 0 or close <= 0
                    or volume < 0 or high < max(opening, close)
                    or low > min(opening, close) or low > high
                    or timestamp <= previous_timestamp
                ):
                    return []
                candles.append(Candle(timestamp, opening, high, low, close, volume))
                previous_timestamp = timestamp
            except (TypeError, ValueError, OverflowError) as exc:
                log.debug("OHLCV validation failed: %s", exc)
                return []
        return candles


# ===========================================================================
# SECTION 4 – STORAGE (SQLite)
# ===========================================================================

class MarketDatabase:
    def __init__(self, path: Path = Config.DB_PATH):
        self.path = path
        self._conn: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._create_tables()
        log.info("SQLite connected: %s", self.path)

    def _create_tables(self) -> None:
        assert self._conn
        self._conn.executescript("""
        CREATE TABLE IF NOT EXISTS markets (
            exchange TEXT NOT NULL, symbol TEXT NOT NULL,
            base TEXT, quote TEXT, settle TEXT, contract_size REAL,
            active INTEGER, meta_json TEXT, updated_at INTEGER,
            PRIMARY KEY (exchange, symbol)
        );
        CREATE TABLE IF NOT EXISTS candles (
            exchange TEXT NOT NULL, symbol TEXT NOT NULL, timeframe TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            open REAL, high REAL, low REAL, close REAL, volume REAL,
            PRIMARY KEY (exchange, symbol, timeframe, timestamp)
        );
        CREATE INDEX IF NOT EXISTS idx_candles_lookup
            ON candles (exchange, symbol, timeframe, timestamp DESC);
        CREATE TABLE IF NOT EXISTS system_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER, level TEXT, component TEXT, message TEXT
        );
        CREATE TABLE IF NOT EXISTS analysis_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange TEXT, symbol TEXT, timeframe TEXT, ts INTEGER,
            regime TEXT, direction TEXT, price REAL, payload TEXT
        );
        CREATE TABLE IF NOT EXISTS opportunity_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            status TEXT,
            score REAL,
            preferred_entry REAL,
            stop_loss REAL,
            tp1 REAL,
            net_rr REAL,
            payload TEXT
        );
        CREATE TABLE IF NOT EXISTS paper_positions (
            id TEXT PRIMARY KEY,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            status TEXT NOT NULL,
            entry_price REAL NOT NULL,
            size REAL NOT NULL,
            stop_loss REAL,
            tp1 REAL,
            tp2 REAL,
            tp3 REAL,
            opened_at REAL,
            closed_at REAL,
            exit_price REAL,
            exit_reason TEXT,
            realized_pnl REAL,
            fees_paid REAL,
            funding_paid REAL,
            r_multiple REAL,
            opportunity_score REAL,
            payload TEXT
        );
        CREATE TABLE IF NOT EXISTS paper_equity (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            balance REAL NOT NULL,
            source TEXT
        );
        CREATE TABLE IF NOT EXISTS paper_pending_opportunities (
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            created_at REAL NOT NULL,
            payload TEXT NOT NULL,
            PRIMARY KEY (exchange, symbol)
        );
        CREATE TABLE IF NOT EXISTS paper_candle_checkpoints (
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            PRIMARY KEY (exchange, symbol, timeframe)
        );
        CREATE TABLE IF NOT EXISTS live_protection_intents (
            symbol TEXT PRIMARY KEY,
            direction TEXT NOT NULL,
            amount REAL NOT NULL,
            stop REAL NOT NULL,
            tp1 REAL,
            tp2 REAL,
            tp3 REAL,
            order_id TEXT,
            completed_targets TEXT NOT NULL DEFAULT '[]',
            updated_at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_opportunity_lookup
            ON opportunity_snapshots (ts DESC, exchange, symbol);
        CREATE INDEX IF NOT EXISTS idx_paper_positions_status
            ON paper_positions (status, exchange, symbol);
        """)
        self._conn.commit()

    def upsert_market(self, meta: MarketMeta) -> None:
        if not self._conn:
            return
        self._conn.execute(
            """INSERT INTO markets (exchange, symbol, base, quote, settle, contract_size, active, meta_json, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(exchange, symbol) DO UPDATE SET
                 base=excluded.base, quote=excluded.quote, settle=excluded.settle,
                 contract_size=excluded.contract_size, active=excluded.active,
                 meta_json=excluded.meta_json, updated_at=excluded.updated_at""",
            (meta.exchange, meta.symbol, meta.base, meta.quote, meta.settle,
             meta.contract_size, 1 if meta.active else 0,
             json.dumps(meta.info or {}, default=str)[:4000], utc_ms()),
        )
        self._conn.commit()

    def insert_candles(self, exchange: str, symbol: str, timeframe: str, candles: List[Candle]) -> int:
        if not self._conn or not candles:
            return 0
        validated = DataValidator.validate_ohlcv([
            [c.timestamp, c.open, c.high, c.low, c.close, c.volume]
            for c in candles
        ])
        if len(validated) != len(candles):
            log.error("Rejected invalid candle batch %s %s %s", exchange, symbol, timeframe)
            return 0
        rows = [(exchange, symbol, timeframe, c.timestamp, c.open, c.high, c.low, c.close, c.volume) for c in candles]
        self._conn.executemany(
            """INSERT OR REPLACE INTO candles
               (exchange, symbol, timeframe, timestamp, open, high, low, close, volume)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", rows)
        self._conn.commit()
        return len(rows)

    def get_candles(self, exchange: str, symbol: str, timeframe: str, limit: int = 500) -> List[Candle]:
        if not self._conn:
            return []
        cur = self._conn.execute(
            """SELECT timestamp, open, high, low, close, volume FROM candles
               WHERE exchange=? AND symbol=? AND timeframe=?
               ORDER BY timestamp DESC LIMIT ?""",
            (exchange, symbol, timeframe, limit),
        )
        rows = cur.fetchall()
        ordered = list(reversed(rows))
        candles = DataValidator.validate_ohlcv(ordered)
        if len(candles) != len(ordered):
            log.error("Rejected corrupt candles from storage %s %s %s", exchange, symbol, timeframe)
            return []
        return candles

    def save_analysis(self, a: AssetAnalysis) -> None:
        if not self._conn:
            return
        payload = {
            "regime": a.regime.primary.value,
            "direction": a.regime.direction,
            "rsi": a.indicators.rsi,
            "atr_pct": a.indicators.atr_pct,
            "adx": a.indicators.adx,
            "structure_trend": a.structure.trend,
            "funding": a.derivatives.funding_rate,
        }
        self._conn.execute(
            """INSERT INTO analysis_snapshots (exchange, symbol, timeframe, ts, regime, direction, price, payload)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (a.exchange, a.symbol, a.timeframe, a.timestamp,
             a.regime.primary.value, a.regime.direction, a.price, json.dumps(payload)),
        )
        self._conn.commit()

    def log_event(self, level: str, component: str, message: str) -> None:
        if not self._conn:
            return
        self._conn.execute(
            "INSERT INTO system_events (ts, level, component, message) VALUES (?, ?, ?, ?)",
            (utc_ms(), level, component, message[:1000]),
        )
        self._conn.commit()

    def save_opportunity(self, opportunity: "TradeOpportunity") -> None:
        if not self._conn:
            return
        payload = asdict(opportunity)
        self._conn.execute(
            """INSERT INTO opportunity_snapshots
               (ts, exchange, symbol, direction, status, score, preferred_entry,
                stop_loss, tp1, net_rr, payload)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (utc_ms(), opportunity.exchange, opportunity.symbol,
             opportunity.direction.value, opportunity.status, opportunity.score,
             opportunity.preferred_entry, opportunity.stop_loss.price,
             opportunity.take_profit.tp1, opportunity.risk_reward.net_rr_tp1,
             json.dumps(payload, default=str)),
        )
        self._conn.commit()

    def save_pending_opportunity(
        self, opportunity: "TradeOpportunity", created_at: float
    ) -> None:
        if not self._conn:
            return
        self._conn.execute(
            """INSERT INTO paper_pending_opportunities
               (exchange, symbol, created_at, payload) VALUES (?, ?, ?, ?)
               ON CONFLICT(exchange, symbol) DO UPDATE SET
                 created_at=excluded.created_at, payload=excluded.payload""",
            (
                opportunity.exchange,
                opportunity.symbol,
                created_at,
                serialize_trade_opportunity(opportunity),
            ),
        )
        self._conn.commit()

    def get_pending_opportunities(self) -> List[Tuple["TradeOpportunity", float]]:
        if not self._conn:
            return []
        rows = self._conn.execute(
            "SELECT payload, created_at FROM paper_pending_opportunities"
        ).fetchall()
        pending = []
        for payload, created_at in rows:
            try:
                pending.append((deserialize_trade_opportunity(payload), float(created_at)))
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                log.exception("Unable to restore paper pending opportunity: %s", exc)
        return pending

    def delete_pending_opportunity(self, exchange: str, symbol: str) -> None:
        if not self._conn:
            return
        self._conn.execute(
            "DELETE FROM paper_pending_opportunities WHERE exchange=? AND symbol=?",
            (exchange, symbol),
        )
        self._conn.commit()

    def save_live_protection_intent(
        self,
        symbol: str,
        direction: str,
        amount: float,
        stop: float,
        tp1: Optional[float],
        tp2: Optional[float],
        tp3: Optional[float],
        order_id: Optional[str] = None,
        completed_targets: Optional[List[str]] = None,
    ) -> None:
        if not self._conn:
            return
        self._conn.execute(
            """INSERT INTO live_protection_intents
               (symbol, direction, amount, stop, tp1, tp2, tp3, order_id,
                completed_targets, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(symbol) DO UPDATE SET
                 direction=excluded.direction, amount=excluded.amount,
                 stop=excluded.stop, tp1=excluded.tp1, tp2=excluded.tp2,
                 tp3=excluded.tp3, order_id=excluded.order_id,
                 completed_targets=excluded.completed_targets,
                 updated_at=excluded.updated_at""",
            (
                symbol, direction, amount, stop, tp1, tp2, tp3, order_id,
                json.dumps(completed_targets or []), time.time(),
            ),
        )
        self._conn.commit()

    def get_live_protection_intents(self) -> Dict[str, Dict[str, Any]]:
        if not self._conn:
            return {}
        rows = self._conn.execute(
            """SELECT symbol, direction, amount, stop, tp1, tp2, tp3, order_id,
                      completed_targets
               FROM live_protection_intents"""
        ).fetchall()
        return {
            row[0]: {
                "direction": row[1], "amount": row[2], "stop": row[3],
                "tp1": row[4], "tp2": row[5], "tp3": row[6], "order_id": row[7],
                "completed_targets": json.loads(row[8] or "[]"),
            }
            for row in rows
        }

    def delete_live_protection_intent(self, symbol: str) -> None:
        if not self._conn:
            return
        self._conn.execute(
            "DELETE FROM live_protection_intents WHERE symbol=?", (symbol,)
        )
        self._conn.commit()

    def save_paper_position(self, pos: "PaperPosition") -> None:
        if not self._conn:
            return
        payload = asdict(pos)
        self._conn.execute(
            """INSERT INTO paper_positions
               (id, exchange, symbol, direction, status, entry_price, size, stop_loss,
                tp1, tp2, tp3, opened_at, closed_at, exit_price, exit_reason,
                realized_pnl, fees_paid, funding_paid, r_multiple,
                opportunity_score, payload)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                 status=excluded.status, closed_at=excluded.closed_at,
                 exit_price=excluded.exit_price, exit_reason=excluded.exit_reason,
                 realized_pnl=excluded.realized_pnl, fees_paid=excluded.fees_paid,
                 funding_paid=excluded.funding_paid, r_multiple=excluded.r_multiple,
                 payload=excluded.payload""",
            (pos.id, pos.exchange, pos.symbol, pos.direction.value, pos.status,
             pos.entry_price, pos.size, pos.stop_loss, pos.take_profit_1,
             pos.take_profit_2, pos.take_profit_3, pos.opened_at, pos.closed_at,
             pos.exit_price, pos.exit_reason, pos.realized_pnl, pos.fees_paid,
             pos.funding_paid, pos.r_multiple, pos.opportunity_score,
             json.dumps(payload, default=str)),
        )
        self._conn.commit()

    def save_paper_equity(self, balance: float, source: str = "paper") -> None:
        if not self._conn:
            return
        self._conn.execute(
            "INSERT INTO paper_equity (ts, balance, source) VALUES (?, ?, ?)",
            (time.time(), balance, source),
        )
        self._conn.commit()

    def get_paper_candle_checkpoint(
        self, exchange: str, symbol: str, timeframe: str
    ) -> Optional[int]:
        if not self._conn:
            return None
        row = self._conn.execute(
            """SELECT timestamp FROM paper_candle_checkpoints
               WHERE exchange=? AND symbol=? AND timeframe=?""",
            (exchange, symbol, timeframe),
        ).fetchone()
        return int(row[0]) if row else None

    def save_paper_candle_checkpoint(
        self, exchange: str, symbol: str, timeframe: str, timestamp: int
    ) -> None:
        if not self._conn:
            return
        self._conn.execute(
            """INSERT INTO paper_candle_checkpoints
               (exchange, symbol, timeframe, timestamp) VALUES (?, ?, ?, ?)
               ON CONFLICT(exchange, symbol, timeframe) DO UPDATE SET
                 timestamp=MAX(timestamp, excluded.timestamp)""",
            (exchange, symbol, timeframe, timestamp),
        )
        self._conn.commit()

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None


# ===========================================================================
# SECTION 5 – EXCHANGE CONNECTION LAYER (Stage 1)
# ===========================================================================

class ExchangeConnector:
    def __init__(self, exchange_id: str):
        self.id = exchange_id.lower()
        self.exchange: Optional[ccxt_async.Exchange] = None
        self.markets: Dict[str, Any] = {}
        self.perp_symbols: List[str] = []
        self.meta: Dict[str, MarketMeta] = {}
        self.connected = False
        self.last_error: Optional[str] = None
        self.last_success: float = 0.0
        self._rate_limit_until: float = 0.0

    async def connect(self) -> bool:
        try:
            klass = getattr(ccxt_async, self.id, None)
            if klass is None:
                self.last_error = f"CCXT does not support {self.id}"
                return False
            options = {"defaultType": "swap", "adjustForTimeDifference": True}
            self.exchange = klass({"enableRateLimit": True, "options": options, "timeout": 30000})
            await self.exchange.load_markets()
            self.markets = self.exchange.markets
            self._discover_perps()
            self.connected = True
            self.last_success = time.time()
            log.info("[%s] Connected. markets=%d  USDT-perps=%d", self.id.upper(), len(self.markets), len(self.perp_symbols))
            return True
        except Exception as e:
            self.last_error = f"{type(e).__name__}: {str(e)[:180]}"
            self.connected = False
            log.warning("[%s] Connection failed: %s", self.id.upper(), self.last_error)
            if self.exchange:
                try:
                    await self.exchange.close()
                except Exception:
                    pass
                self.exchange = None
            return False

    def _discover_perps(self) -> None:
        self.perp_symbols = []
        self.meta = {}
        for symbol, m in self.markets.items():
            try:
                if not m.get("active", True):
                    continue
                is_swap = m.get("swap") or m.get("type") == "swap" or m.get("linear")
                quote = (m.get("quote") or "").upper()
                settle = (m.get("settle") or quote).upper()
                if not is_swap or (quote != Config.QUOTE and settle != Config.QUOTE):
                    continue
                meta = MarketMeta(
                    exchange=self.id, symbol=symbol, base=m.get("base", ""),
                    quote=quote, settle=settle,
                    contract_size=safe_float(m.get("contractSize"), 1.0), active=True,
                    precision_price=m.get("precision", {}).get("price"),
                    precision_amount=m.get("precision", {}).get("amount"),
                    precision_mode=getattr(self.exchange, "precisionMode", None),
                    limits=m.get("limits") or {},
                    info={k: m.get(k) for k in ("id", "type", "linear", "inverse") if k in m},
                )
                self.perp_symbols.append(symbol)
                self.meta[symbol] = meta
            except Exception:
                continue
        self.perp_symbols = sorted(set(self.perp_symbols))

    async def close(self) -> None:
        if self.exchange:
            try:
                await self.exchange.close()
            except Exception:
                pass
            self.exchange = None
        self.connected = False

    async def _throttle(self) -> None:
        now = time.time()
        if now < self._rate_limit_until:
            await asyncio.sleep(self._rate_limit_until - now)

    async def fetch_ticker(self, symbol: str) -> Optional[TickerSnapshot]:
        if not self.connected or not self.exchange:
            return None
        await self._throttle()
        for attempt in range(Config.MAX_RETRIES):
            try:
                raw = await self.exchange.fetch_ticker(symbol)
                snap = DataValidator.validate_ticker(raw, self.id, symbol)
                if snap:
                    self.last_success = time.time()
                    return snap
                # Validation rejected – log once for diagnosis
                log.debug("[%s] ticker validation empty for %s raw_last=%s",
                          self.id, symbol, raw.get("last") if isinstance(raw, dict) else None)
                return None
            except ccxt.RateLimitExceeded:
                self._rate_limit_until = time.time() + 5 + attempt * 2
                await asyncio.sleep(1 + attempt)
            except Exception as e:
                self.last_error = str(e)[:160]
                if attempt == Config.MAX_RETRIES - 1:
                    log.warning("[%s] fetch_ticker failed %s: %s", self.id, symbol, self.last_error)
                await asyncio.sleep(0.5 * (attempt + 1))
        return None

    async def fetch_order_book(self, symbol: str, limit: int = 20) -> Optional[OrderBookSnapshot]:
        if not self.connected or not self.exchange:
            return None
        await self._throttle()
        try:
            raw = await self.exchange.fetch_order_book(symbol, limit=limit)
            return DataValidator.validate_orderbook(raw, self.id, symbol)
        except Exception as e:
            log.debug("[%s] orderbook %s: %s", self.id, symbol, e)
            return None

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1h",
        limit: int = 100,
        since: Optional[int] = None,
    ) -> List[Candle]:
        if not self.connected or not self.exchange:
            return []
        await self._throttle()
        try:
            raw = await self.exchange.fetch_ohlcv(
                symbol, timeframe=timeframe, since=since, limit=limit
            )
            return DataValidator.validate_ohlcv(raw)
        except Exception as e:
            log.debug("[%s] ohlcv %s %s: %s", self.id, symbol, timeframe, e)
            return []

    async def fetch_funding_rate(self, symbol: str) -> Optional[FundingSnapshot]:
        if not self.connected or not self.exchange or not self.exchange.has.get("fetchFundingRate"):
            return None
        await self._throttle()
        try:
            raw = await self.exchange.fetch_funding_rate(symbol)
            return DataValidator.validate_funding_rate(raw, self.id, symbol)
        except Exception as e:
            log.debug("[%s] funding %s: %s", self.id, symbol, e)
            return None

    async def fetch_open_interest(self, symbol: str) -> Optional[OpenInterestSnapshot]:
        if not self.connected or not self.exchange or not self.exchange.has.get("fetchOpenInterest"):
            return None
        await self._throttle()
        try:
            raw = await self.exchange.fetch_open_interest(symbol)
            return DataValidator.validate_open_interest(raw, self.id, symbol)
        except Exception as e:
            log.debug("[%s] OI %s: %s", self.id, symbol, e)
            return None


# ===========================================================================
# SECTION 6 – TECHNICAL INDICATORS ENGINE (Stage 2)
# ===========================================================================

class TechnicalEngine:
    """Pure vectorised / pandas-based indicator calculations. No look-ahead."""

    @staticmethod
    def candles_to_df(candles: List[Candle]) -> pd.DataFrame:
        if not candles:
            return pd.DataFrame()
        df = pd.DataFrame([{
            "ts": c.timestamp, "open": c.open, "high": c.high,
            "low": c.low, "close": c.close, "volume": c.volume
        } for c in candles])
        df = df.sort_values("ts").reset_index(drop=True)
        return df

    @staticmethod
    def rsi(series: pd.Series, period: int = 14) -> pd.Series:
        delta = series.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def ema(series: pd.Series, period: int) -> pd.Series:
        return series.ewm(span=period, adjust=False).mean()

    @staticmethod
    def sma(series: pd.Series, period: int) -> pd.Series:
        return series.rolling(period, min_periods=period).mean()

    @staticmethod
    def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
        high, low, close = df["high"], df["low"], df["close"]
        prev_close = close.shift(1)
        tr = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs()
        ], axis=1).max(axis=1)
        return tr.ewm(alpha=1/period, min_periods=period, adjust=False).mean()

    @staticmethod
    def macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
        ema_f = series.ewm(span=fast, adjust=False).mean()
        ema_s = series.ewm(span=slow, adjust=False).mean()
        line = ema_f - ema_s
        sig = line.ewm(span=signal, adjust=False).mean()
        hist = line - sig
        return line, sig, hist

    @staticmethod
    def bollinger(series: pd.Series, period: int = 20, std_mult: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series]:
        mid = series.rolling(period).mean()
        std = series.rolling(period).std()
        upper = mid + std_mult * std
        lower = mid - std_mult * std
        return upper, mid, lower

    @staticmethod
    def adx(df: pd.DataFrame, period: int = 14) -> Tuple[pd.Series, pd.Series, pd.Series]:
        high, low, close = df["high"], df["low"], df["close"]
        up_move = high.diff()
        down_move = -low.diff()
        plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
        minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

        prev_close = close.shift(1)
        tr = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ], axis=1).max(axis=1)
        atr_w = tr.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
        plus_di = 100 * (
            plus_dm.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
            / atr_w.replace(0, np.nan)
        )
        minus_di = 100 * (
            minus_dm.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
            / atr_w.replace(0, np.nan)
        )
        di_sum = plus_di + minus_di
        dx = (plus_di - minus_di).abs() / di_sum.replace(0, np.nan) * 100
        adx = dx.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
        return adx, plus_di, minus_di

    @staticmethod
    def stochastic(df: pd.DataFrame, k_period: int = 14, d_period: int = 3) -> Tuple[pd.Series, pd.Series]:
        low_min = df["low"].rolling(k_period).min()
        high_max = df["high"].rolling(k_period).max()
        k = 100 * (df["close"] - low_min) / (high_max - low_min).replace(0, np.nan)
        d = k.rolling(d_period).mean()
        return k, d

    @staticmethod
    def obv(df: pd.DataFrame) -> pd.Series:
        direction = np.sign(df["close"].diff()).fillna(0)
        return (direction * df["volume"]).cumsum()

    @staticmethod
    def vwap(df: pd.DataFrame) -> pd.Series:
        typical = (df["high"] + df["low"] + df["close"]) / 3
        cum_vol = df["volume"].cumsum()
        cum_tp_vol = (typical * df["volume"]).cumsum()
        return cum_tp_vol / cum_vol.replace(0, np.nan)

    @classmethod
    def compute(cls, candles: List[Candle]) -> IndicatorSet:
        ind = IndicatorSet()
        if len(candles) < 30:
            return ind
        df = cls.candles_to_df(candles)
        close = df["close"]

        try:
            rsi = cls.rsi(close, Config.RSI_PERIOD)
            ind.rsi = float(rsi.iloc[-1]) if not pd.isna(rsi.iloc[-1]) else None

            macd_line, macd_sig, macd_hist = cls.macd(close)
            ind.macd = float(macd_line.iloc[-1]) if not pd.isna(macd_line.iloc[-1]) else None
            ind.macd_signal = float(macd_sig.iloc[-1]) if not pd.isna(macd_sig.iloc[-1]) else None
            ind.macd_hist = float(macd_hist.iloc[-1]) if not pd.isna(macd_hist.iloc[-1]) else None

            ind.ema_fast = float(cls.ema(close, Config.EMA_FAST).iloc[-1])
            ind.ema_slow = float(cls.ema(close, Config.EMA_SLOW).iloc[-1])
            ind.ema_trend = float(cls.ema(close, Config.EMA_TREND).iloc[-1])
            ind.sma_20 = float(cls.sma(close, 20).iloc[-1]) if len(df) >= 20 else None

            atr = cls.atr(df, Config.ATR_PERIOD)
            ind.atr = float(atr.iloc[-1]) if not pd.isna(atr.iloc[-1]) else None
            if ind.atr and close.iloc[-1] > 0:
                ind.atr_pct = (ind.atr / close.iloc[-1]) * 100

            adx, pdi, mdi = cls.adx(df, Config.ADX_PERIOD)
            ind.adx = float(adx.iloc[-1]) if not pd.isna(adx.iloc[-1]) else None
            ind.plus_di = float(pdi.iloc[-1]) if not pd.isna(pdi.iloc[-1]) else None
            ind.minus_di = float(mdi.iloc[-1]) if not pd.isna(mdi.iloc[-1]) else None

            upper, mid, lower = cls.bollinger(close, Config.BB_PERIOD, Config.BB_STD)
            ind.bb_upper = float(upper.iloc[-1]) if not pd.isna(upper.iloc[-1]) else None
            ind.bb_middle = float(mid.iloc[-1]) if not pd.isna(mid.iloc[-1]) else None
            ind.bb_lower = float(lower.iloc[-1]) if not pd.isna(lower.iloc[-1]) else None
            if ind.bb_upper and ind.bb_lower and ind.bb_middle:
                ind.bb_width = (ind.bb_upper - ind.bb_lower) / ind.bb_middle

            k, d = cls.stochastic(df)
            ind.stoch_k = float(k.iloc[-1]) if not pd.isna(k.iloc[-1]) else None
            ind.stoch_d = float(d.iloc[-1]) if not pd.isna(d.iloc[-1]) else None

            obv = cls.obv(df)
            ind.obv = float(obv.iloc[-1]) if not pd.isna(obv.iloc[-1]) else None

            vwap = cls.vwap(df)
            ind.vwap = float(vwap.iloc[-1]) if not pd.isna(vwap.iloc[-1]) else None

            vol_sma = cls.sma(df["volume"], 20)
            ind.volume_sma = float(vol_sma.iloc[-1]) if not pd.isna(vol_sma.iloc[-1]) else None

        except Exception as e:
            log.debug("Indicator computation error: %s", e)
        return ind


# ===========================================================================
# SECTION 7 – MARKET STRUCTURE ENGINE (Stage 2)
# ===========================================================================

class MarketStructureEngine:
    """Swing detection, HH/HL/LH/LL, BOS, CHoCH, support/resistance zones."""

    @staticmethod
    def find_swings(df: pd.DataFrame, lookback: int = 5) -> List[SwingPoint]:
        swings: List[SwingPoint] = []
        if len(df) < lookback * 2 + 1:
            return swings
        highs = df["high"].values
        lows = df["low"].values
        ts = df["ts"].values
        for i in range(lookback, len(df) - lookback):
            window_h = highs[i - lookback: i + lookback + 1]
            window_l = lows[i - lookback: i + lookback + 1]
            if highs[i] >= window_h.max():
                swings.append(SwingPoint(i, int(ts[i]), float(highs[i]), "high"))
            if lows[i] <= window_l.min():
                swings.append(SwingPoint(i, int(ts[i]), float(lows[i]), "low"))
        return swings

    @classmethod
    def analyse(cls, candles: List[Candle]) -> StructureAnalysis:
        result = StructureAnalysis()
        if len(candles) < 30:
            result.notes.append("Insufficient candles for structure")
            return result

        df = TechnicalEngine.candles_to_df(candles)
        swings = cls.find_swings(df, Config.SWING_LOOKBACK)
        result.swings = swings

        # Separate highs and lows
        swing_highs = [s for s in swings if s.kind == "high"]
        swing_lows = [s for s in swings if s.kind == "low"]

        # Label structure
        if len(swing_highs) >= 2:
            prev, last = swing_highs[-2], swing_highs[-1]
            if last.price > prev.price * 1.0005:
                result.last_hh = last.price
            elif last.price < prev.price * 0.9995:
                result.last_lh = last.price
            else:
                result.last_hh = last.price  # equal-ish

        if len(swing_lows) >= 2:
            prev, last = swing_lows[-2], swing_lows[-1]
            if last.price > prev.price * 1.0005:
                result.last_hl = last.price
            elif last.price < prev.price * 0.9995:
                result.last_ll = last.price
            else:
                result.last_ll = last.price

        # Trend heuristic
        bullish_count = sum(1 for x in [result.last_hh, result.last_hl] if x is not None)
        bearish_count = sum(1 for x in [result.last_lh, result.last_ll] if x is not None)
        if bullish_count >= 2 and bearish_count == 0:
            result.trend = "bullish"
        elif bearish_count >= 2 and bullish_count == 0:
            result.trend = "bearish"
        else:
            result.trend = "range"

        # Simple BOS / CHoCH detection on most recent swings
        if len(swing_highs) >= 2 and len(swing_lows) >= 1:
            last_price = df["close"].iloc[-1]
            # Bullish BOS: close above last significant high while previous structure was higher-lows
            if result.trend in ("bullish", "range") and result.last_hh and last_price > result.last_hh:
                result.bos = "bull"
                result.notes.append("Potential bullish BOS – close above prior swing high")
            if result.trend in ("bearish", "range") and result.last_ll and last_price < result.last_ll:
                result.bos = "bear"
                result.notes.append("Potential bearish BOS – close below prior swing low")

        # Support / Resistance from recent swing points
        recent = swings[-12:] if len(swings) > 12 else swings
        result.resistance = sorted({round(s.price, 8) for s in recent if s.kind == "high"}, reverse=True)[:5]
        result.support = sorted({round(s.price, 8) for s in recent if s.kind == "low"})[:5]

        if result.resistance and result.support:
            result.range_high = max(result.resistance)
            result.range_low = min(result.support)

        return result


# ===========================================================================
# SECTION 8 – LIQUIDITY & ORDER-FLOW (Stage 2)
# ===========================================================================

class LiquidityEngine:
    """Equal highs/lows, liquidity pools, simple sweep detection, order-book imbalance."""

    @staticmethod
    def find_equal_levels(prices: List[float], tolerance_pct: float = 0.0015) -> List[float]:
        if not prices:
            return []
        prices = sorted(prices)
        equals = []
        i = 0
        while i < len(prices) - 1:
            cluster = [prices[i]]
            j = i + 1
            while j < len(prices) and abs(prices[j] - prices[i]) / prices[i] < tolerance_pct:
                cluster.append(prices[j])
                j += 1
            if len(cluster) >= 2:
                equals.append(sum(cluster) / len(cluster))
            i = j
        return equals

    @classmethod
    def analyse(
        cls,
        candles: List[Candle],
        structure: StructureAnalysis,
        orderbook: Optional[OrderBookSnapshot] = None,
        current_price: Optional[float] = None,
    ) -> LiquidityAnalysis:
        result = LiquidityAnalysis()
        if len(candles) < 20:
            return result

        df = TechnicalEngine.candles_to_df(candles)
        price = current_price or float(df["close"].iloc[-1])

        # Equal highs / lows from swing points
        highs = [s.price for s in structure.swings if s.kind == "high"]
        lows = [s.price for s in structure.swings if s.kind == "low"]
        result.equal_highs = cls.find_equal_levels(highs)
        result.equal_lows = cls.find_equal_levels(lows)

        # Liquidity pools relative to price
        result.buy_side_liquidity = [h for h in (result.equal_highs + structure.resistance) if h > price]
        result.sell_side_liquidity = [l for l in (result.equal_lows + structure.support) if l < price]
        result.buy_side_liquidity = sorted(set(round(x, 8) for x in result.buy_side_liquidity))[:4]
        result.sell_side_liquidity = sorted(set(round(x, 8) for x in result.sell_side_liquidity), reverse=True)[:4]

        # Simple sweep detection: recent wick beyond a level that then reversed
        if len(df) >= 5:
            recent = df.iloc[-5:]
            for eh in result.equal_highs:
                if any(recent["high"] > eh * 1.0003) and recent["close"].iloc[-1] < eh:
                    result.recent_sweep = "buy_side"
                    result.notes.append(f"Possible buy-side liquidity sweep near {eh:.4f}")
                    break
            for el in result.equal_lows:
                if any(recent["low"] < el * 0.9997) and recent["close"].iloc[-1] > el:
                    result.recent_sweep = "sell_side"
                    result.notes.append(f"Possible sell-side liquidity sweep near {el:.4f}")
                    break

        # Order-book contribution
        if orderbook and orderbook.freshness() == DataFreshness.FRESH:
            result.orderbook_imbalance = orderbook.imbalance(10)
            bid_d, ask_d = orderbook.depth_usd(10)
            result.bid_depth_usd = bid_d
            result.ask_depth_usd = ask_d
            if result.orderbook_imbalance is not None:
                if result.orderbook_imbalance > 0.25:
                    result.notes.append("Order-book skewed to bids")
                elif result.orderbook_imbalance < -0.25:
                    result.notes.append("Order-book skewed to asks")

        return result


class OrderFlowEngine:
    """
    Lightweight order-flow proxies using only public candle + order-book data.
    True aggressive buy/sell volume requires trade-stream data; we approximate.
    """

    @classmethod
    def analyse(cls, candles: List[Candle], orderbook: Optional[OrderBookSnapshot] = None) -> OrderFlowAnalysis:
        result = OrderFlowAnalysis()
        if len(candles) < 10:
            return result

        df = TechnicalEngine.candles_to_df(candles)
        # Proxy delta: close location inside the bar * volume
        bar_range = (df["high"] - df["low"]).replace(0, np.nan)
        close_loc = (df["close"] - df["low"]) / bar_range
        # 1 = full buy pressure, 0 = full sell pressure
        delta_proxy = (close_loc - 0.5) * 2 * df["volume"]
        result.volume_delta_proxy = float(delta_proxy.iloc[-20:].sum())

        recent_delta = float(delta_proxy.iloc[-5:].sum())
        if recent_delta > 0:
            result.buy_pressure = min(1.0, recent_delta / (df["volume"].iloc[-5:].sum() + 1e-9))
            result.sell_pressure = 1.0 - result.buy_pressure
        else:
            result.sell_pressure = min(1.0, abs(recent_delta) / (df["volume"].iloc[-5:].sum() + 1e-9))
            result.buy_pressure = 1.0 - result.sell_pressure

        # Volume spike + rejection = possible absorption
        vol_mean = df["volume"].iloc[-20:].mean()
        last = df.iloc[-1]
        if last["volume"] > vol_mean * 1.8:
            if last["close"] > last["open"] and (last["high"] - last["close"]) > (last["close"] - last["open"]) * 0.6:
                result.absorption_hint = "possible_sell_absorption"
                result.notes.append("High volume up-bar with upper rejection")
            elif last["close"] < last["open"] and (last["close"] - last["low"]) > (last["open"] - last["close"]) * 0.6:
                result.absorption_hint = "possible_buy_absorption"
                result.notes.append("High volume down-bar with lower rejection")

        if orderbook and orderbook.freshness() == DataFreshness.FRESH:
            imb = orderbook.imbalance(5)
            if abs(imb) > 0.35:
                result.notes.append(f"Strong book imbalance {imb:+.2f}")

        return result


# ===========================================================================
# SECTION 9 – DERIVATIVES INTELLIGENCE (Stage 2)
# ===========================================================================

class DerivativesEngine:
    @classmethod
    def analyse(
        cls,
        funding: Optional[FundingSnapshot],
        oi: Optional[OpenInterestSnapshot],
        prev_oi: Optional[OpenInterestSnapshot],
        ticker: Optional[TickerSnapshot],
        candles: List[Candle],
    ) -> DerivativesAnalysis:
        result = DerivativesAnalysis()

        if funding and funding.freshness() == DataFreshness.FRESH:
            result.funding_rate = funding.funding_rate
            # Approximate annualised (assuming 3 settlements / day for most perps)
            result.funding_annualised = funding.funding_rate * 3 * 365 * 100
            if funding.funding_rate > 0.0005:
                result.funding_extreme = "longs_pay"
                result.notes.append("Elevated positive funding – longs paying shorts")
            elif funding.funding_rate < -0.0005:
                result.funding_extreme = "shorts_pay"
                result.notes.append("Elevated negative funding – shorts paying longs")

        if oi and oi.freshness() == DataFreshness.FRESH:
            result.open_interest = oi.open_interest
            result.open_interest_value = oi.open_interest_value
            if (
                prev_oi and prev_oi.freshness() == DataFreshness.FRESH
                and prev_oi.open_interest > 0
            ):
                result.oi_change_pct = (oi.open_interest - prev_oi.open_interest) / prev_oi.open_interest * 100

        # Price + OI regime (classic interpretation, not definitive)
        if len(candles) >= 5 and result.oi_change_pct is not None:
            price_change = (candles[-1].close - candles[-5].close) / candles[-5].close
            oi_up = result.oi_change_pct > 1.0
            oi_down = result.oi_change_pct < -1.0
            price_up = price_change > 0.003
            price_down = price_change < -0.003
            if price_up and oi_up:
                result.price_oi_regime = "price_up_oi_up"
                result.notes.append("Price↑ + OI↑ → possible new long positioning")
            elif price_down and oi_up:
                result.price_oi_regime = "price_down_oi_up"
                result.notes.append("Price↓ + OI↑ → possible new short positioning")
            elif price_up and oi_down:
                result.price_oi_regime = "price_up_oi_down"
                result.notes.append("Price↑ + OI↓ → possible short covering")
            elif price_down and oi_down:
                result.price_oi_regime = "price_down_oi_down"
                result.notes.append("Price↓ + OI↓ → possible long liquidation / covering")

        if ticker and ticker.mark and ticker.index and ticker.index > 0:
            result.basis_proxy = (ticker.mark - ticker.index) / ticker.index * 100

        return result


# ===========================================================================
# SECTION 10 – VOLATILITY & REGIME DETECTION (Stage 2)
# ===========================================================================

class VolatilityEngine:
    @classmethod
    def analyse(cls, candles: List[Candle], indicators: IndicatorSet) -> VolatilityAnalysis:
        result = VolatilityAnalysis()
        if len(candles) < 30:
            return result

        df = TechnicalEngine.candles_to_df(candles)
        close = df["close"]

        result.atr = indicators.atr
        result.atr_pct = indicators.atr_pct
        result.bb_width = indicators.bb_width

        # Realized vol (simple close-to-close)
        log_ret = np.log(close / close.shift(1)).dropna()
        if len(log_ret) >= 24:
            # annualised-ish for 1h bars: * sqrt(24*365)
            result.realized_vol_24h = float(log_ret.iloc[-24:].std() * math.sqrt(24 * 365) * 100)

        # BB width percentile over recent history
        if indicators.bb_width is not None and len(df) >= 50:
            upper, mid, lower = TechnicalEngine.bollinger(close, Config.BB_PERIOD, Config.BB_STD)
            width_hist = ((upper - lower) / mid).dropna()
            if len(width_hist) > 10:
                result.bb_width_percentile = float((width_hist < indicators.bb_width).mean() * 100)

        # Regime classification
        atr_pct = result.atr_pct or 0
        if atr_pct > 3.5 or (result.realized_vol_24h and result.realized_vol_24h > 80):
            result.vol_regime = MarketRegime.HIGH_VOL
            result.notes.append("High volatility environment")
        elif atr_pct < 1.0:
            result.vol_regime = MarketRegime.LOW_VOL
            result.notes.append("Low volatility environment")
        else:
            result.vol_regime = MarketRegime.SIDEWAYS  # placeholder, refined later

        # Expanding / contracting via BB width trend
        if result.bb_width_percentile is not None:
            if result.bb_width_percentile > 75:
                result.expanding = True
                result.notes.append("Volatility expanding (BB width high percentile)")
            elif result.bb_width_percentile < 25:
                result.contracting = True
                result.notes.append("Volatility contracting (BB width low percentile)")

        return result


class RegimeEngine:
    """Combines structure, indicators, volatility into a single regime view."""

    @classmethod
    def detect(
        cls,
        indicators: IndicatorSet,
        structure: StructureAnalysis,
        volatility: VolatilityAnalysis,
        derivatives: DerivativesAnalysis,
    ) -> RegimeSnapshot:
        snap = RegimeSnapshot()
        scores = {"bull": 0.0, "bear": 0.0, "range": 0.0}
        notes = []

        # Structure weight
        if structure.trend == "bullish":
            scores["bull"] += 2.0
            notes.append("Structure bullish")
        elif structure.trend == "bearish":
            scores["bear"] += 2.0
            notes.append("Structure bearish")
        else:
            scores["range"] += 1.5

        # EMA alignment
        if indicators.ema_fast and indicators.ema_slow and indicators.ema_trend:
            if indicators.ema_fast > indicators.ema_slow > indicators.ema_trend:
                scores["bull"] += 1.5
                notes.append("EMA stack bullish")
            elif indicators.ema_fast < indicators.ema_slow < indicators.ema_trend:
                scores["bear"] += 1.5
                notes.append("EMA stack bearish")

        # ADX trend strength
        if indicators.adx:
            if indicators.adx > 25:
                if indicators.plus_di and indicators.minus_di:
                    if indicators.plus_di > indicators.minus_di:
                        scores["bull"] += 1.0
                    else:
                        scores["bear"] += 1.0
                notes.append(f"ADX {indicators.adx:.1f} – trending")
            else:
                scores["range"] += 1.0
                notes.append(f"ADX {indicators.adx:.1f} – weak trend")

        # RSI extremes (mild weight)
        if indicators.rsi:
            if indicators.rsi > 70:
                scores["bear"] += 0.3  # possible exhaustion
            elif indicators.rsi < 30:
                scores["bull"] += 0.3

        # Funding crowding
        if derivatives.funding_extreme == "longs_pay":
            scores["bear"] += 0.4
            notes.append("Crowded longs (funding)")
        elif derivatives.funding_extreme == "shorts_pay":
            scores["bull"] += 0.4
            notes.append("Crowded shorts (funding)")

        # Determine primary
        total = sum(scores.values()) or 1.0
        best = max(scores, key=scores.get)
        snap.confidence = scores[best] / total

        if best == "bull":
            snap.direction = "bullish"
            if volatility.vol_regime == MarketRegime.HIGH_VOL or volatility.expanding:
                snap.primary = MarketRegime.STRONG_BULL
            else:
                snap.primary = MarketRegime.WEAK_BULL
        elif best == "bear":
            snap.direction = "bearish"
            if volatility.vol_regime == MarketRegime.HIGH_VOL or volatility.expanding:
                snap.primary = MarketRegime.STRONG_BEAR
            else:
                snap.primary = MarketRegime.WEAK_BEAR
        else:
            snap.direction = "neutral"
            snap.primary = MarketRegime.SIDEWAYS

        # Volatility overlay
        if volatility.vol_regime == MarketRegime.HIGH_VOL:
            snap.volatility = "high"
        elif volatility.vol_regime == MarketRegime.LOW_VOL:
            snap.volatility = "low"
        else:
            snap.volatility = "normal"

        snap.structure_bias = structure.trend
        snap.notes = notes
        return snap


# ===========================================================================
# SECTION 11 – ANALYSIS ORCHESTRATOR (Stage 2)
# ===========================================================================

class AnalysisOrchestrator:
    """Runs the full Stage-2 pipeline for a symbol and returns AssetAnalysis."""

    def __init__(self):
        self.prev_oi: Dict[str, OpenInterestSnapshot] = {}

    def analyse_symbol(
        self,
        exchange: str,
        symbol: str,
        candles: List[Candle],
        ticker: Optional[TickerSnapshot] = None,
        orderbook: Optional[OrderBookSnapshot] = None,
        funding: Optional[FundingSnapshot] = None,
        oi: Optional[OpenInterestSnapshot] = None,
        timeframe: str = "1h",
        higher_timeframe_candles: Optional[List[Candle]] = None,
    ) -> AssetAnalysis:
        key = f"{exchange}:{symbol}"
        ticker_freshness = ticker.freshness() if ticker else DataFreshness.MISSING
        price = ticker.last if ticker_freshness == DataFreshness.FRESH else 0.0
        ts = candles[-1].timestamp if candles else utc_ms()

        indicators = TechnicalEngine.compute(candles)
        structure = MarketStructureEngine.analyse(candles)
        liquidity = LiquidityEngine.analyse(candles, structure, orderbook, price)
        orderflow = OrderFlowEngine.analyse(candles, orderbook)

        prev = self.prev_oi.get(key)
        derivatives = DerivativesEngine.analyse(funding, oi, prev, ticker, candles)
        if oi and oi.freshness() == DataFreshness.FRESH:
            self.prev_oi[key] = oi

        volatility = VolatilityEngine.analyse(candles, indicators)
        regime = RegimeEngine.detect(indicators, structure, volatility, derivatives)

        higher_direction = "unknown"
        higher_candles = higher_timeframe_candles or []
        higher_timeframe_is_fresh = bool(
            higher_candles
            and utc_ms() - (
                higher_candles[-1].timestamp + timeframe_milliseconds(Config.HIGHER_TF)
            ) <= timeframe_milliseconds(Config.HIGHER_TF) + Config.FRESHNESS_OHLCV * 1000
        )
        if higher_timeframe_is_fresh and len(higher_candles) >= 50:
            higher_indicators = TechnicalEngine.compute(higher_candles)
            if (
                higher_indicators.ema_fast
                and higher_indicators.ema_slow
                and higher_indicators.ema_trend
            ):
                if higher_indicators.ema_fast > higher_indicators.ema_slow > higher_indicators.ema_trend:
                    higher_direction = "bullish"
                elif higher_indicators.ema_fast < higher_indicators.ema_slow < higher_indicators.ema_trend:
                    higher_direction = "bearish"

        # Data quality assessment
        quality = "good"
        if not candles or len(candles) < 50:
            quality = "limited_history"
        if candles:
            candle_close_ms = candles[-1].timestamp + timeframe_milliseconds(timeframe)
            if utc_ms() - candle_close_ms > timeframe_milliseconds(timeframe) + Config.FRESHNESS_OHLCV * 1000:
                quality = "stale_ohlcv"
        if ticker_freshness == DataFreshness.STALE:
            quality = "stale_ticker"
        elif ticker_freshness == DataFreshness.MISSING:
            quality = "missing_ticker"
        if not funding:
            quality = quality + "+no_funding" if quality != "good" else "no_funding"

        return AssetAnalysis(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            timestamp=ts,
            price=price,
            indicators=indicators,
            structure=structure,
            liquidity=liquidity,
            orderflow=orderflow,
            derivatives=derivatives,
            volatility=volatility,
            regime=regime,
            data_quality=quality,
            higher_timeframe_direction=higher_direction,
        )


# ===========================================================================
# SECTION 12 – MARKET DATA MANAGER (Stage 1 + Stage 2 integration)
# ===========================================================================

class MarketDataManager:
    def __init__(self):
        self.connectors: Dict[str, ExchangeConnector] = {}
        self.db = MarketDatabase()
        self.tickers: Dict[str, TickerSnapshot] = {}
        self.orderbooks: Dict[str, OrderBookSnapshot] = {}
        self.fundings: Dict[str, FundingSnapshot] = {}
        self.open_interests: Dict[str, OpenInterestSnapshot] = {}
        self.analyses: Dict[str, AssetAnalysis] = {}
        self.active_symbols: List[str] = []
        self.orchestrator = AnalysisOrchestrator()
        self._running = False
        self._tasks: List[asyncio.Task] = []
        self.live_startup_complete = asyncio.Event()
        self.live_startup_error: Optional[str] = None
        self.stats = {
            "started_at": None,
            "scans": 0,
            "analyses": 0,
            "tickers_fetched": 0,
            "errors": 0,
            "last_scan_duration": 0.0,
            "last_analysis_duration": 0.0,
            "analysis_cycle": {},
        }

    async def start(self) -> None:
        log.info("=== APEX Stage 1+2 – Market Data + Analytical Engine starting ===")
        self.db.connect()
        self.stats["started_at"] = utc_now().isoformat()

        primary = None
        for eid in Config.EXCHANGE_PRIORITY:
            conn = ExchangeConnector(eid)
            ok = await conn.connect()
            if ok:
                self.connectors[eid] = conn
                if primary is None:
                    primary = eid
                for sym, meta in conn.meta.items():
                    self.db.upsert_market(meta)
            else:
                log.warning("Skipping %s – not reachable", eid.upper())

        if not self.connectors:
            raise RuntimeError("No public exchange endpoints reachable")

        log.info("Active connectors: %s (primary=%s)", list(self.connectors.keys()), primary)
        self._build_active_symbol_list()
        await self._bootstrap_history()

        self._running = True
        self._tasks = [
            asyncio.create_task(self._scan_loop(), name="scan_loop"),
            asyncio.create_task(self._candle_update_loop(), name="candle_loop"),
            asyncio.create_task(self._analysis_loop(), name="analysis_loop"),
            asyncio.create_task(self._health_loop(), name="health_loop"),
        ]
        log.info("Background tasks launched. Monitoring %d symbols.", len(self.active_symbols))

    def _build_active_symbol_list(self) -> None:
        candidates = []
        for eid, conn in self.connectors.items():
            for sym in conn.perp_symbols:
                base = conn.meta[sym].base.upper()
                score = 100 if base in ("BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "LINK", "DOT") else 0
                score += 10 if "USDT" in sym else 0
                candidates.append((score, eid, sym))
        candidates.sort(key=lambda x: -x[0])
        seen = set()
        selected = []
        for score, eid, sym in candidates:
            key = f"{eid}:{sym}"
            if key in seen:
                continue
            seen.add(key)
            selected.append(key)
            if len(selected) >= Config.MAX_PAIRS:
                break
        self.active_symbols = selected
        log.info("Active universe (%d): %s ...", len(self.active_symbols), self.active_symbols[:6])

    async def _bootstrap_history(self) -> None:
        log.info("Bootstrapping historical candles...")
        count = 0
        for key in self.active_symbols:
            eid, sym = key.split(":", 1)
            conn = self.connectors.get(eid)
            if not conn:
                continue
            for tf in (Config.ANALYSIS_TF, Config.HIGHER_TF):
                candles = await conn.fetch_ohlcv(sym, tf, limit=Config.HISTORICAL_LIMIT)
                if candles:
                    count += self.db.insert_candles(eid, sym, tf, candles)
                await asyncio.sleep(Config.REST_SLEEP)
        log.info("Bootstrap complete. ~%d candle rows.", count)

    async def _scan_loop(self) -> None:
        while self._running:
            t0 = time.time()
            try:
                await self._full_scan()
                self.stats["scans"] += 1
                self.stats["last_scan_duration"] = time.time() - t0
            except Exception as e:
                self.stats["errors"] += 1
                log.exception("Scan error: %s", e)
            await asyncio.sleep(Config.SCAN_INTERVAL)

    async def _full_scan(self) -> None:
        """Fetch tickers (priority), then funding / OI / book for active symbols."""
        sem = asyncio.Semaphore(4)
        ok_tick = 0
        fail_tick = 0

        async def one(key: str):
            nonlocal ok_tick, fail_tick
            async with sem:
                eid, sym = key.split(":", 1)
                conn = self.connectors.get(eid)
                if not conn or not conn.connected:
                    fail_tick += 1
                    return
                try:
                    tick = await conn.fetch_ticker(sym)
                    if tick and tick.last > 0:
                        self.tickers[key] = tick
                        self.stats["tickers_fetched"] += 1
                        ok_tick += 1
                    else:
                        fail_tick += 1
                except Exception as e:
                    fail_tick += 1
                    log.debug("ticker path %s: %s", key, e)

                try:
                    if key not in self.fundings or time.time() - self.fundings[key].received_at > 60:
                        fr = await conn.fetch_funding_rate(sym)
                        if fr:
                            self.fundings[key] = fr
                    if key not in self.open_interests or time.time() - self.open_interests[key].received_at > 45:
                        oi = await conn.fetch_open_interest(sym)
                        if oi:
                            self.open_interests[key] = oi
                    if key not in self.orderbooks or time.time() - self.orderbooks[key].received_at > 45:
                        ob = await conn.fetch_order_book(sym, limit=15)
                        if ob:
                            self.orderbooks[key] = ob
                except Exception as e:
                    log.debug("aux data %s: %s", key, e)
                await asyncio.sleep(0.08)

        await asyncio.gather(*(one(k) for k in self.active_symbols), return_exceptions=True)
        log.info(
            "Scan done: tickers ok=%d fail=%d stored=%d  fundings=%d  oi=%d",
            ok_tick, fail_tick, len(self.tickers), len(self.fundings), len(self.open_interests),
        )

    async def _candle_update_loop(self) -> None:
        while self._running:
            try:
                for key in self.active_symbols:
                    eid, sym = key.split(":", 1)
                    conn = self.connectors.get(eid)
                    if not conn:
                        continue
                    for timeframe in (Config.ANALYSIS_TF, Config.HIGHER_TF):
                        candles = await conn.fetch_ohlcv(sym, timeframe, limit=8)
                        if candles:
                            self.db.insert_candles(eid, sym, timeframe, candles)
                        await asyncio.sleep(0.2)
            except Exception as e:
                log.debug("Candle update error: %s", e)
            await asyncio.sleep(Config.CANDLE_UPDATE_INTERVAL)

    async def _analysis_loop(self) -> None:
        while self._running:
            t0 = time.time()
            try:
                await self._run_analyses()
                self.stats["last_analysis_duration"] = time.time() - t0
            except Exception as e:
                self.stats["errors"] += 1
                log.exception("Analysis loop error: %s", e)
            await asyncio.sleep(Config.ANALYSIS_INTERVAL)

    async def _run_analyses(self) -> None:
        eligible = 0
        analysed = 0
        insufficient: List[str] = []
        failed: List[str] = []
        for key in self.active_symbols:
            eid, sym = key.split(":", 1)
            try:
                candles = closed_candles(
                    self.db.get_candles(eid, sym, Config.ANALYSIS_TF, limit=201),
                    Config.ANALYSIS_TF,
                )
                if len(candles) < 40:
                    insufficient.append(f"{key} ({len(candles)}/40 closed candles)")
                    self.analyses.pop(key, None)
                    continue
                eligible += 1
                higher_candles = closed_candles(
                    self.db.get_candles(eid, sym, Config.HIGHER_TF, limit=201),
                    Config.HIGHER_TF,
                )
                analysis = self.orchestrator.analyse_symbol(
                    exchange=eid,
                    symbol=sym,
                    candles=candles,
                    ticker=self.tickers.get(key),
                    orderbook=self.orderbooks.get(key),
                    funding=self.fundings.get(key),
                    oi=self.open_interests.get(key),
                    timeframe=Config.ANALYSIS_TF,
                    higher_timeframe_candles=higher_candles,
                )
                self.db.save_analysis(analysis)
                self.analyses[key] = analysis
                analysed += 1
                self.stats["analyses"] += 1
            except Exception as exc:
                self.analyses.pop(key, None)
                failed.append(f"{key} ({type(exc).__name__}: {str(exc)[:120]})")
        cycle = {
            "total": len(self.active_symbols),
            "eligible": eligible,
            "analysed": analysed,
            "insufficient_history": len(insufficient),
            "failed": len(failed),
            "insufficient_symbols": insufficient,
            "failed_symbols": failed,
        }
        self.stats["analysis_cycle"] = cycle
        log.info(
            "Analysis cycle: total=%d eligible=%d analysed=%d insufficient_history=%d failed=%d; "
            "insufficient=%s failed_symbols=%s",
            cycle["total"], eligible, analysed, len(insufficient), len(failed),
            insufficient, failed,
        )

    async def _health_loop(self) -> None:
        while self._running:
            try:
                self._print_status()
            except Exception:
                pass
            await asyncio.sleep(Config.HEARTBEAT_INTERVAL)

    def _print_status(self) -> None:
        total_t = len(self.tickers)
        fresh = sum(1 for t in self.tickers.values() if t.freshness() == DataFreshness.FRESH)
        lines = [
            f"APEX Stage-1+2 | scans={self.stats['scans']}  analyses={self.stats['analyses']}  errors={self.stats['errors']}",
            f"Connectors: { {k: ('OK' if v.connected else 'DOWN') for k, v in self.connectors.items()} }",
            f"Tickers {total_t}/{len(self.active_symbols)} (fresh={fresh})  Fundings {len(self.fundings)}  OI {len(self.open_interests)}  Analyses {len(self.analyses)}",
        ]
        # Show a few interesting regimes
        interesting = []
        for key, a in list(self.analyses.items())[:8]:
            interesting.append(
                f"  {key}: {a.regime.primary.value} | dir={a.regime.direction} | "
                f"RSI={a.indicators.rsi:.1f if a.indicators.rsi else 'n/a'} | "
                f"ADX={a.indicators.adx:.1f if a.indicators.adx else 'n/a'} | "
                f"fund={a.derivatives.funding_rate*10000:.2f}bps" if a.derivatives.funding_rate else f"  {key}: {a.regime.primary.value}"
            )
        if interesting:
            lines.append("Recent regime snapshots:")
            lines.extend(interesting[:6])
        msg = "\n".join(lines)
        log.info("\n" + msg)
        if RICH_AVAILABLE and console:
            console.print(Panel(msg, title="APEX Health (Stage 2)", border_style="green"))

    # Public API
    def get_analysis(self, exchange: str, symbol: str) -> Optional[AssetAnalysis]:
        return self.analyses.get(f"{exchange}:{symbol}")

    def get_all_analyses(self) -> Dict[str, AssetAnalysis]:
        return dict(self.analyses)

    def get_ticker(self, exchange: str, symbol: str) -> Optional[TickerSnapshot]:
        return self.tickers.get(f"{exchange}:{symbol}")

    def get_candles(self, exchange: str, symbol: str, timeframe: str = "1h", limit: int = 200) -> List[Candle]:
        return self.db.get_candles(exchange, symbol, timeframe, limit)

    def list_active(self) -> List[str]:
        return list(self.active_symbols)

    async def stop(self) -> None:
        log.info("Shutting down...")
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for conn in self.connectors.values():
            await conn.close()
        self.db.close()
        log.info("Shutdown complete.")


# ===========================================================================
# SECTION 13 – STAGE 3 DATA MODELS
# ===========================================================================

class TradeDirection(Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    WAIT = "WAIT"
    NO_TRADE = "NO_TRADE"


class EntryMethod(Enum):
    MARKET = "immediate_market"
    LIMIT_RETEST = "limit_retest"
    BREAKOUT = "breakout"
    BREAKOUT_RETEST = "breakout_retest"
    LIQUIDITY_SWEEP = "liquidity_sweep_reversal"
    MOMENTUM = "momentum_continuation"


@dataclass
class StopLossPlan:
    price: float
    reason: str
    atr_distance: float
    structure_level: Optional[float] = None
    buffer_pct: float = 0.0


@dataclass
class TakeProfitPlan:
    tp1: Optional[float] = None
    tp2: Optional[float] = None
    tp3: Optional[float] = None
    reasons: List[str] = field(default_factory=list)
    partial_suggested: bool = True


@dataclass
class RiskRewardResult:
    entry: float
    stop: float
    tp1: Optional[float]
    tp2: Optional[float]
    tp3: Optional[float]
    gross_risk: float
    gross_reward_tp1: float
    net_risk: float
    net_reward_tp1: float
    net_rr_tp1: float
    net_rr_tp2: Optional[float] = None
    net_rr_tp3: Optional[float] = None
    fees_estimate: float = 0.0
    slippage_estimate: float = 0.0
    funding_estimate: float = 0.0


@dataclass
class PositionSizeResult:
    risk_usd: float
    position_size_coins: float
    notional_usd: float
    leverage_suggested: float
    margin_required: float
    liquidation_distance_pct: Optional[float] = None
    notes: List[str] = field(default_factory=list)


@dataclass
class TradeOpportunity:
    """Canonical Stage-3 output for one candidate trade."""
    exchange: str
    symbol: str
    direction: TradeDirection
    status: str                          # e.g. "WAITING_FOR_CONFIRMATION" / "READY" / "NO_TRADE"
    entry_method: EntryMethod
    entry_zone_low: float
    entry_zone_high: float
    preferred_entry: float
    stop_loss: StopLossPlan
    take_profit: TakeProfitPlan
    risk_reward: RiskRewardResult
    position_size: Optional[PositionSizeResult] = None
    regime: str = ""
    confirmations: List[str] = field(default_factory=list)
    risk_factors: List[str] = field(default_factory=list)
    invalidation: str = ""
    data_quality: str = ""
    score: float = 0.0                   # ranking score 0-100
    rank: int = 0
    computed_at: float = field(default_factory=time.time)

    def to_report(self) -> str:
        """Human-readable trade plan matching the required output format."""
        lines = [
            f"ASSET: {self.symbol}",
            f"EXCHANGE: {self.exchange.upper()}",
            f"CONTRACT: USDT Perpetual",
            f"DIRECTION: {self.direction.value}",
            f"STATUS: {self.status}",
            f"ENTRY METHOD: {self.entry_method.value}",
            "",
            f"ENTRY ZONE: {self.entry_zone_low:.4f} – {self.entry_zone_high:.4f}",
            f"PREFERRED ENTRY: {self.preferred_entry:.4f}",
            "",
            f"STOP LOSS: {self.stop_loss.price:.4f}",
            f"  Reason: {self.stop_loss.reason}",
            "",
        ]
        tp = self.take_profit
        if tp.tp1:
            lines.append(f"TAKE PROFIT 1: {tp.tp1:.4f}")
        if tp.tp2:
            lines.append(f"TAKE PROFIT 2: {tp.tp2:.4f}")
        if tp.tp3:
            lines.append(f"TAKE PROFIT 3: {tp.tp3:.4f}")
        if tp.reasons:
            lines.append("  Targets based on: " + "; ".join(tp.reasons[:3]))
        lines.append("")
        rr = self.risk_reward
        lines.append("EXPECTED NET R/R:")
        lines.append(f"  TP1: 1:{rr.net_rr_tp1:.2f}")
        if rr.net_rr_tp2:
            lines.append(f"  TP2: 1:{rr.net_rr_tp2:.2f}")
        if rr.net_rr_tp3:
            lines.append(f"  TP3: 1:{rr.net_rr_tp3:.2f}")
        lines.append("")
        lines.append(f"MARKET REGIME: {self.regime}")
        lines.append("")
        if self.confirmations:
            lines.append("CONFIRMATIONS:")
            for c in self.confirmations:
                lines.append(f"  - {c}")
            lines.append("")
        if self.risk_factors:
            lines.append("RISK FACTORS:")
            for r in self.risk_factors:
                lines.append(f"  - {r}")
            lines.append("")
        lines.append(f"INVALIDATION: {self.invalidation}")
        lines.append(f"DATA QUALITY: {self.data_quality}")
        lines.append(f"SCORE: {self.score:.1f}/100")
        if self.position_size:
            ps = self.position_size
            lines.append("")
            lines.append(f"POSITION SIZING (for ${Config.ACCOUNT_BALANCE:.0f} account, {Config.RISK_PER_TRADE_PCT}% risk):")
            lines.append(f"  Risk USD: ${ps.risk_usd:.2f}")
            lines.append(f"  Size: {ps.position_size_coins:.6f} contracts/coins")
            lines.append(f"  Notional: ${ps.notional_usd:.2f}")
            lines.append(f"  Suggested leverage: {ps.leverage_suggested:.1f}x")
            lines.append(f"  Est. margin: ${ps.margin_required:.2f}")
        return "\n".join(lines)


def serialize_trade_opportunity(opportunity: TradeOpportunity) -> str:
    return json.dumps(
        asdict(opportunity),
        default=lambda value: value.value if isinstance(value, Enum) else str(value),
    )


def deserialize_trade_opportunity(payload: str) -> TradeOpportunity:
    value = json.loads(payload)
    position_size = value.get("position_size")
    return TradeOpportunity(
        exchange=value["exchange"],
        symbol=value["symbol"],
        direction=TradeDirection(value["direction"]),
        status=value["status"],
        entry_method=EntryMethod(value["entry_method"]),
        entry_zone_low=float(value["entry_zone_low"]),
        entry_zone_high=float(value["entry_zone_high"]),
        preferred_entry=float(value["preferred_entry"]),
        stop_loss=StopLossPlan(**value["stop_loss"]),
        take_profit=TakeProfitPlan(**value["take_profit"]),
        risk_reward=RiskRewardResult(**value["risk_reward"]),
        position_size=PositionSizeResult(**position_size) if position_size else None,
        regime=value.get("regime", ""),
        confirmations=value.get("confirmations", []),
        risk_factors=value.get("risk_factors", []),
        invalidation=value.get("invalidation", ""),
        data_quality=value.get("data_quality", ""),
        score=float(value.get("score", 0)),
        rank=int(value.get("rank", 0)),
        computed_at=float(value.get("computed_at", time.time())),
    )


# ===========================================================================
# SECTION 14 – DYNAMIC STOP-LOSS OPTIMISER
# ===========================================================================

class StopLossOptimiser:
    """Structure + volatility based stops. Never arbitrary fixed percentages."""

    @classmethod
    def compute(
        cls,
        direction: TradeDirection,
        entry: float,
        analysis: AssetAnalysis,
        atr: Optional[float] = None,
    ) -> StopLossPlan:
        atr = atr or analysis.indicators.atr or (entry * 0.01)
        atr_pct = (atr / entry) * 100 if entry > 0 else 1.0

        # Minimum buffer: 0.4 * ATR or 0.15% of price (whichever larger)
        min_buffer = max(atr * 0.4, entry * 0.0015)

        structure = analysis.structure
        liquidity = analysis.liquidity
        structural = None

        if direction == TradeDirection.LONG:
            # Place below nearest relevant support / swing low / sell-side liquidity
            candidates = []
            if structure.support:
                candidates.extend([s for s in structure.support if s < entry])
            if structure.last_ll:
                candidates.append(structure.last_ll)
            if structure.last_hl:
                candidates.append(structure.last_hl)
            if liquidity.sell_side_liquidity:
                candidates.extend([l for l in liquidity.sell_side_liquidity if l < entry])

            if candidates:
                structural = max(c for c in candidates if c < entry)  # closest below
                stop = structural - min_buffer
                reason = f"Below structural support/swing {structural:.4f} + {min_buffer:.4f} ATR buffer"
            else:
                stop = entry - atr * 1.5
                reason = f"1.5×ATR fallback ({atr:.4f}) – no clear structure"

            # Avoid absurdly tight stops in high noise
            if (entry - stop) < atr * 0.6:
                stop = entry - atr * 0.8
                reason += " (widened for noise)"

        else:  # SHORT
            candidates = []
            if structure.resistance:
                candidates.extend([r for r in structure.resistance if r > entry])
            if structure.last_hh:
                candidates.append(structure.last_hh)
            if structure.last_lh:
                candidates.append(structure.last_lh)
            if liquidity.buy_side_liquidity:
                candidates.extend([h for h in liquidity.buy_side_liquidity if h > entry])

            if candidates:
                structural = min(c for c in candidates if c > entry)
                stop = structural + min_buffer
                reason = f"Above structural resistance/swing {structural:.4f} + {min_buffer:.4f} ATR buffer"
            else:
                stop = entry + atr * 1.5
                reason = f"1.5×ATR fallback ({atr:.4f}) – no clear structure"

            if (stop - entry) < atr * 0.6:
                stop = entry + atr * 0.8
                reason += " (widened for noise)"

        buffer_pct = abs(entry - stop) / entry * 100
        return StopLossPlan(
            price=round(stop, 8),
            reason=reason,
            atr_distance=abs(entry - stop) / atr if atr > 0 else 0,
            structure_level=structural,
            buffer_pct=buffer_pct,
        )


# ===========================================================================
# SECTION 15 – TAKE-PROFIT OPTIMISER
# ===========================================================================

class TakeProfitOptimiser:
    """Multi-target TP based on liquidity, structure, ATR and measured moves."""

    @classmethod
    def compute(
        cls,
        direction: TradeDirection,
        entry: float,
        stop: float,
        analysis: AssetAnalysis,
        atr: Optional[float] = None,
    ) -> TakeProfitPlan:
        atr = atr or analysis.indicators.atr or (entry * 0.01)
        risk = abs(entry - stop)
        plan = TakeProfitPlan()
        reasons = []
        cluster_distance = max(atr * 0.20, entry * 0.0005)

        def cluster_targets(values: List[float]) -> List[float]:
            unique: List[float] = []
            for value in sorted(set(round(v, 8) for v in values)):
                if not unique or abs(value - unique[-1]) >= cluster_distance:
                    unique.append(value)
            return unique

        structure = analysis.structure
        liquidity = analysis.liquidity

        if direction == TradeDirection.LONG:
            # TP1: nearest resistance / buy-side liquidity / 1.5R
            targets = []
            if structure.resistance:
                targets.extend([r for r in structure.resistance if r > entry])
            if liquidity.buy_side_liquidity:
                targets.extend([h for h in liquidity.buy_side_liquidity if h > entry])
            targets = cluster_targets(targets)

            # Minimum 1.2R
            min_tp1 = entry + risk * 1.2
            if targets:
                tp1_cand = next((t for t in targets if t >= min_tp1), None)
                if tp1_cand:
                    plan.tp1 = tp1_cand
                    reasons.append(f"Structure/liquidity {tp1_cand:.4f}")
                else:
                    plan.tp1 = entry + risk * 1.5
                    reasons.append("1.5R measured move")
            else:
                plan.tp1 = entry + risk * 1.5
                reasons.append("1.5R measured move")

            # TP2: next level or 2.5R
            remaining = [t for t in targets if plan.tp1 and t > plan.tp1]
            if remaining:
                plan.tp2 = remaining[0]
                reasons.append(f"Next resistance {plan.tp2:.4f}")
            else:
                plan.tp2 = entry + risk * 2.5
                reasons.append("2.5R extension")

            # TP3: 4R or far liquidity
            if len(remaining) > 1:
                plan.tp3 = remaining[1]
            else:
                plan.tp3 = entry + risk * 4.0
                reasons.append("4R runner")

        else:  # SHORT
            targets = []
            if structure.support:
                targets.extend([s for s in structure.support if s < entry])
            if liquidity.sell_side_liquidity:
                targets.extend([l for l in liquidity.sell_side_liquidity if l < entry])
            targets = cluster_targets(targets)
            targets = sorted(targets, reverse=True)

            min_tp1 = entry - risk * 1.2
            if targets:
                tp1_cand = next((t for t in targets if t <= min_tp1), None)
                if tp1_cand:
                    plan.tp1 = tp1_cand
                    reasons.append(f"Structure/liquidity {tp1_cand:.4f}")
                else:
                    plan.tp1 = entry - risk * 1.5
                    reasons.append("1.5R measured move")
            else:
                plan.tp1 = entry - risk * 1.5
                reasons.append("1.5R measured move")

            remaining = [t for t in targets if plan.tp1 and t < plan.tp1]
            if remaining:
                plan.tp2 = remaining[0]
                reasons.append(f"Next support {plan.tp2:.4f}")
            else:
                plan.tp2 = entry - risk * 2.5
                reasons.append("2.5R extension")

            if len(remaining) > 1:
                plan.tp3 = remaining[1]
            else:
                plan.tp3 = entry - risk * 4.0
                reasons.append("4R runner")

        plan.reasons = reasons
        plan.partial_suggested = True
        return plan


# ===========================================================================
# SECTION 16 – RISK/REWARD + POSITION SIZING
# ===========================================================================

class RiskRewardEngine:
    """Net R/R after fees, slippage and estimated funding."""

    @classmethod
    def compute(
        cls,
        direction: TradeDirection,
        entry: float,
        stop: float,
        tp1: Optional[float],
        tp2: Optional[float] = None,
        tp3: Optional[float] = None,
        funding_rate: Optional[float] = None,
        is_maker: bool = False,
    ) -> RiskRewardResult:
        fee_rate = Config.MAKER_FEE_PCT if is_maker else Config.TAKER_FEE_PCT
        slip = entry * (Config.DEFAULT_SLIPPAGE_BPS / 10_000)

        # Round-trip fees on notional (entry + exit)
        # We work in price space; fees are applied as extra cost to risk/reward
        gross_risk = abs(entry - stop)
        # Effective risk includes entry slippage + exit slippage + fees
        fee_cost = entry * fee_rate * 2          # entry + exit
        net_risk = gross_risk + slip * 2 + fee_cost

        def _reward(tp: Optional[float]) -> Tuple[float, float]:
            if tp is None:
                return 0.0, 0.0
            gross = abs(tp - entry)
            net = gross - slip * 2 - fee_cost
            # Funding: if we hold through one interval, add cost/benefit
            if funding_rate is not None:
                # Positive funding = longs pay → cost for LONG
                if direction == TradeDirection.LONG:
                    net -= entry * abs(funding_rate) if funding_rate > 0 else 0
                else:
                    net -= entry * abs(funding_rate) if funding_rate < 0 else 0
            return max(0.0, gross), max(0.0, net)

        g1, n1 = _reward(tp1)
        g2, n2 = _reward(tp2)
        g3, n3 = _reward(tp3)

        rr1 = n1 / net_risk if net_risk > 0 else 0.0
        rr2 = n2 / net_risk if net_risk > 0 and n2 > 0 else None
        rr3 = n3 / net_risk if net_risk > 0 and n3 > 0 else None

        return RiskRewardResult(
            entry=entry,
            stop=stop,
            tp1=tp1,
            tp2=tp2,
            tp3=tp3,
            gross_risk=gross_risk,
            gross_reward_tp1=g1,
            net_risk=net_risk,
            net_reward_tp1=n1,
            net_rr_tp1=rr1,
            net_rr_tp2=rr2,
            net_rr_tp3=rr3,
            fees_estimate=fee_cost,
            slippage_estimate=slip * 2,
            funding_estimate=(entry * abs(funding_rate or 0)),
        )


class PositionSizer:
    """Risk-based position sizing. Leverage is a consequence, never the input."""

    @classmethod
    def compute(
        cls,
        entry: float,
        stop: float,
        net_risk_price: float,
        account_balance: float = Config.ACCOUNT_BALANCE,
        risk_pct: float = Config.RISK_PER_TRADE_PCT,
        market_meta: Optional["MarketMeta"] = None,
    ) -> PositionSizeResult:
        risk_usd = min(account_balance * (risk_pct / 100.0), Config.MAX_RISK_PER_TRADE_USD)
        # Size so that price move of net_risk_price costs exactly risk_usd
        if net_risk_price <= 0 or entry <= 0:
            return PositionSizeResult(risk_usd=0, position_size_coins=0, notional_usd=0,
                                      leverage_suggested=0, margin_required=0,
                                      notes=["Invalid risk distance"])

        contract_size = 1.0
        precision_amount = None
        precision_mode = None
        limits: Dict[str, Any] = {}
        if market_meta is not None:
            contract_size = max(float(market_meta.contract_size or 1.0), 1e-12)
            precision_amount = market_meta.precision_amount
            precision_mode = market_meta.precision_mode
            limits = market_meta.limits or {}

        # CCXT linear swap `amount` is commonly in contracts. Convert risk-sized
        # base/coin exposure through contractSize before calculating notional.
        contracts = risk_usd / (net_risk_price * contract_size)
        notes: List[str] = []
        if isinstance(precision_amount, (int, float)) and precision_amount > 0:
            if precision_mode == ccxt.TICK_SIZE:
                tick_size = float(precision_amount)
                contracts = math.floor((contracts + 1e-12) / tick_size) * tick_size
            elif precision_mode == ccxt.DECIMAL_PLACES:
                decimal_places = int(precision_amount)
                if decimal_places < 0:
                    return PositionSizeResult(
                        risk_usd=0, position_size_coins=0, notional_usd=0,
                        leverage_suggested=0, margin_required=0,
                        notes=["Invalid decimal-place amount precision"],
                    )
                scale = 10 ** decimal_places
                contracts = math.floor(contracts * scale + 1e-12) / scale
            elif precision_mode == ccxt.SIGNIFICANT_DIGITS and contracts > 0:
                significant_digits = int(precision_amount)
                if significant_digits <= 0:
                    return PositionSizeResult(
                        risk_usd=0, position_size_coins=0, notional_usd=0,
                        leverage_suggested=0, margin_required=0,
                        notes=["Invalid significant-digit amount precision"],
                    )
                decimal_places = significant_digits - 1 - math.floor(math.log10(contracts))
                scale = 10.0 ** decimal_places
                contracts = math.floor(contracts * scale + 1e-12) / scale
            elif precision_mode not in (None,):
                notes.append("Unrecognized amount precision mode; exchange precision must be applied at order time")
            elif precision_mode is None:
                notes.append("Amount precision mode unavailable; sizing is unrounded")
        amount_limits = limits.get("amount") or {}
        min_amount = float(amount_limits.get("min") or 0)
        max_amount = float(amount_limits.get("max") or 0)
        if max_amount > 0:
            contracts = min(contracts, max_amount)
        if min_amount > 0 and contracts < min_amount:
            contracts = 0.0
            notes.append("Below exchange minimum amount; size omitted to preserve risk budget")
        if contracts <= 0:
            return PositionSizeResult(
                risk_usd=0, position_size_coins=0, notional_usd=0,
                leverage_suggested=0, margin_required=0, notes=notes,
            )

        size_coins = math.floor(contracts * contract_size * 1e8 + 1e-12) / 1e8
        if size_coins <= 0:
            return PositionSizeResult(
                risk_usd=0, position_size_coins=0, notional_usd=0,
                leverage_suggested=0, margin_required=0,
                notes=notes + ["Rounded position size is zero"],
            )
        notional = size_coins * entry
        # Suggested leverage = notional / (account * some margin fraction)
        # We keep it conservative; user can choose lower
        margin_fraction = 0.1  # assume 10x max comfortable
        margin_req = notional * margin_fraction
        lev = notional / account_balance if account_balance > 0 else 0

        if lev > 15:
            notes.append(f"High implied leverage ({lev:.1f}x) – consider reducing size")
        if risk_usd >= Config.MAX_RISK_PER_TRADE_USD:
            notes.append("Capped at MAX_RISK_PER_TRADE_USD")

        return PositionSizeResult(
            risk_usd=round(risk_usd, 2),
            position_size_coins=round(size_coins, 8),
            notional_usd=round(notional, 2),
            leverage_suggested=round(min(lev, 20), 1),
            margin_required=round(margin_req, 2),
            notes=notes,
        )


# ===========================================================================
# SECTION 17 – ENTRY OPTIMISER + DECISION ENGINE
# ===========================================================================

class EntryOptimiser:
    """
    Evaluates whether an entry is justified and selects the most suitable method.
    Returns LONG / SHORT / WAIT / NO_TRADE together with concrete levels.
    """

    @classmethod
    def evaluate(cls, analysis: AssetAnalysis, market_meta: Optional["MarketMeta"] = None) -> Optional[TradeOpportunity]:
        if "stale" in analysis.data_quality.lower() or "missing" in analysis.data_quality.lower():
            return TradeOpportunity(
                exchange=analysis.exchange,
                symbol=analysis.symbol,
                direction=TradeDirection.NO_TRADE,
                status="NO_TRADE",
                entry_method=EntryMethod.MARKET,
                entry_zone_low=analysis.price,
                entry_zone_high=analysis.price,
                preferred_entry=analysis.price,
                stop_loss=StopLossPlan(price=analysis.price, reason="stale_or_missing_data", atr_distance=0),
                take_profit=TakeProfitPlan(),
                risk_reward=RiskRewardResult(
                    entry=analysis.price, stop=analysis.price, tp1=None, tp2=None, tp3=None,
                    gross_risk=0, gross_reward_tp1=0, net_risk=0,
                    net_reward_tp1=0, net_rr_tp1=0,
                ),
                regime=analysis.regime.primary.value,
                risk_factors=["Ticker or market data is stale/missing"],
                invalidation="N/A",
                data_quality=analysis.data_quality,
                score=0.0,
            )
        if analysis.price <= 0 or not analysis.indicators.atr:
            return None

        regime = analysis.regime
        structure = analysis.structure
        indicators = analysis.indicators
        liquidity = analysis.liquidity
        derivatives = analysis.derivatives
        volatility = analysis.volatility

        # ---------- Direction bias ----------
        direction = TradeDirection.NO_TRADE
        confirmations: List[str] = []
        risk_factors: List[str] = []

        bull_score = 0.0
        bear_score = 0.0

        # Regime
        if regime.direction == "bullish":
            bull_score += 2.0 * regime.confidence
            confirmations.append(f"Regime {regime.primary.value}")
        elif regime.direction == "bearish":
            bear_score += 2.0 * regime.confidence
            confirmations.append(f"Regime {regime.primary.value}")

        # Structure
        if structure.trend == "bullish":
            bull_score += 1.5
            confirmations.append("Bullish market structure")
        elif structure.trend == "bearish":
            bear_score += 1.5
            confirmations.append("Bearish market structure")
        if structure.bos == "bull":
            bull_score += 1.0
            confirmations.append("Bullish BOS")
        elif structure.bos == "bear":
            bear_score += 1.0
            confirmations.append("Bearish BOS")

        # EMA stack
        if indicators.ema_fast and indicators.ema_slow and indicators.ema_trend:
            if indicators.ema_fast > indicators.ema_slow > indicators.ema_trend:
                bull_score += 1.0
                confirmations.append("EMA stack bullish")
            elif indicators.ema_fast < indicators.ema_slow < indicators.ema_trend:
                bear_score += 1.0
                confirmations.append("EMA stack bearish")

        # ADX + DI
        if indicators.adx and indicators.adx > 22:
            if indicators.plus_di and indicators.minus_di:
                if indicators.plus_di > indicators.minus_di:
                    bull_score += 0.8
                else:
                    bear_score += 0.8

        # RSI filter (avoid chasing extremes)
        if indicators.rsi:
            if indicators.rsi > 75:
                risk_factors.append("RSI overbought")
                bull_score *= 0.7
            elif indicators.rsi < 25:
                risk_factors.append("RSI oversold")
                bear_score *= 0.7

        # Funding crowding
        if derivatives.funding_extreme == "longs_pay":
            risk_factors.append("Elevated positive funding (crowded longs)")
            bull_score *= 0.85
        elif derivatives.funding_extreme == "shorts_pay":
            risk_factors.append("Elevated negative funding (crowded shorts)")
            bear_score *= 0.85

        # Liquidity sweep as entry catalyst
        entry_method = EntryMethod.MARKET
        if liquidity.recent_sweep == "sell_side" and bull_score > bear_score:
            bull_score += 1.2
            confirmations.append("Sell-side liquidity sweep")
            entry_method = EntryMethod.LIQUIDITY_SWEEP
        elif liquidity.recent_sweep == "buy_side" and bear_score > bull_score:
            bear_score += 1.2
            confirmations.append("Buy-side liquidity sweep")
            entry_method = EntryMethod.LIQUIDITY_SWEEP

        # Order-flow proxy
        if analysis.orderflow.buy_pressure and analysis.orderflow.buy_pressure > 0.6:
            bull_score += 0.5
        if analysis.orderflow.sell_pressure and analysis.orderflow.sell_pressure > 0.6:
            bear_score += 0.5

        # Volatility filter – avoid extremely unstable conditions for new entries
        if volatility.vol_regime == MarketRegime.HIGH_VOL and volatility.expanding:
            risk_factors.append("High & expanding volatility")
            bull_score *= 0.8
            bear_score *= 0.8

        # Decide direction
        min_edge = 2.0
        if bull_score >= min_edge and bull_score > bear_score * 1.15:
            direction = TradeDirection.LONG
        elif bear_score >= min_edge and bear_score > bull_score * 1.15:
            direction = TradeDirection.SHORT
        else:
            # Not enough edge
            return TradeOpportunity(
                exchange=analysis.exchange,
                symbol=analysis.symbol,
                direction=TradeDirection.NO_TRADE,
                status="NO_TRADE",
                entry_method=EntryMethod.MARKET,
                entry_zone_low=analysis.price,
                entry_zone_high=analysis.price,
                preferred_entry=analysis.price,
                stop_loss=StopLossPlan(price=analysis.price, reason="n/a", atr_distance=0),
                take_profit=TakeProfitPlan(),
                risk_reward=RiskRewardResult(
                    entry=analysis.price, stop=analysis.price, tp1=None, tp2=None, tp3=None,
                    gross_risk=0, gross_reward_tp1=0, net_risk=0, net_reward_tp1=0, net_rr_tp1=0,
                ),
                regime=regime.primary.value,
                confirmations=confirmations,
                risk_factors=risk_factors + ["Insufficient directional edge"],
                invalidation="N/A",
                data_quality=analysis.data_quality,
                score=0.0,
            )

        expected_higher_direction = (
            "bullish" if direction == TradeDirection.LONG else "bearish"
        )
        htf_conflict = analysis.higher_timeframe_direction != expected_higher_direction
        if analysis.higher_timeframe_direction not in ("bullish", "bearish"):
            risk_factors.append("4h trend confirmation is unavailable")
        elif htf_conflict:
            risk_factors.append(
                f"4h direction conflicts with {direction.value.lower()} setup"
            )
        else:
            confirmations.append(
                f"4h trend confirms {analysis.higher_timeframe_direction}"
            )

        # ---------- Entry zone ----------
        price = analysis.price
        atr = indicators.atr or price * 0.01

        if entry_method == EntryMethod.LIQUIDITY_SWEEP:
            # Prefer limit near the swept level
            if direction == TradeDirection.LONG and liquidity.sell_side_liquidity:
                zone_mid = max(liquidity.sell_side_liquidity)
                entry_low = zone_mid - atr * 0.15
                entry_high = zone_mid + atr * 0.25
                preferred = zone_mid
            elif direction == TradeDirection.SHORT and liquidity.buy_side_liquidity:
                zone_mid = min(liquidity.buy_side_liquidity)
                entry_low = zone_mid - atr * 0.25
                entry_high = zone_mid + atr * 0.15
                preferred = zone_mid
            else:
                entry_low = price - atr * 0.2
                entry_high = price + atr * 0.2
                preferred = price
                entry_method = EntryMethod.MARKET
        else:
            # Default: small zone around current price or minor pullback
            if direction == TradeDirection.LONG:
                entry_low = price - atr * 0.35
                entry_high = price + atr * 0.1
                preferred = price - atr * 0.1
                entry_method = EntryMethod.LIMIT_RETEST
            else:
                entry_low = price - atr * 0.1
                entry_high = price + atr * 0.35
                preferred = price + atr * 0.1
                entry_method = EntryMethod.LIMIT_RETEST

        # ---------- Stop & Targets ----------
        stop_plan = StopLossOptimiser.compute(direction, preferred, analysis, atr)
        tp_plan = TakeProfitOptimiser.compute(direction, preferred, stop_plan.price, analysis, atr)

        # ---------- Net R/R ----------
        rr = RiskRewardEngine.compute(
            direction, preferred, stop_plan.price,
            tp_plan.tp1, tp_plan.tp2, tp_plan.tp3,
            funding_rate=derivatives.funding_rate,
            is_maker=(entry_method != EntryMethod.MARKET),
        )

        # Reject if net R/R too poor
        if htf_conflict:
            status = "NO_TRADE"
            direction = TradeDirection.NO_TRADE
        elif rr.net_rr_tp1 < Config.MIN_NET_RR:
            risk_factors.append(f"Net R/R {rr.net_rr_tp1:.2f} below minimum {Config.MIN_NET_RR}")
            status = "NO_TRADE"
            direction = TradeDirection.NO_TRADE
        else:
            status = "READY" if regime.confidence >= Config.MIN_CONFIDENCE else "WAITING_FOR_CONFIRMATION"

        # ---------- Position size ----------
        pos = None
        if direction != TradeDirection.NO_TRADE:
            pos = PositionSizer.compute(preferred, stop_plan.price, rr.net_risk, market_meta=market_meta)
            if pos.position_size_coins <= 0:
                risk_factors.extend(pos.notes or ["Position size is zero"])
                status = "NO_TRADE"
                direction = TradeDirection.NO_TRADE
                pos = None

        # ---------- Invalidation ----------
        if direction == TradeDirection.LONG:
            invalidation = (
                f"Close below {stop_plan.price:.4f} or structure shift to lower-highs/lower-lows"
            )
        elif direction == TradeDirection.SHORT:
            invalidation = (
                f"Close above {stop_plan.price:.4f} or structure shift to higher-highs/higher-lows"
            )
        else:
            invalidation = "N/A"

        # ---------- Score (0-100) ----------
        score = 0.0
        score += min(40.0, (bull_score if direction == TradeDirection.LONG else bear_score) * 8)
        score += min(20.0, rr.net_rr_tp1 * 6)
        score += min(15.0, regime.confidence * 15)
        if liquidity.recent_sweep:
            score += 8
        if analysis.data_quality.startswith("good") or analysis.data_quality == "good":
            score += 7
        else:
            score -= 5
        if risk_factors:
            score -= min(15.0, len(risk_factors) * 3)
        score = clamp(score, 0.0, 100.0)

        return TradeOpportunity(
            exchange=analysis.exchange,
            symbol=analysis.symbol,
            direction=direction,
            status=status,
            entry_method=entry_method,
            entry_zone_low=round(min(entry_low, entry_high), 8),
            entry_zone_high=round(max(entry_low, entry_high), 8),
            preferred_entry=round(preferred, 8),
            stop_loss=stop_plan,
            take_profit=tp_plan,
            risk_reward=rr,
            position_size=pos,
            regime=regime.primary.value,
            confirmations=confirmations,
            risk_factors=risk_factors,
            invalidation=invalidation,
            data_quality=analysis.data_quality,
            score=score,
        )


# ===========================================================================
# SECTION 18 – OPPORTUNITY RANKING ENGINE
# ===========================================================================

class OpportunityRanker:
    """Ranks all current TradeOpportunity objects and filters by quality."""

    @staticmethod
    def rank(opportunities: List[TradeOpportunity]) -> List[TradeOpportunity]:
        # Keep only actionable
        actionable = [
            o for o in opportunities
            if o.direction in (TradeDirection.LONG, TradeDirection.SHORT)
            and o.status in ("READY", "WAITING_FOR_CONFIRMATION")
            and o.score >= 35
            and o.risk_reward.net_rr_tp1 >= Config.MIN_NET_RR
        ]
        # Sort by score descending
        actionable.sort(key=lambda o: o.score, reverse=True)
        for i, o in enumerate(actionable, 1):
            o.rank = i
        return actionable


# ===========================================================================
# SECTION 19 – MARKET DATA MANAGER (extended for Stage 3)
# ===========================================================================

# We patch the existing MarketDataManager by re-defining key methods via
# a thin Stage3Manager that wraps / extends behaviour. For the monolithic
# design we simply replace the class methods in-place by re-assigning
# after the original class definition. The cleanest approach for a single
# file is to continue the same class with additional methods.

# --- Additional methods injected into MarketDataManager ---

def _mdm_run_opportunity_scan(self: MarketDataManager) -> List[TradeOpportunity]:
    """Scan all analysed assets and produce ranked opportunities."""
    opps: List[TradeOpportunity] = []
    for key, analysis in self.analyses.items():
        try:
            eid = analysis.exchange
            connector = self.connectors.get(eid)
            market_meta = connector.meta.get(analysis.symbol) if connector else None
            opp = EntryOptimiser.evaluate(analysis, market_meta=market_meta)
            if opp:
                opps.append(opp)
        except Exception as e:
            log.debug("Entry eval failed %s: %s", key, e)
    ranked = OpportunityRanker.rank(opps)
    self.opportunities = ranked          # type: ignore
    return ranked


# Attach to class
MarketDataManager.run_opportunity_scan = _mdm_run_opportunity_scan  # type: ignore
MarketDataManager.opportunities = []  # type: ignore


# Extend the analysis loop and health display by monkey-patching start
_original_start = MarketDataManager.start

async def _stage3_start(self: MarketDataManager) -> None:
    await _original_start(self)
    # Add opportunity scan task
    self._tasks.append(asyncio.create_task(self._opportunity_loop(), name="opp_loop"))
    log.info("Stage 3 opportunity scanner started")

async def _opportunity_loop(self: MarketDataManager) -> None:
    while self._running:
        try:
            if Config.EXECUTION_MODE == "live" and self.live_executor.exchange:
                await self.live_executor.monitor_positions()
            ranked = self.run_opportunity_scan()
            if ranked:
                log.info("Top opportunities (%d):", len(ranked))
                for o in ranked[:5]:
                    log.info(
                        "  #%d %s %s %s | entry=%.4f stop=%.4f TP1=%.4f R/R=1:%.2f score=%.1f",
                        o.rank, o.symbol, o.direction.value, o.status,
                        o.preferred_entry, o.stop_loss.price,
                        o.take_profit.tp1 or 0, o.risk_reward.net_rr_tp1, o.score,
                    )
                    # Full report for the #1 opportunity
                    if o.rank == 1:
                        log.info("\n" + o.to_report())
            else:
                log.info("No qualifying opportunities this cycle")
        except Exception as e:
            log.exception("Opportunity loop error: %s", e)
        await asyncio.sleep(Config.OPPORTUNITY_SCAN_INTERVAL)

MarketDataManager.start = _stage3_start  # type: ignore
MarketDataManager._opportunity_loop = _opportunity_loop  # type: ignore


# ===========================================================================
# SECTION 21 – STAGE 4 DATA MODELS
# ===========================================================================

@dataclass
class SimulatedFill:
    timestamp: int
    price: float
    size: float
    side: str               # "buy" / "sell"
    fee: float
    is_maker: bool = False


@dataclass
class PaperPosition:
    id: str
    exchange: str
    symbol: str
    direction: TradeDirection
    entry_price: float
    size: float
    stop_loss: float
    take_profit_1: Optional[float]
    take_profit_2: Optional[float]
    take_profit_3: Optional[float]
    opened_at: float
    status: str = "open"          # open / closed
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None
    closed_at: Optional[float] = None
    realized_pnl: float = 0.0
    fees_paid: float = 0.0
    funding_paid: float = 0.0
    r_multiple: float = 0.0
    opportunity_score: float = 0.0
    initial_size: Optional[float] = None
    remaining_size: Optional[float] = None
    entry_fee_paid: float = 0.0
    exit_fees_paid: float = 0.0
    targets_hit: List[int] = field(default_factory=list)
    funding_applied: float = 0.0
    next_funding_time: Optional[int] = None

    def __post_init__(self) -> None:
        if self.initial_size is None:
            self.initial_size = self.size
        if self.remaining_size is None:
            self.remaining_size = self.size if self.status == "open" else 0.0
        if self.entry_fee_paid == 0 and self.fees_paid > self.exit_fees_paid:
            self.entry_fee_paid = self.fees_paid - self.exit_fees_paid


@dataclass
class PerformanceSnapshot:
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    expectancy: float = 0.0
    total_pnl: float = 0.0
    max_drawdown: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    avg_r: float = 0.0
    consecutive_losses: int = 0
    max_consecutive_losses: int = 0
    sharpe_approx: float = 0.0
    avg_holding_seconds: float = 0.0


class PerformanceTracker:
    """Tracks paper / backtest performance. Pure accounting – no look-ahead."""

    def __init__(self, starting_balance: float = Config.ACCOUNT_BALANCE):
        self.starting_balance = starting_balance
        self.balance = starting_balance
        self.equity_curve: List[Tuple[float, float]] = [(time.time(), starting_balance)]
        self.closed_trades: List[PaperPosition] = []
        self.open_positions: Dict[str, PaperPosition] = {}
        self.peak_equity = starting_balance
        self.max_drawdown = 0.0
        self.consecutive_losses = 0
        self.max_consecutive_losses = 0

    def restore_from_database(self, db: "MarketDatabase") -> None:
        if not db._conn:
            return
        try:
            equity_rows = db._conn.execute(
                "SELECT ts, balance FROM paper_equity ORDER BY ts, id"
            ).fetchall()
            if equity_rows:
                self.equity_curve = [
                    (float(timestamp), float(balance))
                    for timestamp, balance in equity_rows
                ]
                self.balance = self.equity_curve[-1][1]
                self._rebuild_drawdown(self.equity_curve)
            for row in db._conn.execute(
                """SELECT id, exchange, symbol, direction, status, entry_price, size,
                          stop_loss, tp1, tp2, tp3, opened_at, closed_at,
                          exit_price, exit_reason, realized_pnl, fees_paid,
                          funding_paid, r_multiple, opportunity_score, payload
                   FROM paper_positions"""
            ):
                payload = json.loads(row[20] or "{}")
                pos = PaperPosition(
                    id=row[0], exchange=row[1], symbol=row[2],
                    direction=TradeDirection(row[3]), entry_price=float(row[5]),
                    size=float(row[6]), stop_loss=float(row[7]),
                    take_profit_1=row[8], take_profit_2=row[9],
                    take_profit_3=row[10], opened_at=float(row[11]),
                    status=row[4], closed_at=row[12], exit_price=row[13],
                    exit_reason=row[14], realized_pnl=float(row[15]),
                    fees_paid=float(row[16]), funding_paid=float(row[17]),
                    r_multiple=float(row[18]), opportunity_score=float(row[19]),
                    initial_size=payload.get("initial_size"),
                    remaining_size=payload.get("remaining_size"),
                    entry_fee_paid=float(payload.get("entry_fee_paid", 0)),
                    exit_fees_paid=float(payload.get("exit_fees_paid", 0)),
                    targets_hit=payload.get("targets_hit", []),
                    funding_applied=float(payload.get("funding_applied") or 0),
                    next_funding_time=(
                        int(payload["next_funding_time"])
                        if payload.get("next_funding_time") is not None
                        else None
                    ),
                )
                if pos.status == "open":
                    self.open_positions[pos.id] = pos
                else:
                    self.closed_trades.append(pos)
            if not equity_rows and self.closed_trades:
                self.balance = self.starting_balance
                self.equity_curve = [(time.time(), self.starting_balance)]
                for trade in sorted(
                    self.closed_trades,
                    key=lambda position: position.closed_at or position.opened_at,
                ):
                    self._record_equity(self.balance + trade.realized_pnl)
            self.consecutive_losses = 0
            self.max_consecutive_losses = 0
            for trade in sorted(
                self.closed_trades,
                key=lambda position: position.closed_at or position.opened_at,
            ):
                if trade.realized_pnl < 0:
                    self.consecutive_losses += 1
                    self.max_consecutive_losses = max(
                        self.max_consecutive_losses, self.consecutive_losses
                    )
                else:
                    self.consecutive_losses = 0
            db.save_paper_equity(self.balance, "restore")
        except Exception as exc:
            log.exception("Paper state restore failed: %s", exc)

    def _rebuild_drawdown(self, equity_curve: List[Tuple[float, float]]) -> None:
        balances = [self.starting_balance]
        balances.extend(balance for _, balance in equity_curve)
        self.peak_equity = self.starting_balance
        self.max_drawdown = 0.0
        for balance in balances:
            self.peak_equity = max(self.peak_equity, balance)
            drawdown = (
                (self.peak_equity - balance) / self.peak_equity
                if self.peak_equity > 0 else 0.0
            )
            self.max_drawdown = max(self.max_drawdown, drawdown)

    def _record_equity(self, balance: float) -> None:
        self.balance = balance
        self.equity_curve.append((time.time(), balance))
        self.peak_equity = max(self.peak_equity, balance)
        drawdown = (
            (self.peak_equity - balance) / self.peak_equity
            if self.peak_equity > 0 else 0.0
        )
        self.max_drawdown = max(self.max_drawdown, drawdown)

    def open_position(self, pos: PaperPosition) -> None:
        self.open_positions[pos.id] = pos

    def close_position(
        self, pos_id: str, exit_price: float, reason: str, fee: float = 0.0
    ) -> Optional[PaperPosition]:
        pos = self.open_positions.get(pos_id)
        if not pos:
            return None
        remaining = max(0.0, float(pos.remaining_size or 0.0))
        if pos.direction == TradeDirection.LONG:
            raw_pnl = (exit_price - pos.entry_price) * remaining
        else:
            raw_pnl = (pos.entry_price - exit_price) * remaining

        initial_size = float(pos.initial_size or pos.size)
        total_entry_fee = pos.entry_fee_paid or max(0.0, pos.fees_paid - pos.exit_fees_paid)
        entry_fee = total_entry_fee * remaining / initial_size if initial_size > 0 else total_entry_fee
        unapplied_funding = pos.funding_paid - pos.funding_applied
        final_pnl = raw_pnl - entry_fee - fee - unapplied_funding
        pos.realized_pnl += final_pnl
        pos.fees_paid = total_entry_fee + pos.exit_fees_paid + fee
        pos.exit_fees_paid += fee
        pos.remaining_size = 0.0
        pos.status = "closed"
        pos.exit_price = exit_price
        pos.exit_reason = reason
        pos.closed_at = time.time()
        risk = abs(pos.entry_price - pos.stop_loss) * initial_size
        pos.r_multiple = pos.realized_pnl / risk if risk > 0 else 0.0

        self._record_equity(self.balance + final_pnl)
        self.open_positions.pop(pos_id, None)
        self.closed_trades.append(pos)

        if pos.realized_pnl < 0:
            self.consecutive_losses += 1
            self.max_consecutive_losses = max(
                self.max_consecutive_losses, self.consecutive_losses
            )
        else:
            self.consecutive_losses = 0

        return pos

    def close_partial(
        self, pos_id: str, exit_price: float, quantity: float, fee: float
    ) -> Optional[PaperPosition]:
        pos = self.open_positions.get(pos_id)
        if not pos:
            return None
        remaining = max(0.0, float(pos.remaining_size or 0.0))
        quantity = min(max(0.0, quantity), remaining)
        if quantity <= 0:
            return None
        initial_size = float(pos.initial_size or pos.size)
        total_entry_fee = pos.entry_fee_paid or max(0.0, pos.fees_paid - pos.exit_fees_paid)
        entry_fee = total_entry_fee * quantity / initial_size if initial_size > 0 else 0.0
        raw_pnl = (
            (exit_price - pos.entry_price) * quantity
            if pos.direction == TradeDirection.LONG
            else (pos.entry_price - exit_price) * quantity
        )
        partial_pnl = raw_pnl - entry_fee - fee
        pos.realized_pnl += partial_pnl
        pos.exit_fees_paid += fee
        pos.fees_paid = total_entry_fee + pos.exit_fees_paid
        pos.remaining_size = max(0.0, remaining - quantity)
        risk = abs(pos.entry_price - pos.stop_loss) * initial_size
        pos.r_multiple = pos.realized_pnl / risk if risk > 0 else 0.0
        self._record_equity(self.balance + partial_pnl)
        return pos

    def snapshot(self) -> PerformanceSnapshot:
        closed = self.closed_trades
        if not closed:
            return PerformanceSnapshot()

        wins = [t for t in closed if t.realized_pnl > 0]
        losses = [t for t in closed if t.realized_pnl <= 0]
        total_win = sum(t.realized_pnl for t in wins)
        total_loss = abs(sum(t.realized_pnl for t in losses)) or 1e-9

        avg_win = total_win / len(wins) if wins else 0.0
        avg_loss = total_loss / len(losses) if losses else 0.0
        win_rate = len(wins) / len(closed)
        expectancy = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)
        avg_r = sum(t.r_multiple for t in closed) / len(closed)

        rs = [t.r_multiple for t in closed]
        sharpe = 0.0
        if len(rs) > 1:
            mean_r = sum(rs) / len(rs)
            var = sum((r - mean_r) ** 2 for r in rs) / (len(rs) - 1)
            std = math.sqrt(var) if var > 0 else 1e-9
            sharpe = mean_r / std

        holding = []
        for t in closed:
            if t.closed_at and t.opened_at:
                holding.append(t.closed_at - t.opened_at)
        avg_hold = sum(holding) / len(holding) if holding else 0.0

        return PerformanceSnapshot(
            total_trades=len(closed),
            winning_trades=len(wins),
            losing_trades=len(losses),
            win_rate=win_rate,
            profit_factor=total_win / total_loss,
            expectancy=expectancy,
            total_pnl=self.balance - self.starting_balance,
            max_drawdown=self.max_drawdown,
            avg_win=avg_win,
            avg_loss=avg_loss,
            avg_r=avg_r,
            consecutive_losses=self.consecutive_losses,
            max_consecutive_losses=self.max_consecutive_losses,
            sharpe_approx=sharpe,
            avg_holding_seconds=avg_hold,
        )

    def summary_text(self) -> str:
        s = self.snapshot()
        return (
            f"Paper Performance | Trades={s.total_trades}  "
            f"WinRate={s.win_rate*100:.1f}%  PF={s.profit_factor:.2f}  "
            f"Expectancy=${s.expectancy:.2f}  PnL=${s.total_pnl:.2f}  "
            f"MaxDD={s.max_drawdown*100:.1f}%  AvgR={s.avg_r:.2f}  "
            f"MaxCL={s.max_consecutive_losses}"
        )


# ===========================================================================
# SECTION 22 – PAPER TRADING SIMULATOR
# ===========================================================================

class PaperTradingSimulator:
    """
    Continuous paper trading against live opportunities.
    - Opens positions when a high-scoring opportunity appears
    - Manages stops and take-profits on every price update
    - Applies realistic fees and simple funding
    - Never risks more than configured limits
    """

    def __init__(
        self, tracker: PerformanceTracker, database: Optional[MarketDatabase] = None
    ):
        self.tracker = tracker
        self.database = database
        self.max_open_positions = 3
        self.min_score_to_enter = 55.0
        self._pos_counter = 0
        self.pending_opportunities: Dict[str, TradeOpportunity] = {}
        self._pending_since: Dict[str, float] = {}
        self._last_processed_bar: Dict[str, int] = {}
        self.restore_pending_opportunities()

    def restore_pending_opportunities(self) -> None:
        if not self.database:
            return
        for opportunity, created_at in self.database.get_pending_opportunities():
            if time.time() - created_at <= Config.PENDING_ORDER_MAX_SECONDS:
                key = f"{opportunity.exchange}:{opportunity.symbol}"
                self.pending_opportunities[key] = opportunity
                self._pending_since[key] = created_at
            else:
                self.database.delete_pending_opportunity(
                    opportunity.exchange, opportunity.symbol
                )

    @staticmethod
    def _base_asset(symbol: str) -> str:
        return symbol.split("/", 1)[0].upper().split("-", 1)[0]

    def _correlation_bucket(self, symbol: str) -> str:
        base = self._base_asset(symbol)
        if base in ("BTC", "WBTC"):
            return "btc"
        if base in ("ETH", "WETH", "STETH"):
            return "eth"
        return base

    def _new_id(self) -> str:
        self._pos_counter += 1
        return f"PAPER-{int(time.time())}-{self._pos_counter}"

    def consider_opportunity(
        self,
        opp: TradeOpportunity,
        allow_pending: bool = True,
        opened_at: Optional[float] = None,
        observed_fill_price: Optional[float] = None,
        funding: Optional[FundingSnapshot] = None,
    ) -> Optional[PaperPosition]:
        if opp.direction not in (TradeDirection.LONG, TradeDirection.SHORT):
            return None
        if opp.status not in ("READY", "WAITING_FOR_CONFIRMATION"):
            return None
        if opp.score < self.min_score_to_enter:
            return None
        if len(self.tracker.open_positions) >= self.max_open_positions:
            return None
        if not opp.position_size or opp.position_size.position_size_coins <= 0:
            return None
        # Avoid duplicate symbol and direction
        for p in self.tracker.open_positions.values():
            if p.symbol == opp.symbol and p.exchange == opp.exchange:
                return None
        key = f"{opp.exchange}:{opp.symbol}"
        if allow_pending and any(
            pending_key == key for pending_key in self.pending_opportunities
        ):
            return None

        notional = sum(
            p.entry_price * float(p.remaining_size or 0.0)
            for p in self.tracker.open_positions.values()
        )
        notional += sum(
            pending.position_size.position_size_coins * pending.preferred_entry
            for pending_key, pending in self.pending_opportunities.items()
            if pending_key != key and pending.position_size
        )
        position_notional = opp.position_size.position_size_coins * (
            observed_fill_price or opp.preferred_entry
        )
        portfolio_limit = self.tracker.balance * (Config.MAX_PORTFOLIO_NOTIONAL_PCT / 100.0)
        if notional + position_notional > portfolio_limit:
            return None

        bucket = self._correlation_bucket(opp.symbol)
        correlated = sum(
            1 for p in self.tracker.open_positions.values()
            if self._correlation_bucket(p.symbol) == bucket
        )
        correlated += sum(
            1 for pending_key, pending in self.pending_opportunities.items()
            if pending_key != key and self._correlation_bucket(pending.symbol) == bucket
        )
        if correlated >= Config.MAX_CORRELATED_POSITIONS:
            return None

        if allow_pending and opp.entry_method in (EntryMethod.LIMIT_RETEST, EntryMethod.LIQUIDITY_SWEEP):
            self.pending_opportunities[key] = opp
            self._pending_since[key] = time.time()
            if self.database:
                self.database.save_pending_opportunity(opp, self._pending_since[key])
            return None

        size = opp.position_size.position_size_coins
        entry = observed_fill_price or opp.preferred_entry
        slip_multiplier = Config.DEFAULT_SLIPPAGE_BPS / 10_000
        if opp.direction == TradeDirection.LONG:
            entry *= (1 + slip_multiplier)
        else:
            entry *= (1 - slip_multiplier)
        fee = entry * size * Config.TAKER_FEE_PCT

        pos = PaperPosition(
            id=self._new_id(),
            exchange=opp.exchange,
            symbol=opp.symbol,
            direction=opp.direction,
            entry_price=entry,
            size=size,
            stop_loss=opp.stop_loss.price,
            take_profit_1=opp.take_profit.tp1,
            take_profit_2=opp.take_profit.tp2,
            take_profit_3=opp.take_profit.tp3,
            opened_at=time.time() if opened_at is None else opened_at,
            fees_paid=fee,
            opportunity_score=opp.score,
            entry_fee_paid=fee,
            next_funding_time=(
                funding.next_funding_time
                if funding and funding.freshness() == DataFreshness.FRESH
                else None
            ),
        )
        self.tracker.open_position(pos)
        log.info(
            "PAPER OPEN %s %s %s @ %.4f  size=%.6f  SL=%.4f  TP1=%.4f  score=%.1f",
            pos.id, pos.direction.value, pos.symbol, pos.entry_price,
            pos.size, pos.stop_loss, pos.take_profit_1 or 0, pos.opportunity_score,
        )
        return pos

    def update_with_price(
        self,
        exchange: str,
        symbol: str,
        price: float,
        high: float,
        low: float,
        bar_timestamp: Optional[int] = None,
        funding: Optional[FundingSnapshot] = None,
    ) -> List[PaperPosition]:
        """Check open positions for stop / TP hits using high/low of the bar."""
        closed: List[PaperPosition] = []
        market_time_ms = (
            bar_timestamp + timeframe_milliseconds("1m")
            if bar_timestamp is not None
            else utc_ms()
        )
        funding_is_fresh = (
            funding is not None
            and funding.freshness() == DataFreshness.FRESH
            and math.isfinite(funding.funding_rate)
            and abs(funding.timestamp - market_time_ms)
            <= Config.FRESHNESS_FUNDING * 1000
        )
        key_prefix = f"{exchange}:{symbol}"
        newly_opened: Set[str] = set()
        for key in list(self.pending_opportunities):
            if key != key_prefix:
                continue
            opp = self.pending_opportunities[key]
            pending_age = time.time() - self._pending_since.get(key, time.time())
            if pending_age > Config.PENDING_ORDER_MAX_SECONDS:
                del self.pending_opportunities[key]
                self._pending_since.pop(key, None)
                if self.database:
                    self.database.delete_pending_opportunity(exchange, symbol)
                continue
            pending_since = self._pending_since.get(key, time.time())
            if bar_timestamp is not None:
                # Ignore ranges that began before the pending setup existed.
                first_full_bar = (
                    math.floor(pending_since * 1000 / timeframe_milliseconds("1m")) + 1
                ) * timeframe_milliseconds("1m")
                if bar_timestamp < first_full_bar:
                    continue
            fill = (
                opp.direction == TradeDirection.LONG
                and low <= opp.entry_zone_high
                and high >= opp.entry_zone_low
            ) or (
                opp.direction == TradeDirection.SHORT
                and high >= opp.entry_zone_low
                and low <= opp.entry_zone_high
            )
            if fill and math.isfinite(price) and price > 0:
                fill_price = min(
                    max(price, opp.entry_zone_low), opp.entry_zone_high
                )
                pos = self.consider_opportunity(
                    opp,
                    allow_pending=False,
                    opened_at=(bar_timestamp / 1000.0 if bar_timestamp is not None else None),
                    observed_fill_price=fill_price,
                    funding=funding,
                )
                if pos:
                    newly_opened.add(pos.id)
                    if self.database:
                        self.database.save_paper_position(pos)
                    del self.pending_opportunities[key]
                    self._pending_since.pop(key, None)
                    if self.database:
                        self.database.delete_pending_opportunity(exchange, symbol)
        to_check = [
            p for p in list(self.tracker.open_positions.values())
            if p.exchange == exchange and p.symbol == symbol
        ]
        for pos in to_check:
            if funding_is_fresh and funding and pos.status == "open":
                next_funding = pos.next_funding_time
                if next_funding is None and funding.next_funding_time:
                    next_funding = funding.next_funding_time
                if next_funding is not None:
                    while next_funding <= market_time_ms:
                        if next_funding > pos.opened_at * 1000:
                            mark_price = safe_float(funding.mark_price)
                            if not math.isfinite(mark_price) or mark_price <= 0:
                                mark_price = price
                            signed_rate = (
                                funding.funding_rate
                                if pos.direction == TradeDirection.LONG
                                else -funding.funding_rate
                            )
                            funding_cost = (
                                mark_price
                                * float(pos.remaining_size or 0.0)
                                * signed_rate
                            )
                            pos.funding_paid += funding_cost
                            pos.funding_applied += funding_cost
                            pos.realized_pnl -= funding_cost
                            self.tracker._record_equity(
                                self.tracker.balance - funding_cost
                            )
                        next_from_exchange = funding.next_funding_time
                        next_interval = next_funding + int(
                            Config.FUNDING_HORIZON_HOURS * 3_600_000
                        )
                        if (
                            next_from_exchange
                            and next_funding < next_from_exchange <= next_interval
                        ):
                            next_funding = next_from_exchange
                        else:
                            next_funding = next_interval
                    pos.next_funding_time = next_funding
            if pos.id in newly_opened:
                continue
            if bar_timestamp is not None:
                # The entry minute can contain movement from before the fill.
                first_full_bar = (
                    math.floor(pos.opened_at * 1000 / timeframe_milliseconds("1m")) + 1
                ) * timeframe_milliseconds("1m")
                if bar_timestamp < first_full_bar:
                    continue
            exit_price = None
            reason = None

            if pos.direction == TradeDirection.LONG:
                if low <= pos.stop_loss:
                    exit_price = pos.stop_loss
                    reason = "stop_loss"
            else:  # SHORT
                if high >= pos.stop_loss:
                    exit_price = pos.stop_loss
                    reason = "stop_loss"

            if exit_price is not None and reason:
                remaining = float(pos.remaining_size or 0.0)
                fee = exit_price * remaining * Config.TAKER_FEE_PCT
                closed_pos = self.tracker.close_position(pos.id, exit_price, reason, fee)
                if closed_pos:
                    log.info(
                        "PAPER CLOSE %s %s %s @ %.4f  reason=%s  PnL=%.4f  R=%.2f",
                        closed_pos.id, closed_pos.direction.value, closed_pos.symbol,
                        exit_price, reason, closed_pos.realized_pnl, closed_pos.r_multiple,
                    )
                    closed.append(closed_pos)
                continue

            active_targets = [
                (index, getattr(pos, f"take_profit_{index}"))
                for index in (1, 2, 3)
                if getattr(pos, f"take_profit_{index}") is not None
            ]
            if not active_targets:
                continue
            weights = {
                1: [1.0],
                2: [0.5, 0.5],
                3: [0.5, 0.25, 0.25],
            }[len(active_targets)]
            for target_order, (index, target) in enumerate(active_targets):
                if index in pos.targets_hit:
                    continue
                hit = high >= target if pos.direction == TradeDirection.LONG else low <= target
                if not hit:
                    continue
                pos.targets_hit.append(index)
                remaining = float(pos.remaining_size or 0.0)
                final_target = target_order == len(active_targets) - 1
                quantity = remaining if final_target else min(
                    remaining, float(pos.initial_size or pos.size) * weights[target_order]
                )
                fee = target * quantity * Config.TAKER_FEE_PCT
                if quantity >= remaining - 1e-12:
                    closed_pos = self.tracker.close_position(
                        pos.id, target, f"take_profit_{index}", fee
                    )
                    if closed_pos:
                        closed.append(closed_pos)
                        log.info(
                            "PAPER CLOSE %s %s %s @ %.4f  reason=take_profit_%d  PnL=%.4f  R=%.2f",
                            closed_pos.id, closed_pos.direction.value, closed_pos.symbol,
                            target, index, closed_pos.realized_pnl, closed_pos.r_multiple,
                        )
                    break
                partial = self.tracker.close_partial(pos.id, target, quantity, fee)
                if partial:
                    log.info(
                        "PAPER PARTIAL %s %s %s @ %.4f  target=TP%d  remaining=%.6f",
                        partial.id, partial.direction.value, partial.symbol,
                        target, index, partial.remaining_size or 0,
                    )
        return closed


# ===========================================================================
# SECTION 23 – SIMPLE BACKTEST ENGINE (historical)
# ===========================================================================

class BacktestEngine:
    """
    Lightweight bar-by-bar backtest on stored candles.
    Assumptions (explicit):
    - Entry at next bar open after signal
    - Stop / TP checked against high/low of subsequent bars
    - Fixed taker fee both sides
    - No partial fills modelling beyond simple size
    - Funding is an approximation using the supplied fixed rate and interval
    """

    def __init__(self, starting_balance: float = Config.ACCOUNT_BALANCE):
        self.starting_balance = starting_balance

    def run(
        self,
        candles: List[Candle],
        direction: TradeDirection,
        entry_price: float,
        stop: float,
        tp1: Optional[float],
        size: float,
        entry_idx: int = 0,
        limit_order: bool = False,
        slippage_bps: float = Config.DEFAULT_SLIPPAGE_BPS,
        funding_rate: float = 0.0,
        funding_interval_bars: int = 8,
    ) -> Dict[str, Any]:
        if (
            entry_idx >= len(candles) - 2
            or size <= 0
            or not math.isfinite(funding_rate)
        ):
            return {"status": "invalid", "pnl": 0.0}

        entry_bar = candles[entry_idx + 1]
        slip = entry_price * (slippage_bps / 10_000)
        fill_price = None
        entry_fill_idx = None

        if limit_order:
            for i in range(entry_idx + 1, len(candles)):
                bar = candles[i]
                if direction == TradeDirection.LONG and bar.low <= entry_price:
                    fill_price = min(entry_price, bar.open + slip)
                    fill_price = fill_price if fill_price > 0 else bar.open
                    entry_fill_idx = i
                    break
                if direction == TradeDirection.SHORT and bar.high >= entry_price:
                    fill_price = max(entry_price, bar.open - slip)
                    fill_price = fill_price if fill_price > 0 else bar.open
                    entry_fill_idx = i
                    break
        else:
            fill_price = entry_bar.open
            fill_price *= (1 + slippage_bps / 10_000) if direction == TradeDirection.LONG else (1 - slippage_bps / 10_000)
            entry_fill_idx = entry_idx + 1

        if fill_price is None:
            return {"status": "unfilled", "pnl": 0.0}
        fee_entry = fill_price * size * Config.TAKER_FEE_PCT

        for i in range(entry_fill_idx + 1, len(candles)):
            bar = candles[i]
            if direction == TradeDirection.LONG:
                if bar.low <= stop:
                    exit_p = max(stop, bar.open - bar.open * (slippage_bps / 10_000))
                    reason = "stop"
                    break
                if tp1 and bar.high >= tp1:
                    exit_p = min(tp1, bar.open + bar.open * (slippage_bps / 10_000))
                    reason = "tp1"
                    break
            else:
                if bar.high >= stop:
                    exit_p = min(stop, bar.open + bar.open * (slippage_bps / 10_000))
                    reason = "stop"
                    break
                if tp1 and bar.low <= tp1:
                    exit_p = max(tp1, bar.open - bar.open * (slippage_bps / 10_000))
                    reason = "tp1"
                    break
        else:
            # End of data – close at last close
            exit_p = candles[-1].close
            reason = "eod"

        fee_exit = exit_p * size * Config.TAKER_FEE_PCT
        if direction == TradeDirection.LONG:
            raw = (exit_p - fill_price) * size
        else:
            raw = (fill_price - exit_p) * size
        held_bars = max(0, len(candles) - 1 - entry_fill_idx)
        funding_intervals = held_bars // max(1, funding_interval_bars)
        funding_cost = (
            fill_price
            * size
            * funding_rate
            * funding_intervals
            * (1 if direction == TradeDirection.LONG else -1)
        )
        pnl = raw - fee_entry - fee_exit - funding_cost
        risk = abs(fill_price - stop) * size
        r_mult = pnl / risk if risk > 0 else 0.0

        return {
            "status": "closed",
            "entry": fill_price,
            "exit": exit_p,
            "reason": reason,
            "pnl": pnl,
            "r_multiple": r_mult,
            "fees": fee_entry + fee_exit,
            "funding": funding_cost,
            "entry_index": entry_fill_idx,
        }

    def replay(
        self,
        candles: List[Candle],
        stop_distance: float,
        target_distance: float,
        direction: TradeDirection,
    ) -> Dict[str, Any]:
        """Walk forward over every safe signal location and aggregate the result.

        This is a research utility: callers must supply stop/target distances
        derived from a bar available at the signal, never future data.
        """
        if len(candles) < 30:
            return {"status": "insufficient_history", "trades": []}
        trades: List[Dict[str, Any]] = []
        for i in range(20, len(candles) - 2):
            entry = candles[i].close
            stop = entry - stop_distance if direction == TradeDirection.LONG else entry + stop_distance
            tp1 = entry + target_distance if direction == TradeDirection.LONG else entry - target_distance
            trade = self.run(candles, direction, entry, stop, tp1, 1.0, i)
            if trade["status"] == "closed":
                trades.append(trade)
        if not trades:
            return {"status": "closed", "trades": [], "total_pnl": 0.0}
        pnls = [t["pnl"] for t in trades]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        gross_win = sum(wins)
        gross_loss = abs(sum(losses)) or 1e-9
        return {
            "status": "closed",
            "trades": trades,
            "total_pnl": sum(pnls),
            "win_rate": len(wins) / len(trades),
            "profit_factor": gross_win / gross_loss,
            "avg_r": sum(t["r_multiple"] for t in trades) / len(trades),
        }


# ===========================================================================
# SECTION 24 – WIRE PAPER TRADING INTO MARKET DATA MANAGER
# ===========================================================================

# Attach paper trading to the existing manager
_original_opportunity_loop = getattr(MarketDataManager, "_opportunity_loop", None)

async def _stage4_opportunity_loop(self: MarketDataManager) -> None:
    """Extended opportunity loop that also feeds the paper simulator."""
    if not hasattr(self, "paper_tracker"):
        self.paper_tracker = PerformanceTracker(Config.ACCOUNT_BALANCE)
        self.paper_sim = PaperTradingSimulator(self.paper_tracker)
        log.info("Paper trading simulator initialised (balance=$%.2f)", Config.ACCOUNT_BALANCE)

    while self._running:
        try:
            ranked = self.run_opportunity_scan()
            if ranked:
                log.info("Top opportunities (%d):", len(ranked))
                for o in ranked[:5]:
                    log.info(
                        "  #%d %s %s %s | entry=%.4f stop=%.4f TP1=%.4f R/R=1:%.2f score=%.1f",
                        o.rank, o.symbol, o.direction.value, o.status,
                        o.preferred_entry, o.stop_loss.price,
                        o.take_profit.tp1 or 0, o.risk_reward.net_rr_tp1, o.score,
                    )
                    if o.rank == 1:
                        log.info("\n" + o.to_report())

                # Paper trade the best opportunities
                for o in ranked[:2]:
                    self.paper_sim.consider_opportunity(
                        o, funding=self.fundings.get(f"{o.exchange}:{o.symbol}")
                    )

            # Periodic performance summary
            if self.paper_tracker.closed_trades or self.paper_tracker.open_positions:
                log.info(self.paper_tracker.summary_text())
                log.info(
                    "Open paper positions: %d", len(self.paper_tracker.open_positions)
                )

        except Exception as e:
            log.exception("Stage-4 opportunity/paper loop error: %s", e)
        await asyncio.sleep(Config.OPPORTUNITY_SCAN_INTERVAL)


MarketDataManager._opportunity_loop = _stage4_opportunity_loop  # type: ignore


# ===========================================================================
# SECTION 26 – TELEGRAM NOTIFICATIONS
# ===========================================================================

class LiveExecutionAdapter:
    """
    Defensive OKX live execution adapter.

    Default mode is paper. Live orders require EXECUTION_MODE=live,
    OKX API credentials, LIVE_CONFIRMATION, and explicit testnet/live selection.
    """

    def __init__(self, database: Optional[MarketDatabase] = None):
        self.database = database
        self.exchange = None
        self.mode = Config.EXECUTION_MODE
        self._order_timestamps: deque = deque(maxlen=100)
        self._open_positions = 0
        self._testnet = Config.OKX_TESTNET
        self._hedged: Optional[bool] = None
        self._daily_realized_pnl: Optional[float] = None
        self._daily_pnl_available = False
        self._positions_available = False
        self._orders_available = False
        self._tracked_symbols: Set[str] = set()
        self._protection_targets: Dict[str, Dict[str, Any]] = (
            database.get_live_protection_intents() if database else {}
        )

    def validate_configuration(self) -> Tuple[bool, str]:
        if self.mode == "paper":
            return True, "paper mode"
        if self.mode != "live":
            return False, "EXECUTION_MODE must be paper or live"
        if Config.CONTRACT_TYPE != "swap" or Config.QUOTE.upper() != "USDT":
            return False, "live OKX execution supports USDT-settled swap markets only"
        if "okx" not in Config.EXCHANGE_PRIORITY:
            return False, "live OKX execution requires OKX in EXCHANGE_PRIORITY"
        if Config.LIVE_KILL_SWITCH:
            return False, "LIVE_KILL_SWITCH is enabled (set false only after reviewing limits)"
        if not Config.OKX_API_KEY or not Config.OKX_API_SECRET or not Config.OKX_API_PASSPHRASE:
            return False, "OKX API credentials are missing"
        if not Config.LIVE_CONFIRMATION_TOKEN:
            return False, "LIVE_CONFIRMATION_TOKEN must be explicitly configured"
        if Config.LIVE_CONFIRMATION != Config.LIVE_CONFIRMATION_TOKEN:
            return False, "LIVE_CONFIRMATION does not match the required live token"
        if Config.MAX_LIVE_NOTIONAL_USD <= 0 or Config.MAX_LIVE_POSITIONS <= 0:
            return False, "Live notional and position limits must be positive"
        if (
            Config.MAX_LIVE_DAILY_LOSS_USD <= 0
            or Config.MAX_LIVE_ORDERS_PER_MINUTE <= 0
            or Config.MAX_LIVE_LEVERAGE <= 0
            or Config.MAX_LIVE_ENTRY_DRIFT_PCT <= 0
        ):
            return False, "Live loss, order-rate, and leverage limits must be positive"
        return True, "live configuration accepted"

    async def connect(self) -> Tuple[bool, str]:
        valid, reason = self.validate_configuration()
        if not valid:
            return False, reason
        if self.mode == "paper":
            return True, "paper mode: no exchange wallet connection"
        try:
            self.exchange = ccxt_async.okx({
                "apiKey": Config.OKX_API_KEY,
                "secret": Config.OKX_API_SECRET,
                "password": Config.OKX_API_PASSPHRASE,
                "enableRateLimit": True,
                "options": {"defaultType": "swap", "adjustForTimeDifference": True},
            })
            if not hasattr(self.exchange, "set_sandbox_mode"):
                raise RuntimeError("OKX adapter cannot explicitly select testnet/live mode")
            self.exchange.set_sandbox_mode(self._testnet)
            await self.exchange.load_markets()
            if not getattr(self.exchange, "has", {}).get("fetchPositionMode"):
                raise RuntimeError("OKX adapter cannot verify the account position mode")
            position_mode = await self.exchange.fetch_position_mode()
            if not isinstance(position_mode, dict) or not isinstance(
                position_mode.get("hedged"), bool
            ):
                raise RuntimeError("OKX account position mode could not be verified")
            self._hedged = position_mode["hedged"]
            return True, f"connected to OKX {'testnet' if self._testnet else 'LIVE'}"
        except Exception as exc:
            await self.close()
            return False, f"OKX connection failed: {type(exc).__name__}: {str(exc)[:180]}"

    def _rate_limit_allowed(self) -> bool:
        now = time.time()
        cutoff = now - 60
        while self._order_timestamps and self._order_timestamps[0] < cutoff:
            self._order_timestamps.popleft()
        return len(self._order_timestamps) < Config.MAX_LIVE_ORDERS_PER_MINUTE

    @staticmethod
    def _correlation_bucket(symbol: str) -> str:
        base = symbol.split("/", 1)[0].upper().split("-", 1)[0]
        if base in ("BTC", "WBTC"):
            return "btc"
        if base in ("ETH", "WETH", "STETH"):
            return "eth"
        return base

    def _check_live_portfolio_limits(
        self,
        positions: List[Dict[str, Any]],
        proposed_symbol: str,
        proposed_notional: float,
        equity: float,
        open_orders: Optional[List[Dict[str, Any]]] = None,
    ) -> Tuple[bool, str]:
        current_notional = 0.0
        correlated_positions = 0
        proposed_bucket = self._correlation_bucket(proposed_symbol)
        for position in positions:
            contracts = safe_float(
                position.get("contracts") or position.get("positionAmt")
            )
            if contracts == 0:
                continue
            symbol = position.get("symbol")
            if not symbol:
                return False, "an open live position has no symbol; exposure is unknown"
            try:
                market = self.exchange.market(symbol)
                settlement = str(market.get("settle") or market.get("quote") or "").upper()
                if settlement != Config.QUOTE.upper():
                    return False, f"cannot value non-{Config.QUOTE.upper()} live position {symbol}"
                raw_notional = position.get("notional")
                if raw_notional is None:
                    mark = safe_float(
                        position.get("markPrice")
                        or position.get("mark")
                        or position.get("entryPrice")
                    )
                    contract_size = safe_float(market.get("contractSize"), 1.0)
                    if mark <= 0 or contract_size <= 0:
                        return False, f"cannot value live position {symbol}"
                    notional = abs(contracts) * contract_size * mark
                else:
                    notional = abs(float(raw_notional))
                if not math.isfinite(notional) or notional <= 0:
                    return False, f"live position notional is invalid for {symbol}"
            except Exception as exc:
                return False, f"cannot reconcile exposure for {symbol}: {str(exc)[:100]}"
            current_notional += notional
            if self._correlation_bucket(symbol) == proposed_bucket:
                correlated_positions += 1

        for order in open_orders or []:
            info = order.get("info") or {}
            reduce_only_values = (order.get("reduceOnly"), info.get("reduceOnly"))
            if any(
                value is True
                or (isinstance(value, str) and value.strip().lower() in ("true", "1"))
                for value in reduce_only_values
            ):
                continue
            symbol = order.get("symbol")
            if not symbol:
                return False, "an open live entry order has no symbol; exposure is unknown"
            try:
                market = self.exchange.market(symbol)
                settlement = str(
                    market.get("settle") or market.get("quote") or ""
                ).upper()
                if settlement != Config.QUOTE.upper():
                    return False, f"cannot value non-{Config.QUOTE.upper()} open order {symbol}"
                amount = safe_float(order.get("remaining"), safe_float(order.get("amount")))
                price = safe_float(
                    order.get("price")
                    or order.get("triggerPrice")
                    or info.get("px")
                    or info.get("triggerPx")
                )
                contract_size = safe_float(market.get("contractSize"), 1.0)
                if amount <= 0 or price <= 0 or contract_size <= 0:
                    return False, f"cannot value outstanding live entry order for {symbol}"
                notional = amount * contract_size * price
                if not math.isfinite(notional) or notional <= 0:
                    return False, f"open order notional is invalid for {symbol}"
            except Exception as exc:
                return False, f"cannot reconcile open-order exposure for {symbol}: {str(exc)[:100]}"
            current_notional += notional
            if self._correlation_bucket(symbol) == proposed_bucket:
                correlated_positions += 1

        if current_notional + proposed_notional > equity * (
            Config.MAX_PORTFOLIO_NOTIONAL_PCT / 100.0
        ):
            return False, "aggregate live portfolio notional limit would be exceeded"
        if correlated_positions + 1 > Config.MAX_CORRELATED_POSITIONS:
            return False, "correlated live position limit would be exceeded"
        return True, "live portfolio limits accepted"

    def _trade_fee_in_quote(self, trade: Dict[str, Any], fee: Dict[str, Any]) -> float:
        cost_value = fee.get("cost")
        if cost_value is None:
            raise RuntimeError("trade fee is missing its exchange-reported cost")
        cost = float(cost_value)
        if not math.isfinite(cost):
            raise RuntimeError("trade fee is not finite")
        if cost == 0:
            return 0.0
        currency = str(fee.get("currency") or fee.get("code") or "").upper()
        if not currency:
            raise RuntimeError("trade fee is missing its currency")
        if currency == Config.QUOTE.upper():
            return cost
        symbol = trade.get("symbol")
        market = self.exchange.market(symbol)
        if currency == str(market.get("base") or "").upper():
            price = safe_float(trade.get("price"))
            if price <= 0:
                raise RuntimeError("base-currency trade fee has no valid execution price")
            return cost * price
        raise RuntimeError(
            f"cannot convert trade fee currency {currency} to {Config.QUOTE.upper()}"
        )

    async def _fetch_daily_net_pnl(self, since: int) -> Tuple[float, float, float, float]:
        if not getattr(self.exchange, "has", {}).get("fetchMyTrades"):
            raise RuntimeError("exchange does not support fetchMyTrades")
        if not getattr(self.exchange, "has", {}).get("fetchFundingHistory"):
            raise RuntimeError("exchange does not support fetchFundingHistory")

        trades = []
        seen_trades: Set[Tuple[str, str]] = set()
        cursor = since
        for page_number in range(50):
            page = await self.exchange.fetch_my_trades(None, since=cursor, limit=100)
            new_trades = []
            for trade in page:
                if not trade.get("id") or not trade.get("symbol"):
                    raise RuntimeError("trade history contains a fill without a unique identity")
                identity = (str(trade["symbol"]), str(trade["id"]))
                if identity not in seen_trades:
                    seen_trades.add(identity)
                    new_trades.append(trade)
            trades.extend(new_trades)
            if len(page) < 100:
                break
            if not new_trades:
                raise RuntimeError("trade-history pagination made no progress")
            page_timestamps = [
                int(trade["timestamp"]) for trade in page
                if trade.get("timestamp") is not None
            ]
            if not page_timestamps:
                raise RuntimeError("trade-history page contains no timestamps")
            cursor = max(page_timestamps)
            if page_number == 49:
                raise RuntimeError("daily trade history exceeded the pagination limit")

        gross_realized = 0.0
        fees = 0.0
        for trade in trades:
            timestamp = trade.get("timestamp")
            if timestamp is None:
                raise RuntimeError("trade history contains a fill without a timestamp")
            if int(timestamp) < since:
                continue
            info = trade.get("info") or {}
            pnl_value = trade.get("realizedPnl")
            if pnl_value is None:
                for field_name in ("fillPnl", "realizedPnl", "realized_pnl"):
                    if field_name in info:
                        pnl_value = info[field_name]
                        break
            if pnl_value is None:
                raise RuntimeError("trade history is missing realized PnL for a fill")
            pnl = float(pnl_value)
            if not math.isfinite(pnl):
                raise RuntimeError("trade history contains a non-finite realized PnL")
            gross_realized += pnl

            if "fees" in trade:
                fee_records = trade["fees"]
            elif "fee" in trade and trade["fee"] is not None:
                fee_records = [trade["fee"]]
            else:
                raise RuntimeError("trade history is missing exchange-reported fee data")
            if not isinstance(fee_records, list) or not fee_records:
                raise RuntimeError("trade history fee data is incomplete")
            fees += sum(
                self._trade_fee_in_quote(trade, fee) for fee in fee_records
            )

        funding_amount = 0.0
        seen_funding: Set[str] = set()
        cursor = since
        for page_number in range(50):
            page = await self.exchange.fetch_funding_history(
                None, since=cursor, limit=100
            )
            new_funding = []
            for funding in page:
                identity = str(funding.get("id") or "")
                if not identity:
                    raise RuntimeError("funding history contains a payment without an identity")
                if identity not in seen_funding:
                    seen_funding.add(identity)
                    new_funding.append(funding)
            if len(page) < 100:
                new_funding.extend(
                    funding for funding in page
                    if str(funding.get("id")) not in seen_funding
                )
            if len(page) >= 100 and not new_funding:
                raise RuntimeError("funding-history pagination made no progress")
            for funding in new_funding:
                timestamp = funding.get("timestamp")
                amount = funding.get("amount")
                currency = str(funding.get("code") or "").upper()
                if timestamp is None or amount is None or not currency:
                    raise RuntimeError("funding history is missing timestamp, amount, or currency")
                if int(timestamp) < since:
                    continue
                if currency != Config.QUOTE.upper():
                    raise RuntimeError(
                        f"cannot convert funding currency {currency} to {Config.QUOTE.upper()}"
                    )
                amount = float(amount)
                if not math.isfinite(amount):
                    raise RuntimeError("funding history contains a non-finite payment")
                funding_amount += amount
            if len(page) < 100:
                break
            timestamps = [
                int(funding["timestamp"]) for funding in page
                if funding.get("timestamp") is not None
            ]
            if not timestamps:
                raise RuntimeError("funding-history page contains no timestamps")
            cursor = max(timestamps)
            if page_number == 49:
                raise RuntimeError("daily funding history exceeded the pagination limit")

        return gross_realized - fees + funding_amount, gross_realized, fees, funding_amount

    async def sync_live_state(self) -> Dict[str, Any]:
        """Refresh exchange state; unknown state is never treated as a clean account."""
        state: Dict[str, Any] = {
            "positions": [],
            "open_orders": [],
            "equity_usd": None,
            "free_usd": None,
            "realized_pnl": None,
            "gross_realized_pnl": None,
            "trading_fees": None,
            "funding_pnl": None,
            "positions_available": False,
            "open_orders_available": False,
            "balance_available": False,
            "daily_pnl_available": False,
        }
        if not self.exchange:
            return state
        try:
            state["positions"] = await self.exchange.fetch_positions()
            state["positions_available"] = True
        except Exception as exc:
            log.warning("Live position reconciliation unavailable: %s", exc)
        try:
            state["open_orders"] = await self.exchange.fetch_open_orders()
            state["open_orders_available"] = True
        except Exception as exc:
            log.warning("Live order reconciliation unavailable: %s", exc)
        try:
            balance = await self.exchange.fetch_balance({"type": "swap"})
            currency = Config.QUOTE.upper()
            currency_balance = balance.get(currency) or {}
            total = (balance.get("total") or {}).get(currency)
            free = (balance.get("free") or {}).get(currency)

            def is_missing(value: Any) -> bool:
                return value is None or (
                    isinstance(value, str) and not value.strip()
                )

            if is_missing(total):
                total = currency_balance.get("total")
            if is_missing(free):
                free = currency_balance.get("free")
            if is_missing(total) or is_missing(free):
                account_info = balance.get("info") or {}
                account_rows = account_info.get("data") or []
                for account_row in account_rows:
                    for detail in account_row.get("details") or []:
                        if str(detail.get("ccy", "")).upper() != currency:
                            continue
                        if is_missing(total):
                            total = detail.get("eq")
                        if is_missing(free):
                            free = detail.get("availEq")
                            if is_missing(free):
                                free = detail.get("availBal")
                        break
            equity = float(total)
            available = float(free)
            if not math.isfinite(equity) or not math.isfinite(available):
                raise ValueError("account balance is not finite")
            if equity <= 0 or available < 0:
                raise ValueError("account equity or available balance is invalid")
            state["equity_usd"] = equity
            state["free_usd"] = available
            state["balance_available"] = True
        except Exception as exc:
            log.warning("Live account balance unavailable; live entries will be blocked: %s", exc)

        live_symbols: Set[str] = set()
        if state["positions_available"]:
            open_positions = []
            for position in state["positions"]:
                symbol = position.get("symbol")
                contracts = safe_float(position.get("contracts") or position.get("positionAmt"))
                if symbol and abs(contracts) > 0:
                    live_symbols.add(symbol)
                    open_positions.append(position)
            self._open_positions = len(open_positions)
        if state["open_orders_available"]:
            live_symbols.update(
                order.get("symbol")
                for order in state["open_orders"]
                if order.get("symbol")
            )
        if state["positions_available"] and state["open_orders_available"]:
            self._tracked_symbols = live_symbols
        if self.database:
            self._protection_targets = self.database.get_live_protection_intents()

        try:
            utc_midnight = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            since = int(utc_midnight.timestamp() * 1000)
            net_pnl, gross_pnl, fees, funding_pnl = await self._fetch_daily_net_pnl(since)
            state["realized_pnl"] = net_pnl
            state["gross_realized_pnl"] = gross_pnl
            state["trading_fees"] = fees
            state["funding_pnl"] = funding_pnl
            state["daily_pnl_available"] = True
            self._daily_realized_pnl = net_pnl
            self._daily_pnl_available = True
        except Exception as exc:
            self._daily_realized_pnl = None
            self._daily_pnl_available = False
            log.warning("Daily realized PnL unavailable; live entries will be blocked: %s", exc)
        state["daily_pnl_available"] = self._daily_pnl_available
        return state

    async def monitor_positions(self) -> Dict[str, Any]:
        state = await self.sync_live_state()
        if not self.exchange:
            return state
        if not state["positions_available"] or not state["open_orders_available"]:
            state["reconciliation_error"] = "positions or open orders could not be fetched"
            return state

        open_orders = state["open_orders"]
        positions_by_symbol: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for position in state["positions"]:
            symbol = position.get("symbol")
            contracts = abs(safe_float(position.get("contracts") or position.get("positionAmt")))
            if symbol and contracts > 0:
                positions_by_symbol[symbol].append(position)
            targets = self._protection_targets.get(symbol)
            if not symbol or contracts <= 0:
                continue
            if not targets:
                log.critical(
                    "Live position %s has no persisted protection intent; manual intervention required",
                    symbol,
                )
                state.setdefault("unprotected_symbols", []).append(symbol)
                continue
            completed = set(targets.get("completed_targets") or [])
            initial_amount = safe_float(targets.get("amount"))
            if initial_amount > 0 and contracts < initial_amount - 1e-8:
                side = "sell" if targets["direction"] == TradeDirection.LONG.value else "buy"
                open_trigger_prices = {
                    safe_float(
                        order.get("triggerPrice") or (order.get("info") or {}).get("triggerPx")
                    )
                    for order in open_orders
                    if order.get("symbol") == symbol and order.get("side") == side
                }
                for name, price, _ in self._protection_legs(
                    targets, initial_amount, initial_amount
                )[1:]:
                    if not any(
                        abs(existing_price - price) <= max(abs(price) * 1e-8, 1e-8)
                        for existing_price in open_trigger_prices
                    ):
                        completed.add(name)
                targets["completed_targets"] = sorted(completed)
                if self.database:
                    self.database.save_live_protection_intent(
                        symbol, targets["direction"], initial_amount, targets["stop"],
                        targets.get("tp1"), targets.get("tp2"), targets.get("tp3"),
                        targets.get("order_id"), targets["completed_targets"],
                    )
            try:
                await self._ensure_protection_orders(
                    symbol, targets, contracts, open_orders
                )
            except Exception as exc:
                position_side = str(position.get("side") or "").lower()
                if position_side in ("long", "buy"):
                    close_side = "sell"
                elif position_side in ("short", "sell"):
                    close_side = "buy"
                else:
                    signed_contracts = safe_float(
                        position.get("contracts") or position.get("positionAmt")
                    )
                    close_side = "sell" if signed_contracts > 0 else "buy"
                try:
                    pos_side = (
                        "long" if close_side == "sell" else "short"
                    ) if self._hedged else "net"
                    close_order = await self.exchange.create_order(
                        symbol,
                        "market",
                        close_side,
                        contracts,
                        None,
                        {
                            "tdMode": "isolated",
                            "reduceOnly": True,
                            "posSide": pos_side,
                        },
                    )
                    if not close_order:
                        raise RuntimeError("exchange returned no emergency-close acknowledgement")
                    try:
                        await self.exchange.cancel_all_orders(symbol)
                    except Exception as cancel_error:
                        log.warning("Orphan protection cleanup failed for %s: %s", symbol, cancel_error)
                    log.critical(
                        "Protection reconciliation failed for %s; emergency reduce-only close submitted",
                        symbol,
                    )
                    state.setdefault("emergency_close_submitted", []).append(symbol)
                except Exception as close_error:
                    log.critical(
                        "UNPROTECTED LIVE POSITION %s: protection reconciliation failed (%s), "
                        "emergency close failed (%s)",
                        symbol, exc, close_error,
                    )
                    state.setdefault("protection_errors", {})[symbol] = str(exc)

        for symbol, targets in list(self._protection_targets.items()):
            if positions_by_symbol.get(symbol):
                continue
            symbol_orders = [order for order in open_orders if order.get("symbol") == symbol]
            entry_order_id = targets.get("order_id")
            if entry_order_id and any(
                str(order.get("id")) == str(entry_order_id) for order in symbol_orders
            ):
                continue
            if not entry_order_id or not hasattr(self.exchange, "fetch_order"):
                continue
            try:
                entry_order = await self.exchange.fetch_order(entry_order_id, symbol)
            except Exception as exc:
                log.warning("Could not reconcile live entry %s for %s: %s", entry_order_id, symbol, exc)
                continue
            terminal_statuses = {"closed", "filled", "canceled", "cancelled", "rejected", "expired"}
            if not entry_order or entry_order.get("status") not in terminal_statuses:
                continue

            expected_triggers = {
                price for _, price, _ in self._protection_legs(
                    targets, safe_float(targets.get("amount"))
                )
            }
            unknown_orders = False
            for order in symbol_orders:
                trigger = safe_float(
                    order.get("triggerPrice") or (order.get("info") or {}).get("triggerPx")
                )
                if trigger <= 0 or not any(
                    abs(trigger - expected) <= max(abs(expected) * 1e-8, 1e-8)
                    for expected in expected_triggers
                ):
                    unknown_orders = True
                    continue
                try:
                    await self.exchange.cancel_order(order["id"], symbol)
                except Exception as exc:
                    unknown_orders = True
                    log.warning("Could not cancel completed protection order %s: %s", order.get("id"), exc)
            if unknown_orders:
                continue
            if self.database:
                self.database.delete_live_protection_intent(symbol)
            self._protection_targets.pop(symbol, None)
        return state

    @staticmethod
    def _protection_legs(
        targets: Dict[str, Any], amount: float, target_amount: Optional[float] = None
    ) -> List[Tuple[str, float, float]]:
        legs = [("stop", float(targets["stop"]), amount)]
        target_amount = amount if target_amount is None else target_amount
        target_prices = [
            (f"tp{index}", safe_float(targets.get(f"tp{index}")))
            for index in (1, 2, 3)
            if safe_float(targets.get(f"tp{index}")) > 0
        ]
        if target_prices:
            weights = {
                1: [1.0],
                2: [0.5, 0.5],
                3: [0.5, 0.25, 0.25],
            }[len(target_prices)]
            for index, (name, price) in enumerate(target_prices):
                leg_amount = target_amount * weights[index] if index < len(target_prices) - 1 else target_amount * (
                    1 - sum(weights[:index])
                )
                legs.append((name, price, leg_amount))
        return legs

    async def _ensure_protection_orders(
        self,
        symbol: str,
        targets: Dict[str, Any],
        position_amount: float,
        open_orders: List[Dict[str, Any]],
    ) -> None:
        direction = targets.get("direction")
        if direction not in (TradeDirection.LONG.value, TradeDirection.SHORT.value):
            raise RuntimeError("protection intent has an invalid direction")
        side = "sell" if direction == TradeDirection.LONG.value else "buy"
        if self._hedged is None:
            raise RuntimeError("OKX account position mode is not verified")
        pos_side = ("long" if side == "sell" else "short") if self._hedged else "net"
        amount = position_amount
        if amount <= 0:
            raise RuntimeError("protection amount is invalid")
        amount_precision = float(self.exchange.amount_to_precision(symbol, amount))
        if amount_precision <= 0:
            raise RuntimeError("exchange precision produced zero protection size")
        if amount_precision + 1e-10 < position_amount:
            raise RuntimeError("exchange precision would leave part of the position unprotected")
        params = {"tdMode": "isolated", "reduceOnly": True, "posSide": pos_side}
        existing: Dict[float, float] = defaultdict(float)
        for order in open_orders:
            if order.get("symbol") != symbol or order.get("side") != side:
                continue
            trigger = safe_float(
                order.get("triggerPrice") or (order.get("info") or {}).get("triggerPx")
            )
            if trigger > 0:
                existing[trigger] += abs(safe_float(order.get("amount") or order.get("remaining")))
        completed = set(targets.get("completed_targets") or [])
        target_amount = safe_float(targets.get("amount"), amount_precision)
        for leg_name, trigger_price, requested_amount in self._protection_legs(
            targets, amount_precision, target_amount
        ):
            if leg_name in completed:
                continue
            already_covered = sum(
                covered for existing_price, covered in existing.items()
                if abs(existing_price - trigger_price) <= max(abs(trigger_price) * 1e-8, 1e-8)
            )
            missing = max(0.0, requested_amount - already_covered)
            missing = float(self.exchange.amount_to_precision(symbol, missing)) if missing > 0 else 0
            if missing <= 0:
                continue
            order = await self.exchange.create_order(
                symbol,
                "market",
                side,
                missing,
                None,
                {**params, "triggerPrice": trigger_price},
            )
            if not order:
                raise RuntimeError(f"exchange returned no acknowledgement for trigger {trigger_price}")
            self._order_timestamps.append(time.time())
            existing[trigger_price] += missing

    async def place_protected_market_order(
        self,
        opportunity: "TradeOpportunity",
    ) -> Tuple[bool, str]:
        if self.mode == "paper":
            return False, "paper mode: real orders disabled"
        if not self.exchange:
            return False, "live adapter is not connected"
        if opportunity.exchange.lower() != "okx":
            return False, "live adapter only executes signals sourced from OKX"
        if opportunity.direction not in (TradeDirection.LONG, TradeDirection.SHORT):
            return False, "invalid direction"
        if not opportunity.position_size:
            return False, "missing position size"
        if not self.database:
            return False, "live execution requires durable protection-intent storage"
        if self._hedged is None:
            return False, "OKX account position mode is not verified"
        state = await self.sync_live_state()
        if not state["positions_available"] or not state["open_orders_available"]:
            return False, "live account state could not be reconciled"
        if not state["daily_pnl_available"] or self._daily_realized_pnl is None:
            return False, "daily realized PnL is unavailable; live entry blocked"
        if not state["balance_available"]:
            return False, "live USDT equity and free balance could not be reconciled"
        if self._open_positions >= Config.MAX_LIVE_POSITIONS:
            return False, "maximum live positions reached"
        if self._daily_realized_pnl <= -Config.MAX_LIVE_DAILY_LOSS_USD:
            return False, "daily live loss limit reached"
        if not self._rate_limit_allowed():
            return False, "live order rate limit reached"
        if opportunity.status != "READY":
            return False, "live execution requires a READY opportunity"
        if not math.isfinite(float(opportunity.score)) or opportunity.score < 55:
            return False, "live opportunity score below required threshold"
        requested_size = float(opportunity.position_size.position_size_coins)
        requested_risk = float(opportunity.position_size.risk_usd)
        if (
            not math.isfinite(requested_size)
            or not math.isfinite(requested_risk)
            or requested_size <= 0
            or requested_risk <= 0
        ):
            return False, "live opportunity has invalid size or risk"
        if opportunity.symbol in self._tracked_symbols:
            return False, "duplicate live entry for this symbol"
        if opportunity.entry_method in (EntryMethod.LIMIT_RETEST, EntryMethod.LIQUIDITY_SWEEP):
            return False, "live limit entries are disabled until exchange-native attached protection is supported"

        side = "buy" if opportunity.direction == TradeDirection.LONG else "sell"
        try:
            raw_ticker = await self.exchange.fetch_ticker(opportunity.symbol)
            ticker = DataValidator.validate_ticker(
                raw_ticker, "okx", opportunity.symbol
            )
            if not ticker or ticker.freshness() != DataFreshness.FRESH:
                return False, "fresh OKX ticker unavailable; live entry blocked"
            live_entry = ticker.last
            signal_entry = float(opportunity.preferred_entry)
            stop_price = float(opportunity.stop_loss.price)
            target_price = opportunity.take_profit.tp1
            if (
                not math.isfinite(signal_entry)
                or not math.isfinite(stop_price)
                or signal_entry <= 0
                or stop_price <= 0
            ):
                return False, "signal entry price is invalid"
            if target_price is not None and (
                not math.isfinite(float(target_price)) or float(target_price) <= 0
            ):
                return False, "signal target price is invalid"
            profit_targets = [
                float(price)
                for price in (
                    opportunity.take_profit.tp1,
                    opportunity.take_profit.tp2,
                    opportunity.take_profit.tp3,
                )
                if price is not None
            ]
            if not all(math.isfinite(price) for price in profit_targets):
                return False, "signal contains a non-finite target"
            if opportunity.direction == TradeDirection.LONG:
                if any(price <= live_entry for price in profit_targets) or any(
                    later <= earlier
                    for earlier, later in zip(profit_targets, profit_targets[1:])
                ):
                    return False, "long targets are invalid at the current market price"
            elif any(price >= live_entry for price in profit_targets) or any(
                later >= earlier
                for earlier, later in zip(profit_targets, profit_targets[1:])
            ):
                return False, "short targets are invalid at the current market price"
            entry_drift_pct = abs(live_entry - signal_entry) / signal_entry * 100
            if entry_drift_pct > Config.MAX_LIVE_ENTRY_DRIFT_PCT:
                return False, (
                    f"market moved {entry_drift_pct:.3f}% from signal entry; "
                    "live entry drift limit exceeded"
                )
            if opportunity.direction == TradeDirection.LONG:
                if live_entry <= stop_price:
                    return False, "market is already at or below the long stop"
                if target_price and live_entry >= target_price:
                    return False, "market has already reached the long first target"
            else:
                if live_entry >= stop_price:
                    return False, "market is already at or above the short stop"
                if target_price and live_entry <= target_price:
                    return False, "market has already reached the short first target"

            market = self.exchange.market(opportunity.symbol)
            contract_size = float(market.get("contractSize") or 1.0)
            if not math.isfinite(contract_size) or contract_size <= 0:
                return False, "OKX contract size is invalid"
            equity = float(state["equity_usd"])
            account_scale = min(1.0, equity / Config.ACCOUNT_BALANCE)
            size_coins = requested_size * account_scale
            risk_budget = requested_risk * account_scale
            amount = size_coins / contract_size
            amount = float(self.exchange.amount_to_precision(opportunity.symbol, amount))
            if not math.isfinite(amount) or amount <= 0:
                return False, "exchange precision produced zero size"
            size_coins = amount * contract_size
            quantized_notional = size_coins * live_entry
            quantized_risk = size_coins * abs(live_entry - stop_price)
            if (
                not math.isfinite(quantized_notional)
                or not math.isfinite(quantized_risk)
                or quantized_notional <= 0
                or quantized_notional > Config.MAX_LIVE_NOTIONAL_USD
            ):
                return False, "contract rounding exceeds live notional limit"
            portfolio_allowed, portfolio_reason = self._check_live_portfolio_limits(
                state["positions"], opportunity.symbol, quantized_notional, equity,
                state["open_orders"],
            )
            if not portfolio_allowed:
                return False, portfolio_reason
            if quantized_risk > risk_budget + 1e-8:
                return False, "contract rounding exceeds the opportunity risk budget"
            amount_limits = market.get("limits", {}).get("amount", {})
            min_amount = safe_float(amount_limits.get("min"))
            if min_amount > 0 and amount < min_amount:
                return False, "exchange minimum amount exceeds the risk-sized quantity"
            leverage = min(Config.MAX_LIVE_LEVERAGE, 3)
            required_margin = quantized_notional / leverage * 1.1
            if not math.isfinite(required_margin):
                return False, "required margin calculation is invalid"
            if required_margin > float(state["free_usd"]):
                return False, "available USDT margin is below the required safety buffer"
            params = {
                "tdMode": "isolated",
                "posSide": (
                    "long" if side == "buy" else "short"
                ) if self._hedged else "net",
            }
            if not hasattr(self.exchange, "set_leverage"):
                return False, "OKX adapter cannot enforce the configured leverage"
            await self.exchange.set_leverage(
                leverage,
                opportunity.symbol,
                params={
                    "mgnMode": "isolated",
                    "posSide": params["posSide"],
                },
            )
            targets = {
                "direction": opportunity.direction.value,
                "amount": amount,
                "stop": float(opportunity.stop_loss.price),
                "tp1": opportunity.take_profit.tp1,
                "tp2": opportunity.take_profit.tp2,
                "tp3": opportunity.take_profit.tp3,
            }
            self.database.save_live_protection_intent(
                opportunity.symbol, targets["direction"], amount,
                targets["stop"], targets["tp1"], targets["tp2"], targets["tp3"],
            )
            self._protection_targets[opportunity.symbol] = targets
            try:
                order = await self.exchange.create_order(
                    opportunity.symbol, "market", side, amount, None, params
                )
            except Exception as entry_error:
                self._tracked_symbols.add(opportunity.symbol)
                log.critical(
                    "Live entry outcome for %s is uncertain after exchange error: %s",
                    opportunity.symbol, entry_error,
                )
                try:
                    reconciliation = await self.sync_live_state()
                    if (
                        reconciliation["positions_available"]
                        and reconciliation["open_orders_available"]
                    ):
                        position = next(
                            (
                                candidate for candidate in reconciliation["positions"]
                                if candidate.get("symbol") == opportunity.symbol
                                and abs(safe_float(
                                    candidate.get("contracts") or candidate.get("positionAmt")
                                )) > 0
                            ),
                            None,
                        )
                        if position:
                            contracts = abs(safe_float(
                                position.get("contracts") or position.get("positionAmt")
                            ))
                            await self._ensure_protection_orders(
                                opportunity.symbol,
                                targets,
                                contracts,
                                reconciliation["open_orders"],
                            )
                            return True, (
                                "entry response was uncertain; exchange position found and protection reconciled"
                            )
                except Exception as reconciliation_error:
                    log.critical(
                        "Could not reconcile uncertain live entry %s: %s",
                        opportunity.symbol, reconciliation_error,
                    )
                close_params = {
                    "tdMode": "isolated",
                    "reduceOnly": True,
                    "posSide": (
                        "long" if side == "buy" else "short"
                    ) if self._hedged else "net",
                }
                try:
                    close_order = await self.exchange.create_order(
                        opportunity.symbol,
                        "market",
                        "sell" if side == "buy" else "buy",
                        amount,
                        None,
                        close_params,
                    )
                    if not close_order:
                        raise RuntimeError("exchange returned no emergency-close acknowledgement")
                    try:
                        await self.exchange.cancel_all_orders(opportunity.symbol)
                    except Exception as cancel_error:
                        log.warning(
                            "Orphan protection cleanup failed for %s: %s",
                            opportunity.symbol, cancel_error,
                        )
                    return True, (
                        "entry response was uncertain; reduce-only emergency close submitted"
                    )
                except Exception as close_error:
                    log.critical(
                        "Live entry outcome for %s remains uncertain; emergency close failed: %s",
                        opportunity.symbol, close_error,
                    )
                return True, (
                    f"entry outcome is uncertain; durable protection intent retained for reconciliation: "
                    f"{type(entry_error).__name__}: {str(entry_error)[:120]}"
                )
            order_id = str(order.get("id") or "") if order else ""
            self._order_timestamps.append(time.time())
            try:
                self.database.save_live_protection_intent(
                    opportunity.symbol, targets["direction"], amount,
                    targets["stop"], targets["tp1"], targets["tp2"], targets["tp3"],
                    order_id or None,
                )
                await self._ensure_protection_orders(
                    opportunity.symbol, targets, amount, []
                )
            except Exception as protection_error:
                close_params = {
                    "tdMode": "isolated",
                    "reduceOnly": True,
                    "posSide": "long" if side == "buy" else "short",
                }
                try:
                    close_order = await self.exchange.create_order(
                        opportunity.symbol,
                        "market",
                        "sell" if side == "buy" else "buy",
                        amount,
                        None,
                        close_params,
                    )
                    if not close_order:
                        raise RuntimeError("exchange returned no acknowledgement for emergency close")
                    try:
                        await self.exchange.cancel_all_orders(opportunity.symbol)
                    except Exception as cancel_error:
                        log.warning("Orphan protection cleanup failed for %s: %s", opportunity.symbol, cancel_error)
                    log.error(
                        "Live protection failed for %s; emergency reduce-only close submitted",
                        opportunity.symbol,
                    )
                    return True, (
                        f"entry was accepted but protection failed; emergency close submitted: "
                        f"{type(protection_error).__name__}: {str(protection_error)[:120]}"
                    )
                except Exception as close_error:
                    log.critical(
                        "UNPROTECTED LIVE POSITION %s: protection failed (%s), emergency close failed (%s)",
                        opportunity.symbol, protection_error, close_error,
                    )
                    return True, (
                        f"ENTRY ACCEPTED; POSITION MAY BE UNPROTECTED. Protection failed: "
                        f"{str(protection_error)[:100]}; emergency close failed: {str(close_error)[:100]}"
                    )
            self._open_positions += 1
            self._tracked_symbols.add(opportunity.symbol)
            return True, str(order.get("id") or "live order accepted with stop and TP protection")
        except Exception as exc:
            return False, f"live order not accepted: {type(exc).__name__}: {str(exc)[:180]}"

    async def close(self) -> None:
        if self.exchange:
            try:
                await self.exchange.close()
            except Exception as exc:
                log.warning("OKX live adapter close failed: %s", exc)
        self.exchange = None

class TelegramNotifier:
    """Lightweight Telegram sender. Completely optional – only active when
    TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are set in the environment."""

    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        self.enabled = bool(self.token and self.chat_id)
        self._last_messages: Dict[str, float] = {}
        self.min_interval = 90  # seconds between identical alerts

    async def send(self, text: str, key: str = "general") -> bool:
        if not self.enabled:
            return False
        now = time.time()
        if key in self._last_messages and now - self._last_messages[key] < self.min_interval:
            return False
        try:
            import aiohttp
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            payload = {
                "chat_id": self.chat_id,
                "text": text[:4000],
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        self._last_messages[key] = now
                        return True
                    log.warning("Telegram HTTP %s", resp.status)
        except Exception as e:
            log.debug("Telegram send failed: %s", e)
        return False

    async def alert_opportunity(self, opp: "TradeOpportunity") -> None:
        if opp.direction in (TradeDirection.NO_TRADE, TradeDirection.WAIT):
            return
        msg = (
            f"<b>APEX Opportunity</b>\n"
            f"{opp.symbol}  {opp.direction.value}\n"
            f"Score: {opp.score:.0f}/100\n"
            f"Entry: {opp.preferred_entry:.4f}\n"
            f"SL: {opp.stop_loss.price:.4f}\n"
            f"TP1: {opp.take_profit.tp1 or 0:.4f}\n"
            f"Net R/R: 1:{opp.risk_reward.net_rr_tp1:.2f}\n"
            f"Regime: {opp.regime}"
        )
        await self.send(msg, key=f"opp:{opp.symbol}:{opp.direction.value}")

    async def alert_paper_close(self, pos: "PaperPosition") -> None:
        emoji = "✅" if pos.realized_pnl >= 0 else "❌"
        msg = (
            f"{emoji} <b>Paper Closed</b>\n"
            f"{pos.symbol} {pos.direction.value}\n"
            f"PnL: ${pos.realized_pnl:.2f}  (R={pos.r_multiple:.2f})\n"
            f"Reason: {pos.exit_reason}"
        )
        await self.send(msg, key=f"close:{pos.id}")

    async def alert_system(self, text: str) -> None:
        await self.send(f"<b>APEX System</b>\n{text}", key="system")


# ===========================================================================
# SECTION 27 – STREAMLIT DASHBOARD
# ===========================================================================

def run_dashboard() -> None:
    """
    Launch the dashboard with:
        streamlit run apex.py -- --dashboard
    """
    try:
        import streamlit as st
    except ImportError:
        print("streamlit not installed. Run: pip install streamlit plotly")
        return

    st.set_page_config(
        page_title="APEX Intelligence",
        layout="wide",
        page_icon="📊",
        initial_sidebar_state="expanded",
    )

    st.title("APEX – Crypto Futures Intelligence")
    paper_mode = (
        Config.EXECUTION_MODE != "live" or Config.PAPER_SHADOW_IN_LIVE
    )
    paper_status = (
        "enabled" if Config.EXECUTION_MODE != "live"
        else "enabled" if paper_mode else "disabled"
    )
    st.caption(
        f"Stages 1–5 | Public data only | Execution: {Config.EXECUTION_MODE.upper()} | "
        f"Paper simulator: {paper_status} | "
        "Opportunity score is a ranking heuristic, not a win probability"
    )

    # Sidebar
    st.sidebar.header("Configuration")
    st.sidebar.write(f"**Primary exchange:** `{Config.PRIMARY_EXCHANGE.upper()}`")
    st.sidebar.write(f"**Max pairs:** {Config.MAX_PAIRS}")
    st.sidebar.write(f"**Analysis TF:** {Config.ANALYSIS_TF}")
    st.sidebar.write(f"**Account (paper):** ${Config.ACCOUNT_BALANCE:,.0f}")
    st.sidebar.write(f"**Risk / trade:** {Config.RISK_PER_TRADE_PCT}%")
    st.sidebar.write(f"**Min net R/R:** {Config.MIN_NET_RR}")
    st.sidebar.write(
        f"**Paper shadow in live mode:** {'Enabled' if Config.PAPER_SHADOW_IN_LIVE else 'Disabled'}"
    )
    st.sidebar.markdown("---")
    st.sidebar.info(
        "The live engine must be running in another terminal:\n\n"
        "`python apex.py`\n\n"
        "This dashboard reads status and results from the shared SQLite database "
        "and log files."
    )

    tab_overview, tab_opps, tab_perf, tab_system = st.tabs(
        ["Overview", "Opportunities", "Performance", "System Health"]
    )

    with tab_overview:
        st.subheader("Market Overview")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Monitored Pairs", Config.MAX_PAIRS)
        c2.metric("Primary Exchange", Config.PRIMARY_EXCHANGE.upper())
        c3.metric("Default TF", Config.DEFAULT_TF)
        c4.metric("Execution Mode", Config.EXECUTION_MODE.upper())

        st.markdown("### Engine Status")
        db_exists = Config.DB_PATH.exists()
        st.write(f"SQLite database present: **{'Yes' if db_exists else 'No'}**")
        if db_exists:
            st.write(f"Path: `{Config.DB_PATH}`")
            try:
                import sqlite3 as _sq
                conn = _sq.connect(str(Config.DB_PATH))
                n_candles = conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
                n_analysis = conn.execute("SELECT COUNT(*) FROM analysis_snapshots").fetchone()[0]
                n_opportunities = conn.execute("SELECT COUNT(*) FROM opportunity_snapshots").fetchone()[0]
                n_positions = conn.execute("SELECT COUNT(*) FROM paper_positions").fetchone()[0]
                latest_equity = conn.execute(
                    "SELECT balance FROM paper_equity ORDER BY ts DESC LIMIT 1"
                ).fetchone()
                conn.close()
                st.write(f"Candles stored: **{n_candles:,}**")
                st.write(f"Analysis snapshots: **{n_analysis:,}**")
                st.write(f"Opportunity snapshots: **{n_opportunities:,}**")
                st.write(f"Paper position records: **{n_positions:,}**")
                if latest_equity:
                    st.metric("Persisted Paper Balance", f"${float(latest_equity[0]):,.2f}")
            except Exception as e:
                st.warning(f"Could not read DB stats: {e}")

    with tab_opps:
        st.subheader("Opportunity Scanner")
        if db_exists:
            conn = sqlite3.connect(str(Config.DB_PATH))
            opp_df = pd.read_sql_query(
                """SELECT datetime(ts/1000, 'unixepoch') AS time,
                          exchange, symbol, direction, status, score,
                          preferred_entry, stop_loss, tp1, net_rr
                   FROM opportunity_snapshots
                   ORDER BY ts DESC LIMIT 100""",
                conn,
            )
            pos_df = pd.read_sql_query(
                """SELECT id, exchange, symbol, direction, status,
                          entry_price, size, tp1, tp2, tp3, stop_loss,
                          opened_at, closed_at, exit_price, exit_reason,
                          realized_pnl, r_multiple, opportunity_score, payload
                   FROM paper_positions
                   ORDER BY opened_at DESC""",
                conn,
            )
            conn.close()
            if not pos_df.empty:
                pos_df["remaining_size"] = pos_df.apply(
                    lambda row: json.loads(row["payload"] or "{}").get(
                        "remaining_size", row["size"]
                    ),
                    axis=1,
                )
                pos_df = pos_df.drop(columns=["payload"])
            if not opp_df.empty:
                st.dataframe(opp_df, use_container_width=True)
            else:
                st.info("No opportunity snapshots yet.")
            st.subheader("Paper Positions")
            if not pos_df.empty:
                st.dataframe(pos_df, use_container_width=True)
            else:
                st.info("No paper positions yet.")
        else:
            st.info("Start the engine to create the database.")

    with tab_perf:
        st.subheader("Paper Trading Performance")
        if db_exists:
            conn = sqlite3.connect(str(Config.DB_PATH))
            equity_df = pd.read_sql_query(
                "SELECT ts, balance FROM paper_equity ORDER BY ts",
                conn,
            )
            closed_df = pd.read_sql_query(
                """SELECT realized_pnl, r_multiple, fees_paid
                   FROM paper_positions WHERE status='closed'""",
                conn,
            )
            conn.close()
            if not closed_df.empty:
                pnls = closed_df["realized_pnl"].astype(float)
                rs = closed_df["r_multiple"].astype(float)
                wins = pnls[pnls > 0]
                losses = pnls[pnls <= 0].abs()
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Closed Trades", len(closed_df))
                c2.metric("Win Rate", f"{len(wins)/len(closed_df)*100:.1f}%")
                c3.metric("Profit Factor", f"{wins.sum()/losses.sum() if losses.sum() else 0:.2f}")
                c4.metric("Average R", f"{rs.mean():.2f}")
            if not equity_df.empty:
                equity_df["time"] = pd.to_datetime(equity_df["ts"], unit="s", utc=True)
                st.line_chart(equity_df.set_index("time")["balance"])
            else:
                st.info("No persisted equity samples yet.")
        else:
            st.info("Start the engine to create the database.")

    with tab_system:
        st.subheader("System Health & Monitoring")
        st.markdown(
            """
            The main engine reports every 30 s:
            - Exchange connectivity
            - Ticker / funding / OI freshness
            - Number of analyses completed
            - Open paper positions
            - Error counts
            """
        )
        log_file = Config.LOG_DIR / "apex.log"
        if log_file.exists():
            st.write(f"Log file: `{log_file}`")
            try:
                with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
                    lines = f.readlines()[-40:]
                st.code("".join(lines), language="log")
            except Exception:
                st.write("Could not read log tail.")
        else:
            st.write("No log file yet – start the engine first.")

    st.sidebar.success("Dashboard ready")


# ===========================================================================
# SECTION 28 – WIRE TELEGRAM INTO PAPER / OPPORTUNITY LOOP
# ===========================================================================

_original_stage4_loop = MarketDataManager._opportunity_loop

async def _stage5_opportunity_loop(self: MarketDataManager) -> None:
    """Stage-5 extended loop: paper trading + Telegram alerts."""
    if not hasattr(self, "paper_tracker"):
        self.paper_tracker = PerformanceTracker(Config.ACCOUNT_BALANCE)
        self.paper_tracker.restore_from_database(self.db)
        self.paper_sim = PaperTradingSimulator(self.paper_tracker, self.db)
        self.live_executor = LiveExecutionAdapter(self.db)
        if Config.EXECUTION_MODE == "live":
            connected, reason = await self.live_executor.connect()
            if connected:
                log.warning("LIVE EXECUTION ADAPTER CONNECTED: %s", reason)
            else:
                log.critical("LIVE EXECUTION DISABLED: %s", reason)
                self.live_startup_error = reason
                self.live_startup_complete.set()
                return
            self.live_startup_complete.set()
        self.telegram = TelegramNotifier()
        if self.telegram.enabled:
            log.info("Telegram notifications ENABLED")
            await self.telegram.alert_system("APEX Stage 5 engine started")
        else:
            log.info("Telegram notifications disabled (set TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID)")

    while self._running:
        try:
            paper_enabled = (
                Config.EXECUTION_MODE != "live" or Config.PAPER_SHADOW_IN_LIVE
            )
            if Config.EXECUTION_MODE == "live" and self.live_executor.exchange:
                await self.live_executor.monitor_positions()
            ranked = self.run_opportunity_scan()
            if ranked:
                log.info("Top opportunities (%d):", len(ranked))
                for o in ranked[:5]:
                    self.db.save_opportunity(o)
                    log.info(
                        "  #%d %s %s %s | entry=%.4f stop=%.4f TP1=%.4f R/R=1:%.2f score=%.1f",
                        o.rank, o.symbol, o.direction.value, o.status,
                        o.preferred_entry, o.stop_loss.price,
                        o.take_profit.tp1 or 0, o.risk_reward.net_rr_tp1, o.score,
                    )
                    if o.rank == 1:
                        log.info("\n" + o.to_report())

                # Paper + Telegram
                for o in ranked[:3]:
                    if paper_enabled:
                        pos = self.paper_sim.consider_opportunity(
                            o, funding=self.fundings.get(f"{o.exchange}:{o.symbol}")
                        )
                        if pos:
                            self.db.save_paper_position(pos)
                            self.db.save_paper_equity(self.paper_tracker.balance)
                        if pos and self.telegram.enabled:
                            await self.telegram.alert_opportunity(o)
                    if Config.EXECUTION_MODE == "live" and self.live_executor.exchange:
                        accepted, reason = await self.live_executor.place_protected_market_order(o)
                        if accepted:
                            log.warning("LIVE order accepted: %s", reason)
                            self.db.log_event("WARNING", "live_execution", f"{o.symbol}: {reason}")
                        else:
                            log.info("LIVE order not placed: %s", reason)

            tracked_symbols = set()
            if paper_enabled:
                tracked_symbols = {
                    (position.exchange, position.symbol)
                    for position in self.paper_tracker.open_positions.values()
                }
                tracked_symbols.update(
                    (opportunity.exchange, opportunity.symbol)
                    for opportunity in self.paper_sim.pending_opportunities.values()
                )
            for eid, sym in tracked_symbols:
                connector = self.connectors.get(eid)
                if not connector:
                    continue
                key = f"{eid}:{sym}"
                timeframe = "1m"
                interval_ms = timeframe_milliseconds(timeframe)
                checkpoint = self.db.get_paper_candle_checkpoint(
                    eid, sym, timeframe
                )
                if checkpoint is None:
                    checkpoint = self.paper_sim._last_processed_bar.get(key)
                recovery_start = None
                if checkpoint is None:
                    active_since = [
                        position.opened_at
                        for position in self.paper_tracker.open_positions.values()
                        if position.exchange == eid and position.symbol == sym
                    ]
                    if key in self.paper_sim._pending_since:
                        active_since.append(self.paper_sim._pending_since[key])
                    if active_since:
                        recovery_start = (
                            math.floor(min(active_since) * 1000 / interval_ms) + 1
                        ) * interval_ms
                since = (
                    checkpoint + interval_ms
                    if checkpoint is not None
                    else recovery_start
                )
                expected_timestamp = since
                while True:
                    fetched = await connector.fetch_ohlcv(
                        sym, timeframe, limit=1000, since=since
                    )
                    bars = closed_candles(fetched, timeframe)
                    if checkpoint is not None:
                        bars = [bar for bar in bars if bar.timestamp > checkpoint]
                    elif recovery_start is not None:
                        bars = [bar for bar in bars if bar.timestamp >= recovery_start]
                    if not bars:
                        break
                    history_gap = False
                    for bar in bars:
                        if expected_timestamp is not None and bar.timestamp > expected_timestamp:
                            log.error(
                                "Paper candle history gap for %s %s: expected %d, received %d; "
                                "checkpoint remains at %s",
                                eid, sym, expected_timestamp, bar.timestamp, checkpoint,
                            )
                            history_gap = True
                            break
                        if expected_timestamp is not None and bar.timestamp < expected_timestamp:
                            continue
                        prior_balance = self.paper_tracker.balance
                        closed = self.paper_sim.update_with_price(
                            eid, sym, bar.close, bar.high, bar.low, bar.timestamp,
                            funding=self.fundings.get(key),
                        )
                        for position in self.paper_tracker.open_positions.values():
                            if position.exchange == eid and position.symbol == sym:
                                self.db.save_paper_position(position)
                        for closed_position in closed:
                            self.db.save_paper_position(closed_position)
                            if self.telegram.enabled:
                                await self.telegram.alert_paper_close(closed_position)
                        if self.paper_tracker.balance != prior_balance:
                            self.db.save_paper_equity(self.paper_tracker.balance)
                        self.db.save_paper_candle_checkpoint(
                            eid, sym, timeframe, bar.timestamp
                        )
                        self.paper_sim._last_processed_bar[key] = bar.timestamp
                        checkpoint = bar.timestamp
                        expected_timestamp = bar.timestamp + interval_ms
                    if history_gap:
                        break
                    if len(fetched) < 1000:
                        break
                    since = checkpoint + interval_ms

            if paper_enabled and (
                self.paper_tracker.closed_trades or self.paper_tracker.open_positions
            ):
                log.info(self.paper_tracker.summary_text())
                log.info("Open paper positions: %d", len(self.paper_tracker.open_positions))

        except Exception as e:
            log.exception("Stage-5 loop error: %s", e)
        await asyncio.sleep(Config.OPPORTUNITY_SCAN_INTERVAL)


MarketDataManager._opportunity_loop = _stage5_opportunity_loop  # type: ignore


# ===========================================================================
# SECTION 29 – MAIN (Stages 1-5)
# ===========================================================================

async def run_engine() -> None:
    log.info("=" * 72)
    log.info("PROJECT APEX – Stages 1–5  |  Engine mode")
    log.info(
        "Data → Analysis → Trade Plans → %s execution → Telegram",
        Config.EXECUTION_MODE.upper(),
    )
    log.info("=" * 72)

    mgr = MarketDataManager()
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _sig():
        log.info("Signal received – graceful shutdown")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _sig)
        except NotImplementedError:
            pass

    try:
        await mgr.start()
        if Config.EXECUTION_MODE == "live":
            try:
                await asyncio.wait_for(mgr.live_startup_complete.wait(), timeout=60)
            except asyncio.TimeoutError as exc:
                raise RuntimeError("Timed out while connecting the OKX live adapter") from exc
            if mgr.live_startup_error:
                raise RuntimeError(
                    f"Live execution could not be initialized: {mgr.live_startup_error}"
                )
            if not getattr(mgr, "live_executor", None) or not mgr.live_executor.exchange:
                raise RuntimeError("Live adapter startup completed without an OKX connection")
        log.info(
            "APEX Stage 5 engine started in %s mode. Ctrl+C to stop.",
            Config.EXECUTION_MODE.upper(),
        )
        await stop_event.wait()
    except KeyboardInterrupt:
        pass
    except Exception:
        log.exception("Fatal engine error")
        raise
    finally:
        if hasattr(mgr, "paper_tracker"):
            log.info("Final paper performance:")
            log.info(mgr.paper_tracker.summary_text())
            snap = mgr.paper_tracker.snapshot()
            log.info(
                "Closed trades: %d | Open: %d | Final balance: $%.2f",
                snap.total_trades, len(mgr.paper_tracker.open_positions),
                mgr.paper_tracker.balance,
            )
        if getattr(mgr, "live_executor", None):
            await mgr.live_executor.close()
        await mgr.stop()


def main() -> None:
    if sys.argv[1:2] == ["config"]:
        sys.exit(run_config_command(sys.argv[2:]))

    # Support: python apex.py --dashboard
    if "--dashboard" in sys.argv or "-d" in sys.argv:
        run_dashboard()
        return

    try:
        asyncio.run(run_engine())
    except KeyboardInterrupt:
        print("\nExited.")
    except Exception as e:
        log.exception("Unhandled: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()