# PROJECT APEX – Research and Execution Platform

**Advanced Multi-Exchange Cryptocurrency Futures Intelligence System**  
Monolithic single-file implementation (`apex.py`).

## Implemented Capabilities

| Stage | Content |
|-------|---------|
| 1 | Market data acquisition, pair discovery, SQLite, validation, health |
| 2 | Indicators, structure (BOS/CHoCH), liquidity, order-flow, derivatives, regime |
| 3 | Entry optimiser, dynamic stops, multi-TP, net R/R, position sizing, ranking |
| 4 | Paper trading simulator, performance tracker, simple backtest engine |
| 5 | Streamlit dashboard + Telegram notifications + system monitoring |

## Safety and Execution

- The default execution mode is `paper`; no credentials or wallet connection are required.
- Signal-driven OKX futures orders are available when `EXECUTION_MODE=live`.
- Live mode additionally requires OKX credentials, an explicit confirmation token,
  and `LIVE_KILL_SWITCH=false`.
- Testnet is selected by default through `OKX_TESTNET=true`.
- Live order size, aggregate portfolio notional, correlated positions, position
  count, leverage, order rate, daily loss, and signal-entry drift are capped. The
  order size is reduced to fit reconciled OKX USDT equity and free margin; live
  execution blocks if account state or exposure cannot be valued.
- At startup the adapter selects testnet/live before loading markets and verifies the
  account's net/hedged position mode. It exits instead of silently running paper-only
  when live mode cannot connect.
- The live adapter is OKX-only and will not route signals sourced from Bybit or Binance
  into the OKX account.
- Live entries require a `READY` signal; setups still waiting for confirmation are
  not submitted.
- The live loss guard uses current-UTC-day realized fills, subtracts exchange-reported
  fees, and includes signed funding payments. Fees are converted from the contract's
  base currency at fill price; unsupported fee currencies or incomplete records block
  new live entries.
- Live limit entries remain disabled until exchange-native attached protection is supported.
  If post-entry protection fails on a market order, the adapter submits a reduce-only
  emergency close and reports uncertain outcomes explicitly.
- Paper positions, pending entry setups, 1-minute replay checkpoints, and protection
  intents persist in SQLite. Recovery backfills from the last checkpoint (or the
  active position/setup start) and does not advance across missing candle timestamps.
- Paper funding uses fresh exchange funding snapshots at settlement boundaries, with
  the configured funding horizon as the fallback interval for missed intervals. It
  remains an estimate; unavailable historical rates cannot be reconstructed.
- Pending paper entries fill at the observed bar price clamped to the touched entry
  zone, with configured slippage; OHLC bars cannot reproduce tick-level execution.
- Paper simulation is disabled during live execution unless
  `PAPER_SHADOW_IN_LIVE=true` is explicitly configured. The dashboard reports whether
  paper simulation is active.
- The dashboard reads opportunities, paper positions, performance, and equity from SQLite.
- Historical backtests model next-bar entry, limit unfilled states, taker fees,
  slippage, bar-based stop/TP outcomes, and approximate funding costs.
- Analysis uses closed 1h/4h candles and rejects missing or stale tickers. Each cycle
  reports eligible, analyzed, insufficient-history, and failed symbol counts with
  per-symbol reasons. Ticker freshness defaults to 75 seconds for the 50-second scan
  interval. A fresh 4h EMA trend must confirm 1h direction; conflicting or unavailable
  confirmation blocks a trade plan.
- Opportunity scores are ranking heuristics, not probabilities of success; a
  probability interpretation requires out-of-sample calibration.
- Market-data checks reject malformed, crossed, non-finite, out-of-order, and stale
  inputs. Validation reduces data risk but cannot guarantee that an exchange or its
  upstream data is correct, complete, or available.
- Paper position management consumes closed 1-minute candle ranges. It resolves
  same-bar stop/target ambiguity in favor of the stop and scales out 50/25/25
  across TP1/TP2/TP3 (or evenly across the available targets).
- This project does not provide WalletConnect or on-chain DEX execution. Its live
  adapter connects to OKX using exchange API credentials; live trading can lose
  funds and is not suitable for unattended production use without independent review.

## How to Run

### 1. Engine (live analysis + paper trading)
```bash
pip install -r requirements.txt
python apex.py
```

### 2. Dashboard (separate terminal)
```bash
streamlit run apex.py -- --dashboard
```

### 3. Extensive runtime configuration
Inspect supported parameters and their current effective values:
```bash
python apex.py config show
```

Set one or more validated parameters; values persist in
`data_store/apex_settings.json` and apply on the next engine start:
```bash
python apex.py config set RISK_PER_TRADE_PCT=0.5 MAX_PAIRS=50 MIN_NET_RR=2.0
python apex.py config set EMA_FAST=12 EMA_SLOW=26 EMA_TREND=55
python apex.py config set EXCHANGE_PRIORITY='["okx","bybit","binance"]'
python apex.py config set PAPER_SHADOW_IN_LIVE=true
```

Environment variables take precedence over persisted settings. Credentials,
live-mode confirmation, testnet selection, and the kill switch remain
environment-only controls. Invalid or unsafe values are rejected, and the bot
fails at startup if its saved configuration is invalid.

### 4. Optional Telegram
Set environment variables:
```bash
export TELEGRAM_BOT_TOKEN="your_bot_token"
export TELEGRAM_CHAT_ID="your_chat_id"
```
Then start the engine – high-score opportunities and paper closes will be pushed.

#### Enabling real OKX orders

Live mode submits orders automatically from APEX's `READY` signals; there is no
WalletConnect flow. Create an OKX API key with **read and trade** permissions only,
disable withdrawals, and use OKX IP allowlisting. Store the key, secret, and
passphrase only in a local `.env` or secret manager—never commit or paste them into
chat.

After you have validated the same setup on OKX testnet, configure your local
environment (or uncomment and fill the corresponding entries in `.env`):
```bash
export EXECUTION_MODE=live
export OKX_TESTNET=false
export LIVE_KILL_SWITCH=false
export LIVE_CONFIRMATION=ENABLE-LIVE-APEX
export OKX_API_KEY="..."
export OKX_API_SECRET="..."
export OKX_API_PASSPHRASE="..."
```

Set conservative caps before starting:
```bash
python apex.py config set ACCOUNT_BALANCE=1000 RISK_PER_TRADE_PCT=0.25 \
  MAX_LIVE_NOTIONAL_USD=50 MAX_LIVE_POSITIONS=1 MAX_LIVE_DAILY_LOSS_USD=10 \
  MAX_LIVE_ENTRY_DRIFT_PCT=0.25
python apex.py
```
Use values appropriate to your account; the example values are not recommendations.
`ACCOUNT_BALANCE` is the maximum sizing reference; if reconciled OKX equity is lower,
the bot scales the order down rather than sizing from the larger configured value.
The engine will refuse to start in live mode if OKX authentication/position-mode
verification fails, and it will block entries if balance, daily PnL, current prices,
margin, or protection reconciliation is unavailable. Keep a human monitoring the
account: software cannot guarantee fills, stop execution, market-data correctness,
or loss limits during exchange/network outages.

The adapter cannot eliminate the interval between a market fill and exchange
acknowledgement of protective orders. Do not treat this as unattended or
institutional-grade execution.

## Configuration (env vars)
- `ACCOUNT_BALANCE=1000`
- `RISK_PER_TRADE_PCT=1.0`
- `MIN_NET_RR=1.5`
- `MAX_PAIRS=30`
- `PRIMARY_EXCHANGE=okx`
- `TELEGRAM_BOT_TOKEN=`
- `TELEGRAM_CHAT_ID=`

## Design Principles Honoured
- Zero paid dependencies / APIs
- No look-ahead bias
- No forced trades
- Structure + volatility based stops (never fixed %)
- Net R/R after costs
- Risk-based position sizing
- Graceful degradation when exchanges are geo-blocked
- Fully monolithic single file

APEX is a research and paper-trading workstation. No signal or market-data source
can be guaranteed flawless, and the software does not guarantee trading results.
