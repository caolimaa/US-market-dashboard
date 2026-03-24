import yfinance as yf
import pandas as pd
import json
import os
from datetime import datetime
import pytz

# ── Tickers to track ──────────────────────────────────────────────────
INDICES = [
    {"ticker": "SPY",    "name": "S&P 500"},
    {"ticker": "QQQ",    "name": "NASDAQ 100"},
    {"ticker": "DIA",    "name": "Dow Jones"},
    {"ticker": "IWM",    "name": "Russell 2000"},
    {"ticker": "^VIX",   "name": "Volatility (VIX)"},
    {"ticker": "^FTSE",  "name": "FTSE 100"},
    {"ticker": "^N225",  "name": "Nikkei 225"},
    {"ticker": "^HSI",   "name": "Hang Seng"},
    {"ticker": "^GDAXI", "name": "DAX"},
    {"ticker": "^FCHI",  "name": "CAC 40"},
    {"ticker": "EEM",    "name": "Emerging Markets"},
    {"ticker": "GLD",    "name": "Gold ETF"},
]

os.makedirs("data", exist_ok=True)
results = []

for item in INDICES:
    ticker = item["ticker"]
    try:
        raw = yf.download(
            ticker,
            period="1y",
            interval="1d",
            progress=False,
            auto_adjust=True
        )

        if raw.empty or len(raw) < 50:
            print(f"[SKIP] {ticker}: not enough data ({len(raw)} rows)")
            continue

        # yfinance sometimes returns multi-level column headers — flatten them
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)

        df = raw[["Open", "High", "Low", "Close"]].copy()
        df.dropna(subset=["Close"], inplace=True)

        # ── Calculate all moving averages ──────────────────────────
        df["EMA10"]  = df["Close"].ewm(span=10,  adjust=False).mean()
        df["EMA20"]  = df["Close"].ewm(span=20,  adjust=False).mean()
        df["SMA50"]  = df["Close"].rolling(50).mean()
        df["SMA200"] = df["Close"].rolling(200).mean()

        last = df.iloc[-1]   # most recent trading day
        prev = df.iloc[-2]   # day before

        price      = float(last["Close"])
        prev_close = float(prev["Close"])
        open_price = float(last["Open"])

        daily_chg    = round((price - prev_close) / prev_close * 100, 2)
        intraday_chg = round((price - open_price)  / open_price  * 100, 2) \
                       if open_price != 0 else 0.0

        def ma_tag(price_above_ma, ma_is_rising):
            """Return a 4-state label for price vs MA and MA trend direction."""
            if price_above_ma and ma_is_rising:      return "above_up"
            elif price_above_ma and not ma_is_rising: return "above_down"
            elif not price_above_ma and ma_is_rising: return "below_up"
            else:                                     return "below_down"

        results.append({
            "ticker":       ticker,
            "name":         item["name"],
            "price":        round(price, 2),
            "daily_chg":    daily_chg,
            "intraday_chg": intraday_chg,
            "ema10":  ma_tag(price > float(last["EMA10"]),
                             float(last["EMA10"])  > float(prev["EMA10"])),
            "ema20":  ma_tag(price > float(last["EMA20"]),
                             float(last["EMA20"])  > float(prev["EMA20"])),
            "sma50":  ma_tag(price > float(last["SMA50"]),
                             float(last["SMA50"])  > float(prev["SMA50"])),
            "sma200": ma_tag(price > float(last["SMA200"]),
                             float(last["SMA200"]) > float(prev["SMA200"])),
        })
        print(f"[OK]   {ticker}")

    except Exception as exc:
        print(f"[ERR]  {ticker}: {exc}")

hkt     = pytz.timezone("Asia/Hong_Kong")
updated = datetime.now(hkt).strftime("%d %b %Y, %H:%M HKT")

with open("data/indices.json", "w") as fh:
    json.dump({"updated": updated, "indices": results}, fh, indent=2)

print(f"\n✅  Saved {len(results)} tickers → data/indices.json  ({updated})")
