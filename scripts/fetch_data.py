import yfinance as yf
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime
import pytz

# ── Tickers to track ──────────────────────────────────────────────────
INDICES = [
    {"ticker": "^VIX", "name": "Volatility (VIX)"},
    {"ticker": "IWM",  "name": "Russell 2000"},
    {"ticker": "DIA",  "name": "Dow Jones"},
    {"ticker": "SPY",  "name": "S&P 500"},
    {"ticker": "QQQ",  "name": "NASDAQ 100"},
]

# ── RS Rating helpers ─────────────────────────────────────────────────

def calc_rs_raw(closes):
    """
    RS Score = 40% * P3 + 20% * P6 + 20% * P9 + 20% * P12
    Using approx trading-day lookbacks: 3M=63, 6M=126, 9M=189, 12M=252
    """
    c = closes.dropna().values
    n = len(c)
    if n < 64:
        return None
    cur = c[-1]
    def perf(days):
        # Price from 'days' trading days ago; cap at oldest available
        idx = min(days + 1, n)
        return cur / c[-idx] - 1
    p3  = perf(63)
    p6  = perf(126) if n >= 127 else p3
    p9  = perf(189) if n >= 190 else p6
    p12 = perf(252) if n >= 253 else p9
    return 0.4 * p3 + 0.2 * p6 + 0.2 * p9 + 0.2 * p12

def raw_to_rating(raw_score, universe_scores):
    """Convert a raw RS score to 1-99 by percentile-ranking vs the universe."""
    if raw_score is None or not universe_scores:
        return None
    pct = sum(1 for s in universe_scores if s < raw_score) / len(universe_scores)
    return max(1, min(99, round(pct * 98 + 1)))

def build_universe():
    """
    Download S&P 500 components from Wikipedia, batch-fetch 1 year of closes,
    return a list of their raw RS scores for percentile comparison.
    """
    print("[INFO] Fetching S&P 500 ticker list from Wikipedia…")
    try:
        table  = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
        tickers = table["Symbol"].str.replace(".", "-", regex=False).tolist()
    except Exception as e:
        print(f"[WARN] Could not load S&P 500 list: {e}")
        return []

    print(f"[INFO] Batch-downloading {len(tickers)} S&P 500 stocks (1y)…")
    try:
        raw_uni = yf.download(
            tickers,
            period="1y",
            interval="1d",
            progress=False,
            auto_adjust=True,
            group_by="ticker"
        )
    except Exception as e:
        print(f"[WARN] Universe download failed: {e}")
        return []

    scores = []
    for t in tickers:
        try:
            if isinstance(raw_uni.columns, pd.MultiIndex):
                closes = raw_uni[t]["Close"].dropna()
            else:
                closes = raw_uni["Close"].dropna()
            score = calc_rs_raw(closes)
            if score is not None:
                scores.append(score)
        except Exception:
            pass

    print(f"[INFO] Universe built: {len(scores)} valid stocks")
    return scores


os.makedirs("data", exist_ok=True)

# ── Build comparison universe once ────────────────────────────────────
universe_scores = build_universe()

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

        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)

        df = raw[["Open", "High", "Low", "Close"]].copy()
        df.dropna(subset=["Close"], inplace=True)

        # ── Moving averages ───────────────────────────────────────────
        df["EMA10"]  = df["Close"].ewm(span=10,  adjust=False).mean()
        df["EMA20"]  = df["Close"].ewm(span=20,  adjust=False).mean()
        df["SMA50"]  = df["Close"].rolling(50).mean()
        df["SMA200"] = df["Close"].rolling(200).mean()

        last = df.iloc[-1]
        prev = df.iloc[-2]

        price      = float(last["Close"])
        prev_close = float(prev["Close"])
        open_price = float(last["Open"])

        daily_chg    = round((price - prev_close) / prev_close * 100, 2)
        intraday_chg = round((price - open_price)  / open_price * 100, 2) \
                       if open_price != 0 else 0.0

        def ma_tag(price_above_ma, ma_is_rising):
            if price_above_ma and ma_is_rising:       return "above_up"
            elif price_above_ma and not ma_is_rising: return "above_down"
            elif not price_above_ma and ma_is_rising: return "below_up"
            else:                                     return "below_down"

        # ── RS Rating ─────────────────────────────────────────────────
        rs_raw    = calc_rs_raw(df["Close"])
        rs_rating = raw_to_rating(rs_raw, universe_scores)

        results.append({
            "ticker":       ticker,
            "name":         item["name"],
            "price":        round(price, 2),
            "daily_chg":    daily_chg,
            "intraday_chg": intraday_chg,
            "ema10":  ma_tag(price > float(last["EMA10"]),  float(last["EMA10"])  > float(prev["EMA10"])),
            "ema20":  ma_tag(price > float(last["EMA20"]),  float(last["EMA20"])  > float(prev["EMA20"])),
            "sma50":  ma_tag(price > float(last["SMA50"]),  float(last["SMA50"])  > float(prev["SMA50"])),
            "sma200": ma_tag(price > float(last["SMA200"]), float(last["SMA200"]) > float(prev["SMA200"])),
            "rs_rating": rs_rating,
        })
        print(f"[OK]   {ticker}  RS={rs_rating}")

    except Exception as exc:
        print(f"[ERR]  {ticker}: {exc}")

hkt     = pytz.timezone("Asia/Hong_Kong")
updated = datetime.now(hkt).strftime("%d %b %Y, %H:%M HKT")

with open("data/indices.json", "w") as fh:
    json.dump({"updated": updated, "indices": results}, fh, indent=2)

print(f"\n✅  Saved {len(results)} tickers → data/indices.json  ({updated})")
