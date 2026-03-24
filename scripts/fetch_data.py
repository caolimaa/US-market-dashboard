import yfinance as yf
import pandas as pd
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

# ── Hardcoded S&P 500 universe (200 liquid stocks across all sectors) ─
SP500_UNIVERSE = [
    # Technology
    "AAPL","MSFT","NVDA","AVGO","ORCL","CRM","AMD","QCOM","TXN","AMAT",
    "KLAC","LRCX","ADI","MU","SNPS","CDNS","FTNT","PANW","CRWD","NOW",
    "ADBE","INTU","INTC","HPQ","HPE","CSCO","IBM","DELL","STX","WDC",
    # Communication Services
    "META","GOOGL","GOOG","NFLX","DIS","CMCSA","T","VZ","TMUS","CHTR",
    "EA","TTWO","NWSA","OMC","IPG","LYV","WBD","PARA",
    # Consumer Discretionary
    "AMZN","TSLA","HD","MCD","NKE","SBUX","LOW","TJX","BKNG","CMG",
    "MAR","HLT","ORLY","AZO","ROST","YUM","DHI","LEN","PHM","NVR",
    "TSCO","EBAY","ETSY","BBY","DRI","LVS","MGM","WYNN","HAS","MHK",
    # Consumer Staples
    "WMT","COST","PG","KO","PEP","PM","MO","MDLZ","CL","GIS",
    "KMB","SYY","ADM","MKC","K","HRL","CAG","CPB","CHD","CLX",
    # Healthcare
    "LLY","UNH","JNJ","ABBV","MRK","ABT","TMO","DHR","PFE","AMGN",
    "GILD","ISRG","SYK","BDX","BSX","EW","ZBH","BAX","HOLX","RMD",
    "IDXX","IQV","CRL","CTLT","VTRS","HUM","CVS","CI","CNC","MOH",
    # Financials
    "BRK-B","JPM","BAC","WFC","GS","MS","C","AXP","BLK","SCHW",
    "CB","PGR","AIG","MET","PRU","AFL","TRV","ALL","HIG","WRB",
    "V","MA","PYPL","COF","DFS","SYF","AMP","BEN","IVZ","TROW",
    # Industrials
    "CAT","DE","HON","UPS","RTX","LMT","GE","MMM","EMR","ETN",
    "PH","ITW","ROK","CMI","PCAR","FDX","CSX","UNP","NSC","DAL",
    "UAL","LUV","AAL","WM","RSG","FAST","GWW","XYL","IEX","AME",
    # Energy
    "XOM","CVX","COP","SLB","EOG","PXD","MPC","VLO","PSX","HAL",
    "DVN","FANG","HES","APA","OXY","BKR","NOV","FTI",
    # Materials
    "LIN","APD","SHW","ECL","PPG","NEM","FCX","NUE","STLD","CF",
    "MOS","ALB","CE","EMN","IFF","FMC","SON","PKG","IP","WRK",
    # Utilities
    "NEE","DUK","SO","D","AEP","EXC","SRE","XEL","ES","WEC",
    "ETR","FE","PPL","EIX","PCG","AWK","AES","CMS","NI","PNW",
    # Real Estate
    "AMT","PLD","CCI","EQIX","PSA","O","WELL","DLR","SPG","AVB",
    "EQR","MAA","UDR","CPT","ESS","EXR","VICI","CBRE","ARE","BXP",
]

# ── RS Rating helpers ─────────────────────────────────────────────────

def calc_rs_raw(closes):
    """RS Score = 40%*P3 + 20%*P6 + 20%*P9 + 20%*P12"""
    c = closes.dropna().values
    n = len(c)
    if n < 64:
        return None
    cur = c[-1]
    def perf(days):
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
    """Batch-download universe stocks, return list of raw RS scores."""
    print(f"[INFO] Batch-downloading {len(SP500_UNIVERSE)} universe stocks (1y)…")
    try:
        raw_uni = yf.download(
            SP500_UNIVERSE,
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
    for t in SP500_UNIVERSE:
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

# ── ATR% multiple from 50-MA helper ──────────────────────────────────

def calc_atr_multiple(df, atr_len=14, ma_len=50):
    """
    Replicates the TradingView indicator by jfsrev:
      A  = ATR% = ATR(14) / Close * 100
      B  = % distance from SMA50 = (Close - SMA50) / SMA50 * 100
      Result = B / A
    Returns None if not enough data or ATR% is zero.
    """
    if len(df) < max(atr_len, ma_len) + 1:
        return None

    # True Range = max(H-L, |H-PrevC|, |L-PrevC|)
    high  = df["High"]
    low   = df["Low"]
    close = df["Close"]
    prev_close = close.shift(1)

    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs()
    ], axis=1).max(axis=1)

    # Wilder's smoothing (same as TradingView's ta.atr default)
    atr = tr.ewm(alpha=1 / atr_len, adjust=False).mean()

    sma50 = close.rolling(ma_len).mean()

    last_close = float(close.iloc[-1])
    last_atr   = float(atr.iloc[-1])
    last_sma50 = float(sma50.iloc[-1])

    if last_close == 0 or last_atr == 0 or pd.isna(last_sma50):
        return None

    atr_pct        = last_atr / last_close * 100          # A
    pct_from_50ma  = (last_close - last_sma50) / last_sma50 * 100  # B
    atr_multiple   = pct_from_50ma / atr_pct              # B / A

    return round(atr_multiple, 2)


os.makedirs("data", exist_ok=True)

# ── Build RS comparison universe once ────────────────────────────────
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

        # ── ATR% multiple from 50-MA ───────────────────────────────────
        atr_multiple = calc_atr_multiple(df, atr_len=14, ma_len=50)

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
            "rs_rating":    rs_rating,
            "atr_multiple": atr_multiple,
        })
        print(f"[OK]   {ticker}  RS={rs_rating}  ATRx={atr_multiple}")

    except Exception as exc:
        print(f"[ERR]  {ticker}: {exc}")

hkt     = pytz.timezone("Asia/Hong_Kong")
updated = datetime.now(hkt).strftime("%d %b %Y, %H:%M HKT")

with open("data/indices.json", "w") as fh:
    json.dump({"updated": updated, "indices": results}, fh, indent=2)

print(f"\n✅  Saved {len(results)} tickers → data/indices.json  ({updated})")
