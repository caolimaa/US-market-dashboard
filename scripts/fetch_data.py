import yfinance as yf
import pandas as pd
import numpy as np
import json
import requests
from io import StringIO
from datetime import datetime
from pathlib import Path

OUTPUT_FILE = Path("data/indices.json")
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

# ── Ticker definitions ────────────────────────────────────────────────────

INDICES = [
    {"ticker": "^VIX",     "name": "CBOE Volatility Index"},
    {"ticker": "IWM",      "name": "Russell 2000 ETF"},
    {"ticker": "DIA",      "name": "Dow Jones ETF"},
    {"ticker": "SPY",      "name": "S&P 500 ETF"},
    {"ticker": "QQQ",      "name": "Nasdaq 100 ETF"},
    {"ticker": "QQQE",     "name": "Nasdaq 100 Equal Weight ETF"},
    {"ticker": "EDOW",     "name": "Dow Jones Equal Weight ETF"},
    {"ticker": "RSP",      "name": "S&P 500 Equal Weight ETF"},
    {"ticker": "DX-Y.NYB", "name": "US Dollar Index"},
    {"ticker": "ARKK",     "name": "ARK Innovation ETF"},
    {"ticker": "^TNX",     "name": "US 10Y Treasury Yield"},
]

SECTORS = [
    {"ticker": "XLK",  "name": "Technology"},
    {"ticker": "XLF",  "name": "Financials"},
    {"ticker": "XLV",  "name": "Health Care"},
    {"ticker": "XLE",  "name": "Energy"},
    {"ticker": "XLI",  "name": "Industrials"},
    {"ticker": "XLY",  "name": "Consumer Discretionary"},
    {"ticker": "XLP",  "name": "Consumer Staples"},
    {"ticker": "XLB",  "name": "Materials"},
    {"ticker": "XLU",  "name": "Utilities"},
    {"ticker": "XLRE", "name": "Real Estate"},
    {"ticker": "XLC",  "name": "Communication Services"},
]

SECTORS_EW = [
    {"ticker": "RSPG", "name": "EW Energy"},
    {"ticker": "RSPT", "name": "EW Technology"},
    {"ticker": "RSPF", "name": "EW Financials"},
    {"ticker": "RSPH", "name": "EW Health Care"},
    {"ticker": "RSPI", "name": "EW Industrials"},
    {"ticker": "RSPS", "name": "EW Consumer Staples"},
    {"ticker": "RSPU", "name": "EW Utilities"},
    {"ticker": "RSPN", "name": "EW Materials"},
    {"ticker": "RSPD", "name": "EW Consumer Disc"},
    {"ticker": "RSPR", "name": "EW Real Estate"},
    {"ticker": "RSPC", "name": "EW Comm Services"},
]

COMMODITIES = [
    {"ticker": "SLV",     "name": "Silver (SLV ETF)"},
    {"ticker": "GLD",     "name": "Gold (GLD ETF)"},
    {"ticker": "PPLT",    "name": "Platinum (PPLT ETF)"},
    {"ticker": "PALL",    "name": "Palladium (PALL ETF)"},
    {"ticker": "USO",     "name": "WTI Crude Oil (USO ETF)"},
    {"ticker": "JJU",     "name": "Aluminum (JJU ETF)"},
    {"ticker": "UNG",     "name": "Natural Gas (UNG ETF)"},
    {"ticker": "CPER",    "name": "Copper (CPER ETF)"},
    {"ticker": "SOL-USD", "name": "Solana"},
    {"ticker": "BTC-USD", "name": "Bitcoin"},
    {"ticker": "ETH-USD", "name": "Ethereum"},
]

THEMATIC = [
    {"ticker": "SLV",   "name": "Silver ETF"},
    {"ticker": "USO",   "name": "Oil ETF"},
    {"ticker": "XOP",   "name": "Oil & Gas Exploration"},
    {"ticker": "REMX",  "name": "Rare Earth Metals"},
    {"ticker": "SILJ",  "name": "Silver Juniors"},
    {"ticker": "GDXJ",  "name": "Gold Miners Junior"},
    {"ticker": "DXYZ",  "name": "Destiny Tech100"},
    {"ticker": "GDX",   "name": "Gold Miners"},
    {"ticker": "COPX",  "name": "Copper Miners"},
    {"ticker": "GLD",   "name": "Gold ETF"},
    {"ticker": "UNG",   "name": "Natural Gas ETF"},
    {"ticker": "SIL",   "name": "Silver Miners"},
    {"ticker": "OIH",   "name": "Oil Services"},
    {"ticker": "ITA",   "name": "Aerospace & Defense"},
    {"ticker": "ITB",   "name": "Home Construction"},
    {"ticker": "XBI",   "name": "Biotech (EW)"},
    {"ticker": "LIT",   "name": "Lithium & Battery Tech"},
    {"ticker": "AIQ",   "name": "AI & Big Data"},
    {"ticker": "XME",   "name": "Metals & Mining"},
    {"ticker": "XRT",   "name": "Retail"},
    {"ticker": "XHE",   "name": "Health Care Equipment"},
    {"ticker": "UFO",   "name": "Space"},
    {"ticker": "IGV",   "name": "Software"},
    {"ticker": "KWEB",  "name": "China Internet"},
    {"ticker": "SMH",   "name": "Semiconductors"},
    {"ticker": "MAGS",  "name": "Magnificent 7"},
    {"ticker": "ARKK",  "name": "ARK Innovation"},
    {"ticker": "IBIT",  "name": "iShares Bitcoin Trust"},
    {"ticker": "MSOS",  "name": "Cannabis"},
    {"ticker": "JETS",  "name": "Airlines"},
    {"ticker": "ETHA",  "name": "iShares Ethereum Trust"},
    {"ticker": "KRE",   "name": "Regional Banks"},
    {"ticker": "WGMI",  "name": "Bitcoin Miners"},
    {"ticker": "IEZ",   "name": "Oil Equipment & Services"},
    {"ticker": "TAN",   "name": "Solar Energy"},
    {"ticker": "ICLN",  "name": "Clean Energy"},
    {"ticker": "DRNZ",  "name": "Drone Technology"},
    {"ticker": "ARKG",  "name": "ARK Genomic Revolution"},
    {"ticker": "CQQQ",  "name": "China Technology"},
    {"ticker": "XAR",   "name": "Aerospace & Defense (EW)"},
    {"ticker": "BOTZ",  "name": "Robotics & AI"},
    {"ticker": "NUKZ",  "name": "Nuclear Energy"},
    {"ticker": "ROBO",  "name": "Robotics Global"},
    {"ticker": "NLR",   "name": "Nuclear & Uranium"},
    {"ticker": "ARKQ",  "name": "ARK Autonomous Tech"},
    {"ticker": "URA",   "name": "Uranium"},
    {"ticker": "QTUM",  "name": "Quantum Computing"},
    {"ticker": "XHB",   "name": "Homebuilders"},
    {"ticker": "BLOK",  "name": "Blockchain"},
    {"ticker": "CIBR",  "name": "Cybersecurity"},
    {"ticker": "IAI",   "name": "Broker-Dealers"},
    {"ticker": "ARKX",  "name": "ARK Space Exploration"},
    {"ticker": "XSW",   "name": "Software & Services"},
    {"ticker": "IHI",   "name": "Medical Devices"},
    {"ticker": "IYT",   "name": "Transportation"},
    {"ticker": "WCLD",  "name": "Cloud Computing"},
    {"ticker": "ARKW",  "name": "ARK Next Generation Internet"},
    {"ticker": "SOCL",  "name": "Social Media"},
    {"ticker": "BAI",   "name": "Big AI"},
    {"ticker": "CLOU",  "name": "Cloud Computing (Global X)"},
    {"ticker": "XTL",   "name": "Telecom"},
    {"ticker": "SLX",   "name": "Steel"},
    {"ticker": "HACK",  "name": "Cybersecurity (ETFMG)"},
    {"ticker": "PBW",   "name": "Clean Energy (Invesco)"},
    {"ticker": "XHS",   "name": "Health Care Services"},
    {"ticker": "PBJ",   "name": "Food & Beverage"},
    {"ticker": "DRIV",  "name": "Electric & Autonomous Vehicles"},
    {"ticker": "PAVE",  "name": "US Infrastructure"},
    {"ticker": "KBE",   "name": "Bank ETF"},
    {"ticker": "IDGT",  "name": "Industrials & Infrastructure"},
    {"ticker": "PPH",   "name": "Pharma"},
    {"ticker": "DTCR",  "name": "Data Center REIT"},
    {"ticker": "IBUY",  "name": "E-Commerce"},
    {"ticker": "KIE",   "name": "Insurance"},
    {"ticker": "SPPP",  "name": "Platinum & Palladium"},
    {"ticker": "COPJ",  "name": "Copper Juniors"},
    {"ticker": "SOLZ",  "name": "Solana Strategy"},
    {"ticker": "HYDR",  "name": "Hydrogen"},
    {"ticker": "PSIL",  "name": "Psychedelics"},
]


# ── RS universe: Fred6724's live CSV ─────────────────────────────────────
RS_CSV_URL = "https://raw.githubusercontent.com/Fred6724/rs-log/main/output/rs_stocks.csv"

# Hardcoded fallback percentile curve derived from Fred6724's known distribution.
# Maps raw RS score (no SPX division) → percentile rating (1-99).
# Calibrated from observed stats: range [0-547], mean ~100, p50≈100, p90≈155.
_FALLBACK_CURVE = [
    (0,   1), (30,  3), (40,  5), (50,  8),
    (60, 12), (70, 20), (80, 30), (90, 40),
    (95, 45), (100, 50), (105, 55), (110, 60),
    (118, 67), (128, 73), (140, 80), (155, 87),
    (175, 92), (200, 95), (250, 97), (300, 98), (400, 99),
]

def _curve_rating(score):
    """Piecewise linear interpolation of raw RS score → rating (1-99)."""
    xs = [p[0] for p in _FALLBACK_CURVE]
    ys = [p[1] for p in _FALLBACK_CURVE]
    if score <= xs[0]:  return ys[0]
    if score >= xs[-1]: return ys[-1]
    for i in range(len(xs) - 1):
        if xs[i] <= score <= xs[i+1]:
            t = (score - xs[i]) / (xs[i+1] - xs[i])
            return max(1, min(99, round(ys[i] + t * (ys[i+1] - ys[i]))))
    return 50


def fetch_rs_scores_array():
    """
    Download Fred6724's rs_stocks.csv (raw RS scores, no SPX division).
    Retries 3x with timeout. Returns sorted numpy array, or None if
    unavailable (score_to_rating will fall back to hardcoded curve).
    """
    for attempt in range(3):
        try:
            resp = requests.get(RS_CSV_URL, timeout=12)
            if resp.status_code == 200:
                df = pd.read_csv(StringIO(resp.text))
                scores = np.sort(df["Relative Strength"].dropna().values)
                print(f"  RS CSV loaded: {len(scores)} stocks  "
                      f"[{scores[0]:.1f} – {scores[-1]:.1f}]")
                return scores
        except Exception:
            pass
    print("  RS CSV unavailable — using fallback percentile curve.")
    return None


# ── RS Score helpers ──────────────────────────────────────────────────────

def compute_rs_score(close):
    """
    Raw RS performance score matching Fred6724's formula exactly:
      (0.4×P63d + 0.2×P126d + 0.2×P189d + 0.2×P252d) × 100
    No SPX division — comparable to Fred6724's universe CSV.
    Requires >=253 bars. Returns float or None.
    """
    if len(close) < 253:
        return None
    return float(
        (0.4 * (close.iloc[-1] / close.iloc[-63])
       + 0.2 * (close.iloc[-1] / close.iloc[-126])
       + 0.2 * (close.iloc[-1] / close.iloc[-189])
       + 0.2 * (close.iloc[-1] / close.iloc[-252])) * 100
    )


def score_to_rating(score, scores_array):
    """
    Convert raw RS score to 1-99 rating.
    Uses live CSV distribution when available, fallback curve otherwise.
    Never returns None when score is valid.
    """
    if score is None:
        return None
    if scores_array is not None:
        n    = len(scores_array)
        rank = int(np.searchsorted(scores_array, score, side="left"))
        return max(1, min(99, round((rank / n) * 100)))
    return _curve_rating(score)


def compute_1m_rs_score(close, bars_back=0):
    """
    1-month raw RS score: 21-day price return × 100.
    bars_back=0 = today, bars_back=N = N trading days ago.
    """
    c = close.iloc[:-bars_back] if bars_back > 0 else close
    if len(c) < 22:
        return None
    return float((c.iloc[-1] / c.iloc[-22]) * 100)


def compute_1m_rs_new_high(close):
    """
    Returns True if today's 1M RS score is strictly higher than
    all 21 prior trading days. Returns None if insufficient data.
    """
    window = []
    for i in range(21, -1, -1):
        sc = compute_1m_rs_score(close, bars_back=i)
        window.append(sc)
    today = window[-1]
    past  = [r for r in window[:-1] if r is not None]
    if today is None or not past:
        return None
    return bool(today > max(past))


# ── MA / ATR helpers ──────────────────────────────────────────────────────

def ma_status(price, ma_val, ma_prev):
    if price >= ma_val:
        return "above_up" if ma_val >= ma_prev else "above_down"
    else:
        return "below_up" if ma_val >= ma_prev else "below_down"


def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def vars_histogram(close, window=20, lookback=50):
    if len(close) < lookback + window:
        return []
    sma_lb = close.rolling(lookback).mean()
    hist   = close - sma_lb
    sma20  = hist.rolling(20).mean()
    result = []
    for i in range(-window, 0):
        v = hist.iloc[i]
        m = sma20.iloc[i]
        if pd.isna(v) or pd.isna(m):
            continue
        result.append({"v": round(float(v), 4), "m": round(float(m), 4)})
    return result


# ── Core per-ticker calculation ───────────────────────────────────────────

def compute_row(ticker_def, hist, rs_scores):
    ticker = ticker_def["ticker"]
    name   = ticker_def["name"]

    close = hist["Close"].dropna()
    open_ = hist["Open"].dropna()

    if len(close) < 30:
        print(f"  {ticker}: not enough data ({len(close)} bars)")
        return None

    price        = round(float(close.iloc[-1]), 4)
    prev_close   = float(close.iloc[-2])
    daily_chg    = round((price / prev_close - 1) * 100, 4)
    last_open    = float(open_.iloc[-1]) if len(open_) > 0 else None
    intraday_chg = round((price / last_open - 1) * 100, 4) if last_open else None
    chg_5d       = round((price / float(close.iloc[-6]) - 1) * 100, 4) if len(close) >= 6 else None

    # EMAs for MA status columns
    ema9_s   = ema(close, 9)
    ema9_st  = ma_status(price, float(ema9_s.iloc[-1]),  float(ema9_s.iloc[-2]))  if len(ema9_s)  >= 2 else None
    ema21_s  = ema(close, 21)
    ema21_st = ma_status(price, float(ema21_s.iloc[-1]), float(ema21_s.iloc[-2])) if len(ema21_s) >= 2 else None
    ema50_s  = ema(close, 50)
    ema50_st = ma_status(price, float(ema50_s.iloc[-1]), float(ema50_s.iloc[-2])) if len(ema50_s) >= 2 else None

    # ── SMA 50 — used for ATRx benchmark ─────────────────────────────────
    sma50_val = None
    sma50_prev = None
    if len(close) >= 51:
        sma50_s   = close.rolling(50).mean()
        sma50_val  = float(sma50_s.iloc[-1])
        sma50_prev = float(sma50_s.iloc[-2])

    sma150_st = None
    if len(close) >= 151:
        sma150_s  = close.rolling(150).mean()
        sma150_st = ma_status(price, float(sma150_s.iloc[-1]), float(sma150_s.iloc[-2]))

    sma200_st = None
    if len(close) >= 201:
        sma200_s  = close.rolling(200).mean()
        sma200_st = ma_status(price, float(sma200_s.iloc[-1]), float(sma200_s.iloc[-2]))

    # ── ATRx SMA50: (Price - SMA50) / ATR14 ──────────────────────────────
    atr_mult = None
    if sma50_val is not None and len(close) >= 15:
        try:
            high = hist["High"].dropna()
            low_ = hist["Low"].dropna()
            tr   = pd.concat([
                high - low_,
                (high - close.shift(1)).abs(),
                (low_ - close.shift(1)).abs()
            ], axis=1).max(axis=1)
            atr14    = float(tr.rolling(14).mean().iloc[-1])
            atr_mult = round((price - sma50_val) / atr14, 4) if atr14 else None
        except Exception:
            atr_mult = None

    vars_hist = vars_histogram(close, window=20, lookback=50) if len(close) >= 70 else []

    rs_score       = compute_rs_score(close)
    rs_rating      = score_to_rating(rs_score, rs_scores)
    rs_1m_new_high = compute_1m_rs_new_high(close)

    return {
        "ticker":          ticker,
        "name":            name,
        "price":           price,
        "daily_chg":       daily_chg,
        "intraday_chg":    intraday_chg,
        "chg_5d":          chg_5d,
        "ema9":            ema9_st,
        "ema21":           ema21_st,
        "ema50":           ema50_st,
        "sma150":          sma150_st,
        "sma200":          sma200_st,
        "atr_multiple":    atr_mult,
        "rs_rating":       rs_rating,
        "rs_1m_new_high":  rs_1m_new_high,
        "vars_history":    vars_hist,
    }


# ── Download + process one section ───────────────────────────────────────

def process_section(ticker_defs, rs_scores):
    rows = []
    for td in ticker_defs:
        sym = td["ticker"]
        try:
            hist = yf.download(sym, period="2y", interval="1d",
                               auto_adjust=True, progress=False)
            if hist is None or hist.empty:
                print(f"  {sym}: no data returned")
                continue
            if isinstance(hist.columns, pd.MultiIndex):
                hist.columns = hist.columns.get_level_values(0)
            row = compute_row(td, hist, rs_scores)
            if row:
                rows.append(row)
                rs_str = str(row["rs_rating"]) if row["rs_rating"] is not None else "N/A"
                nh_str = "NEW HIGH" if row["rs_1m_new_high"] else ("—" if row["rs_1m_new_high"] is False else "N/A")
                print(f"  {sym}  RS:{rs_str}  1M:{nh_str}")
        except Exception as e:
            print(f"  {sym}: {e}")
    return rows


# ── Main ──────────────────────────────────────────────────────────────────

def main():
    print("=" * 55)
    print("  Market Dashboard — fetch_data.py")
    print(f"  Run: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 55)

    print("\n── RS universe (Fred6724) ──")
    rs_scores = fetch_rs_scores_array()

    sections = {
        "indices":     INDICES,
        "sectors":     SECTORS,
        "sectors_ew":  SECTORS_EW,
        "commodities": COMMODITIES,
        "thematic":    THEMATIC,
    }

    output = {"updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}

    for section_name, ticker_defs in sections.items():
        print(f"\n── {section_name.upper()} ({len(ticker_defs)} tickers) ──")
        output[section_name] = process_section(ticker_defs, rs_scores)
        print(f"   {len(output[section_name])} rows written")

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    total = sum(len(v) for v in output.values() if isinstance(v, list))
    print(f"\nDone  {OUTPUT_FILE}  ({total} total rows)")


if __name__ == "__main__":
    main()
