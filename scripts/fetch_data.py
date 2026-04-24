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
    {"ticker": "XLU",  "name": "Utilities"},
    {"ticker": "XLB",  "name": "Materials"},
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


# ── RS Rating: Fred6724's live universe ───────────────────────────────────
RS_CSV_URL = "https://raw.githubusercontent.com/Fred6725/rs-log/main/output/rs_stocks.csv"

def fetch_rs_scores_array() -> np.ndarray | None:
    """
    Download Fred6724's rs_stocks.csv (auto-updated daily via GitHub Actions)
    and return a sorted numpy array of all RS scores in the ~5800 stock universe.
    Used for direct percentile lookup — no approximation needed.
    """
    try:
        resp = requests.get(RS_CSV_URL, timeout=15)
        resp.raise_for_status()
        df = pd.read_csv(StringIO(resp.text))
        scores = df["Relative Strength"].dropna().values
        scores_sorted = np.sort(scores)
        print(f"  ✓  RS universe: {len(scores_sorted)} stocks, "
              f"range [{scores_sorted[0]:.2f} – {scores_sorted[-1]:.2f}]")
        return scores_sorted
    except Exception as e:
        print(f"  ⚠  Could not fetch RS universe ({e}). RS Rating will be None.")
        return None


def compute_rs_score(ticker_close: pd.Series, spx_close: pd.Series) -> float | None:
    """
    Fred6724's RS performance score formula:
      0.4 × P(63d) + 0.2 × P(126d) + 0.2 × P(189d) + 0.2 × P(252d)
    divided by same for SPX, scaled × 100.
    Requires at least 253 aligned bars.
    """
    combined = pd.concat([ticker_close, spx_close], axis=1).dropna()
    combined.columns = ["t", "s"]
    if len(combined) < 253:
        return None
    t = combined["t"]
    s = combined["s"]
    n63  = min(63,  len(t) - 1)
    n126 = min(126, len(t) - 1)
    n189 = min(189, len(t) - 1)
    n252 = min(252, len(t) - 1)
    rs_stock = (0.4 * (t.iloc[-1] / t.iloc[-1-n63])
              + 0.2 * (t.iloc[-1] / t.iloc[-1-n126])
              + 0.2 * (t.iloc[-1] / t.iloc[-1-n189])
              + 0.2 * (t.iloc[-1] / t.iloc[-1-n252]))
    rs_ref   = (0.4 * (s.iloc[-1] / s.iloc[-1-n63])
              + 0.2 * (s.iloc[-1] / s.iloc[-1-n126])
              + 0.2 * (s.iloc[-1] / s.iloc[-1-n189])
              + 0.2 * (s.iloc[-1] / s.iloc[-1-n252]))
    if rs_ref == 0:
        return None
    return float((rs_stock / rs_ref) * 100)


def score_to_rating(score: float, scores_array: np.ndarray) -> int | None:
    """
    Direct percentile rank of score against the full universe distribution.
    Uses binary search (np.searchsorted) for O(log n) lookup.
    Returns integer 1–99.
    """
    if score is None or scores_array is None:
        return None
    n = len(scores_array)
    rank_from_bottom = int(np.searchsorted(scores_array, score, side="left"))
    percentile = (rank_from_bottom / n) * 100
    return max(1, min(99, round(percentile)))


# ── MA status helper ──────────────────────────────────────────────────────
def ma_status(price: float, ma_val: float, ma_prev: float) -> str:
    if price >= ma_val:
        return "above_up" if ma_val >= ma_prev else "above_down"
    else:
        return "below_up" if ma_val >= ma_prev else "below_down"


def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def vars_histogram(close: pd.Series, window: int = 20, lookback: int = 50) -> list:
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
def compute_row(ticker_def: dict, hist: pd.DataFrame,
                spx_close: pd.Series, rs_scores: np.ndarray) -> dict:
    ticker = ticker_def["ticker"]
    name   = ticker_def["name"]

    close = hist["Close"].dropna()
    open_ = hist["Open"].dropna()

    if len(close) < 30:
        print(f"  ⚠  {ticker}: not enough data ({len(close)} bars)")
        return None

    price      = round(float(close.iloc[-1]), 4)
    prev_close = float(close.iloc[-2])
    daily_chg  = round((price / prev_close - 1) * 100, 4)

    last_open    = float(open_.iloc[-1]) if len(open_) > 0 else None
    intraday_chg = round((price / last_open - 1) * 100, 4) if last_open else None
    chg_5d       = round((price / float(close.iloc[-6]) - 1) * 100, 4) if len(close) >= 6 else None

    ema9_s   = ema(close, 9)
    ema9_st  = ma_status(price, float(ema9_s.iloc[-1]), float(ema9_s.iloc[-2])) if len(ema9_s) >= 2 else None

    ema21_s  = ema(close, 21)
    ema21_st = ma_status(price, float(ema21_s.iloc[-1]), float(ema21_s.iloc[-2])) if len(ema21_s) >= 2 else None

    ema50_s   = ema(close, 50)
    ema50_val = float(ema50_s.iloc[-1]) if len(ema50_s) >= 2 else None
    ema50_st  = ma_status(price, ema50_val, float(ema50_s.iloc[-2])) if ema50_val and len(ema50_s) >= 2 else None

    sma150_st = None
    if len(close) >= 151:
        sma150_s  = close.rolling(150).mean()
        sma150_st = ma_status(price, float(sma150_s.iloc[-1]), float(sma150_s.iloc[-2]))

    sma200_st = None
    if len(close) >= 201:
        sma200_s  = close.rolling(200).mean()
        sma200_st = ma_status(price, float(sma200_s.iloc[-1]), float(sma200_s.iloc[-2]))

    atr_mult = None
    if ema50_val and len(close) >= 15:
        try:
            high = hist["High"].dropna()
            low_ = hist["Low"].dropna()
            tr   = pd.concat([
                high - low_,
                (high - close.shift(1)).abs(),
                (low_ - close.shift(1)).abs()
            ], axis=1).max(axis=1)
            atr14    = float(tr.rolling(14).mean().iloc[-1])
            atr_mult = round((price - ema50_val) / atr14, 4) if atr14 else None
        except Exception:
            atr_mult = None

    vars_hist = vars_histogram(close, window=20, lookback=50) if len(close) >= 70 else []

    rs_score  = compute_rs_score(close, spx_close)
    rs_rating = score_to_rating(rs_score, rs_scores)

    return {
        "ticker":       ticker,
        "name":         name,
        "price":        price,
        "daily_chg":    daily_chg,
        "intraday_chg": intraday_chg,
        "chg_5d":       chg_5d,
        "ema9":         ema9_st,
        "ema21":        ema21_st,
        "ema50":        ema50_st,
        "sma150":       sma150_st,
        "sma200":       sma200_st,
        "atr_multiple": atr_mult,
        "rs_rating":    rs_rating,
        "vars_history": vars_hist,
    }


# ── Download + process one section ───────────────────────────────────────
def process_section(ticker_defs: list, spx_close: pd.Series,
                    rs_scores: np.ndarray) -> list:
    rows = []
    for td in ticker_defs:
        sym = td["ticker"]
        try:
            hist = yf.download(sym, period="2y", interval="1d",
                               auto_adjust=True, progress=False)
            if hist is None or hist.empty:
                print(f"  ⚠  {sym}: no data returned")
                continue
            if isinstance(hist.columns, pd.MultiIndex):
                hist.columns = hist.columns.get_level_values(0)
            row = compute_row(td, hist, spx_close, rs_scores)
            if row:
                rows.append(row)
                rs_str = str(row["rs_rating"]) if row["rs_rating"] is not None else "N/A"
                print(f"  ✓  {sym}  (RS: {rs_str})")
        except Exception as e:
            print(f"  ✗  {sym}: {e}")
    return rows


# ── Main ──────────────────────────────────────────────────────────────────
def main():
    print("=" * 55)
    print("  Market Dashboard — fetch_data.py")
    print(f"  Run time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 55)

    print("\n── Downloading SPX reference ──")
    spx_hist = yf.download("^GSPC", period="2y", interval="1d",
                           auto_adjust=True, progress=False)
    if isinstance(spx_hist.columns, pd.MultiIndex):
        spx_hist.columns = spx_hist.columns.get_level_values(0)
    spx_close = spx_hist["Close"].dropna()
    print(f"  ✓  SPX: {len(spx_close)} bars")

    print("\n── Fetching RS universe (Fred6724) ──")
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
        output[section_name] = process_section(ticker_defs, spx_close, rs_scores)
        print(f"   → {len(output[section_name])} rows written")

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    total = sum(len(v) for v in output.values() if isinstance(v, list))
    print(f"\n✅  Done → {OUTPUT_FILE}  ({total} total rows)")


if __name__ == "__main__":
    main()
