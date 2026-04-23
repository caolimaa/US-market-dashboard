import yfinance as yf
import pandas as pd
import json
import requests
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


# ── Fred6724 RS Rating calibration thresholds ─────────────────────────────
# Pulled from his public rs-log repo — same thresholds his Pine Script uses.
# Falls back to his last known approximate values if the fetch fails.
FALLBACK_THRESHOLDS = [195.93, 117.11, 99.04, 91.66, 80.96, 53.64, 24.86]

def fetch_rs_thresholds() -> list:
    """
    Fetch Fred6724's 7 RS calibration thresholds from his public GitHub CSV.
    Returns [first, scnd, thrd, frth, ffth, sxth, svth] — same order as Pine Script.
    """
    url = "https://raw.githubusercontent.com/Fred6725/rs-log/main/output/rs_stocks.csv"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        lines = [l.strip() for l in resp.text.strip().split('\n') if l.strip()]
        # The CSV contains RS scores for thousands of stocks; we extract the
        # boundary percentiles at ranks 99, 90, 70, 50, 30, 10, 1
        df = pd.read_csv(pd.io.common.StringIO(resp.text))
        # Column expected: 'rs_score' or similar — try to find it
        score_col = [c for c in df.columns if 'score' in c.lower() or 'rs' in c.lower()]
        if score_col:
            scores = df[score_col[0]].dropna().sort_values(ascending=False).reset_index(drop=True)
            n = len(scores)
            def pct_val(p): return float(scores.iloc[max(0, int(n * (1 - p/100)) - 1)])
            thresholds = [pct_val(99), pct_val(90), pct_val(70), pct_val(50),
                          pct_val(30), pct_val(10), pct_val(1)]
            print(f"  ✓  RS thresholds fetched from Fred6724 repo: {[round(t,2) for t in thresholds]}")
            return thresholds
    except Exception as e:
        print(f"  ⚠  Could not fetch RS thresholds ({e}), using fallback values.")
    return FALLBACK_THRESHOLDS


def f_attribute_percentile(score, taller, smaller, range_up, range_dn, weight):
    """Python port of Fred6724's f_attributePercentile() Pine Script function."""
    total = score + (score - smaller) * weight
    if total > taller - 1:
        total = taller - 1
    k1 = smaller / range_dn
    k2 = (taller - 1) / range_up
    k3 = (k1 - k2) / (taller - 1 - smaller)
    rating = total / (k1 - k3 * (score - smaller))
    return max(range_dn, min(range_up, rating))


def compute_rs_rating(ticker_close: pd.Series, spx_close: pd.Series,
                      thresholds: list) -> int | None:
    """
    Compute Fred6724's RS Rating (1–99) for a ticker given its daily close
    series and the SPX daily close series. Returns None if insufficient data.
    """
    first, scnd, thrd, frth, ffth, sxth, svth = thresholds

    min_bars = max(bar_index for bar_index in [63, 126, 189, 252])
    if len(ticker_close) < min_bars + 1 or len(spx_close) < min_bars + 1:
        return None

    # Align series
    combined = pd.concat([ticker_close, spx_close], axis=1).dropna()
    combined.columns = ['ticker', 'spx']
    if len(combined) < 253:
        return None

    t = combined['ticker']
    s = combined['spx']

    n63  = min(63,  len(t) - 1)
    n126 = min(126, len(t) - 1)
    n189 = min(189, len(t) - 1)
    n252 = min(252, len(t) - 1)

    pt63  = t.iloc[-1] / t.iloc[-1 - n63]
    pt126 = t.iloc[-1] / t.iloc[-1 - n126]
    pt189 = t.iloc[-1] / t.iloc[-1 - n189]
    pt252 = t.iloc[-1] / t.iloc[-1 - n252]

    ps63  = s.iloc[-1] / s.iloc[-1 - n63]
    ps126 = s.iloc[-1] / s.iloc[-1 - n126]
    ps189 = s.iloc[-1] / s.iloc[-1 - n189]
    ps252 = s.iloc[-1] / s.iloc[-1 - n252]

    rs_stock = 0.4 * pt63 + 0.2 * pt126 + 0.2 * pt189 + 0.2 * pt252
    rs_ref   = 0.4 * ps63 + 0.2 * ps126 + 0.2 * ps189 + 0.2 * ps252

    score = (rs_stock / rs_ref) * 100

    if score >= first: return 99
    if score <= svth:  return 1
    if score < first and score >= scnd:
        return round(f_attribute_percentile(score, first, scnd, 98, 90, 0.33))
    if score < scnd  and score >= thrd:
        return round(f_attribute_percentile(score, scnd,  thrd, 89, 70, 2.1))
    if score < thrd  and score >= frth:
        return round(f_attribute_percentile(score, thrd,  frth, 69, 50, 0))
    if score < frth  and score >= ffth:
        return round(f_attribute_percentile(score, frth,  ffth, 49, 30, 0))
    if score < ffth  and score >= sxth:
        return round(f_attribute_percentile(score, ffth,  sxth, 29, 10, 0))
    if score < sxth  and score >= svth:
        return round(f_attribute_percentile(score, sxth,  svth,  9,  2, 0))
    return None


# ── MA status helper ──────────────────────────────────────────────────────
def ma_status(price: float, ma_val: float, ma_prev: float) -> str:
    if price >= ma_val:
        return "above_up" if ma_val >= ma_prev else "above_down"
    else:
        return "below_up" if ma_val >= ma_prev else "below_down"


# ── EMA helper ────────────────────────────────────────────────────────────
def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


# ── VARS histogram helper ─────────────────────────────────────────────────
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
                spx_close: pd.Series, thresholds: list) -> dict:
    ticker = ticker_def["ticker"]
    name   = ticker_def["name"]

    close  = hist["Close"].dropna()
    open_  = hist["Open"].dropna()

    if len(close) < 30:
        print(f"  ⚠  {ticker}: not enough data ({len(close)} bars)")
        return None

    price      = round(float(close.iloc[-1]), 4)
    prev_close = float(close.iloc[-2])
    daily_chg  = round((price / prev_close - 1) * 100, 4)

    last_open    = float(open_.iloc[-1]) if len(open_) > 0 else None
    intraday_chg = round((price / last_open - 1) * 100, 4) if last_open else None

    chg_5d = round((price / float(close.iloc[-6]) - 1) * 100, 4) if len(close) >= 6 else None

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

    # RS Rating — Fred6724 formula
    rs_rating = compute_rs_rating(close, spx_close, thresholds)

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
                    thresholds: list) -> list:
    rows = []
    for td in ticker_defs:
        sym = td["ticker"]
        try:
            hist = yf.download(
                sym,
                period="2y",
                interval="1d",
                auto_adjust=True,
                progress=False,
            )
            if hist is None or hist.empty:
                print(f"  ⚠  {sym}: no data returned")
                continue
            if isinstance(hist.columns, pd.MultiIndex):
                hist.columns = hist.columns.get_level_values(0)
            row = compute_row(td, hist, spx_close, thresholds)
            if row:
                rows.append(row)
                rs_str = str(row['rs_rating']) if row['rs_rating'] else 'N/A'
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

    # Fetch SPX once and reuse across all sections
    print("\n── Downloading SPX reference data ──")
    spx_hist = yf.download("^GSPC", period="2y", interval="1d",
                           auto_adjust=True, progress=False)
    if isinstance(spx_hist.columns, pd.MultiIndex):
        spx_hist.columns = spx_hist.columns.get_level_values(0)
    spx_close = spx_hist["Close"].dropna()
    print(f"  ✓  SPX: {len(spx_close)} bars")

    # Fetch Fred6724's RS calibration thresholds once
    print("\n── Fetching RS Rating thresholds (Fred6724) ──")
    thresholds = fetch_rs_thresholds()

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
        output[section_name] = process_section(ticker_defs, spx_close, thresholds)
        print(f"   → {len(output[section_name])} rows written")

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    total = sum(len(v) for v in output.values() if isinstance(v, list))
    print(f"\n✅  Done → {OUTPUT_FILE}  ({total} total rows)")


if __name__ == "__main__":
    main()
