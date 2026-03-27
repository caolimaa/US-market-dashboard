import yfinance as yf
import pandas as pd
import json
import os
from datetime import datetime
import pytz
import urllib.request
import re
import io


# ── Indices ───────────────────────────────────────────────────────────
INDICES = [
    {"ticker": "^VIX",  "name": "Volatility (VIX)"},
    {"ticker": "IWM",   "name": "Russell 2000"},
    {"ticker": "DIA",   "name": "Dow Jones"},
    {"ticker": "SPY",   "name": "S&P 500"},
    {"ticker": "QQQ",   "name": "NASDAQ 100"},
    {"ticker": "RSP",   "name": "S&P 500 Equal Weight"},
    {"ticker": "EDOW",  "name": "Dow 30 Equal Weight"},
    {"ticker": "QQQE",  "name": "NASDAQ 100 Equal Weight"},
    {"ticker": "ARKK",  "name": "ARK Innovation ETF"},
    {"ticker": "DXY",   "yf_ticker": "DX-Y.NYB", "name": "US Dollar Index"},
    {"ticker": "US10Y", "yf_ticker": "^TNX",      "name": "10Y Treasury Yield"},
]


# ── Cap-weighted Sectors ──────────────────────────────────────────────
SECTORS = [
    {"ticker": "XLE",  "name": "Energy"},
    {"ticker": "XLU",  "name": "Utilities"},
    {"ticker": "XLI",  "name": "Industrials"},
    {"ticker": "XLB",  "name": "Materials"},
    {"ticker": "XLP",  "name": "Consumer Staples"},
    {"ticker": "XLV",  "name": "Health Care"},
    {"ticker": "XLK",  "name": "Technology"},
    {"ticker": "XLC",  "name": "Communication Svcs"},
    {"ticker": "XLRE", "name": "Real Estate"},
    {"ticker": "XLF",  "name": "Financials"},
    {"ticker": "XLY",  "name": "Consumer Discret."},
]

# ── Equal-Weight Sectors ──────────────────────────────────────────────
SECTORS_EW = [
    {"ticker": "RSPT",  "name": "EW Technology"},
    {"ticker": "RSPC",  "name": "EW Consumer Discret."},
    {"ticker": "RSPN",  "name": "EW Industrials"},
    {"ticker": "RSPF",  "name": "EW Financials"},
    {"ticker": "RSPD",  "name": "EW Consumer Staples"},
    {"ticker": "RSPU",  "name": "EW Utilities"},
    {"ticker": "RSPR",  "name": "EW Real Estate"},
    {"ticker": "RSPH",  "name": "EW Health Care"},
    {"ticker": "RSPM",  "name": "EW Materials"},
    {"ticker": "RSPS",  "name": "EW Energy"},
    {"ticker": "RSPG",  "name": "EW Communication Svcs"},
]

# ── Commodities & Crypto ──────────────────────────────────────────────
# `ticker`    = display name shown in dashboard
# `yf_ticker` = symbol used for Yahoo Finance download
COMMODITIES_CRYPTO = [
    {"ticker": "XAGUSD", "yf_ticker": "SI=F",    "name": "Silver"},
    {"ticker": "XPTUSD", "yf_ticker": "PL=F",    "name": "Platinum"},
    {"ticker": "XAUUSD", "yf_ticker": "GC=F",    "name": "Gold"},
    {"ticker": "XPDUSD", "yf_ticker": "PA=F",    "name": "Palladium"},
    {"ticker": "WTI",    "yf_ticker": "CL=F",    "name": "Crude Oil (WTI)"},
    {"ticker": "ALIUSD", "yf_ticker": "ALI=F",   "name": "Aluminum"},
    {"ticker": "XNGUSD", "yf_ticker": "NG=F",    "name": "Natural Gas"},
    {"ticker": "XCUUSD", "yf_ticker": "HG=F",    "name": "Copper"},
    {"ticker": "SOLUSD", "yf_ticker": "SOL-USD", "name": "Solana"},
    {"ticker": "BTCUSD", "yf_ticker": "BTC-USD", "name": "Bitcoin"},
    {"ticker": "ETHUSD", "yf_ticker": "ETH-USD", "name": "Ethereum"},
]

# ── Thematic ETFs ─────────────────────────────────────────────────────
THEMATIC = [
    {"ticker": "SLV",  "name": "Silver"},
    {"ticker": "USO",  "name": "US Oil Fund"},
    {"ticker": "XOP",  "name": "Oil & Gas E&P"},
    {"ticker": "REMX", "name": "Rare Earth/Strategic Metals"},
    {"ticker": "SILJ", "name": "Junior Silver Miners"},
    {"ticker": "GDXJ", "name": "Junior Gold Miners"},
    {"ticker": "DXYZ", "name": "Destiny Tech100"},
    {"ticker": "GDX",  "name": "Gold Miners"},
    {"ticker": "COPX", "name": "Copper Miners"},
    {"ticker": "GLD",  "name": "Gold"},
    {"ticker": "UNG",  "name": "US Natural Gas"},
    {"ticker": "SIL",  "name": "Silver Miners"},
    {"ticker": "OIH",  "name": "Oil Services"},
    {"ticker": "ITA",  "name": "Aerospace & Defense"},
    {"ticker": "ITB",  "name": "US Home Construction"},
    {"ticker": "XBI",  "name": "S&P Biotech"},
    {"ticker": "LIT",  "name": "Lithium & Battery Tech"},
    {"ticker": "AIQ",  "name": "Artificial Intelligence"},
    {"ticker": "XME",  "name": "Metals & Mining"},
    {"ticker": "XRT",  "name": "S&P Retail"},
    {"ticker": "XHE",  "name": "Health Care Equipment"},
    {"ticker": "UFO",  "name": "Space"},
    {"ticker": "IGV",  "name": "Tech-Software Sector"},
    {"ticker": "KWEB", "name": "China Internet"},
    {"ticker": "SMH",  "name": "Semiconductors"},
    {"ticker": "MAGS", "name": "Magnificent Seven"},
    {"ticker": "ARKK", "name": "ARK Innovation"},
    {"ticker": "IBIT", "name": "iShares Bitcoin Trust"},
    {"ticker": "MSOS", "name": "US Cannabis"},
    {"ticker": "JETS", "name": "Global Airlines"},
    {"ticker": "ETHA", "name": "iShares Ethereum Trust"},
    {"ticker": "KRE",  "name": "Regional Banking"},
    {"ticker": "WGMI", "name": "Bitcoin Miners"},
    {"ticker": "IEZ",  "name": "US Oil Equipment & Svcs"},
    {"ticker": "TAN",  "name": "Solar Energy"},
    {"ticker": "ICLN", "name": "Global Clean Energy"},
    {"ticker": "DRNZ", "name": "Drone Economy"},
    {"ticker": "ARKG", "name": "ARK Genomic Revolution"},
    {"ticker": "CQQQ", "name": "China Technology"},
    {"ticker": "XAR",  "name": "S&P Aerospace & Defense"},
    {"ticker": "BOTZ", "name": "Robotics & AI"},
    {"ticker": "NUKZ", "name": "Nuclear Energy"},
    {"ticker": "ROBO", "name": "Global Robotics & AI"},
    {"ticker": "NLR",  "name": "Uranium & Nuclear Energy"},
    {"ticker": "ARKQ", "name": "ARK Autonomous Tech"},
    {"ticker": "URA",  "name": "Uranium"},
    {"ticker": "QTUM", "name": "Quantum Computing"},
    {"ticker": "XHB",  "name": "S&P Homebuilders"},
    {"ticker": "BLOK", "name": "Blockchain"},
    {"ticker": "CIBR", "name": "Cybersecurity"},
    {"ticker": "IAI",  "name": "US Broker-Dealers"},
    {"ticker": "ARKX", "name": "ARK Space Exploration"},
    {"ticker": "XSW",  "name": "S&P Software & Services"},
    {"ticker": "IHI",  "name": "US Medical Devices"},
    {"ticker": "IYT",  "name": "Transportation"},
    {"ticker": "WCLD", "name": "Cloud Computing"},
    {"ticker": "ARKW", "name": "ARK Next Gen Internet"},
    {"ticker": "SOCL", "name": "Social Media"},
    {"ticker": "BAI",  "name": "Broadscale AI"},
    {"ticker": "CLOU", "name": "Cloud Computing (Global X)"},
    {"ticker": "XTL",  "name": "S&P Telecom"},
    {"ticker": "SLX",  "name": "Steel"},
    {"ticker": "HACK", "name": "Cyber Security"},
    {"ticker": "PBW",  "name": "WilderHill Clean Energy"},
    {"ticker": "XHS",  "name": "Health Care Services"},
    {"ticker": "PBJ",  "name": "Food & Beverage"},
    {"ticker": "DRIV", "name": "Autonomous & EV"},
    {"ticker": "PAVE", "name": "US Infrastructure"},
    {"ticker": "KBE",  "name": "S&P Bank"},
    {"ticker": "IDGT", "name": "Indxx Digital Assets"},
    {"ticker": "PPH",  "name": "Pharmaceutical"},
    {"ticker": "DTCR", "name": "Digital Currency"},
    {"ticker": "IBUY", "name": "Online Retail"},
    {"ticker": "KIE",  "name": "S&P Insurance"},
    {"ticker": "SPPP", "name": "Platinum & Palladium"},
    {"ticker": "COPJ", "name": "Junior Copper Miners"},
    {"ticker": "SOLZ", "name": "Solana Strategy"},
    {"ticker": "HYDR", "name": "Hydrogen"},
    {"ticker": "PSIL", "name": "Psychedelics"},
]


# ── ~490 S&P 500 universe for RS Rating percentile ────────────────────
SP500_UNIVERSE = [
    # Technology (~65)
    "AAPL","MSFT","NVDA","AVGO","ORCL","CRM","AMD","QCOM","TXN","AMAT",
    "KLAC","LRCX","ADI","MU","SNPS","CDNS","FTNT","PANW","CRWD","NOW",
    "ADBE","INTU","INTC","HPQ","HPE","CSCO","IBM","DELL","STX","WDC",
    "FICO","ANSS","MPWR","TER","KEYS","ROP","TRMB","AKAM","NTAP","ZBRA",
    "VRSN","CTSH","GLW","ON","MCHP","NXPI","ENTG","PAYC","ZS","FSLR",
    "ENPH","ACN","MSCI","IT","GDDY","FISV","FIS","GPN","BR","TYL",
    "NDAQ","PLTR","WDAY","TTD","FFIV",
    # Communication Services (~24)
    "META","GOOGL","GOOG","NFLX","DIS","CMCSA","T","VZ","TMUS","CHTR",
    "EA","TTWO","NWSA","OMC","IPG","LYV","WBD","PARA",
    "MTCH","SNAP","FOXA","FOX","IAC","PINS",
    # Consumer Discretionary (~60)
    "AMZN","TSLA","HD","MCD","NKE","SBUX","LOW","TJX","BKNG","CMG",
    "MAR","HLT","ORLY","AZO","ROST","YUM","DHI","LEN","PHM","NVR",
    "TSCO","EBAY","ETSY","BBY","DRI","LVS","MGM","WYNN","HAS","MHK",
    "APTV","BWA","GM","F","GPC","KMX","GRMN","POOL","RCL","CCL",
    "NCLH","PVH","RL","LULU","ULTA","EXPE","ABNB","SNA","SWK","GNRC",
    "FND","FIVE","DKNG","PENN","CZR","NVT","BLDR","MTH","TREX","CRI",
    # Consumer Staples (~35)
    "WMT","COST","PG","KO","PEP","PM","MO","MDLZ","CL","GIS",
    "KMB","SYY","ADM","MKC","K","HRL","CAG","CPB","CHD","CLX",
    "EL","STZ","BG","TAP","KVUE","USFD","POST","INGR","WBA","SFM",
    "HSY","EPC","COTY","MNST","CELH",
    # Healthcare (~60)
    "LLY","UNH","JNJ","ABBV","MRK","ABT","TMO","DHR","PFE","AMGN",
    "GILD","ISRG","SYK","BDX","BSX","EW","ZBH","BAX","HOLX","RMD",
    "IDXX","IQV","CRL","CTLT","VTRS","HUM","CVS","CI","CNC","MOH",
    "BIIB","REGN","VRTX","MRNA","BMY","ZTS","MCK","ABC","CAH","DXCM",
    "PODD","ALGN","STE","DGX","LH","MTD","WAT","A","GEHC","HCA",
    "THC","UHS","DVA","HSIC","PDCO","INCY","EXAS","VEEV","TECH","SEM",
    # Financials (~64)
    "BRK-B","JPM","BAC","WFC","GS","MS","C","AXP","BLK","SCHW",
    "CB","PGR","AIG","MET","PRU","AFL","TRV","ALL","HIG","WRB",
    "V","MA","PYPL","COF","DFS","SYF","AMP","BEN","IVZ","TROW",
    "BX","KKR","APO","ARES","RJF","LNC","GL","PFG","MMC","AON",
    "WTW","CINF","ACGL","BK","STT","NTRS","RF","HBAN","CFG","KEY",
    "USB","PNC","TFC","FITB","MTB","ZION","CMA","ALLY","CBOE","ICE",
    "CME","SPGI","MCO","FDS",
    # Industrials (~60)
    "CAT","DE","HON","UPS","RTX","LMT","GE","MMM","EMR","ETN",
    "PH","ITW","ROK","CMI","PCAR","FDX","CSX","UNP","NSC","DAL",
    "UAL","LUV","AAL","WM","RSG","FAST","GWW","XYL","IEX","AME",
    "TDG","TT","IR","CARR","OTIS","GEV","LHX","NOC","GD","BA",
    "AXON","LDOS","SAIC","BAH","J","MAS","ALLE","HII","TXT","HUBB",
    "FLR","PWR","ACM","NDSN","CHRW","EXPD","JBHT","ODFL","SAIA","RXO",
    # Energy (~28)
    "XOM","CVX","COP","SLB","EOG","MPC","VLO","PSX","HAL","DVN",
    "FANG","HES","APA","OXY","BKR","NOV","FTI","MRO","EQT","CTRA",
    "OVV","PR","AR","RRC","SM","CNX","LNG","TRGP",
    # Materials (~35)
    "LIN","APD","SHW","ECL","PPG","NEM","FCX","NUE","STLD","CF",
    "MOS","ALB","CE","EMN","IFF","FMC","SON","PKG","IP","WRK",
    "VMC","MLM","CRH","DOW","DD","LYB","RPM","ATI","CMC","RS",
    "OLN","AXTA","SEE","OI","AVNT",
    # Utilities (~30)
    "NEE","DUK","SO","D","AEP","EXC","SRE","XEL","ES","WEC",
    "ETR","FE","PPL","EIX","PCG","AWK","AES","CMS","NI","PNW",
    "ATO","CNP","NRG","EVRG","LNT","OGS","WTRG","SWX","UGI","VST",
    # Real Estate (~30)
    "AMT","PLD","CCI","EQIX","PSA","O","WELL","DLR","SPG","AVB",
    "EQR","MAA","UDR","CPT","ESS","EXR","VICI","CBRE","ARE","BXP",
    "WY","HST","KIM","REG","FRT","NNN","STAG","CUBE","LSI","IRM",
]

# ─────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────
def calc_atr_series(df, atr_len=14):
    high       = df["High"]
    low        = df["Low"]
    close      = df["Close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / atr_len, adjust=False).mean()

def calc_rs_raw(closes):
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
    if raw_score is None or not universe_scores:
        return None
    pct = sum(1 for s in universe_scores if s < raw_score) / len(universe_scores)
    return max(1, min(99, round(pct * 98 + 1)))

def build_universe():
    print(f"[INFO] Batch-downloading {len(SP500_UNIVERSE)} universe stocks (1y)…")
    try:
        raw_uni = yf.download(
            SP500_UNIVERSE, period="1y", interval="1d",
            progress=False, auto_adjust=True, group_by="ticker"
        )
    except Exception as e:
        print(f"[WARN] Universe download failed: {e}")
        return []
    scores = []
    for t in SP500_UNIVERSE:
        try:
            closes = raw_uni[t]["Close"].dropna() if isinstance(raw_uni.columns, pd.MultiIndex) \
                     else raw_uni["Close"].dropna()
            score = calc_rs_raw(closes)
            if score is not None:
                scores.append(score)
        except Exception:
            pass
    print(f"[INFO] Universe built: {len(scores)} valid stocks")
    return scores

def calc_atr_multiple(df, atr_len=14, ma_len=50):
    if len(df) < max(atr_len, ma_len) + 1:
        return None
    atr   = calc_atr_series(df, atr_len)
    sma50 = df["Close"].rolling(ma_len).mean()
    last_close = float(df["Close"].iloc[-1])
    last_atr   = float(atr.iloc[-1])
    last_sma50 = float(sma50.iloc[-1])
    if last_close == 0 or last_atr == 0 or pd.isna(last_sma50):
        return None
    atr_pct       = last_atr / last_close * 100
    pct_from_50ma = (last_close - last_sma50) / last_sma50 * 100
    return round(pct_from_50ma / atr_pct, 2)

def calc_vars_history(df_stock, df_spy, lookback=50, ma_len=20, atr_len=14, n_bars=20):
    common  = df_stock.index.intersection(df_spy.index)
    min_len = lookback + ma_len + n_bars + 5
    if len(common) < min_len:
        return None
    s = df_stock.loc[common][["High","Low","Close"]].copy()
    b = df_spy.loc[common][["High","Low","Close"]].copy()
    atr_s      = calc_atr_series(s, atr_len)
    atr_b      = calc_atr_series(b, atr_len)
    delta_s    = s["Close"].diff() / atr_s.replace(0, float("nan"))
    delta_b    = b["Close"].diff() / atr_b.replace(0, float("nan"))
    daily_vars = (delta_s - delta_b).fillna(0)
    vars_line  = daily_vars.rolling(lookback).sum()
    ma_line    = vars_line.rolling(ma_len).mean()
    combined   = pd.DataFrame({"v": vars_line, "m": ma_line}).dropna()
    if len(combined) < n_bars:
        return None
    tail = combined.iloc[-n_bars:]
    return [{"v": round(float(r.v), 4), "m": round(float(r.m), 4)}
            for _, r in tail.iterrows()]

# ─────────────────────────────────────────────────────────────────────
# Core processing function
# `yf_ticker` field overrides `ticker` for the Yahoo Finance download
# while `ticker` is always used as the display label in the dashboard
# ─────────────────────────────────────────────────────────────────────
def process_tickers(ticker_list, df_spy, universe_scores):
    results = []
    for item in ticker_list:
        display_ticker = item["ticker"]
        dl_ticker      = item.get("yf_ticker", display_ticker)  # ← key change
        try:
            raw = yf.download(dl_ticker, period="1y", interval="1d",
                              progress=False, auto_adjust=True)
            if raw.empty or len(raw) < 50:
                print(f"[SKIP] {display_ticker} ({dl_ticker}): not enough data ({len(raw)} rows)")
                continue
            if isinstance(raw.columns, pd.MultiIndex):
                raw.columns = raw.columns.get_level_values(0)

            df = raw[["Open","High","Low","Close"]].copy()
            df.dropna(subset=["Close"], inplace=True)

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

# 5-day % change (uses close from 5 trading days ago, index -6)
try:
    close_5d   = float(df["Close"].iloc[-6])
    chg_5d     = round((price - close_5d) / close_5d * 100, 2) \
                 if close_5d != 0 else 0.0
except IndexError:
    chg_5d = None


            def ma_tag(price_above_ma, ma_is_rising):
                if price_above_ma and ma_is_rising:       return "above_up"
                elif price_above_ma and not ma_is_rising: return "above_down"
                elif not price_above_ma and ma_is_rising: return "below_up"
                else:                                     return "below_down"

            rs_raw    = calc_rs_raw(df["Close"])
            rs_rating = raw_to_rating(rs_raw, universe_scores)

            atr_multiple = calc_atr_multiple(df, atr_len=14, ma_len=50)

            vars_history = None
            if df_spy is not None:
                vars_history = calc_vars_history(
                    df[["High","Low","Close"]], df_spy,
                    lookback=50, ma_len=20, atr_len=14, n_bars=20
                )

            results.append({
                "ticker":       display_ticker,
                "name":         item["name"],
                "price":        round(price, 2),
                "daily_chg":    daily_chg,
                "intraday_chg": intraday_chg,
                "chg_5d":       chg_5d,
                "ema10":  ma_tag(price > float(last["EMA10"]),  float(last["EMA10"])  > float(prev["EMA10"])),
                "ema20":  ma_tag(price > float(last["EMA20"]),  float(last["EMA20"])  > float(prev["EMA20"])),
                "sma50":  ma_tag(price > float(last["SMA50"]),  float(last["SMA50"])  > float(prev["SMA50"])),
                "sma200": ma_tag(price > float(last["SMA200"]), float(last["SMA200"]) > float(prev["SMA200"])),
                "rs_rating":    rs_rating,
                "atr_multiple": atr_multiple,
                "vars_history": vars_history,
            })
            print(f"[OK]   {display_ticker}  RS={rs_rating}  ATRx={atr_multiple}  VARS={'ok' if vars_history else 'N/A'}")

        except Exception as exc:
            print(f"[ERR]  {display_ticker} ({dl_ticker}): {exc}")

    return results

# ─────────────────────────────────────────────────────────────────────
# Market Breadth fetch helpers
# ─────────────────────────────────────────────────────────────────────

def fetch_fear_greed():
    """CNN Fear & Greed via their dataviz API."""
    try:
        url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept":     "application/json",
            "Referer":    "https://edition.cnn.com/markets/fear-and-greed",
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        fg = data["fear_and_greed"]
        rating = fg["rating"].replace("_", " ").title()
        return {
            "score":      round(float(fg["score"]), 1),
            "rating":     rating,
            "prev_close": round(float(fg["previous_close"]), 1),
            "prev_week":  round(float(fg.get("previous_1_week", fg["score"])), 1),
        }
    except Exception as e:
        print(f"[WARN] Fear & Greed: {e}")
        return None


def fetch_naaim():
    """NAAIM Exposure Index — scrape the weekly data table."""
    try:
        url = "https://naaim.org/programs/naaim-exposure-index/"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8")
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL | re.IGNORECASE)
        for row in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            cells = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]
            if len(cells) >= 2:
                try:
                    val = float(cells[1].replace(',', ''))
                    if -50 <= val <= 300:
                        return {"value": round(val, 2), "date": cells[0]}
                except (ValueError, IndexError):
                    pass
        return None
    except Exception as e:
        print(f"[WARN] NAAIM: {e}")
        return None


def fetch_cboe_pcr():
    """CBOE Total Put/Call Ratio — try CDN CSV endpoint."""
    urls = [
        "https://cdn.cboe.com/api/global/us_options_distribution/daily_stats.csv",
        "https://www.cboe.com/data/us-options-market-statistics-daily-download/",
    ]
    for url in urls:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                content = resp.read().decode("utf-8")
            df_cboe = pd.read_csv(io.StringIO(content))
            df_cboe.columns = [c.strip() for c in df_cboe.columns]
            # Find the Total P/C column (name varies by CBOE version)
            pcr_col = None
            for col in df_cboe.columns:
                cu = col.upper()
                if ("TOTAL" in cu or "ALL" in cu) and ("P/C" in cu or "PUT" in cu or "RATIO" in cu):
                    pcr_col = col
                    break
            # Fallback: last numeric column
            if pcr_col is None:
                for col in reversed(df_cboe.columns):
                    try:
                        pd.to_numeric(df_cboe[col].dropna())
                        pcr_col = col
                        break
                    except Exception:
                        pass
            if pcr_col:
                clean = df_cboe.dropna(subset=[pcr_col])
                if not clean.empty:
                    latest = clean.iloc[-1]
                    return {
                        "value": round(float(latest[pcr_col]), 2),
                        "date":  str(latest.iloc[0]),
                    }
        except Exception as e:
            print(f"[WARN] CBOE P/C ({url[:50]}…): {e}")
    return None


def fetch_mmtw_mmfi():
    """% Stocks above 20-day (^MMTW) and 50-day (^MMFI) MA via yfinance."""
    result = {}
    for sym, key in [("^MMTW", "mmtw"), ("^MMFI", "mmfi")]:
        try:
            raw = yf.download(sym, period="5d", interval="1d",
                              progress=False, auto_adjust=True)
            if raw.empty:
                result[key] = None
                continue
            if isinstance(raw.columns, pd.MultiIndex):
                raw.columns = raw.columns.get_level_values(0)
            result[key] = round(float(raw["Close"].dropna().iloc[-1]), 2)
        except Exception as e:
            print(f"[WARN] {sym}: {e}")
            result[key] = None
    return result


def fetch_52wk_highs_lows():
    """52-week Highs & Lows — scrape Barchart summary table."""
    try:
        url = "https://www.barchart.com/stocks/highs-lows/summary"
        req = urllib.request.Request(url, headers={
            "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                               "AppleWebKit/537.36 (KHTML, like Gecko) "
                               "Chrome/124.0.0.0 Safari/537.36",
            "Accept":          "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8")
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL | re.IGNORECASE)
        highs = lows = None
        for row in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            cells = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]
            row_text = ' '.join(cells).lower()
            if 'all' in row_text or 'total' in row_text:
                nums = []
                for c in cells:
                    try:
                        n = int(c.replace(',', ''))
                        if n >= 0:
                            nums.append(n)
                    except ValueError:
                        pass
                if len(nums) >= 2:
                    highs, lows = nums[0], nums[1]
                    break
        return {"highs": highs, "lows": lows}
    except Exception as e:
        print(f"[WARN] 52W H/L Barchart: {e}")
        return {"highs": None, "lows": None}


# ═════════════════════════════════════════════════════════════════════
os.makedirs("data", exist_ok=True)

print("[INFO] Fetching SPY benchmark for VARS…")
try:
    spy_raw = yf.download("SPY", period="1y", interval="1d",
                          progress=False, auto_adjust=True)
    if isinstance(spy_raw.columns, pd.MultiIndex):
        spy_raw.columns = spy_raw.columns.get_level_values(0)
    df_spy = spy_raw[["High","Low","Close"]].dropna()
    print(f"[INFO] SPY ready ({len(df_spy)} rows)")
except Exception as e:
    df_spy = None
    print(f"[WARN] SPY fetch failed: {e}")

universe_scores = build_universe()

print("\n── Indices ──────────────────────────────────────────")
indices_results = process_tickers(INDICES, df_spy, universe_scores)

print("\n── Sectors ──────────────────────────────────────────")
sectors_results = process_tickers(SECTORS, df_spy, universe_scores)

print("\n── EW Sectors ───────────────────────────────────────")
sectors_ew_results = process_tickers(SECTORS_EW, df_spy, universe_scores)

# ─────────────────────────────────────────────────────────────────────
# Market Breadth fetch helpers
# ─────────────────────────────────────────────────────────────────────

def fetch_fear_greed():
    """CNN Fear & Greed via their dataviz API."""
    try:
        url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept":     "application/json",
            "Referer":    "https://edition.cnn.com/markets/fear-and-greed",
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        fg = data["fear_and_greed"]
        rating = fg["rating"].replace("_", " ").title()
        return {
            "score":      round(float(fg["score"]), 1),
            "rating":     rating,
            "prev_close": round(float(fg["previous_close"]), 1),
            "prev_week":  round(float(fg.get("previous_1_week", fg["score"])), 1),
        }
    except Exception as e:
        print(f"[WARN] Fear & Greed: {e}")
        return None


def fetch_naaim():
    """NAAIM Exposure Index — scrape the weekly data table."""
    try:
        url = "https://naaim.org/programs/naaim-exposure-index/"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8")
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL | re.IGNORECASE)
        for row in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            cells = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]
            if len(cells) >= 2:
                try:
                    val = float(cells[1].replace(',', ''))
                    if -50 <= val <= 300:
                        return {"value": round(val, 2), "date": cells[0]}
                except (ValueError, IndexError):
                    pass
        return None
    except Exception as e:
        print(f"[WARN] NAAIM: {e}")
        return None


def fetch_cboe_pcr():
    """CBOE Total Put/Call Ratio — try CDN CSV endpoint."""
    urls = [
        "https://cdn.cboe.com/api/global/us_options_distribution/daily_stats.csv",
        "https://www.cboe.com/data/us-options-market-statistics-daily-download/",
    ]
    for url in urls:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                content = resp.read().decode("utf-8")
            df_cboe = pd.read_csv(io.StringIO(content))
            df_cboe.columns = [c.strip() for c in df_cboe.columns]
            # Find the Total P/C column (name varies by CBOE version)
            pcr_col = None
            for col in df_cboe.columns:
                cu = col.upper()
                if ("TOTAL" in cu or "ALL" in cu) and ("P/C" in cu or "PUT" in cu or "RATIO" in cu):
                    pcr_col = col
                    break
            # Fallback: last numeric column
            if pcr_col is None:
                for col in reversed(df_cboe.columns):
                    try:
                        pd.to_numeric(df_cboe[col].dropna())
                        pcr_col = col
                        break
                    except Exception:
                        pass
            if pcr_col:
                clean = df_cboe.dropna(subset=[pcr_col])
                if not clean.empty:
                    latest = clean.iloc[-1]
                    return {
                        "value": round(float(latest[pcr_col]), 2),
                        "date":  str(latest.iloc[0]),
                    }
        except Exception as e:
            print(f"[WARN] CBOE P/C ({url[:50]}…): {e}")
    return None


def fetch_mmtw_mmfi():
    """% Stocks above 20-day (^MMTW) and 50-day (^MMFI) MA via yfinance."""
    result = {}
    for sym, key in [("^MMTW", "mmtw"), ("^MMFI", "mmfi")]:
        try:
            raw = yf.download(sym, period="5d", interval="1d",
                              progress=False, auto_adjust=True)
            if raw.empty:
                result[key] = None
                continue
            if isinstance(raw.columns, pd.MultiIndex):
                raw.columns = raw.columns.get_level_values(0)
            result[key] = round(float(raw["Close"].dropna().iloc[-1]), 2)
        except Exception as e:
            print(f"[WARN] {sym}: {e}")
            result[key] = None
    return result


def fetch_52wk_highs_lows():
    """52-week Highs & Lows — scrape Barchart summary table."""
    try:
        url = "https://www.barchart.com/stocks/highs-lows/summary"
        req = urllib.request.Request(url, headers={
            "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                               "AppleWebKit/537.36 (KHTML, like Gecko) "
                               "Chrome/124.0.0.0 Safari/537.36",
            "Accept":          "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8")
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL | re.IGNORECASE)
        highs = lows = None
        for row in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            cells = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]
            row_text = ' '.join(cells).lower()
            if 'all' in row_text or 'total' in row_text:
                nums = []
                for c in cells:
                    try:
                        n = int(c.replace(',', ''))
                        if n >= 0:
                            nums.append(n)
                    except ValueError:
                        pass
                if len(nums) >= 2:
                    highs, lows = nums[0], nums[1]
                    break
        return {"highs": highs, "lows": lows}
    except Exception as e:
        print(f"[WARN] 52W H/L Barchart: {e}")
        return {"highs": None, "lows": None}

print("\n── Thematic ─────────────────────────────────────────")
thematic_results = process_tickers(THEMATIC, df_spy, universe_scores)

print("\n── Market Breadth ───────────────────────────────────")
fear_greed  = fetch_fear_greed()
naaim       = fetch_naaim()
cboe_pcr    = fetch_cboe_pcr()
mmtw_mmfi   = fetch_mmtw_mmfi()
highs_lows  = fetch_52wk_highs_lows()

breadth = {
    "fear_greed": fear_greed,
    "naaim":      naaim,
    "cboe_pcr":   cboe_pcr,
    "mmtw":       mmtw_mmfi.get("mmtw"),
    "mmfi":       mmtw_mmfi.get("mmfi"),
    "highs":      highs_lows.get("highs"),
    "lows":       highs_lows.get("lows"),
}
print(f"[OK]   Breadth  F&G={fear_greed and fear_greed['score']}  "
      f"NAAIM={naaim and naaim['value']}  "
      f"PCR={cboe_pcr and cboe_pcr['value']}  "
      f"MMTW={mmtw_mmfi.get('mmtw')}  MMFI={mmtw_mmfi.get('mmfi')}  "
      f"Highs={highs_lows.get('highs')}  Lows={highs_lows.get('lows')}")


print("\n── Commodities & Crypto ─────────────────────────────")
commodities_results = process_tickers(COMMODITIES_CRYPTO, df_spy, universe_scores)

hkt     = pytz.timezone("Asia/Hong_Kong")
updated = datetime.now(hkt).strftime("%d %b %Y, %H:%M HKT")

with open("data/indices.json", "w") as fh:
    json.dump({
        "updated":      updated,
        "breadth":      breadth,
        "indices":      indices_results,
        "sectors":      sectors_results,
        "sectors_ew":   sectors_ew_results,
        "commodities":  commodities_results,
        "thematic":     thematic_results,
    }, fh, indent=2)

print(f"\n✅  Saved → data/indices.json  ({updated})")
print(f"    Breadth         : 7 indicators")
print(f"    Indices         : {len(indices_results)} tickers")
print(f"    Sectors         : {len(sectors_results)} tickers")
print(f"    EW Sectors      : {len(sectors_ew_results)} tickers")
print(f"    Commodities     : {len(commodities_results)} tickers")
print(f"    Thematic        : {len(thematic_results)} tickers")

