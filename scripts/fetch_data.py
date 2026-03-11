#!/usr/bin/env python3
"""
fetch_data.py — Clement Wong data fetcher
Pulls EOD/prev-close data from Yahoo Finance via yfinance and writes data/snapshot.json
Run: python scripts/fetch_data.py
"""

import json
import math
import sys
from datetime import datetime, timezone
import pytz

import yfinance as yf

# ─── INSTRUMENT REGISTRY ────────────────────────────────────────────────────
GROUPS = [
    {
        "id": "futures",
        "title": "US Index Futures",
        "section": "macro",
        "col0": "Contract",
        "instruments": [
            {"ticker": "ES=F",  "label": "ES=F · S&P 500 Futures"},
            {"ticker": "NQ=F",  "label": "NQ=F · Nasdaq 100 Futures"},
            {"ticker": "YM=F",  "label": "YM=F · Dow Futures"},
            {"ticker": "RTY=F", "label": "RTY=F · Russell 2000 Futures"},
        ],
    },
    {
        "id": "vix_dollar",
        "title": "Volatility & Dollar",
        "section": "macro",
        "col0": "Instrument",
        "instruments": [
            {"ticker": "^VIX",  "label": "VIX · CBOE Volatility Index"},
            {"ticker": "^VVIX", "label": "VVIX · Vol of Vol Index"},
            {"ticker": "DX-Y.NYB", "label": "DXY · US Dollar Index"},
            {"ticker": "UUP",   "label": "UUP · Invesco Dollar ETF"},
        ],
    },
    {
        "id": "crypto",
        "title": "Crypto",
        "section": "macro",
        "col0": "Asset",
        "instruments": [
            {"ticker": "BTC-USD", "label": "BTC-USD · Bitcoin"},
            {"ticker": "ETH-USD", "label": "ETH-USD · Ethereum"},
            {"ticker": "BNB-USD", "label": "BNB-USD · BNB"},
            {"ticker": "SOL-USD", "label": "SOL-USD · Solana"},
            {"ticker": "XRP-USD", "label": "XRP-USD · Ripple"},
        ],
    },
    {
        "id": "metals",
        "title": "Precious & Base Metals",
        "section": "macro",
        "col0": "Metal",
        "instruments": [
            {"ticker": "GC=F", "label": "GC=F · Gold Futures"},
            {"ticker": "SI=F", "label": "SI=F · Silver Futures"},
            {"ticker": "HG=F", "label": "HG=F · Copper Futures"},
            {"ticker": "PL=F", "label": "PL=F · Platinum Futures"},
            {"ticker": "PA=F", "label": "PA=F · Palladium Futures"},
        ],
    },
    {
        "id": "energy",
        "title": "Energy Commodities",
        "section": "macro",
        "col0": "Commodity",
        "instruments": [
            {"ticker": "CL=F", "label": "CL=F · WTI Crude Oil"},
            {"ticker": "BZ=F", "label": "BZ=F · Brent Crude Oil"},
            {"ticker": "NG=F", "label": "NG=F · Natural Gas"},
            {"ticker": "RB=F", "label": "RB=F · RBOB Gasoline"},
        ],
    },
    {
        "id": "yields",
        "title": "US Treasury Yields",
        "section": "macro",
        "col0": "Tenor",
        "is_yield": True,
        "instruments": [
            {"ticker": "^IRX",  "label": "3M · 3-Month T-Bill"},
            {"ticker": "^FVX",  "label": "5Y · 5-Year Note"},
            {"ticker": "^TNX",  "label": "10Y · 10-Year Note"},
            {"ticker": "^TYX",  "label": "30Y · 30-Year Bond"},
        ],
    },
    {
        "id": "global",
        "title": "Global Market Indices",
        "section": "macro",
        "col0": "Index",
        "instruments": [
            {"ticker": "^GSPC",  "label": "SPX · S&P 500"},
            {"ticker": "^HSI",   "label": "HSI · Hang Seng"},
            {"ticker": "^N225",  "label": "N225 · Nikkei 225"},
            {"ticker": "^GDAXI", "label": "DAX · Germany 40"},
            {"ticker": "^FTSE",  "label": "FTSE · UK 100"},
            {"ticker": "^AXJO",  "label": "ASX · Australia 200"},
            {"ticker": "^STOXX50E", "label": "EURO STOXX 50"},
        ],
    },
    # ── EQUITIES ──────────────────────────────────────────────────────────
    {
        "id": "major_etfs",
        "title": "Major ETF Stats",
        "section": "equities",
        "col0": "ETF",
        "has_trend": True,
        "instruments": [
            {"ticker": "SPY",  "label": "SPY · SPDR S&P 500"},
            {"ticker": "QQQ",  "label": "QQQ · Nasdaq 100"},
            {"ticker": "IWM",  "label": "IWM · Russell 2000"},
            {"ticker": "DIA",  "label": "DIA · Dow Jones"},
            {"ticker": "VTI",  "label": "VTI · Total Market"},
            {"ticker": "EEM",  "label": "EEM · Emerging Markets"},
            {"ticker": "GLD",  "label": "GLD · Gold ETF"},
            {"ticker": "TLT",  "label": "TLT · 20Y Treasury"},
            {"ticker": "HYG",  "label": "HYG · High Yield Bond"},
            {"ticker": "LQD",  "label": "LQD · IG Corp Bond"},
        ],
    },
    {
        "id": "sp500_sectors",
        "title": "S&P 500 Sub-Sector — Ranked by 1W",
        "section": "equities",
        "col0": "Sector",
        "has_trend": True,
        "has_rank": True,
        "sort_by": "1w",
        "instruments": [
            {"ticker": "XLK",  "label": "XLK · Technology",          "holdings": "AAPL MSFT NVDA AVGO ORCL"},
            {"ticker": "XLV",  "label": "XLV · Health Care",          "holdings": "UNH JNJ LLY ABBV MRK"},
            {"ticker": "XLP",  "label": "XLP · Consumer Staples",     "holdings": "PG KO COST PM MO"},
            {"ticker": "XLU",  "label": "XLU · Utilities",            "holdings": "NEE DUK SO D SRE"},
            {"ticker": "XLF",  "label": "XLF · Financials",           "holdings": "JPM BAC WFC GS MS"},
            {"ticker": "XLI",  "label": "XLI · Industrials",          "holdings": "RTX HON UPS CAT DE"},
            {"ticker": "XLE",  "label": "XLE · Energy",               "holdings": "XOM CVX EOG COP SLB"},
            {"ticker": "XLB",  "label": "XLB · Materials",            "holdings": "LIN APD SHW FCX NEM"},
            {"ticker": "XLY",  "label": "XLY · Consumer Discret.",    "holdings": "AMZN TSLA MCD HD BKNG"},
            {"ticker": "XLRE", "label": "XLRE · Real Estate",         "holdings": "PLD AMT EQIX CCI SPG"},
            {"ticker": "XLC",  "label": "XLC · Comm. Services",       "holdings": "META GOOGL NFLX DIS T"},
        ],
    },
    {
        "id": "sp500_ew_sectors",
        "title": "S&P 500 EW Sub-Sector — Ranked by 1W",
        "section": "equities",
        "col0": "EW Sector",
        "has_trend": True,
        "has_rank": True,
        "sort_by": "1w",
        "instruments": [
            {"ticker": "RSPT",  "label": "RSPT · EW Technology",       "holdings": "AAPL MSFT NVDA"},
            {"ticker": "RSPH",  "label": "RSPH · EW Health Care",      "holdings": "UNH JNJ LLY"},
            {"ticker": "RSPS",  "label": "RSPS · EW Cons. Staples",    "holdings": "PG KO COST"},
            {"ticker": "RSPU",  "label": "RSPU · EW Utilities",        "holdings": "NEE DUK SO"},
            {"ticker": "RYF",   "label": "RYF · EW Financials",        "holdings": "JPM BAC WFC"},
            {"ticker": "RGI",   "label": "RGI · EW Industrials",       "holdings": "RTX HON UPS"},
            {"ticker": "RYE",   "label": "RYE · EW Energy",            "holdings": "XOM CVX EOG"},
            {"ticker": "RTM",   "label": "RTM · EW Materials",         "holdings": "LIN APD SHW"},
            {"ticker": "RCD",   "label": "RCD · EW Cons. Discret.",    "holdings": "AMZN TSLA MCD"},
            {"ticker": "EWRE",  "label": "EWRE · EW Real Estate",      "holdings": "PLD AMT EQIX"},
            {"ticker": "EWCO",  "label": "EWCO · EW Comm. Services",   "holdings": "META GOOGL NFLX"},
        ],
    },
    {
        "id": "thematic",
        "title": "Top 10 Thematic Sectors — Ranked by 1W",
        "section": "equities",
        "col0": "Theme / ETF",
        "has_trend": True,
        "has_rank": True,
        "has_price": True,
        "sort_by": "1w",
        "top_n": 10,
        "instruments": [
            {"ticker": "KWEB",  "label": "KWEB · China Internet",      "holdings": "BABA PDD JD BIDU TCEHY"},
            {"ticker": "CQQQ",  "label": "CQQQ · China Tech",          "holdings": "BABA TCEHY BIDU"},
            {"ticker": "ARKK",  "label": "ARKK · ARK Innovation",      "holdings": "TSLA CRISPR ROKU COIN"},
            {"ticker": "GDX",   "label": "GDX · Gold Miners",          "holdings": "NEM GOLD AEM WPM AGI"},
            {"ticker": "SOXX",  "label": "SOXX · Semiconductors",      "holdings": "NVDA AMD QCOM INTC AMAT"},
            {"ticker": "IBB",   "label": "IBB · Biotech",              "holdings": "GILD BIIB VRTX REGN MRNA"},
            {"ticker": "XBI",   "label": "XBI · S&P Biotech",          "holdings": "SMMT RXRX DNLI SAGE"},
            {"ticker": "ICLN",  "label": "ICLN · Clean Energy",        "holdings": "ENPH NEE BEP FSLR"},
            {"ticker": "XHB",   "label": "XHB · Homebuilders",         "holdings": "DHI LEN NVR TOL PHM"},
            {"ticker": "LIT",   "label": "LIT · Lithium & Battery",    "holdings": "ALB SQM LTHM LAC"},
            {"ticker": "ROBO",  "label": "ROBO · Robotics & AI",       "holdings": "ISRG ABB FANUY IRBT"},
            {"ticker": "BOTZ",  "label": "BOTZ · Global Robotics",     "holdings": "NVDA ABB FANUY ISRG"},
            {"ticker": "HACK",  "label": "HACK · Cybersecurity",       "holdings": "PANW CRWD FTNT ZS"},
            {"ticker": "SKYY",  "label": "SKYY · Cloud Computing",     "holdings": "AMZN MSFT GOOGL SNOW"},
            {"ticker": "JETS",  "label": "JETS · Airlines",            "holdings": "AAL DAL UAL LUV"},
        ],
    },
    {
        "id": "country_etfs",
        "title": "Country ETFs — Top 10 by 1W Performance",
        "section": "equities",
        "col0": "Country / ETF",
        "has_trend": True,
        "has_rank": True,
        "has_price": True,
        "sort_by": "1w",
        "top_n": 10,
        "instruments": [
            {"ticker": "MCHI", "label": "MCHI · China",         "holdings": "TENCENT BABA MEITUAN"},
            {"ticker": "EWG",  "label": "EWG · Germany",        "holdings": "SAP SIE DTE"},
            {"ticker": "EWJ",  "label": "EWJ · Japan",          "holdings": "SONY TM 7203"},
            {"ticker": "EWA",  "label": "EWA · Australia",      "holdings": "BHP RIO CSL"},
            {"ticker": "EWC",  "label": "EWC · Canada",         "holdings": "RY TD ENB"},
            {"ticker": "EWY",  "label": "EWY · South Korea",    "holdings": "SAMSUNG SK HYUNDAI"},
            {"ticker": "EWT",  "label": "EWT · Taiwan",         "holdings": "TSMC MEDI UMC"},
            {"ticker": "EWZ",  "label": "EWZ · Brazil",         "holdings": "VALE PETR ITUB"},
            {"ticker": "EWU",  "label": "EWU · UK",             "holdings": "SHEL AZN HSBC"},
            {"ticker": "EWI",  "label": "EWI · Italy",          "holdings": "ENI ISP UCG"},
            {"ticker": "EWL",  "label": "EWL · Switzerland",    "holdings": "NESN ROG NOVN"},
            {"ticker": "EWP",  "label": "EWP · Spain",          "holdings": "ITX SAN BBVA"},
            {"ticker": "EWQ",  "label": "EWQ · France",         "holdings": "LVMH TTE BNP"},
            {"ticker": "EWS",  "label": "EWS · Singapore",      "holdings": "DBS OCBC UOB"},
            {"ticker": "EWH",  "label": "EWH · Hong Kong",      "holdings": "AIA HSBC MTR"},
            {"ticker": "INDA", "label": "INDA · India",         "holdings": "RELIANCE INFY TCS"},
            {"ticker": "EWW",  "label": "EWW · Mexico",         "holdings": "AMXL GFNORTEO FEMSAUBD"},
        ],
    },
]

# Breadth/sentiment tickers (fetched separately)
BREADTH_TICKERS = {
    "vix":      "^VIX",
    "sp500":    "^GSPC",
    "put_call": None,   # Not available via yfinance – will be placeholder
}

# ─── HELPERS ────────────────────────────────────────────────────────────────

def safe_float(v):
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return None


def pct_str(val, decimals=2):
    if val is None:
        return None
    sign = "+" if val >= 0 else ""
    return f"{sign}{val:.{decimals}f}%"


def price_str(val, is_yield=False):
    if val is None:
        return None
    if is_yield:
        return f"{val:.3f}%"
    if val >= 1000:
        return f"{val:,.2f}"
    return f"{val:.4g}"


def trend_arrow(pct_1w):
    if pct_1w is None:
        return "→"
    if pct_1w > 1:
        return "↗"
    if pct_1w < -1:
        return "↘"
    return "→"


def bars_array(history_closes):
    """Return list of 5 ints: +1 / -1 / 0 for the last 5 daily candles."""
    if not history_closes or len(history_closes) < 2:
        return [0, 0, 0, 0, 0]
    closes = list(history_closes)[-6:]   # up to 6 values → 5 diffs
    bars = []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        bars.append(1 if diff > 0 else (-1 if diff < 0 else 0))
    # pad / trim to exactly 5
    while len(bars) < 5:
        bars.insert(0, 0)
    return bars[-5:]


# ─── FETCH ──────────────────────────────────────────────────────────────────

def fetch_instruments(instruments, is_yield=False):
    tickers = [i["ticker"] for i in instruments]
    print(f"  Fetching {len(tickers)} tickers: {tickers}")

    # Download 1 year of daily history for all tickers at once
    try:
        hist = yf.download(
            tickers,
            period="1y",
            interval="1d",
            auto_adjust=True,
            progress=False,
            group_by="ticker",
        )
    except Exception as e:
        print(f"  WARNING: download failed: {e}")
        hist = None

    rows = []
    for instr in instruments:
        tk = instr["ticker"]
        label = instr["label"]
        holdings = instr.get("holdings", "")

        try:
            obj = yf.Ticker(tk)
            info = obj.fast_info

            price = safe_float(getattr(info, "last_price", None))
            prev_close = safe_float(getattr(info, "previous_close", None))
            week52_high = safe_float(getattr(info, "year_high", None))

            if price is None:
                # fallback: use hist last close
                try:
                    if len(tickers) == 1:
                        c = hist["Close"]
                    else:
                        c = hist["Close"][tk]
                    price = safe_float(c.dropna().iloc[-1])
                    prev_close = safe_float(c.dropna().iloc[-2])
                except Exception:
                    pass

            # 1D change
            if price and prev_close:
                chg_1d = (price - prev_close) / prev_close * 100
            else:
                chg_1d = None

            # Pull history closes for 1W, YTD, 52W Hi%
            try:
                if len(tickers) == 1:
                    closes = hist["Close"].dropna()
                else:
                    closes = hist["Close"][tk].dropna()
                closes = closes.tolist()
            except Exception:
                closes = []

            # 1W (5 trading days ago)
            chg_1w = None
            if len(closes) >= 6:
                ref = closes[-6]
                if ref and price:
                    chg_1w = (price - ref) / ref * 100

            # YTD: first close of current year
            try:
                if len(tickers) == 1:
                    idx = hist["Close"].dropna().index
                else:
                    idx = hist["Close"][tk].dropna().index
                ytd_closes = [
                    (i, v) for i, v in zip(idx, closes)
                    if i.year == datetime.now().year
                ]
                if ytd_closes and price:
                    ytd_ref = ytd_closes[0][1]
                    chg_ytd = (price - ytd_ref) / ytd_ref * 100
                else:
                    chg_ytd = None
            except Exception:
                chg_ytd = None

            # 52W Hi%
            if week52_high is None and closes:
                week52_high = max(closes)
            chg_52w_hi = None
            if week52_high and price:
                chg_52w_hi = (price - week52_high) / week52_high * 100

            bars = bars_array(closes)

            row = {
                "ticker": tk,
                "label": label,
                "holdings": holdings,
                "price": price_str(price, is_yield),
                "price_raw": price,
                "chg_1d": pct_str(chg_1d),
                "chg_1d_raw": chg_1d,
                "chg_1w": pct_str(chg_1w),
                "chg_1w_raw": chg_1w,
                "chg_52w_hi": pct_str(chg_52w_hi),
                "chg_52w_hi_raw": chg_52w_hi,
                "chg_ytd": pct_str(chg_ytd),
                "chg_ytd_raw": chg_ytd,
                "bars": bars,
                "trend": trend_arrow(chg_1w),
            }
            # For yields: 1D in bps
            if is_yield and chg_1d is not None and price:
                bps = chg_1d / 100 * (price / 100) * 10000  # rough bps
                row["chg_1d"] = f"{'+' if bps >= 0 else ''}{bps:.1f}bps"

            rows.append(row)
            print(f"    ✓ {tk}: price={price_str(price, is_yield)}  1D={pct_str(chg_1d)}  1W={pct_str(chg_1w)}")

        except Exception as e:
            print(f"    ✗ {tk}: {e}")
            rows.append({
                "ticker": tk, "label": label, "holdings": holdings,
                "price": None, "price_raw": None,
                "chg_1d": None, "chg_1d_raw": None,
                "chg_1w": None, "chg_1w_raw": None,
                "chg_52w_hi": None, "chg_52w_hi_raw": None,
                "chg_ytd": None, "chg_ytd_raw": None,
                "bars": [0, 0, 0, 0, 0], "trend": "→",
            })

    return rows


def fetch_breadth():
    """Fetch VIX and S&P 500 data to build sentiment panel."""
    print("  Fetching breadth data...")
    cards = []

    try:
        vix = yf.Ticker("^VIX")
        vix_info = vix.fast_info
        vix_price = safe_float(getattr(vix_info, "last_price", None))
    except Exception:
        vix_price = None

    try:
        sp = yf.Ticker("^GSPC")
        sp_info = sp.fast_info
        sp_price = safe_float(getattr(sp_info, "last_price", None))
        sp_prev = safe_float(getattr(sp_info, "previous_close", None))
        sp_year_high = safe_float(getattr(sp_info, "year_high", None))
        sp_year_low = safe_float(getattr(sp_info, "year_low", None))
    except Exception:
        sp_price = sp_prev = sp_year_high = sp_year_low = None

    # Pull S&P 500 component proxies for breadth approximation
    # We use sector ETF advances/declines as proxy for breadth
    sector_tickers = ["XLK","XLV","XLP","XLU","XLF","XLI","XLE","XLB","XLY","XLRE","XLC"]
    up_sectors = 0
    try:
        for tk in sector_tickers:
            t = yf.Ticker(tk)
            fi = t.fast_info
            pr = safe_float(getattr(fi, "last_price", None))
            pc = safe_float(getattr(fi, "previous_close", None))
            if pr and pc and pr > pc:
                up_sectors += 1
    except Exception:
        up_sectors = 5

    adv_pct = round(up_sectors / len(sector_tickers) * 100)
    dec_pct = 100 - adv_pct

    # VIX level → fear/greed proxy
    fear_greed = 50
    if vix_price:
        if vix_price < 12:   fear_greed = 80
        elif vix_price < 16: fear_greed = 65
        elif vix_price < 20: fear_greed = 50
        elif vix_price < 25: fear_greed = 35
        elif vix_price < 30: fear_greed = 20
        else:                fear_greed = 10

    # S&P position within 52W range
    sp_range_pct = 50
    if sp_year_high and sp_year_low and sp_price:
        rng = sp_year_high - sp_year_low
        if rng > 0:
            sp_range_pct = round((sp_price - sp_year_low) / rng * 100)

    above_50ma_proxy = max(20, min(80, sp_range_pct + 5))

    cards = [
        {"label": "% Sectors Above 50MA", "value": f"{above_50ma_proxy}%",
         "dir": "up" if above_50ma_proxy > 50 else "dn",
         "sub": "S&P 500 sector proxy", "pct": above_50ma_proxy},

        {"label": "Advancing Sectors (1D)", "value": f"{up_sectors}/{len(sector_tickers)}",
         "dir": "up" if up_sectors >= 6 else "dn",
         "sub": "S&P 500 GICS sectors", "pct": adv_pct},

        {"label": "VIX · CBOE Volatility", "value": f"{vix_price:.2f}" if vix_price else "—",
         "dir": "dn" if (vix_price or 20) > 20 else "up",
         "sub": "Elevated > 20", "pct": min(int(vix_price or 20), 100) if vix_price else 20},

        {"label": "Fear & Greed (VIX proxy)", "value": str(fear_greed),
         "dir": "up" if fear_greed > 50 else "dn",
         "sub": "0=Extreme Fear, 100=Greed", "pct": fear_greed},

        {"label": "S&P 52W Range Position", "value": f"{sp_range_pct}%",
         "dir": "up" if sp_range_pct > 50 else "dn",
         "sub": "% between 52W Lo→Hi", "pct": sp_range_pct},

        {"label": "S&P 500 Price", "value": f"{sp_price:,.2f}" if sp_price else "—",
         "dir": "up" if (sp_price and sp_prev and sp_price > sp_prev) else "dn",
         "sub": "Prev close basis", "pct": 50},

        {"label": "S&P 52W High", "value": f"{sp_year_high:,.2f}" if sp_year_high else "—",
         "dir": "up", "sub": "52-Week High", "pct": 90},

        {"label": "S&P 52W Low", "value": f"{sp_year_low:,.2f}" if sp_year_low else "—",
         "dir": "dn", "sub": "52-Week Low", "pct": 10},

        {"label": "Declining Sectors (1D)", "value": f"{len(sector_tickers)-up_sectors}/{len(sector_tickers)}",
         "dir": "dn" if up_sectors < 6 else "up",
         "sub": "S&P 500 GICS sectors", "pct": dec_pct},

        {"label": "Volatility Regime", "value": "HIGH" if (vix_price or 0) > 25 else ("MED" if (vix_price or 0) > 18 else "LOW"),
         "dir": "dn" if (vix_price or 0) > 20 else "up",
         "sub": f"VIX: {vix_price:.1f}" if vix_price else "—", "pct": min(int((vix_price or 20) * 2), 100)},
    ]

    return cards


# ─── MAIN ───────────────────────────────────────────────────────────────────

def main():
    hkt = pytz.timezone("Asia/Singapore")
    now_hkt = datetime.now(hkt)
    timestamp = now_hkt.strftime("%Y-%m-%d %H:%M SGT")
    print(f"\n{'='*60}")
    print(f"  Clement Wong — Data Fetch")
    print(f"  {timestamp}")
    print(f"{'='*60}\n")

    snapshot = {
        "updated": timestamp,
        "updated_iso": datetime.now(timezone.utc).isoformat(),
        "groups": [],
        "breadth": [],
    }

    for group in GROUPS:
        print(f"\n[{group['id']}] {group['title']}")
        rows = fetch_instruments(
            group["instruments"],
            is_yield=group.get("is_yield", False),
        )

        # Sort if needed
        if group.get("sort_by") == "1w":
            rows.sort(key=lambda r: r.get("chg_1w_raw") or -999, reverse=True)

        # Trim to top_n
        if group.get("top_n"):
            rows = rows[:group["top_n"]]

        snapshot["groups"].append({
            "id": group["id"],
            "title": group["title"],
            "section": group["section"],
            "col0": group["col0"],
            "has_trend": group.get("has_trend", False),
            "has_rank": group.get("has_rank", False),
            "has_price": group.get("has_price", False),
            "is_yield": group.get("is_yield", False),
            "rows": rows,
        })

    print("\n[breadth] Sentiment & Internals")
    snapshot["breadth"] = fetch_breadth()

    out_path = "data/snapshot.json"
    with open(out_path, "w") as f:
        json.dump(snapshot, f, indent=2)

    print(f"\n✓ Written {out_path}")
    print(f"  Groups: {len(snapshot['groups'])}")
    total_rows = sum(len(g['rows']) for g in snapshot['groups'])
    print(f"  Instruments: {total_rows}")
    print(f"  Breadth cards: {len(snapshot['breadth'])}")
    print(f"\nDone — {timestamp}\n")


if __name__ == "__main__":
    main()
