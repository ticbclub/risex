#!/usr/bin/env python3
"""
RISEx Live Stats Dashboard  ·  v5 (experimental)
================================================
Everything v4 has, plus the new features:
  - Liquidation heatmap per market (positions clustered around liq prices)
  - Long/Short ratio per market with historical chart
  - Hyperliquid added to funding rates comparison (3 DEXes now)
  - Market share donut between DEXes (volume share)
  - Personal watchlist (localStorage) with quick view of saved wallets
  - Trader stats on wallet page: win rate, avg trade size, max drawdown
  - CSV export buttons on every ranking
  - Twitter share button on wallet detail
  - Light mode toggle
  - Insurance fund balance from RISEx contracts

Local usage:
    python3 rise_dashboard_v5.py  (default port 8788 to avoid clashing with v4)
Cloud (Railway): auto-detects PORT env and binds to 0.0.0.0.
"""

import os
import json
import time
import threading
import urllib.request
import urllib.parse
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", 8788))    # v5 usa 8788 por defecto en local (v4 usa 8787)
IS_CLOUD = bool(os.environ.get("PORT") or os.environ.get("RAILWAY_ENVIRONMENT"))
BIND = "0.0.0.0" if IS_CLOUD else "127.0.0.1"

RISEX = "https://api.rise.trade"
LLAMA_TVL = "https://api.llama.fi/protocol/risex"
LLAMA_OI = "https://api.llama.fi/summary/open-interest/risex"
STATS_BASE = "https://stats-explorer.risechain.com/api/v1"
RPC_URL = "https://rpc.risechain.com/"

ACCOUNT_REGISTRY = "0x1238991Cac4E65902C08213e79909A9c813Eebc3"
PERPS_MANAGER = "0x53f10fAcFC8965750494E6965F5d6dA39B41d852"
COLLATERAL_MANAGER = "0x2c03c7d7e2974c6599b6b108879109281ef3f818"

SEL_TOTAL_TOKEN_BAL_USDC = "0xfd694891"
SEL_TOTAL_CROSS_MAINT    = "0xce330cd8"
SEL_CROSS_MARGIN_BAL     = "0x59c0b4eb"
SEL_TOTAL_CROSS_FUNDING  = "0x48b73914"
SEL_TOTAL_CROSS_UNSETTLED= "0xbb4b15f0"
AR_DEPLOY_BLOCK = 7345365
EXPLORER_ADDR = "https://explorer.risechain.com/address/"
TOPIC_TAKE = "0x3e92827023687af833e2eb9abe60e0726acfc9f7f82839dec79cf9e138b983ff"
TOPIC_SETTLE = "0x572a85e40cc9183c961148c546e88431898ab9938b85992ea5f6577ea06d9888"

TAKER_BPS, MAKER_BPS = 3.00, 1.00
BLENDED_BPS = (TAKER_BPS + MAKER_BPS) / 2.0
WAD = 1e18
WINDOW_SECONDS = 86400
CHUNK = 5000
CACHE_TTL = 15
REFRESH_SECONDS = 180

VOL_MAX_PAGES = 1000
VOL_PER_ACCOUNT_TIMEOUT = 60
VOL_REFRESH_S = 600
LARGE_TRADE_USD = 10000
FEED_MAX = 1500
FEED_WINDOW_S = 86400

# Ventana "custom" de OI medio: desde 29 mayo 2026, 07:00 UTC. Calculado con
# calendar.timegm((2026,5,29,7,0,0)) = 1780038000. Cambiar este valor o anadir
# mas opciones es trivial: edita esta linea + el boton del frontend.
CUSTOM_SINCE_TS = 1780038000
CUSTOM_LABEL = "29may"  # clave del periodo en la API

LIGHTER_FUND = "https://mainnet.zklighter.elliot.ai/api/v1/funding-rates"
PACIFICA_PRICES = "https://api.pacifica.fi/api/v1/info/prices"
HYPERLIQUID_INFO = "https://api.hyperliquid.xyz/info"   # POST endpoint for metaAndAssetCtxs

# Optional: address of the USDC token on RISE for insurance fund / treasury reads.
# RISEx insurance_fund_account_index lives onchain; we'll derive from PerpsManager events
# (set when contract initialized). For now, leave as None — frontend handles gracefully.
INSURANCE_FUND_ADDRESS = None

HISTORY_MIN_GAP = 120
HISTORY_MAX_POINTS = 5000


def _history_path():
    for base in ("/data", os.path.dirname(os.path.abspath(__file__)), "/tmp"):
        try:
            if os.path.isdir(base) and os.access(base, os.W_OK):
                return os.path.join(base, "rise_history.json")
        except Exception:
            pass
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "rise_history.json")
HISTORY_FILE = _history_path()

_CACHE = {"ts": 0, "data": None}
_HIST_LOCK = threading.Lock()


# ============================== utilidades ==============================
def fetch_json(url, timeout=15, retries=4):
    """GET con reintentos exponenciales para 429/5xx y errores de red."""
    last_err = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "rise-dashboard/4.0",
                                                        "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code in (429, 500, 502, 503, 504) and i < retries - 1:
                time.sleep(0.8 + i * 1.5)
                continue
            raise
        except Exception as e:
            last_err = e
            if i < retries - 1:
                time.sleep(0.4 + i * 0.6)
                continue
            raise
    if last_err:
        raise last_err


def f(x, d=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return d


def rpc(method, params, retries=5):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    for i in range(retries):
        try:
            req = urllib.request.Request(
                RPC_URL, data=body,
                headers={"content-type": "application/json", "User-Agent": "Mozilla/5.0 rise-dashboard"})
            with urllib.request.urlopen(req, timeout=20) as r:
                d = json.loads(r.read().decode())
            if "error" in d:
                raise RuntimeError(d["error"])
            return d["result"]
        except urllib.error.HTTPError as e:
            if e.code in (403, 429) and i < retries - 1:
                time.sleep(0.6 + i * 0.6); continue
            raise
        except Exception:
            if i < retries - 1:
                time.sleep(0.4); continue
            raise


def block_ts(b):
    return int(rpc("eth_getBlockByNumber", [hex(b), False])["timestamp"], 16)


def block_at_ts(target_ts, lo, hi):
    while lo < hi:
        mid = (lo + hi) // 2
        try:
            ts = block_ts(mid)
        except Exception:
            break
        if ts < target_ts:
            lo = mid + 1
        else:
            hi = mid
    return lo


def get_logs(address, frm, to, topics=None):
    params = {"address": address, "fromBlock": hex(frm), "toBlock": hex(to)}
    if topics:
        params["topics"] = topics
    try:
        return rpc("eth_getLogs", [params]) or []
    except RuntimeError as e:
        if "max results" in str(e).lower() and to > frm:
            mid = (frm + to) // 2
            return get_logs(address, frm, mid, topics) + get_logs(address, mid + 1, to, topics)
        raise


def _signed256(word):
    v = int(word, 16)
    return v - (1 << 256) if v >= (1 << 255) else v


def _eth_call(to_addr, data_hex):
    try:
        return rpc("eth_call", [{"to": to_addr, "data": data_hex}, "latest"])
    except Exception:
        return None


def onchain_account_state(account):
    """Onchain truth: portfolio (including non-USDC collateral), maintenance, funding,
    unsettled USDC and adjusted cross balance. None on failure."""
    a = account.lower().replace("0x", "").rjust(64, "0")
    try:
        d1 = _eth_call(COLLATERAL_MANAGER, SEL_TOTAL_TOKEN_BAL_USDC + a)
        if not d1:
            return None
        portfolio = _signed256(d1[2:]) / WAD
        d2 = _eth_call(PERPS_MANAGER, SEL_TOTAL_CROSS_MAINT + a)
        total_maint = (_signed256(d2[2:]) / WAD) if d2 else 0.0
        d3 = _eth_call(PERPS_MANAGER, SEL_TOTAL_CROSS_FUNDING + a)
        funding = (_signed256(d3[2:]) / WAD) if d3 else 0.0
        d4 = _eth_call(PERPS_MANAGER, SEL_TOTAL_CROSS_UNSETTLED + a)
        unsettled = (_signed256(d4[2:]) / WAD) if d4 else 0.0
        portfolio_hex = hex(int(portfolio * WAD) & ((1 << 256) - 1))[2:].rjust(64, "0")
        d5 = _eth_call(PERPS_MANAGER, SEL_CROSS_MARGIN_BAL + portfolio_hex + a)
        cross_balance = (_signed256(d5[2:]) / WAD) if d5 else (portfolio + funding + unsettled)
        return {"portfolio": portfolio, "total_maint": total_maint,
                "funding": funding, "unsettled": unsettled,
                "cross_balance": cross_balance}
    except Exception:
        return None


# ============================== overview ==============================
def fetch_orderbook(market_id):
    try:
        d = fetch_json(f"{RISEX}/v1/orderbook?market_id={market_id}&limit=50")["data"]
        return market_id, d
    except Exception:
        return market_id, None


def build_data():
    out = {"ok": True, "generated_at": int(time.time()), "errors": [],
           "markets": [], "totals": {}, "tvl": {"current": None, "series": []},
           "big_orders": []}
    markets = []
    try:
        md = fetch_json(f"{RISEX}/v1/markets")["data"]["markets"]
    except Exception as e:
        out["errors"].append(f"markets: {e}"); md = []
    mk_by_id = {}
    for m in md:
        if not m.get("visible", True):
            continue
        mid = str(m.get("market_id"))
        mark = f(m.get("mark_price")); index = f(m.get("index_price"))
        oi_base = f(m.get("open_interest")); fund8h = f(m.get("funding_rate_8h"))
        row = {
            "market_id": mid,
            "name": m.get("display_name") or m.get("base_asset_symbol"),
            "last_price": f(m.get("last_price")), "mark_price": mark, "index_price": index,
            "change_24h": f(m.get("change_24h")), "volume_24h": f(m.get("quote_volume_24h")),
            "oi_base": oi_base, "oi_usd": oi_base * mark,
            "funding_8h": fund8h, "funding_apr": fund8h * 3 * 365 * 100,
            "basis_pct": ((mark - index) / index * 100) if index else 0.0,
            "max_leverage": f(m.get("config", {}).get("max_leverage")),
            "spread_bps": None, "best_bid": None, "best_ask": None,
        }
        markets.append(row); mk_by_id[mid] = row

    big = []
    if markets:
        with ThreadPoolExecutor(max_workers=8) as ex:
            for mid, ob in ex.map(lambda r: fetch_orderbook(r["market_id"]), markets):
                if not ob:
                    continue
                row = mk_by_id.get(mid)
                bids = ob.get("bids") or []; asks = ob.get("asks") or []
                if row and bids and asks:
                    bb, ba = f(bids[0]["price"]), f(asks[0]["price"])
                    row["best_bid"], row["best_ask"] = bb, ba
                    mid_px = (bb + ba) / 2 or 1
                    row["spread_bps"] = (ba - bb) / mid_px * 10000
                nm = row["name"] if row else mid
                for side, arr in (("bid", bids), ("ask", asks)):
                    for o in arr:
                        px, qty = f(o.get("price")), f(o.get("quantity"))
                        big.append({"market": nm, "side": side, "price": px,
                                    "qty": qty, "notional": px * qty,
                                    "orders": int(o.get("order_count") or 1)})
        big.sort(key=lambda x: x["notional"], reverse=True)
        out["big_orders"] = big[:25]

    markets.sort(key=lambda x: x["volume_24h"], reverse=True)
    out["markets"] = markets
    total_vol = sum(m["volume_24h"] for m in markets)
    total_oi = sum(m["oi_usd"] for m in markets)

    tvl_cur, tvl_series = None, []
    try:
        td = fetch_json(LLAMA_TVL)
        tvl_series = [{"date": int(p["date"]) * 1000, "tvl": f(p.get("totalLiquidityUSD"))}
                      for p in (td.get("tvl") or [])]
        cc = td.get("currentChainTvls", {}) or {}
        tvl_cur = sum(f(v) for v in cc.values()) if cc else (tvl_series[-1]["tvl"] if tvl_series else None)
    except Exception as e:
        out["errors"].append(f"tvl: {e}")
    out["tvl"] = {"current": tvl_cur, "series": tvl_series}

    oi_llama = None
    try:
        oi_llama = f(fetch_json(LLAMA_OI).get("total24h"))
    except Exception:
        pass

    out["totals"] = {
        "volume_24h": total_vol, "open_interest_usd": total_oi,
        "open_interest_llama": oi_llama, "tvl": tvl_cur,
        "oi_tvl_ratio": (total_oi / tvl_cur) if tvl_cur else None,
        "fees_24h_est": total_vol * BLENDED_BPS / 10000.0,
        "fees_24h_low": total_vol * MAKER_BPS / 10000.0,
        "fees_24h_high": total_vol * TAKER_BPS / 10000.0,
        "num_markets": len(markets),
    }
    try:
        with _FEE_LOCK:
            if _FEE["ready"]:
                out["totals"]["fees_24h_real"] = _FEE["fees_24h"]
                out["totals"]["fees_taker"] = _FEE["taker"]
                out["totals"]["fees_maker"] = _FEE["maker"]
                out["totals"]["fees_window_h"] = _FEE["window_h"]
            else:
                out["totals"]["fees_24h_real"] = None
    except Exception:
        out["totals"]["fees_24h_real"] = None

    record_history(out["totals"], tvl_cur)
    return out


def get_overview():
    now = time.time()
    if _CACHE["data"] is None or now - _CACHE["ts"] > CACHE_TTL:
        _CACHE["data"] = build_data()
        _CACHE["ts"] = now
    return _CACHE["data"]


# ============================== historico ==============================
def load_history():
    try:
        with open(HISTORY_FILE) as fh:
            return json.load(fh)
    except Exception:
        return []


def record_history(totals, tvl):
    with _HIST_LOCK:
        h = load_history()
        now = int(time.time())
        if h and now - h[-1]["t"] < HISTORY_MIN_GAP:
            return
        h.append({"t": now, "vol": round(totals.get("volume_24h") or 0),
                  "oi": round(totals.get("open_interest_usd") or 0), "tvl": round(tvl or 0)})
        h = h[-HISTORY_MAX_POINTS:]
        try:
            with open(HISTORY_FILE, "w") as fh:
                json.dump(h, fh)
        except Exception:
            pass


# ============================== wallet ==============================
def get_wallet(account):
    account = (account or "").strip()
    res = {"ok": True, "account": account, "positions": [], "open_orders": [],
           "trades": [], "summary": {}, "balance": None, "errors": []}
    if not (account.startswith("0x") and len(account) == 42):
        res["ok"] = False; res["errors"].append("Invalid address (0x + 40 hex).")
        return res

    # Mapa de mercados con: nombre, mark, maintenance_margin_factor (bps), max_leverage
    mk = {}
    try:
        for m in fetch_json(f"{RISEX}/v1/markets")["data"]["markets"]:
            cfg = m.get("config", {}) or {}
            mk[str(m.get("market_id"))] = {
                "name": m.get("display_name"),
                "mark": f(m.get("mark_price")),
                "mmf_bps": f(cfg.get("maintenance_margin_factor")),
                "max_lev": f(cfg.get("max_leverage")),
            }
    except Exception:
        pass

    q = urllib.parse.quote(account)
    try:
        bd = fetch_json(f"{RISEX}/v1/account/cross-margin-balance?account={q}")["data"]
        res["balance"] = f(bd.get("balance"))
    except Exception:
        pass

    def is_long(side, size):
        s = str(side).upper()
        if s in ("BUY", "0", "LONG"): return True
        if s in ("SELL", "1", "SHORT"): return False
        return size >= 0

    # ----- PARSE all positions first (context needed for cross margin liq calc) -----
    raw_positions = []
    total_upnl = 0.0
    try:
        pd = fetch_json(f"{RISEX}/v1/positions?account={q}")["data"]
        for p in pd.get("positions", []):
            mid = str(p.get("market_id"))
            info = mk.get(mid, {})
            size = abs(f(p.get("size")) / WAD)
            if size == 0:
                continue
            entry = f(p.get("avg_entry_price")) / WAD
            mark = info.get("mark") or f(p.get("mark_price"))
            longp = is_long(p.get("side"), f(p.get("size")))
            sign = 1 if longp else -1
            upnl = sign * (mark - entry) * size
            total_upnl += upnl
            mode = "Isolated" if p.get("margin_mode") in (1, "1") else "Cross"
            iso_usdc = f(p.get("isolated_usdc_balance")) / WAD
            lev = f(p.get("leverage")) / WAD
            mmf_bps = info.get("mmf_bps", 0.0)
            mmf = mmf_bps / 10000.0
            maint_req = mmf * size * mark
            raw_positions.append({
                "mid": mid, "info": info, "size": size, "entry": entry, "mark": mark,
                "longp": longp, "side_lbl": "Long" if longp else "Short",
                "upnl": upnl, "mode": mode, "iso_usdc": iso_usdc,
                "lev": lev, "mmf": mmf, "maint_req": maint_req,
            })
    except Exception as e:
        res["errors"].append(f"positions: {e}")

    cross_positions = [r for r in raw_positions if r["mode"] == "Cross"]
    cross_upnl_total = sum(r["upnl"] for r in cross_positions)

    onchain = onchain_account_state(account)
    total_notional_cross = sum(r["size"] * r["mark"] for r in cross_positions)
    if onchain and onchain["total_maint"] > 0 and total_notional_cross > 0:
        mmf_eff = onchain["total_maint"] / total_notional_cross
        portfolio = onchain["portfolio"]; funding = onchain["funding"]; unsettled = onchain["unsettled"]
    else:
        mmf_eff = None
        portfolio = res["balance"]; funding = 0.0; unsettled = 0.0

    for rp in raw_positions:
        lp = None
        if rp["mode"] == "Cross" and portfolio is not None and mmf_eff is not None:
            sz = rp["size"]; mark = rp["mark"]
            equity_now = portfolio + funding + unsettled
            maint_now = onchain["total_maint"]
            if rp["longp"]:
                den = sz * (1 - mmf_eff)
                if den > 0:
                    lp = mark + (maint_now - equity_now) / den
            else:
                den = sz * (1 + mmf_eff)
                if den > 0:
                    lp = mark + (equity_now - maint_now) / den
        elif rp["mode"] == "Cross" and portfolio is not None:
            B = portfolio
            cross_maint_total_approx = sum(r["mmf"] * r["size"] * r["mark"] for r in cross_positions)
            M_others = cross_maint_total_approx - rp["mmf"] * rp["size"] * rp["mark"]
            upnl_others = cross_upnl_total - rp["upnl"]
            sz = rp["size"]; entry = rp["entry"]; mmf = rp["mmf"]
            if rp["longp"]:
                den = sz * (1 - mmf)
                if den > 0:
                    lp = (M_others + entry * sz - B - upnl_others) / den
            else:
                den = sz * (1 + mmf)
                if den > 0:
                    lp = (B + upnl_others + entry * sz - M_others) / den
        else:
            iso = rp["iso_usdc"] if rp["iso_usdc"] > 0 else (
                  rp["size"] * rp["entry"] / max(1, rp["lev"]))
            sz = rp["size"]; entry = rp["entry"]; mmf = rp["mmf"]
            if rp["longp"]:
                den = sz * (1 - mmf)
                if den > 0:
                    lp = entry - iso / den
            else:
                den = sz * (1 + mmf)
                if den > 0:
                    lp = (iso + entry * sz) / den

        if lp is not None and lp < 0:
            lp = 0
        dist_to_liq_pct = None
        mark = rp["mark"]
        if lp is not None and mark:
            dist_to_liq_pct = (mark - lp) / mark * 100 if rp["longp"] else (lp - mark) / mark * 100

        res["positions"].append({
            "market": rp["info"].get("name") or rp["mid"], "side": rp["side_lbl"],
            "size": rp["size"], "entry": rp["entry"], "mark": rp["mark"],
            "notional": rp["size"] * rp["mark"], "upnl": rp["upnl"],
            "upnl_pct": (rp["upnl"] / (rp["size"] * rp["entry"]) * 100) if (rp["size"] and rp["entry"]) else 0.0,
            "leverage": rp["lev"], "margin_mode": rp["mode"],
            "isolated_usdc": rp["iso_usdc"],
            "liq_price": lp,
            "dist_to_liq_pct": dist_to_liq_pct,
        })

    try:
        od = fetch_json(f"{RISEX}/v1/orders/open?account={q}")["data"]
        for o in od.get("orders") or []:
            mid = str(o.get("market_id"))
            res["open_orders"].append({
                "market": mk.get(mid, {}).get("name") or mid,
                "side": ("Long" if str(o.get("side")).upper() in ("BUY", "0") else "Short"),
                "price": f(o.get("price")) / WAD,
                "size": f(o.get("size") or o.get("quantity")) / WAD,
            })
    except Exception as e:
        res["errors"].append(f"orders: {e}")

    # historial de trades recientes (ya vienen en unidades humanas, no x1e18)
    try:
        th = fetch_json(f"{RISEX}/v1/trade-history?account={q}&limit=200")["data"]
        for t in (th.get("trades") or [])[:200]:
            mid = str(t.get("market_id"))
            res["trades"].append({
                "market": mk.get(mid, {}).get("name") or mid,
                "ts": int(t.get("time", 0)) // 1_000_000_000,
                "side": "Buy" if str(t.get("side")).upper() == "BUY" else "Sell",
                "position_side": "Long" if str(t.get("position_side")).upper() == "BUY" else "Short",
                "price": f(t.get("price")),
                "size": f(t.get("size")),
                "notional": f(t.get("price")) * f(t.get("size")),
                "fee": f(t.get("fee")),
                "realized_pnl": f(t.get("realized_pnl")),
                "realized_pnl_pct": f(t.get("realized_pnl_percentage")),
                "role": str(t.get("liquidity_indicator") or "").upper(),  # TAKER/MAKER
                "is_liq": bool(t.get("is_liquidation")),
                "leverage": f(t.get("leverage")),
            })
    except Exception as e:
        res["errors"].append(f"trades: {e}")

    total_realized = sum(t["realized_pnl"] for t in res["trades"])
    total_fees = sum(t["fee"] for t in res["trades"])
    vol_summary = None
    realized_pnl_30d = None
    fees_30d = None
    n_liquidations = None
    try:
        now_s = int(time.time())
        with _VOL_LOCK:
            v = _VOL["by_account"].get(account)
            fresh = v and (now_s - v.get("last_refresh", 0) < 900)
        if not fresh:
            v_new = _account_metrics(account, now_s)
            v_new.pop("feed_entries", None)
            with _VOL_LOCK:
                _VOL["by_account"][account] = v_new
            v = v_new
        if v:
            vol_summary = {"1d": v.get("1d", 0), "7d": v.get("7d", 0),
                            "30d": v.get("30d", 0),
                            CUSTOM_LABEL: v.get(CUSTOM_LABEL, 0),
                            "trades_30d": v.get("trades", 0)}
            realized_pnl_30d = v.get("realized_pnl_30d", 0)
            fees_30d = v.get("fees_30d", 0)
            n_liquidations = v.get("n_liquidations", 0)
            ranks = {}
            with _VOL_LOCK:
                snapshot = list(_VOL["by_account"].items())
            for period in ("1d", "7d", "30d", CUSTOM_LABEL):
                my_vol = v.get(period, 0)
                if my_vol <= 0:
                    ranks[period] = None
                    continue
                better = sum(1 for _, val in snapshot if val.get(period, 0) > my_vol)
                total = sum(1 for _, val in snapshot if val.get(period, 0) > 0)
                ranks[period] = {"rank": better + 1, "of": total}
            vol_summary["ranks"] = ranks
    except Exception:
        pass

    res["summary"] = {
        "num_positions": len(res["positions"]),
        "total_notional": sum(p["notional"] for p in res["positions"]),
        "total_upnl": total_upnl,
        "num_open_orders": len(res["open_orders"]),
        "num_trades": len(res["trades"]),
        "realized_pnl_shown": total_realized,
        "fees_shown": total_fees,
        "volume": vol_summary,
        "realized_pnl_30d": realized_pnl_30d,
        "fees_30d": fees_30d,
        "n_liquidations": n_liquidations,
    }
    return res


# ============================== indexer: cuentas + ranking de posiciones ==============================
_INDEX = {"started": int(time.time()), "phase": "scanning", "cursor": None,
          "accounts": set(), "reg_blocks": {}, "scan_done": 0, "scan_total": 0,
          "positions_count": 0, "active_accounts": 0, "ranking": {},
          "account_oi_ranking": [], "last_update": 0}
_IDX_LOCK = threading.Lock()


def _accounts_from_logs(logs):
    out = {}
    for l in logs:
        tops = l.get("topics", [])
        if len(tops) >= 2 and isinstance(tops[1], str) and tops[1][2:26] == "0" * 24:
            a = "0x" + tops[1][26:]
            bn = int(l.get("blockNumber", "0x0"), 16)
            if a not in out or bn < out[a]:
                out[a] = bn
    return out


def _market_map():
    m = {}
    try:
        for x in fetch_json(f"{RISEX}/v1/markets")["data"]["markets"]:
            m[str(x.get("market_id"))] = {"name": x.get("display_name"),
                                          "mark": f(x.get("mark_price"))}
    except Exception:
        pass
    return m


def _account_positions(a, mm):
    try:
        d = fetch_json(f"{RISEX}/v1/positions?account={a}", timeout=10)["data"]
    except Exception:
        return []
    out = []
    for p in d.get("positions", []):
        size = abs(f(p.get("size")) / WAD)
        if size == 0:
            continue
        mid = str(p.get("market_id"))
        info = mm.get(mid, {})
        entry = f(p.get("avg_entry_price")) / WAD
        mark = info.get("mark") or 0
        longp = str(p.get("side")).upper() in ("BUY", "0", "LONG")
        sign = 1 if longp else -1
        out.append({"account": a, "market": info.get("name") or mid, "mid": mid,
                    "side": "Long" if longp else "Short", "size": size, "entry": entry,
                    "mark": mark, "notional": size * mark,
                    "upnl": sign * (mark - entry) * size,
                    "leverage": f(p.get("leverage")) / WAD})
    return out


def refresh_ranking():
    with _IDX_LOCK:
        accts = list(_INDEX["accounts"])
    if not accts:
        return
    mm = _market_map()
    rows = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        for r in ex.map(lambda a: _account_positions(a, mm), accts):
            rows.extend(r)
    rank = {}
    for r in rows:
        m = rank.setdefault(r["market"], {"longs": [], "shorts": [], "oi_long": 0.0,
                                          "oi_short": 0.0, "n_long": 0, "n_short": 0})
        if r["side"] == "Long":
            m["longs"].append(r); m["oi_long"] += r["notional"]; m["n_long"] += 1
        else:
            m["shorts"].append(r); m["oi_short"] += r["notional"]; m["n_short"] += 1
    for m in rank.values():
        m["longs"].sort(key=lambda x: x["notional"], reverse=True)
        m["shorts"].sort(key=lambda x: x["notional"], reverse=True)
        m["longs"] = m["longs"][:50]; m["shorts"] = m["shorts"][:50]
    # ranking POR CUENTA: agrega notional de todos los mercados por cuenta
    per_acct = {}
    for r in rows:
        a = r["account"]
        v = per_acct.setdefault(a, {"account": a, "total_oi": 0.0, "positions": 0,
                                     "long_oi": 0.0, "short_oi": 0.0,
                                     "markets": [], "upnl": 0.0})
        v["total_oi"] += r["notional"]; v["positions"] += 1; v["upnl"] += r["upnl"]
        if r["side"] == "Long": v["long_oi"] += r["notional"]
        else: v["short_oi"] += r["notional"]
        v["markets"].append(f"{r['market']} {r['side'][0]}")
    account_oi_ranking = sorted(per_acct.values(), key=lambda x: x["total_oi"], reverse=True)
    active = len(per_acct)
    with _IDX_LOCK:
        _INDEX["ranking"] = rank
        _INDEX["positions_count"] = len(rows)
        _INDEX["active_accounts"] = active
        _INDEX["account_oi_ranking"] = account_oi_ranking
        _INDEX["last_update"] = int(time.time())


def scan_all_accounts():
    latest = int(rpc("eth_blockNumber", []), 16)
    ranges = [(b, min(b + CHUNK - 1, latest)) for b in range(AR_DEPLOY_BLOCK, latest + 1, CHUNK)]
    with _IDX_LOCK:
        _INDEX["scan_total"] = len(ranges); _INDEX["cursor"] = latest

    def one(r):
        try:
            return _accounts_from_logs(get_logs(ACCOUNT_REGISTRY, r[0], r[1]))
        except Exception:
            return {}

    with ThreadPoolExecutor(max_workers=6) as ex:
        for found in ex.map(one, ranges):
            with _IDX_LOCK:
                for a, bn in found.items():
                    if a not in _INDEX["reg_blocks"] or bn < _INDEX["reg_blocks"][a]:
                        _INDEX["reg_blocks"][a] = bn
                _INDEX["accounts"] = set(_INDEX["reg_blocks"].keys())
                _INDEX["scan_done"] += 1
    return latest


def indexer_loop():
    try:
        scan_all_accounts()
    except Exception:
        pass
    with _IDX_LOCK:
        _INDEX["phase"] = "loading"
    try:
        refresh_ranking()
    except Exception:
        pass
    with _IDX_LOCK:
        _INDEX["phase"] = "live"
    last_refresh = time.time()
    while True:
        try:
            latest = int(rpc("eth_blockNumber", []), 16)
            with _IDX_LOCK:
                frm = (_INDEX["cursor"] or latest) + 1
            if latest >= frm:
                new = {}
                b = frm
                while b <= latest:
                    to = min(b + CHUNK - 1, latest)
                    found = _accounts_from_logs(get_logs(ACCOUNT_REGISTRY, b, to))
                    for a, bn in found.items():
                        if a not in new or bn < new[a]:
                            new[a] = bn
                    b = to + 1
                with _IDX_LOCK:
                    for a, bn in new.items():
                        if a not in _INDEX["reg_blocks"] or bn < _INDEX["reg_blocks"][a]:
                            _INDEX["reg_blocks"][a] = bn
                    _INDEX["accounts"] = set(_INDEX["reg_blocks"].keys())
                    _INDEX["cursor"] = latest
            if time.time() - last_refresh >= REFRESH_SECONDS:
                refresh_ranking(); last_refresh = time.time()
        except Exception:
            pass
        time.sleep(15)


def get_ranking():
    with _IDX_LOCK:
        return {"ok": True, "phase": _INDEX["phase"], "started": _INDEX["started"],
                "accounts": len(_INDEX["accounts"]),
                "scan_done": _INDEX["scan_done"], "scan_total": _INDEX["scan_total"],
                "positions_count": _INDEX["positions_count"],
                "last_update": _INDEX["last_update"], "ranking": _INDEX["ranking"]}


def get_account_oi_ranking(limit=100):
    with _IDX_LOCK:
        rk = list(_INDEX["account_oi_ranking"])
        ph = _INDEX["phase"]; last = _INDEX["last_update"]
        active = _INDEX["active_accounts"]; positions = _INDEX["positions_count"]
    total = sum(r["total_oi"] for r in rk)
    return {"ok": True, "phase": ph, "last_update": last,
            "active_accounts": active, "positions_count": positions,
            "total_oi": total, "count": len(rk), "ranking": rk[:limit]}


# ============================== usuarios ==============================
_USERS_CACHE = {"ts": 0, "data": None}


def get_users():
    import datetime
    now = time.time()
    if _USERS_CACHE["data"] and now - _USERS_CACHE["ts"] < 300:
        return _USERS_CACHE["data"]
    out = {"ok": True, "errors": []}
    try:
        cs = fetch_json(f"{STATS_BASE}/counters")["counters"]
        cmap = {c.get("id"): c.get("value") for c in cs}
        out["total_accounts"] = int(cmap.get("totalAccounts", 0))
        out["total_addresses"] = int(cmap.get("totalAddresses", 0))
    except Exception as e:
        out["total_accounts"] = None; out["total_addresses"] = None
        out["errors"].append(f"counters: {e}")

    def line(name):
        try:
            return fetch_json(f"{STATS_BASE}/lines/{name}?resolution=DAY")["chart"]
        except Exception:
            return []

    newa = line("newAccounts"); growth = line("accountsGrowth"); active = line("activeAccounts")
    gmap = {p["date"]: int(p["value"]) for p in growth}
    amap = {p["date"]: int(p["value"]) for p in active}
    series = [{"date": p["date"], "new": int(p["value"]),
               "cum": gmap.get(p["date"]), "active": amap.get(p["date"])} for p in newa]
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    out["new_today"] = next((int(p["value"]) for p in newa if p["date"] == today), 0)
    out["new_7d"] = sum(int(p["value"]) for p in newa[-7:])
    out["active_today"] = int(active[-1]["value"]) if active else None
    out["series"] = series
    with _IDX_LOCK:
        out["active_with_position"] = _INDEX["active_accounts"]
        out["phase"] = _INDEX["phase"]
    _USERS_CACHE["ts"] = now; _USERS_CACHE["data"] = out
    return out


# ============================== fees 24h reales (onchain) ==============================
_FEE = {"blocks": {}, "cursor": None, "ready": False, "cutoff_block": None,
        "taker": 0.0, "maker": 0.0, "fees_24h": 0.0, "window_h": 0.0, "last_update": 0}
_FEE_LOCK = threading.Lock()


def fetch_fee_logs(frm, to):
    return get_logs(PERPS_MANAGER, frm, to, topics=[[TOPIC_TAKE, TOPIC_SETTLE]])


def fee_indexer_loop():
    last_cutoff_calc = 0
    while True:
        try:
            latest = int(rpc("eth_blockNumber", []), 16)
            latest_ts = block_ts(latest)
            with _FEE_LOCK:
                cutoff = _FEE["cutoff_block"]
            if cutoff is None or time.time() - last_cutoff_calc > 300:
                cutoff = block_at_ts(latest_ts - WINDOW_SECONDS,
                                     max(AR_DEPLOY_BLOCK, latest - 2 * WINDOW_SECONDS), latest)
                last_cutoff_calc = time.time()
            try:
                cutoff_ts = block_ts(cutoff)
            except Exception:
                cutoff_ts = latest_ts - WINDOW_SECONDS
            window_h = round((latest_ts - cutoff_ts) / 3600.0, 1)

            with _FEE_LOCK:
                if _FEE["cursor"] is None:
                    _FEE["cursor"] = cutoff - 1
                frm = _FEE["cursor"] + 1
            if latest >= frm:
                b = frm
                while b <= latest:
                    to = min(b + CHUNK - 1, latest)
                    logs = fetch_fee_logs(b, to)
                    with _FEE_LOCK:
                        for l in logs:
                            try:
                                fee = _signed256(l["data"][2:][128:192]) / WAD
                            except Exception:
                                continue
                            bn = int(l["blockNumber"], 16)
                            rec = _FEE["blocks"].setdefault(bn, [0.0, 0.0])
                            if l["topics"][0] == TOPIC_TAKE:
                                rec[0] += fee
                            else:
                                rec[1] += fee
                    b = to + 1
                with _FEE_LOCK:
                    for bn in [x for x in _FEE["blocks"] if x < cutoff]:
                        del _FEE["blocks"][bn]
                    _FEE["taker"] = sum(v[0] for v in _FEE["blocks"].values())
                    _FEE["maker"] = sum(v[1] for v in _FEE["blocks"].values())
                    _FEE["fees_24h"] = _FEE["taker"] + _FEE["maker"]
                    _FEE["cutoff_block"] = cutoff
                    _FEE["cursor"] = latest
                    _FEE["ready"] = True
                    _FEE["window_h"] = window_h
                    _FEE["last_update"] = int(time.time())
        except Exception:
            pass
        time.sleep(30)


# ============================== volumen + OI medio (TWAP) ==============================
_VOL = {"by_account": {}, "ready": False, "phase": "waiting",
        "scanned": 0, "total": 0, "last_update": 0}
_VOL_LOCK = threading.Lock()
# Feed global de actividad (trades grandes + liquidaciones, ultimas 24h)
_FEED = {"entries": [], "last_update": 0}
_FEED_LOCK = threading.Lock()


def _account_metrics(account, now_s):
    """Para cada cuenta, calcula en una sola pasada:
       - VOLUMEN realizado por ventana (sum price*size)
       - TWAP OI por ventana (time-weighted)
       - REALIZED PnL acumulado (suma de realized_pnl de cada trade en 30d)
       - Lista de eventos relevantes (trades grandes + liquidaciones) para el feed global
    """
    out = {"trades": 0, "last_refresh": now_s,
           "1d": 0.0, "7d": 0.0, "30d": 0.0, CUSTOM_LABEL: 0.0,
           "oi_1d": 0.0, "oi_7d": 0.0, "oi_30d": 0.0,
           "oi_" + CUSTOM_LABEL: 0.0,
           "realized_pnl_30d": 0.0, "fees_30d": 0.0, "n_liquidations": 0,
           "feed_entries": []}
    windows = [("1d", 86400), ("7d", 7 * 86400),
               ("30d", 30 * 86400), (CUSTOM_LABEL, max(1, now_s - CUSTOM_SINCE_TS))]
    cutoffs = {k: now_s - d for k, d in windows}
    # cutoff mas antiguo para decidir cuando parar de paginar
    earliest_cutoff = min(cutoffs.values())

    events = []
    page = 1
    feed_cutoff = now_s - FEED_WINDOW_S
    deadline = time.time() + VOL_PER_ACCOUNT_TIMEOUT
    while page <= VOL_MAX_PAGES:
        if time.time() > deadline:
            out["timed_out"] = True
            break
        try:
            d = fetch_json(
                f"{RISEX}/v1/trade-history?account={account}&limit=1000&page={page}", timeout=10)["data"]
        except Exception:
            break
        trades = d.get("trades") or []
        if not trades:
            break
        for t in trades:
            try:
                ts = int(t.get("time", 0)) / 1e9
            except Exception:
                continue
            side = str(t.get("side")).upper()
            sgn = 1 if side in ("BUY", "0", "LONG") else -1
            size = f(t.get("size")) * sgn
            price = f(t.get("price"))
            mid = str(t.get("market_id"))
            events.append((ts, mid, size, price))
            if ts >= earliest_cutoff:
                notional = abs(size) * price
                # contar trades una sola vez (usamos 30d como referencia base)
                if ts >= cutoffs["30d"]:
                    out["trades"] += 1
                    out["realized_pnl_30d"] += f(t.get("realized_pnl"))
                    out["fees_30d"] += f(t.get("fee"))
                    if bool(t.get("is_liquidation")):
                        out["n_liquidations"] += 1
                for k in ("1d", "7d", "30d", CUSTOM_LABEL):
                    if ts >= cutoffs[k]:
                        out[k] += notional
                # alimentar el feed global: grandes trades + todas las liquidaciones (24h)
                if ts >= feed_cutoff and (notional >= LARGE_TRADE_USD or t.get("is_liquidation")):
                    out["feed_entries"].append({
                        "ts": int(ts), "account": account, "market_id": mid,
                        "side": "Buy" if side == "BUY" else "Sell",
                        "position_side": "Long" if str(t.get("position_side")).upper() == "BUY" else "Short",
                        "price": price, "size": abs(size), "notional": notional,
                        "realized_pnl": f(t.get("realized_pnl")),
                        "fee": f(t.get("fee")),
                        "role": str(t.get("liquidity_indicator") or "").upper(),
                        "is_liq": bool(t.get("is_liquidation")),
                    })
        if not d.get("has_next_page"):
            break
        # si la pagina mas reciente ya tiene trades anteriores al cutoff mas antiguo, ya hemos visto todo
        oldest_in_page = min((int(t.get("time", 0)) / 1e9 for t in trades), default=None)
        if oldest_in_page is not None and oldest_in_page < earliest_cutoff:
            break
        page += 1

    if not events:
        return out
    events.sort(key=lambda e: e[0])

    # TWAP OI por ventana
    for label, dur in windows:
        start = now_s - dur
        if dur <= 0:
            continue
        pos = {}; last_px = {}
        in_win = []
        for ts, mid, size, price in events:
            if ts < start:
                pos[mid] = pos.get(mid, 0.0) + size
                last_px[mid] = price
            else:
                in_win.append((ts, mid, size, price))
        prev_t = start
        sum_notional_dt = 0.0
        for ts, mid, size, price in in_win:
            dt = ts - prev_t
            if dt > 0:
                notional_now = sum(abs(pos.get(m, 0.0)) * last_px.get(m, 0.0) for m in last_px)
                sum_notional_dt += notional_now * dt
            pos[mid] = pos.get(mid, 0.0) + size
            last_px[mid] = price
            prev_t = ts
        dt = now_s - prev_t
        if dt > 0:
            notional_now = sum(abs(pos.get(m, 0.0)) * last_px.get(m, 0.0) for m in last_px)
            sum_notional_dt += notional_now * dt
        out["oi_" + label] = sum_notional_dt / dur
    return out


_account_volume = _account_metrics


def volume_indexer_loop():
    while True:
        with _IDX_LOCK:
            n = len(_INDEX["accounts"]); ph = _INDEX["phase"]
        if n > 0 and ph in ("loading", "live"):
            break
        time.sleep(5)
    while True:
        try:
            with _IDX_LOCK:
                accts = list(_INDEX["accounts"])
            now_s = int(time.time())
            with _VOL_LOCK:
                known = {a: _VOL["by_account"].get(a, {}).get("30d", 0) for a in accts}
                _VOL["total"] = len(accts); _VOL["scanned"] = 0; _VOL["phase"] = "scanning"
            accts.sort(key=lambda a: known.get(a, 0), reverse=True)

            def one(a):
                return a, _account_metrics(a, now_s)

            # mapa de mercados precargado una sola vez para enriquecer entries
            mm = _market_map()
            feed_cutoff = now_s - FEED_WINDOW_S
            # buffer interno: combinamos accounts ya conocidos con nuevas entries del pase actual.
            # Asi el feed se va publicando incrementalmente.
            seen_ids = set()
            with _FEED_LOCK:
                # arrancamos del estado actual del feed (mantenemos entries de pases anteriores
                # mientras siguen dentro de la ventana de 24h)
                buf = [e for e in _FEED["entries"] if e["ts"] >= feed_cutoff]
                for e in buf:
                    # deduplicar por (account, ts, market_id, price, size) aprox
                    seen_ids.add((e.get("account"), e.get("ts"), e.get("market_id"),
                                   round(e.get("price", 0), 6), round(e.get("size", 0), 8)))

            t_start = time.time()
            n_timed_out = 0
            with ThreadPoolExecutor(max_workers=3) as ex:
                for a, v in ex.map(one, accts):
                    if v.get("timed_out"):
                        n_timed_out += 1
                    new_entries = v.pop("feed_entries", [])
                    for e in new_entries:
                        key = (e["account"], e["ts"], e["market_id"],
                                round(e["price"], 6), round(e["size"], 8))
                        if key in seen_ids:
                            continue
                        seen_ids.add(key)
                        e["market"] = (mm.get(e["market_id"]) or {}).get("name") or e["market_id"]
                        buf.append(e)
                    with _VOL_LOCK:
                        _VOL["by_account"][a] = v
                        _VOL["scanned"] += 1
                        n = _VOL["scanned"]
                    if n % 100 == 0:
                        elapsed = time.time() - t_start
                        rate = n / elapsed if elapsed > 0 else 0
                        eta = (len(accts) - n) / rate / 60 if rate > 0 else 0
                        print(f"[vol_indexer] {n}/{len(accts)} accts in {elapsed:.0f}s "
                              f"({rate:.1f}/s, ETA {eta:.1f}min, {n_timed_out} timeouts)",
                              flush=True)
                    if n % 50 == 0 and new_entries:
                        buf.sort(key=lambda e: e["ts"], reverse=True)
                        with _FEED_LOCK:
                            _FEED["entries"] = buf[:FEED_MAX]
                            _FEED["last_update"] = int(time.time())
            elapsed_total = time.time() - t_start
            print(f"[vol_indexer] PASS DONE: {len(accts)} accts in {elapsed_total:.0f}s, "
                  f"{n_timed_out} timed out", flush=True)

            # publicacion final del pase
            buf.sort(key=lambda e: e["ts"], reverse=True)
            with _FEED_LOCK:
                _FEED["entries"] = buf[:FEED_MAX]
                _FEED["last_update"] = int(time.time())
            with _VOL_LOCK:
                _VOL["ready"] = True; _VOL["phase"] = "live"
                _VOL["last_update"] = int(time.time())
        except Exception:
            pass
        time.sleep(VOL_REFRESH_S)


def get_oi_ranking(period="1d", limit=200):
    if period not in ("1d", "7d", "30d", CUSTOM_LABEL):
        period = "1d"
    key = "oi_" + period
    with _VOL_LOCK:
        ph = _VOL["phase"]; ready = _VOL["ready"]
        scanned = _VOL["scanned"]; total = _VOL["total"]; last = _VOL["last_update"]
        items = []
        for a, v in _VOL["by_account"].items():
            avg = v.get(key, 0)
            if avg > 0:
                items.append({"account": a, "avg_oi": avg, "trades": v.get("trades", 0)})
    items.sort(key=lambda x: x["avg_oi"], reverse=True)
    total_oi = sum(x["avg_oi"] for x in items)
    out = {"ok": True, "period": period, "ready": ready, "phase": ph,
           "scanned": scanned, "total_accounts": total, "last_update": last,
           "count_with_oi": len(items), "total_avg_oi": total_oi,
           "ranking": items[:limit]}
    if period == CUSTOM_LABEL:
        out["since_ts"] = CUSTOM_SINCE_TS
    return out


def get_live_activity(limit=200, only_liq=False, market=None):
    with _FEED_LOCK:
        entries = list(_FEED["entries"])
        last = _FEED["last_update"]
    if only_liq:
        entries = [e for e in entries if e.get("is_liq")]
    if market:
        entries = [e for e in entries if e.get("market") == market]
    # totales para cabecera
    total_notional = sum(e["notional"] for e in entries)
    total_liq_loss = sum(abs(e["realized_pnl"]) for e in entries if e.get("is_liq"))
    return {"ok": True, "last_update": last, "count": len(entries),
            "total_notional": total_notional,
            "total_liq_loss": total_liq_loss,
            "entries": entries[:limit]}


def get_pnl_ranking(limit=100):
    """Ranking de top traders por PnL = realized (30d) + unrealized (snapshot actual)."""
    # unrealized PnL agregada por cuenta (del indexer de posiciones)
    unreal = {}
    with _IDX_LOCK:
        for r in _INDEX["account_oi_ranking"]:
            unreal[r["account"]] = r.get("upnl", 0.0)
    rows = []
    with _VOL_LOCK:
        for a, v in _VOL["by_account"].items():
            rp = v.get("realized_pnl_30d", 0.0)
            up = unreal.get(a, 0.0)
            total = rp + up
            if rp == 0.0 and up == 0.0:
                continue
            rows.append({"account": a, "realized": rp, "unrealized": up, "total": total,
                          "fees": v.get("fees_30d", 0.0),
                          "trades": v.get("trades", 0),
                          "n_liquidations": v.get("n_liquidations", 0)})
        last = _VOL["last_update"]; ph = _VOL["phase"]
    winners = sorted(rows, key=lambda x: x["total"], reverse=True)[:limit]
    losers = sorted(rows, key=lambda x: x["total"])[:limit]
    return {"ok": True, "phase": ph, "last_update": last,
            "count": len(rows), "winners": winners, "losers": losers}


def get_market_detail(market_id):
    """Detalle por mercado: stats + top longs/shorts + recent feed para ese mercado."""
    mid = str(market_id)
    info = {}
    try:
        for m in fetch_json(f"{RISEX}/v1/markets")["data"]["markets"]:
            if str(m.get("market_id")) == mid:
                cfg = m.get("config", {}) or {}
                fund8h = f(m.get("funding_rate_8h"))
                mark = f(m.get("mark_price"))
                index = f(m.get("index_price"))
                oi_base = f(m.get("open_interest"))
                info = {"market_id": mid,
                        "name": m.get("display_name"),
                        "last_price": f(m.get("last_price")),
                        "mark_price": mark, "index_price": index,
                        "volume_24h": f(m.get("quote_volume_24h")),
                        "change_24h": f(m.get("change_24h")),
                        "oi_usd": oi_base * mark,
                        "funding_8h": fund8h,
                        "funding_apr": fund8h * 3 * 365 * 100,
                        "basis_pct": ((mark - index) / index * 100) if index else 0.0,
                        "max_leverage": f(cfg.get("max_leverage")),
                        "mmf_bps": f(cfg.get("maintenance_margin_factor")),
                        "min_order_size": f(cfg.get("min_order_size")),
                        "high_24h": f(m.get("high_24h")), "low_24h": f(m.get("low_24h"))}
                break
    except Exception:
        pass
    # orderbook
    ob = None
    try:
        ob = fetch_json(f"{RISEX}/v1/orderbook?market_id={mid}&limit=15")["data"]
    except Exception:
        pass
    # top longs/shorts en ese mercado (del indexer de posiciones)
    market_name = info.get("name")
    with _IDX_LOCK:
        mr = (_INDEX["ranking"] or {}).get(market_name, {})
    # feed reciente solo de ese mercado
    feed = get_live_activity(limit=80, market=market_name)
    return {"ok": True, "info": info, "orderbook": ob,
            "longs": (mr.get("longs") or [])[:20], "shorts": (mr.get("shorts") or [])[:20],
            "oi_long": mr.get("oi_long", 0), "oi_short": mr.get("oi_short", 0),
            "n_long": mr.get("n_long", 0), "n_short": mr.get("n_short", 0),
            "feed": feed["entries"]}


def get_volume_ranking(period="1d", limit=200):
    if period not in ("1d", "7d", "30d", CUSTOM_LABEL):
        period = "1d"
    with _VOL_LOCK:
        ph = _VOL["phase"]; ready = _VOL["ready"]
        scanned = _VOL["scanned"]; total = _VOL["total"]; last = _VOL["last_update"]
        items = []
        for a, v in _VOL["by_account"].items():
            vol = v.get(period, 0)
            if vol > 0:
                items.append({"account": a, "volume": vol, "trades": v.get("trades", 0)})
    items.sort(key=lambda x: x["volume"], reverse=True)
    total_vol = sum(x["volume"] for x in items)
    return {"ok": True, "period": period, "ready": ready, "phase": ph,
            "scanned": scanned, "total_accounts": total, "last_update": last,
            "count_with_volume": len(items), "total_volume": total_vol,
            "ranking": items[:limit]}


# ============================== comparativa de funding ==============================
_FUND_CACHE = {"ts": 0, "data": None}


def _risex_base(name):
    if not name: return ""
    return name.split("/")[0].split("-")[0].upper()


def get_funding_compare():
    now = time.time()
    if _FUND_CACHE["data"] and now - _FUND_CACHE["ts"] < 30:
        return _FUND_CACHE["data"]
    out = {"ok": True, "generated_at": int(now), "errors": [], "rows": []}
    risex = {}
    try:
        for m in fetch_json(f"{RISEX}/v1/markets")["data"]["markets"]:
            if not m.get("visible", True): continue
            sym = _risex_base(m.get("display_name") or m.get("base_asset_symbol"))
            cf = f(m.get("current_funding_rate"))
            risex[sym] = {"hourly": cf, "mark": f(m.get("mark_price"))}
    except Exception as e:
        out["errors"].append(f"risex: {e}")
    lighter = {}
    # detectar el "default rate" de Lighter (el mas comun, suele ser su valor floor para
    # mercados con poca actividad). Lo marcamos como "default" en la respuesta para que
    # el frontend pueda señalarlos visualmente.
    try:
        ldata = fetch_json(LIGHTER_FUND).get("funding_rates", [])
        lighter_only = [e for e in ldata if e.get("exchange") == "lighter"]
        from collections import Counter
        rates_counter = Counter(round(f(e.get("rate")), 12) for e in lighter_only)
        # el rate mas repetido es el "default" si aparece en mas del 30% de los mercados
        default_rate = None
        if rates_counter:
            most, count = rates_counter.most_common(1)[0]
            if count / max(1, len(lighter_only)) > 0.3:
                default_rate = most
        for e in lighter_only:
            r = f(e.get("rate"))
            is_default = (default_rate is not None and round(r, 12) == default_rate)
            lighter[e.get("symbol", "").upper()] = {"hourly": r, "is_default": is_default}
    except Exception as e:
        out["errors"].append(f"lighter: {e}")
    pacifica = {}
    try:
        for p in fetch_json(PACIFICA_PRICES).get("data", []):
            sym = (p.get("symbol") or "").upper()
            pacifica[sym] = {"hourly": f(p.get("funding")), "next": f(p.get("next_funding"))}
    except Exception as e:
        out["errors"].append(f"pacifica: {e}")
    # Hyperliquid funding
    hyperliquid = fetch_hyperliquid_funding()

    symbols_risex = list(risex.keys())
    others = sorted(set(list(lighter.keys()) + list(pacifica.keys()) + list(hyperliquid.keys())) - set(symbols_risex))
    all_syms = symbols_risex + others
    for s in all_syms:
        r = risex.get(s, {}).get("hourly")
        l = lighter.get(s, {}).get("hourly")
        p = pacifica.get(s, {}).get("hourly")
        hl = hyperliquid.get(s, {}).get("hourly")
        def apr(x): return x * 24 * 365 * 100 if x is not None else None
        row = {"symbol": s, "in_risex": s in risex,
               "risex_h": r, "lighter_h": l, "pacifica_h": p, "hyperliquid_h": hl,
               "risex_apr": apr(r), "lighter_apr": apr(l), "pacifica_apr": apr(p),
               "hyperliquid_apr": apr(hl),
               "lighter_default": (lighter.get(s) or {}).get("is_default", False)}
        if r is not None and l is not None: row["diff_lighter_apr"] = (r - l) * 24 * 365 * 100
        if r is not None and p is not None: row["diff_pacifica_apr"] = (r - p) * 24 * 365 * 100
        if r is not None and hl is not None: row["diff_hyperliquid_apr"] = (r - hl) * 24 * 365 * 100
        out["rows"].append(row)
    _FUND_CACHE["ts"] = now; _FUND_CACHE["data"] = out
    return out


# ============================== v5 features ==============================

# ----- Hyperliquid funding rates -----
def fetch_hyperliquid_funding():
    """POST /info with {type: 'metaAndAssetCtxs'} returns markets + funding per asset."""
    try:
        body = json.dumps({"type": "metaAndAssetCtxs"}).encode()
        req = urllib.request.Request(HYPERLIQUID_INFO, data=body,
                                      headers={"Content-Type": "application/json",
                                                "User-Agent": "rise-dashboard/5.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            d = json.loads(r.read().decode())
        # response is [meta, assetCtxs]; meta.universe lists coins in order, assetCtxs is parallel
        if isinstance(d, list) and len(d) == 2:
            universe = d[0].get("universe", [])
            ctxs = d[1]
            out = {}
            for i, c in enumerate(universe):
                if i >= len(ctxs): break
                sym = (c.get("name") or "").upper()
                ctx = ctxs[i]
                if sym:
                    out[sym] = {"hourly": f(ctx.get("funding")),
                                 "mark": f(ctx.get("markPx")),
                                 "open_interest": f(ctx.get("openInterest"))}
            return out
    except Exception:
        return {}
    return {}


# ----- Long/Short ratio per market (current + history snapshots) -----
_LS_HIST = {"by_market": {}, "last_update": 0}    # {market_name: [{t, long, short}, ...]}
_LS_LOCK = threading.Lock()


def get_long_short_ratio():
    """Snapshot actual + serie de los snapshots auto-grabados."""
    with _IDX_LOCK:
        rk = dict(_INDEX["ranking"])
        last_idx = _INDEX["last_update"]
    rows = []
    for name, m in rk.items():
        ll = m.get("oi_long", 0.0)
        ss = m.get("oi_short", 0.0)
        total = ll + ss
        if total == 0:
            continue
        rows.append({"market": name, "long_oi": ll, "short_oi": ss,
                      "long_pct": ll / total * 100, "short_pct": ss / total * 100,
                      "n_long": m.get("n_long", 0), "n_short": m.get("n_short", 0),
                      "skew": (ll - ss) / total * 100})
    rows.sort(key=lambda x: x["long_oi"] + x["short_oi"], reverse=True)
    # registrar snapshot historico
    now = int(time.time())
    with _LS_LOCK:
        for r in rows:
            hist = _LS_HIST["by_market"].setdefault(r["market"], [])
            if not hist or now - hist[-1]["t"] > 300:    # cada 5 min
                hist.append({"t": now, "long": r["long_oi"], "short": r["short_oi"]})
                _LS_HIST["by_market"][r["market"]] = hist[-300:]   # max 300 puntos
        _LS_HIST["last_update"] = now
        history = {m: list(h) for m, h in _LS_HIST["by_market"].items()}
    return {"ok": True, "last_update": last_idx, "markets": rows, "history": history}


# ----- Liquidation heatmap per market -----
def get_liquidation_heatmap(market_id):
    """Para un mercado, agrupa los notionals de las posiciones abiertas por precio de
    liquidacion en bins. Devuelve bins con notional total (long + short) y mark actual."""
    mid = str(market_id)
    # encontrar nombre y mark del mercado
    name = None; mark = 0.0; mmf_bps = 0.0; max_lev = 0.0
    try:
        for m in fetch_json(f"{RISEX}/v1/markets")["data"]["markets"]:
            if str(m.get("market_id")) == mid:
                name = m.get("display_name")
                mark = f(m.get("mark_price"))
                cfg = m.get("config", {}) or {}
                mmf_bps = f(cfg.get("maintenance_margin_factor"))
                max_lev = f(cfg.get("max_leverage"))
                break
    except Exception:
        pass
    if not name or not mark:
        return {"ok": False, "error": "market not found"}

    with _IDX_LOCK:
        mr = (_INDEX["ranking"] or {}).get(name, {})
    longs = (mr.get("longs") or [])
    shorts = (mr.get("shorts") or [])

    def liq_for(p):
        # usamos el liq_price si lo tiene la posicion, si no lo computamos
        lev = max(1, p.get("leverage", 1))
        mmf = mmf_bps / 10000.0
        side = p.get("side", "Long")
        entry = p.get("entry", mark)
        if side == "Long":
            return entry * (1 - 1 / lev + mmf)
        else:
            return entry * (1 + 1 / lev - mmf)

    # crear bins de +/- 30% del mark, 40 bins
    span = 0.30
    bin_lo = mark * (1 - span)
    bin_hi = mark * (1 + span)
    n_bins = 40
    bin_size = (bin_hi - bin_lo) / n_bins
    bins = [{"price_low": bin_lo + i * bin_size,
              "price_high": bin_lo + (i + 1) * bin_size,
              "long_notional": 0.0, "short_notional": 0.0,
              "n_long": 0, "n_short": 0}
             for i in range(n_bins)]

    def add(p, side):
        lp = liq_for(p)
        if lp is None or lp < bin_lo or lp >= bin_hi:
            return
        idx = min(n_bins - 1, max(0, int((lp - bin_lo) / bin_size)))
        nt = p.get("notional", 0.0)
        if side == "Long":
            bins[idx]["long_notional"] += nt
            bins[idx]["n_long"] += 1
        else:
            bins[idx]["short_notional"] += nt
            bins[idx]["n_short"] += 1

    for p in longs: add(p, "Long")
    for p in shorts: add(p, "Short")

    return {"ok": True, "market": name, "market_id": mid, "mark_price": mark,
            "max_leverage": max_lev, "bin_size": bin_size,
            "n_long_pos": len(longs), "n_short_pos": len(shorts),
            "bins": bins}


# ----- Market share between DEXes (volume 24h) -----
def get_market_share():
    """Compara volumen 24h y OI entre RISEx, Lighter, Pacifica, Hyperliquid."""
    out = {"ok": True, "platforms": [], "errors": []}

    # RISEx
    try:
        risex_vol = 0.0; risex_oi = 0.0
        for m in fetch_json(f"{RISEX}/v1/markets")["data"]["markets"]:
            risex_vol += f(m.get("quote_volume_24h"))
            risex_oi += f(m.get("open_interest")) * f(m.get("mark_price"))
        out["platforms"].append({"name": "RISEx", "volume_24h": risex_vol, "oi": risex_oi,
                                  "color": "#b692ff"})
    except Exception as e:
        out["errors"].append(f"risex: {e}")

    # Lighter (exchangeStats)
    try:
        s = fetch_json("https://mainnet.zklighter.elliot.ai/api/v1/exchangeStats")
        vol = f(s.get("daily_usd_volume"))
        out["platforms"].append({"name": "Lighter", "volume_24h": vol, "oi": None,
                                  "color": "#5dc8ff"})
    except Exception as e:
        out["errors"].append(f"lighter: {e}")

    # Pacifica (sum across symbols)
    try:
        pv = 0.0; po = 0.0
        for p in fetch_json(PACIFICA_PRICES).get("data", []):
            pv += f(p.get("volume_24h"))
            po += f(p.get("open_interest")) * f(p.get("mark"))
        out["platforms"].append({"name": "Pacifica", "volume_24h": pv, "oi": po,
                                  "color": "#36d39c"})
    except Exception as e:
        out["errors"].append(f"pacifica: {e}")

    # Hyperliquid (metaAndAssetCtxs - sum dayNtlVlm)
    try:
        body = json.dumps({"type": "metaAndAssetCtxs"}).encode()
        req = urllib.request.Request(HYPERLIQUID_INFO, data=body,
                                      headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as r:
            d = json.loads(r.read().decode())
        hv = 0.0; ho = 0.0
        if isinstance(d, list) and len(d) == 2:
            for c in d[1]:
                hv += f(c.get("dayNtlVlm"))
                ho += f(c.get("openInterest")) * f(c.get("markPx"))
        out["platforms"].append({"name": "Hyperliquid", "volume_24h": hv, "oi": ho,
                                  "color": "#ffb454"})
    except Exception as e:
        out["errors"].append(f"hyperliquid: {e}")

    total_vol = sum(p["volume_24h"] for p in out["platforms"] if p["volume_24h"])
    for p in out["platforms"]:
        p["volume_share_pct"] = (p["volume_24h"] / total_vol * 100) if total_vol else 0
    out["total_volume_24h"] = total_vol
    return out


# ----- Enhanced wallet stats (win rate, drawdown, avg trade size) -----
def get_wallet_stats(account):
    """Computa estadisticas avanzadas del trader analizando su historial entero."""
    account = (account or "").strip()
    out = {"ok": True, "account": account, "errors": []}
    if not (account.startswith("0x") and len(account) == 42):
        out["ok"] = False; out["errors"].append("Invalid address.")
        return out
    q = urllib.parse.quote(account)
    trades = []
    page = 1
    while page <= 20:    # hasta 20k trades para estadisticas
        try:
            d = fetch_json(f"{RISEX}/v1/trade-history?account={q}&limit=1000&page={page}")["data"]
        except Exception:
            break
        batch = d.get("trades") or []
        if not batch:
            break
        trades.extend(batch)
        if not d.get("has_next_page"):
            break
        page += 1

    if not trades:
        return {"ok": True, "account": account, "trades_analyzed": 0}

    # parse trades
    parsed = []
    for t in trades:
        try:
            ts = int(t.get("time", 0)) / 1e9
            pnl = f(t.get("realized_pnl"))
            size = f(t.get("size"))
            price = f(t.get("price"))
            fee = f(t.get("fee"))
            parsed.append({"ts": ts, "pnl": pnl, "size": size, "price": price,
                            "fee": fee, "notional": size * price,
                            "is_liq": bool(t.get("is_liquidation"))})
        except Exception:
            continue
    if not parsed:
        return {"ok": True, "account": account, "trades_analyzed": 0}

    parsed.sort(key=lambda x: x["ts"])

    # win rate (sobre trades con realized_pnl != 0, que son cierres)
    closes = [t for t in parsed if t["pnl"] != 0]
    wins = [t for t in closes if t["pnl"] > 0]
    losses = [t for t in closes if t["pnl"] < 0]
    win_rate = (len(wins) / len(closes) * 100) if closes else 0

    # avg trade size
    avg_size = sum(t["notional"] for t in parsed) / len(parsed)
    largest_trade = max(parsed, key=lambda t: t["notional"]) if parsed else None

    # max drawdown sobre el cumulative realized PnL
    cum = 0
    peak = 0
    max_dd = 0
    for t in parsed:
        cum += t["pnl"]
        if cum > peak:
            peak = cum
        dd = cum - peak
        if dd < max_dd:
            max_dd = dd

    # total PnL
    total_realized = sum(t["pnl"] for t in parsed)
    total_fees = sum(t["fee"] for t in parsed)
    n_liq = sum(1 for t in parsed if t["is_liq"])

    # profit factor (sum wins / abs sum losses)
    sum_wins = sum(t["pnl"] for t in wins)
    sum_losses = abs(sum(t["pnl"] for t in losses)) or 1e-9
    profit_factor = sum_wins / sum_losses

    # best / worst trade
    best = max(closes, key=lambda t: t["pnl"]) if closes else None
    worst = min(closes, key=lambda t: t["pnl"]) if closes else None

    # actividad: primer y ultimo trade
    first_ts = parsed[0]["ts"]
    last_ts = parsed[-1]["ts"]
    days_active = max(1, (last_ts - first_ts) / 86400)

    return {"ok": True, "account": account,
            "trades_analyzed": len(parsed),
            "closes_analyzed": len(closes),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate_pct": win_rate,
            "avg_trade_size": avg_size,
            "largest_trade_notional": largest_trade["notional"] if largest_trade else 0,
            "max_drawdown": max_dd,
            "total_realized_pnl": total_realized,
            "total_fees_paid": total_fees,
            "n_liquidations": n_liq,
            "profit_factor": profit_factor,
            "best_trade_pnl": best["pnl"] if best else 0,
            "worst_trade_pnl": worst["pnl"] if worst else 0,
            "first_trade_ts": int(first_ts),
            "last_trade_ts": int(last_ts),
            "days_active": round(days_active, 1),
            "trades_per_day": round(len(parsed) / days_active, 2)}


# ============================== servidor ==============================
_RL = {}
_RL_LOCK = threading.Lock()
RL_LIMITS = {"/api/wallet": (30, 60), "/api/funding-compare": (60, 60)}


def _client_ip(headers, fallback):
    for h in ("x-forwarded-for", "x-real-ip", "cf-connecting-ip"):
        v = headers.get(h)
        if v: return v.split(",")[0].strip()
    return fallback


def _rate_limit_ok(ip, endpoint):
    limit = RL_LIMITS.get(endpoint)
    if not limit: return True
    n, window = limit
    now = time.time()
    with _RL_LOCK:
        key = (ip, endpoint)
        hist = [t for t in _RL.get(key, []) if t > now - window]
        if len(hist) >= n:
            _RL[key] = hist; return False
        hist.append(now); _RL[key] = hist
    return True


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def _send(self, code, body, ctype):
        self.send_response(code); self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers(); self.wfile.write(body)

    def _json(self, obj, code=200):
        self._send(code, json.dumps(obj).encode("utf-8"), "application/json")

    def do_GET(self):
        path = urllib.parse.urlparse(self.path)
        ip = _client_ip({k.lower(): v for k, v in self.headers.items()}, self.client_address[0])
        if not _rate_limit_ok(ip, path.path):
            self._json({"ok": False, "error": "Rate limit exceeded."}, 429); return
        try:
            if path.path == "/api/data":
                self._json(get_overview())
            elif path.path == "/api/history":
                self._json({"ok": True, "points": load_history()})
            elif path.path == "/api/ranking":
                self._json(get_ranking())
            elif path.path == "/api/account-oi-ranking":
                self._json(get_account_oi_ranking())
            elif path.path == "/api/users":
                self._json(get_users())
            elif path.path == "/api/wallet":
                qs = urllib.parse.parse_qs(path.query)
                self._json(get_wallet((qs.get("account") or [""])[0]))
            elif path.path == "/api/volume-ranking":
                qs = urllib.parse.parse_qs(path.query)
                period = (qs.get("period") or ["1d"])[0]
                self._json(get_volume_ranking(period=period))
            elif path.path == "/api/oi-ranking":
                qs = urllib.parse.parse_qs(path.query)
                period = (qs.get("period") or ["1d"])[0]
                self._json(get_oi_ranking(period=period))
            elif path.path == "/api/funding-compare":
                self._json(get_funding_compare())
            elif path.path == "/api/live-activity":
                qs = urllib.parse.parse_qs(path.query)
                only_liq = (qs.get("only_liq") or ["false"])[0].lower() in ("true", "1", "yes")
                mkt = (qs.get("market") or [None])[0]
                self._json(get_live_activity(only_liq=only_liq, market=mkt))
            elif path.path == "/api/liquidations":
                self._json(get_live_activity(only_liq=True))
            elif path.path == "/api/pnl-ranking":
                self._json(get_pnl_ranking())
            elif path.path == "/api/market-detail":
                qs = urllib.parse.parse_qs(path.query)
                mid = (qs.get("market_id") or ["1"])[0]
                self._json(get_market_detail(mid))
            elif path.path == "/api/long-short-ratio":
                self._json(get_long_short_ratio())
            elif path.path == "/api/liquidation-heatmap":
                qs = urllib.parse.parse_qs(path.query)
                mid = (qs.get("market_id") or ["1"])[0]
                self._json(get_liquidation_heatmap(mid))
            elif path.path == "/api/market-share":
                self._json(get_market_share())
            elif path.path == "/api/wallet-stats":
                qs = urllib.parse.parse_qs(path.query)
                self._json(get_wallet_stats((qs.get("account") or [""])[0]))
            elif path.path in ("/", "/index.html"):
                self._send(200, HTML.encode("utf-8"), "text/html; charset=utf-8")
            else:
                self._send(404, b"not found", "text/plain")
        except Exception as e:
            self._json({"ok": False, "error": str(e)}, 500)


def main():
    url = f"http://localhost:{PORT}" if not IS_CLOUD else f"port {PORT}"
    print("=" * 56)
    print("  RISEx Live Stats Dashboard  ·  v4")
    if IS_CLOUD:
        print(f"  Cloud mode · listening on {BIND}:{PORT}")
        print(f"  History at: {HISTORY_FILE}")
    else:
        print(f"  Opening browser at: {url}")
    print("=" * 56)
    try:
        srv = ThreadingHTTPServer((BIND, PORT), Handler)
    except OSError as e:
        print(f"\nCould not open port {PORT}: {e}")
        try: input("\nPress ENTER to close...")
        except EOFError: pass
        return
    threading.Thread(target=indexer_loop, daemon=True).start()
    threading.Thread(target=fee_indexer_loop, daemon=True).start()
    threading.Thread(target=volume_indexer_loop, daemon=True).start()
    if not IS_CLOUD:
        try:
            import webbrowser
            threading.Timer(1.0, lambda: webbrowser.open(url)).start()
        except Exception:
            pass
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")


HTML = r"""<!DOCTYPE html>
<html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>RISEx · Live Stats</title>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 252 303'><rect width='252' height='303' fill='%23080809'/><path d='M176.12 0.39H0.59V50.72H176.12C189.97 50.72 201.20 61.98 201.20 75.88V101.04H77.36C34.96 101.04 0.59 135.41 0.59 177.81V302.34H50.74V184.00L177.66 302.33H251.36L89.42 151.37H201.20V101.29H251.36V75.88C251.36 34.19 217.66 0.39 176.12 0.39Z' fill='%23fff'/></svg>">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.5.0/dist/chart.umd.js"></script>
<style>
:root{--bg:#080809;--bg2:#0c0c0e;--panel:#131316;--panel2:#181820;--line:#242429;--line2:#1a1a1f;
 --txt:#f5f5f7;--muted:#8b8b94;--muted2:#4f4f57;
 --accent:#ffffff;--accent2:#b692ff;--accent-glow:rgba(182,146,255,.18);
 --green:#36d39c;--red:#ff5d6c;--amber:#ffb454;color-scheme:dark}
[data-theme="light"]{--bg:#fafafa;--bg2:#f4f4f5;--panel:#ffffff;--panel2:#f9f9fb;
 --line:#e6e6ea;--line2:#ececf0;--txt:#1a1a1f;--muted:#65656e;--muted2:#9a9aa0;
 --accent:#1a1a1f;--accent2:#7c5cff;--accent-glow:rgba(124,92,255,.12);color-scheme:light}
*{box-sizing:border-box}
html,body{margin:0;padding:0;background:var(--bg);color:var(--txt);font-family:-apple-system,BlinkMacSystemFont,"Inter","Segoe UI",Roboto,sans-serif;-webkit-font-smoothing:antialiased;text-rendering:optimizeLegibility}
body{
 background-image:
  radial-gradient(900px 500px at 100% -8%, rgba(182,146,255,.06), transparent 60%),
  radial-gradient(700px 400px at -10% 110%, rgba(54,211,156,.04), transparent 60%),
  radial-gradient(circle at 1px 1px, rgba(255,255,255,.022) 1px, transparent 0);
 background-size:auto,auto,24px 24px;
 background-attachment:fixed
}
a{color:var(--accent2);text-decoration:none;transition:color .15s}
a:hover{color:#d4c2ff}
/* numeros tabulares para tablas y KPI */
.val,td,th{font-variant-numeric:tabular-nums}

/* ===== Layout app-like: sidebar + main ===== */
.app{display:grid;grid-template-columns:230px 1fr;min-height:100vh}
.sidebar{background:var(--bg2);border-right:1px solid var(--line);padding:18px 0;position:sticky;top:0;height:100vh;overflow-y:auto}
.sidebar::-webkit-scrollbar{width:6px}.sidebar::-webkit-scrollbar-thumb{background:var(--line);border-radius:3px}
.brand{padding:0 20px 16px;display:flex;align-items:center;gap:11px;border-bottom:1px solid var(--line2);margin-bottom:14px}
.brand .logo{width:36px;height:36px;border-radius:10px;background:linear-gradient(135deg,#1d1d22,#0e0e12);border:1px solid var(--line);display:flex;align-items:center;justify-content:center;box-shadow:0 4px 14px rgba(0,0,0,.4)}
.brand .logo svg{width:22px;height:auto;display:block}
.brand .name{font-weight:800;font-size:14px;letter-spacing:.2px}.brand .sub{color:var(--muted);font-size:10.5px;margin-top:1px}
.navgroup{padding:0 10px;margin-bottom:10px}
.navtitle{color:var(--muted2);font-size:10px;text-transform:uppercase;letter-spacing:.9px;padding:6px 10px 4px;font-weight:700}
.navitem{display:flex;align-items:center;gap:10px;padding:8px 10px;border-radius:8px;color:var(--muted);cursor:pointer;font-size:13px;font-weight:600;transition:all .12s;user-select:none}
.navitem:hover{background:#16161b;color:var(--txt)}
.navitem.active{background:linear-gradient(135deg,#1c1828,#161320);color:#fff;border:1px solid rgba(182,146,255,.18);box-shadow:0 3px 10px var(--accent-glow) inset}
.navitem .ico{width:18px;text-align:center;opacity:.85;font-size:14px}

/* ===== Top bar ===== */
.main{min-width:0}
.topbar{display:flex;gap:12px;align-items:center;padding:14px 28px;border-bottom:1px solid var(--line2);position:sticky;top:0;background:rgba(10,11,15,.85);backdrop-filter:saturate(180%) blur(8px);-webkit-backdrop-filter:saturate(180%) blur(8px);z-index:10}
.topbar .menu-btn{display:none;background:none;border:none;color:var(--txt);font-size:20px;cursor:pointer;padding:4px 8px}
.topbar .gsearch{flex:1;max-width:520px;position:relative}
.topbar .gsearch input{width:100%;padding:9px 12px 9px 36px;background:#0f1117;border:1px solid var(--line);border-radius:10px;color:var(--txt);font-size:13.5px;transition:border-color .15s}
.topbar .gsearch input:focus{outline:none;border-color:var(--accent)}
.topbar .gsearch::before{content:"🔍";position:absolute;left:12px;top:50%;transform:translateY(-50%);font-size:13px;opacity:.6;pointer-events:none}
.topbar .live{margin-left:auto;display:flex;gap:10px;align-items:center;font-size:12.5px;color:var(--muted)}
.dot{width:8px;height:8px;border-radius:50%;background:var(--green);box-shadow:0 0 0 4px rgba(54,211,156,.15);animation:pulse 2.4s ease-in-out infinite}
@keyframes pulse{0%,100%{box-shadow:0 0 0 0 rgba(54,211,156,.45)}50%{box-shadow:0 0 0 8px rgba(54,211,156,0)}}
.rf,.btn{background:#23202e;border:1px solid var(--line);color:var(--txt);border-radius:9px;padding:6px 11px;font-size:12.5px;cursor:pointer;transition:border-color .15s}
.rf:hover,.btn:hover{border-color:var(--accent)}

.wrap{padding:24px 28px;max-width:1320px;margin:0 auto}
.view{display:none;animation:fade .25s ease}.view.active{display:block}
@keyframes fade{from{opacity:0;transform:translateY(2px)}to{opacity:1;transform:none}}

/* compat: la .tab antigua redirige a .navitem (por si queda algun selector) */
.tabs{display:none}
.cards{display:grid;grid-template-columns:repeat(5,1fr);gap:14px;margin-bottom:18px}
.card{position:relative;background:linear-gradient(180deg,var(--panel),var(--panel2));border:1px solid var(--line);border-radius:14px;padding:16px 18px;transition:all .2s;overflow:hidden}
.card::before{content:"";position:absolute;top:0;left:14px;right:14px;height:1px;background:linear-gradient(90deg,transparent,rgba(182,146,255,.35),transparent);opacity:0;transition:opacity .2s}
.card:hover{transform:translateY(-1px);border-color:rgba(182,146,255,.22);box-shadow:0 8px 24px -10px rgba(0,0,0,.4),0 0 0 1px rgba(182,146,255,.04)}
.card:hover::before{opacity:1}
.card.alert{border-color:var(--amber);box-shadow:0 0 0 3px rgba(255,180,84,.12)}
.card .lbl{color:var(--muted);font-size:10.5px;text-transform:uppercase;letter-spacing:.7px;font-weight:600;display:flex;align-items:center;gap:6px}
.card .val{font-size:25px;font-weight:700;margin-top:8px;letter-spacing:-.5px;background:linear-gradient(180deg,#ffffff,#bcbcc7);-webkit-background-clip:text;background-clip:text;color:transparent;line-height:1.15}
.card .val.pos{background:linear-gradient(180deg,#5feab6,#36d39c);-webkit-background-clip:text;background-clip:text;color:transparent}
.card .val.neg{background:linear-gradient(180deg,#ff8a96,#ff5d6c);-webkit-background-clip:text;background-clip:text;color:transparent}
.card .meta{font-size:11.5px;color:var(--muted);margin-top:6px}
/* .tag definida mas abajo con estilo refinado */
.grid2{display:grid;grid-template-columns:1.4fr 1fr;gap:14px;margin-bottom:16px}
.panel{background:linear-gradient(180deg,var(--panel),var(--panel2));border:1px solid var(--line);border-radius:14px;padding:18px 20px;transition:border-color .15s}
.panel:hover{border-color:rgba(182,146,255,.12)}
.panel h2{font-size:14px;margin:0 0 14px;font-weight:650;color:#cfd5e3;display:flex;align-items:center;gap:10px;letter-spacing:-.1px}
.panel h2::before{content:"";display:inline-block;width:3px;height:14px;border-radius:2px;background:linear-gradient(180deg,var(--accent2),rgba(182,146,255,.25))}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{padding:10px 9px;text-align:right;border-bottom:1px solid var(--line2);white-space:nowrap}
th:first-child,td:first-child{text-align:left}
th{color:var(--muted);font-weight:600;font-size:10.5px;text-transform:uppercase;letter-spacing:.6px;cursor:pointer;user-select:none;background:rgba(255,255,255,.012)}
th:hover{color:var(--txt)}
tbody tr{transition:background .12s}
tbody tr:nth-child(even){background:rgba(255,255,255,.013)}
tbody tr:hover{background:rgba(182,146,255,.06)}
.pos{color:var(--green)}.neg{color:var(--red)}.mkt{font-weight:600;color:#fff}
.bar{height:5px;border-radius:3px;background:rgba(255,255,255,.05);overflow:hidden;margin-top:5px}
.bar>i{display:block;height:100%;background:linear-gradient(90deg,#7c5cff,#b692ff);border-radius:3px;box-shadow:0 0 8px rgba(124,92,255,.4)}
.pill{font-size:11px;padding:3px 10px;border-radius:999px;font-weight:700;letter-spacing:.2px}
.pill.bid{background:rgba(54,211,156,.13);color:var(--green);border:1px solid rgba(54,211,156,.25)}
.pill.ask{background:rgba(255,93,108,.13);color:var(--red);border:1px solid rgba(255,93,108,.25)}
input,select{background:#0f1117;border:1px solid var(--line);color:var(--txt);border-radius:9px;padding:9px 11px;font-size:13px}
input{width:430px;max-width:100%}
.row{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-bottom:14px}
.note{color:var(--muted);font-size:12px;line-height:1.6}
.empty{color:var(--muted);font-size:13px;padding:18px 4px}
.alertgrid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}
label.fl{display:block;font-size:12px;color:var(--muted);margin-bottom:6px}
.seg{display:inline-flex;background:#0c0c10;border:1px solid var(--line);border-radius:10px;overflow:hidden;padding:3px}
.seg button{background:none;border:none;color:var(--muted);padding:6px 12px;font-size:12.5px;cursor:pointer;border-radius:7px;font-weight:600;transition:all .12s}
.seg button:hover{color:var(--txt)}
.seg button.active{background:linear-gradient(180deg,#221d36,#1a1726);color:#fff;border:1px solid rgba(182,146,255,.18);box-shadow:0 1px 6px rgba(0,0,0,.3)}
a{color:var(--accent2);text-decoration:none}
footer{color:var(--muted);font-size:11.5px;margin-top:18px;line-height:1.6}
/* Wallet detail page */
.whead{background:linear-gradient(135deg,#1a1726,#0e0e12 60%);border:1px solid var(--line);border-radius:16px;padding:24px 26px;margin-bottom:18px;position:relative;overflow:hidden}
.whead::before{content:"";position:absolute;right:-80px;top:-80px;width:280px;height:280px;background:radial-gradient(closest-side,rgba(182,146,255,.22),transparent);pointer-events:none}
.whead::after{content:"";position:absolute;left:-60px;bottom:-60px;width:200px;height:200px;background:radial-gradient(closest-side,rgba(54,211,156,.08),transparent);pointer-events:none}
.whead .row1{display:flex;align-items:center;gap:14px;flex-wrap:wrap}
.whead .avatar{width:46px;height:46px;border-radius:50%;background:linear-gradient(135deg,var(--accent),var(--accent2));display:flex;align-items:center;justify-content:center;color:#fff;font-weight:800;font-size:18px}
.whead .addr{font-family:ui-monospace,Menlo,monospace;font-size:14.5px;letter-spacing:.3px;color:#e9edf5}
.whead .actions{margin-left:auto;display:flex;gap:8px;flex-wrap:wrap}
.chip{display:inline-flex;align-items:center;gap:5px;background:#1a1a20;border:1px solid var(--line);color:var(--txt);padding:7px 11px;border-radius:9px;font-size:12px;cursor:pointer;text-decoration:none;transition:all .12s;font-weight:500}
.chip:hover{border-color:rgba(182,146,255,.4);background:#1e1e26;transform:translateY(-1px)}
.sectitle{font-size:14px;font-weight:650;color:#cfd5e3;margin:22px 0 12px;letter-spacing:-.1px;display:flex;align-items:center;gap:10px}
.sectitle::before{content:"";display:inline-block;width:3px;height:14px;border-radius:2px;background:linear-gradient(180deg,var(--accent2),rgba(182,146,255,.25))}
.pillside{display:inline-block;font-size:10.5px;font-weight:700;padding:3px 9px;border-radius:999px;letter-spacing:.3px}
.pillside.Long,.pillside.Buy{background:rgba(54,211,156,.14);color:var(--green);border:1px solid rgba(54,211,156,.28)}
.pillside.Short,.pillside.Sell{background:rgba(255,93,108,.14);color:var(--red);border:1px solid rgba(255,93,108,.28)}
.tag{display:inline-flex;align-items:center;gap:4px;font-size:10px;padding:2px 7px;border-radius:6px;vertical-align:middle;margin-left:6px;font-weight:700;letter-spacing:.2px}
.tag.live{background:rgba(54,211,156,.13);color:var(--green);border:1px solid rgba(54,211,156,.25)}
.tag.snap{background:rgba(182,146,255,.10);color:var(--accent2);border:1px solid rgba(182,146,255,.22)}
.liq-near{color:var(--red);font-weight:600}
.liq-mid{color:var(--amber);font-weight:600}
.liq-far{color:var(--green)}
.role-T{color:var(--amber);font-size:11px;font-weight:600}
.role-M{color:var(--accent2);font-size:11px;font-weight:600}
@media(max-width:980px){
 .cards{grid-template-columns:repeat(2,1fr)}.grid2{grid-template-columns:1fr}.alertgrid{grid-template-columns:1fr}
 .app{grid-template-columns:1fr}
 .sidebar{position:fixed;left:-260px;top:0;width:240px;transition:left .22s;z-index:100;box-shadow:0 0 40px rgba(0,0,0,.6)}
 .sidebar.open{left:0}
 .topbar .menu-btn{display:block}
 .wrap{padding:18px 16px}
 .topbar{padding:12px 16px}
}
</style></head>
<body><div class="app">
<aside class="sidebar" id="sidebar">
 <div class="brand">
  <div class="logo"><svg viewBox="0 0 252 303" xmlns="http://www.w3.org/2000/svg"><path d="M176.12 0.39H0.59V50.72H176.12C189.97 50.72 201.20 61.98 201.20 75.88V101.04H77.36C34.96 101.04 0.59 135.41 0.59 177.81V302.34H50.74V184.00L177.66 302.33H251.36L89.42 151.37H201.20V101.29H251.36V75.88C251.36 34.19 217.66 0.39 176.12 0.39Z" fill="#fff"/></svg></div>
  <div><div class="name">RISEx Stats</div><div class="sub">live perp analytics</div></div>
 </div>
 <nav>
  <div class="navgroup">
   <div class="navtitle">Overview</div>
   <div class="navitem active" data-v="overview"><span class="ico">📊</span>Overview</div>
   <div class="navitem" data-v="markets"><span class="ico">📈</span>Markets</div>
  </div>
  <div class="navgroup">
   <div class="navtitle">Rankings</div>
   <div class="navitem" data-v="ranking"><span class="ico">⚖️</span>Positions</div>
   <div class="navitem" data-v="acctoi"><span class="ico">💼</span>Current OI</div>
   <div class="navitem" data-v="volranking"><span class="ico">💹</span>Volume</div>
   <div class="navitem" data-v="oiranking"><span class="ico">⏱️</span>Avg OI</div>
   <div class="navitem" data-v="pnl"><span class="ico">🏆</span>Top PnL</div>
  </div>
  <div class="navgroup">
   <div class="navtitle">Activity</div>
   <div class="navitem" data-v="liq"><span class="ico">💀</span>Liquidations</div>
   <div class="navitem" data-v="feed"><span class="ico">⚡</span>Live activity</div>
  </div>
  <div class="navgroup">
   <div class="navtitle">Insights</div>
   <div class="navitem" data-v="longshort"><span class="ico">⚖️</span>Long / Short</div>
   <div class="navitem" data-v="heatmap"><span class="ico">🔥</span>Liq. heatmap</div>
   <div class="navitem" data-v="marketshare"><span class="ico">🥧</span>Market share</div>
  </div>
  <div class="navgroup">
   <div class="navtitle">Market data</div>
   <div class="navitem" data-v="funding"><span class="ico">💱</span>Funding vs DEXes</div>
   <div class="navitem" data-v="users"><span class="ico">👥</span>Users</div>
  </div>
  <div class="navgroup">
   <div class="navtitle">Tools</div>
   <div class="navitem" data-v="watchlist"><span class="ico">⭐</span>Watchlist</div>
   <div class="navitem" data-v="alerts"><span class="ico">🔔</span>Alerts</div>
  </div>
 </nav>
</aside>
<main class="main">
<div class="topbar">
 <button class="menu-btn" onclick="document.getElementById('sidebar').classList.toggle('open')">☰</button>
 <div class="gsearch"><input id="gs_input" placeholder="Search wallet (0x…) or market (BTC, ETH…)" autocomplete="off" /></div>
 <div class="live">
  <span class="dot" id="dot"></span><span id="updated">loading…</span>
  <button class="rf" onclick="loadAll(true)" title="Refresh">↻</button>
  <button class="rf" onclick="toggleTheme()" id="theme_btn" title="Toggle light/dark">🌙</button>
 </div>
</div>
<div class="wrap">

<div class="view active" id="v_overview">
 <div class="cards">
  <div class="card" id="k_vol"><div class="lbl">24h Volume<span class="tag live">live</span></div><div class="val" id="c_vol">—</div><div class="meta" id="c_vol_m"></div></div>
  <div class="card" id="k_oi"><div class="lbl">Open Interest<span class="tag live">live</span></div><div class="val" id="c_oi">—</div><div class="meta" id="c_oi_m"></div></div>
  <div class="card"><div class="lbl">TVL<span class="tag snap">DefiLlama</span></div><div class="val" id="c_tvl">—</div><div class="meta">total value locked</div></div>
  <div class="card"><div class="lbl">OI / TVL ratio</div><div class="val" id="c_ratio">—</div><div class="meta">protocol leverage</div></div>
  <div class="card"><div class="lbl">24h Fees<span class="tag snap" id="fee_tag">est.</span></div><div class="val" id="c_fee">—</div><div class="meta" id="c_fee_m"></div></div>
 </div>
 <div class="grid2">
  <div class="panel"><h2>TVL · evolution</h2><canvas id="tvlChart" height="150"></canvas></div>
  <div class="panel"><h2>24h Volume by market</h2><canvas id="volChart" height="150"></canvas></div>
 </div>
 <div class="grid2">
  <div class="panel"><h2>24h Volume · auto-recorded history</h2><canvas id="hVol" height="150"></canvas></div>
  <div class="panel"><h2>Open Interest · auto-recorded history</h2><canvas id="hOi" height="150"></canvas></div>
 </div>
 <div class="note" style="text-align:center">History is recorded every couple of minutes while the server is running (file <code>rise_history.json</code>).</div>
</div>

<div class="view" id="v_markets">
 <div class="panel"><h2>Markets</h2>
  <table id="tbl"><thead><tr>
   <th data-k="name">Market</th><th data-k="last_price">Price</th><th data-k="change_24h">24h %</th>
   <th data-k="volume_24h">24h Volume</th><th data-k="oi_usd">Open Interest</th>
   <th data-k="funding_8h">Funding 8h</th><th data-k="funding_apr">Funding APR</th>
   <th data-k="basis_pct">Basis</th><th data-k="spread_bps">Spread</th><th data-k="max_leverage">Lev.</th>
  </tr></thead><tbody id="tbody"></tbody></table>
 </div>
</div>

<div class="view" id="v_ranking">
 <div class="panel">
  <h2>Largest open positions by market</h2>
  <div class="note" id="rk_status" style="margin-bottom:12px">Starting…</div>
  <div class="row"><label class="fl" style="margin:0">Market:</label>
   <select id="rk_market"></select><span class="note" id="rk_oi"></span></div>
  <div class="grid2">
   <div><h2 style="font-size:13px;color:var(--green)">▲ Longs</h2>
    <table><thead><tr><th>#</th><th>Account</th><th>Size</th><th>Notional</th><th>Entry</th><th>PnL</th><th>Lev</th></tr></thead><tbody id="rk_long"></tbody></table></div>
   <div><h2 style="font-size:13px;color:var(--red)">▼ Shorts</h2>
    <table><thead><tr><th>#</th><th>Account</th><th>Size</th><th>Notional</th><th>Entry</th><th>PnL</th><th>Lev</th></tr></thead><tbody id="rk_short"></tbody></table></div>
  </div>
 </div>
</div>

<div class="view" id="v_acctoi">
 <div class="panel">
  <h2>Account ranking by Open Interest open RIGHT NOW</h2>
  <div class="note" id="ao_status" style="margin-bottom:12px">Loading…</div>
  <span class="note" id="ao_totals" style="display:block;margin-bottom:10px"></span>
  <table><thead><tr><th>#</th><th>Account</th><th>Total OI</th><th>% of total</th>
   <th>Long</th><th>Short</th><th>Unrealized PnL</th><th>Positions</th><th>Markets</th></tr></thead>
   <tbody id="ao_body"></tbody></table>
  <div class="note" style="margin-top:10px">Sum of notional of all open positions each account currently has. Current snapshot (not time-weighted). Refreshes every 3 min.</div>
 </div>
</div>

<div class="view" id="v_volranking">
 <div class="panel">
  <h2>Account ranking by realized volume</h2>
  <div class="note" id="vr_status" style="margin-bottom:12px">Calculating…</div>
  <div class="row">
   <span class="seg" id="vr_seg">
    <button data-p="1d" class="active">1 day</button>
    <button data-p="7d">7 days</button>
    <button data-p="30d">30 days</button>
    <button data-p="29may">Since May 29 07:00 UTC</button>
   </span>
   <span class="note" id="vr_totals"></span>
  </div>
  <table><thead><tr><th>#</th><th>Account</th><th>Volume</th><th>% of total</th><th>Trades</th></tr></thead>
   <tbody id="vr_body"></tbody></table>
  <div class="note" style="margin-top:10px">Reconstructed by summing price×size of every trade of every account. First load: a few minutes to scan all accounts; refreshes every 10 min.</div>
 </div>
</div>

<div class="view" id="v_oiranking">
 <div class="panel">
  <h2>Account ranking by Average Open Interest (TWAP OI)</h2>
  <div class="note" id="oir_status" style="margin-bottom:12px">Calculating…</div>
  <div class="row">
   <span class="seg" id="oir_seg">
    <button data-p="1d" class="active">1 day</button>
    <button data-p="7d">7 days</button>
    <button data-p="30d">30 days</button>
    <button data-p="29may">Since May 29 07:00 UTC</button>
   </span>
   <span class="note" id="oir_totals"></span>
  </div>
  <table><thead><tr><th>#</th><th>Account</th><th>Avg OI (TWAP)</th><th>% of total</th><th>Trades in 30d</th></tr></thead>
   <tbody id="oir_body"></tbody></table>
  <div class="note" style="margin-top:10px">"Avg OI" = TWAP OI: time-weighted average of position notional within the window. Reconstructed trade by trade: we update each position and multiply |position| × price by the time it stayed alive. The custom window <b>Since May 29 07:00 UTC</b> spans from that moment to now.</div>
 </div>
</div>

<div class="view" id="v_pnl">
 <div class="panel">
  <h2>Top Traders by PnL · realized (30d) + unrealized</h2>
  <div class="note" id="pn_status" style="margin-bottom:12px">Calculating…</div>
  <div class="grid2">
   <div><h2 style="font-size:13px;color:var(--green)">🏆 Winners</h2>
    <table><thead><tr><th>#</th><th>Account</th><th>Total PnL</th><th>Realized</th><th>Unrealized</th><th>Trades</th></tr></thead><tbody id="pn_winners"></tbody></table></div>
   <div><h2 style="font-size:13px;color:var(--red)">💀 Losers</h2>
    <table><thead><tr><th>#</th><th>Account</th><th>Total PnL</th><th>Realized</th><th>Unrealized</th><th>Liq.</th></tr></thead><tbody id="pn_losers"></tbody></table></div>
  </div>
  <div class="note" style="margin-top:10px">Realized PnL: summing the <code>realized_pnl</code> field of each trade of the account in the last 30 days (capped by the configured page limit). Unrealized PnL: from current snapshot of open positions. Total = both.</div>
 </div>
</div>

<div class="view" id="v_liq">
 <div class="panel">
  <h2>Recent liquidations (last 24h)</h2>
  <div class="note" id="lq_status" style="margin-bottom:12px">Loading…</div>
  <span class="note" id="lq_totals" style="display:block;margin-bottom:10px"></span>
  <div class="panel" style="padding:0;max-height:600px;overflow:auto">
   <table><thead><tr><th>Time</th><th>Account</th><th>Market</th><th>Side</th><th>Size</th><th>Price</th><th>Notional</th><th>Loss</th></tr></thead>
    <tbody id="lq_body"></tbody></table>
  </div>
 </div>
</div>

<div class="view" id="v_feed">
 <div class="panel">
  <h2>Live Activity · large trades and liquidations (24h)</h2>
  <div class="note" id="fd_status" style="margin-bottom:12px">Loading…</div>
  <div class="row">
   <span class="seg" id="fd_filter">
    <button data-f="all" class="active">All</button>
    <button data-f="liq">Liquidations only</button>
    <button data-f="big">Large trades only</button>
   </span>
   <span class="note" id="fd_totals"></span>
  </div>
  <div class="panel" style="padding:0;max-height:680px;overflow:auto">
   <table><thead><tr><th>Time</th><th>Type</th><th>Account</th><th>Market</th><th>Side</th><th>Role</th><th>Size</th><th>Price</th><th>Notional</th><th>PnL</th></tr></thead>
    <tbody id="fd_body"></tbody></table>
  </div>
  <div class="note" style="margin-top:10px">Only trades with notional ≥ $10K and all liquidations are shown. Refreshes when the account indexer cycles again (~10 min).</div>
 </div>
</div>

<div class="view" id="v_marketdetail">
 <div class="panel" id="md_panel">
  <div class="row" style="margin-bottom:8px"><span class="chip" onclick="goHome()">← Back</span></div>
  <div id="md_content"><div class="empty">Loading market…</div></div>
 </div>
</div>

<div class="view" id="v_longshort">
 <div class="panel">
  <h2>Long / Short ratio per market</h2>
  <div class="note" id="ls_status" style="margin-bottom:12px">Loading…</div>
  <div class="panel" style="padding:0"><table><thead><tr>
   <th>Market</th><th>OI Long</th><th>OI Short</th><th>Long %</th><th>Short %</th>
   <th>Skew</th><th># Longs</th><th># Shorts</th></tr></thead>
   <tbody id="ls_body"></tbody></table></div>
  <div class="grid2" style="margin-top:14px">
   <div class="panel" style="border:none;background:none;padding:0"><h2>Long % over time · top markets</h2><canvas id="lsChart" height="180"></canvas></div>
   <div class="panel" style="border:none;background:none;padding:0"><h2>Long vs Short notional · current</h2><canvas id="lsBars" height="180"></canvas></div>
  </div>
  <div class="note" style="margin-top:10px">Skew positive = market is net long (more long OI than short). The chart shows long % evolution over time for the markets with most OI.</div>
 </div>
</div>

<div class="view" id="v_heatmap">
 <div class="panel">
  <h2>Liquidation heatmap</h2>
  <div class="note" id="hm_status" style="margin-bottom:12px">Loading…</div>
  <div class="row">
   <label class="fl" style="margin:0">Market:</label>
   <select id="hm_market"></select>
   <span class="note" id="hm_mark"></span>
  </div>
  <canvas id="hmChart" height="240"></canvas>
  <div class="note" style="margin-top:10px">Each bar shows the total <b>notional</b> of open positions whose liquidation price falls within that price bin. Green = longs (would be liquidated if price drops), red = shorts (would be liquidated if price rises). Dashed line = current mark price. Helps you spot price levels where lots of positions would unwind.</div>
 </div>
</div>

<div class="view" id="v_marketshare">
 <div class="panel">
  <h2>Market share across perp DEXes (24h volume)</h2>
  <div class="note" id="mshare_status" style="margin-bottom:12px">Loading…</div>
  <div class="grid2">
   <div class="panel" style="border:none;background:none;padding:0"><canvas id="msPie" height="280"></canvas></div>
   <div class="panel" style="border:none;background:none;padding:0;display:flex;align-items:center"><div id="ms_legend" style="width:100%"></div></div>
  </div>
  <div class="note" style="margin-top:10px">RISEx vs Lighter vs Pacifica vs Hyperliquid. Volume in USD. Lighter uses <code>exchangeStats.daily_usd_volume</code>; Hyperliquid sums <code>dayNtlVlm</code> across all assets.</div>
 </div>
</div>

<div class="view" id="v_watchlist">
 <div class="panel">
  <h2>Watchlist · saved wallets</h2>
  <div class="note" style="margin-bottom:12px">Add wallets to follow them. Saved locally in your browser (localStorage), not on the server.</div>
  <div class="row">
   <input id="wl_input" placeholder="0x… address to add" />
   <input id="wl_label" placeholder="Optional label (e.g. 'Whale BTC')" style="width:230px" />
   <button class="rf" onclick="wlAdd()">Add</button>
  </div>
  <div id="wl_body"></div>
 </div>
</div>

<div class="view" id="v_funding">
 <div class="panel">
  <h2>Funding rates · comparison between perp DEXes</h2>
  <div class="note" id="fc_status" style="margin-bottom:12px">Loading…</div>
  <div class="row">
   <span class="seg" id="fc_mode">
    <button data-m="apr" class="active">APR (% annual)</button>
    <button data-m="hourly">% per hour</button>
   </span>
   <span class="seg" id="fc_filter">
    <button data-f="risex" class="active">RISEx markets only</button>
    <button data-f="all">All</button>
   </span>
  </div>
  <table id="fc_tbl"><thead><tr>
   <th>Symbol</th><th>RISEx</th><th>Lighter</th><th>Pacifica</th><th>Hyperliquid</th>
   <th>Δ vs Lighter</th><th>Δ vs Pacifica</th><th>Δ vs HL</th>
  </tr></thead><tbody id="fc_body"></tbody></table>
  <div class="note" style="margin-top:10px">All three platforms pay funding every hour. Positive Δ = longs on RISEx pay more than on the other. Sources: <code>api.rise.trade</code>, <code>mainnet.zklighter.elliot.ai</code> (exchange=lighter, public endpoint), <code>api.pacifica.fi</code>.<br>
  <b>*</b> Default value from Lighter's public API (this is NOT the actual rate shown on <code>lighter.xyz</code> for these low-activity markets). Lighter classifies ~54% of its markets with this common value on the public endpoint.</div>
 </div>
</div>

<div class="view" id="v_users">
 <div class="panel">
  <h2>Platform adoption</h2>
  <div class="note" id="us_status" style="margin-bottom:12px">Loading…</div>
  <div class="cards">
   <div class="card"><div class="lbl">Accounts (≥1 tx)</div><div class="val" id="us_total">—</div><div class="meta">have traded at least once</div></div>
   <div class="card"><div class="lbl">Total addresses</div><div class="val" id="us_addr">—</div><div class="meta">includes smart-accounts</div></div>
   <div class="card"><div class="lbl">With open position</div><div class="val" id="us_active">—</div><div class="meta">right now</div></div>
   <div class="card"><div class="lbl">New today</div><div class="val" id="us_today">—</div><div class="meta">today's signups (UTC)</div></div>
   <div class="card"><div class="lbl">New (7 days)</div><div class="val" id="us_7d">—</div><div class="meta">last week</div></div>
  </div>
  <div class="grid2">
   <div class="panel" style="border:none;background:none;padding:0"><h2>New accounts per day</h2><canvas id="usNew" height="150"></canvas></div>
   <div class="panel" style="border:none;background:none;padding:0"><h2>Cumulative growth</h2><canvas id="usCum" height="150"></canvas></div>
  </div>
  <div class="panel" style="border:none;background:none;padding:0;margin-top:14px"><h2>Active accounts per day</h2><canvas id="usAct" height="120"></canvas></div>
 </div>
</div>

<div class="view" id="v_wallet">
 <div id="walletOut"></div>
</div>

<div class="view" id="v_alerts">
 <div class="panel"><h2>Alert thresholds</h2>
  <div class="alertgrid">
   <div><label class="fl">24h Volume greater than ($)</label><input id="al_vol" style="width:100%" type="number"></div>
   <div><label class="fl">Open Interest greater than ($)</label><input id="al_oi" style="width:100%" type="number"></div>
   <div><label class="fl">Funding 8h (abs) greater than (%)</label><input id="al_fund" style="width:100%" type="number" step="0.001"></div>
  </div>
  <div class="row" style="margin-top:14px">
   <button class="rf" onclick="saveAlerts()">Save</button>
   <button class="rf" onclick="askNotif()">Enable notifications</button>
   <span class="note" id="al_status"></span>
  </div>
 </div>
</div>

</div>
</main></div>

<script>
let DATA=null,sortK='volume_24h',sortDir=-1,charts={};
const U=n=>{if(n==null||isNaN(n))return'—';const a=Math.abs(n);
 if(a>=1e9)return'$'+(n/1e9).toFixed(2)+'B';if(a>=1e6)return'$'+(n/1e6).toFixed(2)+'M';
 if(a>=1e3)return'$'+(n/1e3).toFixed(1)+'K';return'$'+n.toFixed(2);};
const P=n=>n>=100?n.toLocaleString('en-US',{maximumFractionDigits:1}):n.toLocaleString('en-US',{maximumFractionDigits:4});
const shortAddr=a=>a.slice(0,6)+'…'+a.slice(-4);
const agoStr=ts=>{const s=Math.floor(Date.now()/1000)-ts;if(s<60)return s+'s';if(s<3600)return Math.floor(s/60)+'min';return Math.floor(s/3600)+'h'};
const alerts=JSON.parse(localStorage.getItem('rise_alerts')||'{}');

document.querySelectorAll('.navitem').forEach(t=>t.onclick=()=>{
 document.querySelectorAll('.navitem').forEach(x=>x.classList.remove('active'));
 document.querySelectorAll('.view').forEach(x=>x.classList.remove('active'));
 t.classList.add('active');document.getElementById('v_'+t.dataset.v).classList.add('active');
 // cerrar sidebar en movil tras seleccionar
 const sb=document.getElementById('sidebar');if(sb)sb.classList.remove('open');
 // limpiar hash si estamos saliendo de wallet/market detail
 if(t.dataset.v!=='wallet'&&t.dataset.v!=='marketdetail'&&(location.hash.includes('wallet=')||location.hash.includes('market=')))history.replaceState(null,'',location.pathname);
 if(t.dataset.v==='ranking')loadRanking();
 if(t.dataset.v==='acctoi')loadAcctOi();
 if(t.dataset.v==='users')loadUsers();
 if(t.dataset.v==='volranking')loadVolRanking();
 if(t.dataset.v==='oiranking')loadOiRanking();
 if(t.dataset.v==='funding')loadFunding();
 if(t.dataset.v==='pnl')loadPnl();
 if(t.dataset.v==='liq')loadLiq();
 if(t.dataset.v==='feed')loadFeed();
 if(t.dataset.v==='longshort')loadLongShort();
 if(t.dataset.v==='heatmap')loadHeatmap();
 if(t.dataset.v==='marketshare')loadMarketShare();
 if(t.dataset.v==='watchlist')loadWatchlist();
});

// ======== Light/Dark theme toggle ========
function applyTheme(t){
 if(t==='light'){document.documentElement.setAttribute('data-theme','light');document.getElementById('theme_btn').textContent='☀️';}
 else{document.documentElement.removeAttribute('data-theme');document.getElementById('theme_btn').textContent='🌙';}
 // refresh charts so colors update
 Object.values(charts||{}).forEach(c=>{try{c.update('none')}catch(e){}});
}
function toggleTheme(){
 const cur=localStorage.getItem('rise_theme')||'dark';
 const nxt=cur==='light'?'dark':'light';
 localStorage.setItem('rise_theme',nxt);applyTheme(nxt);
}
applyTheme(localStorage.getItem('rise_theme')||'dark');

// ======== CSV export utility ========
function exportCsv(filename, rows){
 if(!rows||!rows.length)return;
 const cols=Object.keys(rows[0]);
 const lines=[cols.join(',')];
 for(const r of rows){
  lines.push(cols.map(c=>{let v=r[c];if(v==null)return'';if(typeof v==='string')v='"'+v.replace(/"/g,'""')+'"';return v;}).join(','));
 }
 const blob=new Blob([lines.join('\n')],{type:'text/csv'});
 const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download=filename;a.click();
}

// ======== Long / Short ratio ========
let LS_DATA=null;
async function loadLongShort(){
 try{const d=await (await fetch('/api/long-short-ratio')).json();LS_DATA=d;
  document.getElementById('ls_status').innerHTML=`<b style="color:var(--green)">Live</b> · ${d.markets.length} markets`;
  const tb=document.getElementById('ls_body');
  tb.innerHTML=d.markets.map(r=>{const sk=r.skew>=0?'pos':'neg';
   return `<tr><td class="mkt"><a href="#market=${(DATA&&DATA.markets||[]).find(x=>x.name===r.market)?.market_id||''}">${r.market}</a></td>
    <td class="pos">${U(r.long_oi)}</td><td class="neg">${U(r.short_oi)}</td>
    <td>${r.long_pct.toFixed(1)}%</td><td>${r.short_pct.toFixed(1)}%</td>
    <td class="${sk}">${r.skew>=0?'+':''}${r.skew.toFixed(2)}%</td>
    <td>${r.n_long}</td><td>${r.n_short}</td></tr>`;}).join('')||'<tr><td class="empty" colspan=8>No data yet.</td></tr>';
  // chart: long % over time for top 5 markets by OI
  const top=d.markets.slice(0,5).map(m=>m.market);
  const colors=['#b692ff','#36d39c','#ffb454','#5dc8ff','#ff8a96'];
  const datasets=top.map((m,i)=>{const series=(d.history[m]||[]).map(p=>({x:p.t*1000,y:p.long/(p.long+p.short)*100}));
   return {label:m,data:series,borderColor:colors[i%colors.length],backgroundColor:'transparent',tension:.3,pointRadius:0,borderWidth:2};});
  const c=document.getElementById('lsChart');
  if(charts.lsChart)charts.lsChart.destroy();
  charts.lsChart=new Chart(c,{type:'line',data:{datasets},options:{plugins:{legend:{display:true,labels:{color:'#8b8b94',font:{size:10}}}},
   scales:{x:{type:'time',ticks:{color:'#8b8b94',maxTicksLimit:6},grid:{display:false},adapters:{}},y:{min:0,max:100,ticks:{color:'#8b8b94',callback:v=>v+'%'},grid:{color:'#21242e'}}}}});
  // bars: long vs short for top 10
  const top10=d.markets.slice(0,10);
  if(charts.lsBars)charts.lsBars.destroy();
  charts.lsBars=new Chart(document.getElementById('lsBars'),{type:'bar',data:{labels:top10.map(m=>m.market),datasets:[
   {label:'Long',data:top10.map(m=>m.long_oi),backgroundColor:'#36d39c',stack:'s'},
   {label:'Short',data:top10.map(m=>-m.short_oi),backgroundColor:'#ff5d6c',stack:'s'}
  ]},options:{plugins:{legend:{labels:{color:'#8b8b94'}}},scales:{x:{stacked:true,ticks:{color:'#8b8b94'},grid:{display:false}},y:{stacked:true,ticks:{color:'#8b8b94',callback:v=>U(Math.abs(v))},grid:{color:'#21242e'}}}}});
 }catch(e){document.getElementById('ls_status').textContent='Error.';}}

// ======== Liquidation heatmap ========
async function loadHeatmap(){
 const sel=document.getElementById('hm_market');
 if(!sel.options.length && DATA && DATA.markets){
  sel.innerHTML=DATA.markets.map(m=>`<option value="${m.market_id}">${m.name}</option>`).join('');
 }
 if(!sel.value && sel.options.length)sel.value=sel.options[0].value;
 const mid=sel.value;if(!mid){document.getElementById('hm_status').textContent='No markets.';return;}
 try{const d=await (await fetch('/api/liquidation-heatmap?market_id='+encodeURIComponent(mid))).json();
  if(!d.ok){document.getElementById('hm_status').textContent='Error: '+(d.error||'unknown');return;}
  document.getElementById('hm_status').innerHTML=`<b style="color:var(--green)">${d.market}</b> · ${d.n_long_pos} longs + ${d.n_short_pos} shorts plotted`;
  document.getElementById('hm_mark').innerHTML=`Mark: <b>${P(d.mark_price)}</b> · max leverage x${d.max_leverage}`;
  const labels=d.bins.map(b=>P((b.price_low+b.price_high)/2));
  const longs=d.bins.map(b=>b.long_notional);
  const shorts=d.bins.map(b=>b.short_notional);
  if(charts.hmChart)charts.hmChart.destroy();
  charts.hmChart=new Chart(document.getElementById('hmChart'),{
   type:'bar',
   data:{labels,datasets:[
    {label:'Longs would liq',data:longs,backgroundColor:'rgba(54,211,156,.6)',borderColor:'#36d39c',borderWidth:1,stack:'s'},
    {label:'Shorts would liq',data:shorts,backgroundColor:'rgba(255,93,108,.6)',borderColor:'#ff5d6c',borderWidth:1,stack:'s'}
   ]},
   options:{plugins:{legend:{labels:{color:'#8b8b94'}},
    annotation:{}},
    scales:{x:{stacked:true,ticks:{color:'#8b8b94',maxTicksLimit:12,autoSkip:true},grid:{display:false}},
     y:{stacked:true,ticks:{color:'#8b8b94',callback:v=>U(v)},grid:{color:'#21242e'}}}}});
 }catch(e){document.getElementById('hm_status').textContent='Error loading.';}}
document.getElementById('hm_market').onchange=loadHeatmap;

// ======== Market share between DEXes ========
async function loadMarketShare(){
 try{const d=await (await fetch('/api/market-share')).json();
  document.getElementById('mshare_status').innerHTML=`Total 24h volume across the 4 DEXes: <b>${U(d.total_volume_24h)}</b>`;
  const labels=d.platforms.map(p=>p.name);
  const vals=d.platforms.map(p=>p.volume_24h);
  const cols=d.platforms.map(p=>p.color);
  if(charts.msPie)charts.msPie.destroy();
  charts.msPie=new Chart(document.getElementById('msPie'),{type:'doughnut',data:{labels,datasets:[{data:vals,backgroundColor:cols,borderColor:'transparent',borderWidth:2}]},options:{plugins:{legend:{display:false}},cutout:'62%'}});
  document.getElementById('ms_legend').innerHTML=d.platforms.map(p=>`<div style="display:flex;align-items:center;gap:10px;padding:9px 12px;border-bottom:1px solid var(--line2)">
    <span style="width:12px;height:12px;border-radius:3px;background:${p.color}"></span>
    <span style="font-weight:600;flex:1">${p.name}</span>
    <span class="mkt">${U(p.volume_24h)}</span>
    <span style="color:var(--muted);min-width:60px;text-align:right">${p.volume_share_pct.toFixed(2)}%</span>
   </div>`).join('');
 }catch(e){document.getElementById('mshare_status').textContent='Error.';}}

// ======== Watchlist ========
function wlGet(){try{return JSON.parse(localStorage.getItem('rise_watchlist')||'[]');}catch(e){return [];}}
function wlSet(l){localStorage.setItem('rise_watchlist',JSON.stringify(l));}
function wlAdd(){
 const a=document.getElementById('wl_input').value.trim();
 const lbl=document.getElementById('wl_label').value.trim();
 if(!/^0x[0-9a-fA-F]{40}$/.test(a)){alert('Invalid 0x address');return;}
 const l=wlGet();if(l.some(x=>x.account.toLowerCase()===a.toLowerCase())){alert('Already in watchlist');return;}
 l.push({account:a,label:lbl||'',added:Date.now()});wlSet(l);
 document.getElementById('wl_input').value='';document.getElementById('wl_label').value='';
 loadWatchlist();
}
function wlRemove(a){const l=wlGet().filter(x=>x.account!==a);wlSet(l);loadWatchlist();}
async function loadWatchlist(){
 const list=wlGet();const out=document.getElementById('wl_body');
 if(!list.length){out.innerHTML='<div class="empty">Watchlist empty. Add a wallet above.</div>';return;}
 out.innerHTML='<div class="empty">Loading positions for '+list.length+' wallets…</div>';
 const rows=await Promise.all(list.map(async e=>{
  try{const d=await (await fetch('/api/wallet?account='+encodeURIComponent(e.account))).json();
   return {entry:e,data:d};}catch(err){return {entry:e,data:null};}
 }));
 out.innerHTML=`<div class="panel" style="padding:0"><table><thead><tr>
  <th>Label</th><th>Account</th><th>Balance</th><th>Positions</th><th>Notional</th><th>uPnL</th><th>Open orders</th><th>Action</th></tr></thead><tbody>`+
  rows.map(r=>{const d=r.data;if(!d||!d.ok)return `<tr><td>${r.entry.label||'—'}</td><td class="mkt"><a href="#wallet=${r.entry.account}">${shortAddr(r.entry.account)}</a></td><td colspan=5>Error</td><td><button class="chip" onclick="wlRemove('${r.entry.account}')">✕</button></td></tr>`;
   const s=d.summary||{};const pc=(s.total_upnl||0)>=0?'pos':'neg';
   return `<tr><td><b>${r.entry.label||'—'}</b></td>
    <td class="mkt"><a href="#wallet=${r.entry.account}">${shortAddr(r.entry.account)}</a></td>
    <td>${d.balance!=null?U(d.balance):'—'}</td>
    <td>${s.num_positions||0}</td>
    <td>${U(s.total_notional)}</td>
    <td class="${pc}">${(s.total_upnl||0)>=0?'+':''}${U(s.total_upnl)}</td>
    <td>${s.num_open_orders||0}</td>
    <td><button class="chip" onclick="wlRemove('${r.entry.account}')">✕</button></td></tr>`;}).join('')+
  '</tbody></table></div>';
}

function lineChart(id,labels,vals,color){const c=document.getElementById(id);if(!c)return;
 if(charts[id]){charts[id].data.labels=labels;charts[id].data.datasets[0].data=vals;charts[id].update('none');return;}
 charts[id]=new Chart(c,{type:'line',data:{labels,datasets:[{data:vals,borderColor:color,
  backgroundColor:color.replace('rgb','rgba').replace(')',',.15)'),fill:true,tension:.3,pointRadius:0,borderWidth:2}]},
  options:{plugins:{legend:{display:false}},scales:{x:{ticks:{color:'#8a91a3',maxTicksLimit:7},grid:{display:false}},
   y:{ticks:{color:'#8a91a3',callback:v=>U(v)},grid:{color:'#21242e'}}}}});}
function barChart(id,labels,vals){const c=document.getElementById(id);if(!c)return;
 if(charts[id]){charts[id].data.labels=labels;charts[id].data.datasets[0].data=vals;charts[id].update('none');return;}
 charts[id]=new Chart(c,{type:'bar',data:{labels,datasets:[{data:vals,backgroundColor:'#33d6a6',borderRadius:5,maxBarThickness:26}]},
  options:{plugins:{legend:{display:false}},scales:{x:{ticks:{color:'#8a91a3'},grid:{display:false}},
   y:{ticks:{color:'#8a91a3',callback:v=>U(v)},grid:{color:'#21242e'}}}}});}
function intChart(id,type,labels,vals,color){const c=document.getElementById(id);if(!c)return;
 if(charts[id]){charts[id].data.labels=labels;charts[id].data.datasets[0].data=vals;charts[id].update('none');return;}
 const ds=type==='line'?{data:vals,borderColor:color,backgroundColor:color+'26',fill:true,tension:.3,pointRadius:0,borderWidth:2}
  :{data:vals,backgroundColor:color,borderRadius:4,maxBarThickness:18};
 charts[id]=new Chart(c,{type,data:{labels,datasets:[ds]},options:{plugins:{legend:{display:false}},
  scales:{x:{ticks:{color:'#8a91a3',maxTicksLimit:8},grid:{display:false}},y:{ticks:{color:'#8a91a3'},grid:{color:'#21242e'}}}}});}

function renderOverview(d){
 const t=d.totals||{};
 document.getElementById('c_vol').textContent=U(t.volume_24h);
 document.getElementById('c_vol_m').textContent=(t.num_markets||0)+' markets';
 document.getElementById('c_oi').textContent=U(t.open_interest_usd);
 document.getElementById('c_oi_m').textContent='DefiLlama: '+U(t.open_interest_llama);
 document.getElementById('c_tvl').textContent=U(t.tvl);
 document.getElementById('c_ratio').textContent=t.oi_tvl_ratio!=null?t.oi_tvl_ratio.toFixed(2)+'x':'—';
 const feeTag=document.getElementById('fee_tag');
 if(t.fees_24h_real!=null){
  document.getElementById('c_fee').textContent=U(t.fees_24h_real);
  document.getElementById('c_fee_m').textContent='real ('+(t.fees_window_h||24)+'h) · taker '+U(t.fees_taker)+' / maker '+U(t.fees_maker);
  feeTag.textContent='real';feeTag.className='tag live';
 }else{
  document.getElementById('c_fee').textContent=U(t.fees_24h_est);
  document.getElementById('c_fee_m').textContent='estimated · range '+U(t.fees_24h_low)+' – '+U(t.fees_24h_high);
  feeTag.textContent='est.';feeTag.className='tag snap';
 }
 toggleAlert('k_vol',alerts.vol&&t.volume_24h>+alerts.vol,'24h Volume exceeds '+U(+alerts.vol));
 toggleAlert('k_oi',alerts.oi&&t.open_interest_usd>+alerts.oi,'OI exceeds '+U(+alerts.oi));
 lineChart('tvlChart',(d.tvl.series||[]).map(p=>new Date(p.date).toLocaleDateString('es-ES',{month:'short',day:'numeric'})),(d.tvl.series||[]).map(p=>p.tvl),'rgb(124,92,255)');
 const mv=[...(d.markets||[])].sort((a,b)=>b.volume_24h-a.volume_24h);
 barChart('volChart',mv.map(m=>m.name),mv.map(m=>m.volume_24h));
 const maxFund=Math.max(0,...(d.markets||[]).map(m=>Math.abs(m.funding_8h*100)));
 if(alerts.fund&&maxFund>+alerts.fund)notify('High funding: '+maxFund.toFixed(3)+'%');
}
function toggleAlert(id,on,msg){const el=document.getElementById(id);if(!el)return;
 el.classList.toggle('alert',!!on);if(on)notify(msg);}

function renderMarkets(){
 const m=[...(DATA.markets||[])];m.sort((a,b)=>{let x=a[sortK],y=b[sortK];
  if(typeof x==='string')return sortDir*x.localeCompare(y);return sortDir*((x||0)-(y||0));});
 const mx=Math.max(...m.map(r=>r.volume_24h||0),1);const tb=document.getElementById('tbody');tb.innerHTML='';
 for(const r of m){const cc=r.change_24h>=0?'pos':'neg',fd=r.funding_8h*100,fc=fd>=0?'pos':'neg',
  ap=r.funding_apr,ac=ap>=0?'pos':'neg',bc=r.basis_pct>=0?'pos':'neg';
  const tr=document.createElement('tr');tr.style.cursor='pointer';
  tr.onclick=()=>{location.hash='market='+r.market_id;};
  tr.innerHTML=`<td class="mkt">${r.name} <span style="color:var(--muted);font-size:11px">↗</span></td><td>${P(r.last_price)}</td>
   <td class="${cc}">${r.change_24h>=0?'+':''}${r.change_24h.toFixed(2)}%</td>
   <td>${U(r.volume_24h)}<div class="bar"><i style="width:${(100*(r.volume_24h||0)/mx).toFixed(1)}%"></i></div></td>
   <td>${U(r.oi_usd)}</td><td class="${fc}">${fd>=0?'+':''}${fd.toFixed(4)}%</td>
   <td class="${ac}">${ap>=0?'+':''}${ap.toFixed(1)}%</td>
   <td class="${bc}">${r.basis_pct>=0?'+':''}${r.basis_pct.toFixed(3)}%</td>
   <td>${r.spread_bps!=null?r.spread_bps.toFixed(1)+' bps':'—'}</td>
   <td>${r.max_leverage?'x'+r.max_leverage:'—'}</td>`;tb.appendChild(tr);}
}
document.querySelectorAll('#tbl th').forEach(th=>th.onclick=()=>{const k=th.dataset.k;
 if(sortK===k)sortDir*=-1;else{sortK=k;sortDir=k==='name'?1:-1;}renderMarkets();});

// renderBig eliminado: la pestaña Órdenes grandes ya no existe

// Navega a una wallet via hash routing
function goWallet(addr){
 if(!addr) return;
 location.hash='wallet='+addr;
}
async function loadWallet(addr){
 const out=document.getElementById('walletOut');
 if(!addr){out.innerHTML='';return;}
 out.innerHTML='<div class="empty">Loading wallet '+addr.slice(0,10)+'…</div>';
 try{const d=await (await fetch('/api/wallet?account='+encodeURIComponent(addr))).json();
  if(!d.ok){out.innerHTML='<div class="empty">'+(d.errors||['Error']).join(' ')+'</div>';return;}
  out.innerHTML=renderWalletPage(d);
 }catch(e){out.innerHTML='<div class="empty">Error fetching data.</div>';}
}

function renderWalletPage(d){
 const s=d.summary||{};const upc=(s.total_upnl||0)>=0?'pos':'neg';
 const bal=d.balance;const equity=(bal!=null)?bal+(s.total_upnl||0):null;
 const rpc=(s.realized_pnl_shown||0)>=0?'pos':'neg';
 // Cabecera con avatar + address + acciones
 let h=`<div class="whead">
  <div class="row1">
   <div class="avatar">${d.account.slice(2,4).toUpperCase()}</div>
   <div>
    <div class="addr" id="full_addr">${d.account}</div>
    <div class="note" style="margin-top:2px">${s.num_positions||0} positions · ${s.num_open_orders||0} orders · ${s.num_trades||0} trades loaded</div>
   </div>
   <div class="actions">
    <span class="chip" onclick="navigator.clipboard.writeText('${d.account}');this.textContent='Copied ✓';setTimeout(()=>this.textContent='Copy address',1500)">Copy address</span>
    <a class="chip" href="${'https://explorer.risechain.com/address/'+d.account}" target="_blank">View on explorer ↗</a>
    <span class="chip" onclick="goHome()">← Back</span>
   </div>
  </div>
 </div>`;

 // KPI cards
 h+=`<div class="cards" style="grid-template-columns:repeat(5,1fr)">
  <div class="card"><div class="lbl">Balance (collateral)</div><div class="val">${bal!=null?U(bal):'—'}</div><div class="meta">USDC cross margin</div></div>
  <div class="card"><div class="lbl">Equity</div><div class="val">${equity!=null?U(equity):'—'}</div><div class="meta">balance + unrealized PnL</div></div>
  <div class="card"><div class="lbl">Unrealized PnL</div><div class="val ${upc}">${(s.total_upnl||0)>=0?'+':''}${U(s.total_upnl)}</div><div class="meta">${U(s.total_notional)} total notional</div></div>
  <div class="card"><div class="lbl">Realized PnL<span class="tag snap">sample</span></div><div class="val ${rpc}">${(s.realized_pnl_shown||0)>=0?'+':''}${U(s.realized_pnl_shown)}</div><div class="meta">last ${s.num_trades||0} trades · fees ${U(s.fees_shown)}</div></div>
  <div class="card"><div class="lbl">Positions</div><div class="val">${s.num_positions||0}</div><div class="meta">${s.num_open_orders||0} open orders</div></div>
 </div>`;

 // Volumen realizado por ventana (del indexer global)
 if(s.volume){
  const v=s.volume;
  const cf=(s.realized_pnl_30d||0)>=0?'pos':'neg';
  const rk=v.ranks||{};
  const rankBadge=k=>{const r=rk[k];if(!r||!r.rank)return '';
   return `<div class="meta" style="color:var(--accent2);font-weight:600">#${r.rank.toLocaleString('en-US')} <span style="color:var(--muted2);font-weight:400">of ${r.of.toLocaleString('en-US')}</span></div>`;};
  h+=`<div class="sectitle">Realized volume</div>
  <div class="cards" style="grid-template-columns:repeat(4,1fr)">
   <div class="card"><div class="lbl">1 day</div><div class="val">${U(v['1d'])}</div>${rankBadge('1d')}</div>
   <div class="card"><div class="lbl">7 days</div><div class="val">${U(v['7d'])}</div>${rankBadge('7d')}</div>
   <div class="card"><div class="lbl">30 days</div><div class="val">${U(v['30d'])}</div>${rankBadge('30d')}<div class="meta">${v.trades_30d} trades · fees ${U(s.fees_30d||0)}</div></div>
   <div class="card"><div class="lbl">Since May 29 07:00 UTC</div><div class="val">${U(v['29may'])}</div>${rankBadge('29may')}<div class="meta">30d realized PnL: <span class="${cf}">${(s.realized_pnl_30d||0)>=0?'+':''}${U(s.realized_pnl_30d||0)}</span>${s.n_liquidations?' · <span class="liq-near">'+s.n_liquidations+' liq.</span>':''}</div></div>
  </div>`;
 } else {
  h+=`<div class="note" style="margin:10px 0">Realized volume not yet available (the indexer may take a few minutes to process this account). Try again in a bit.</div>`;
 }

 // Positions
 h+=`<div class="sectitle">Open positions</div>`;
 if(d.positions.length){
  h+=`<div class="panel" style="padding:0"><table><thead><tr>
   <th>Market</th><th>Side</th><th>Size</th><th>Entry</th><th>Mark</th>
   <th>Liq. price</th><th>Dist. to liq.</th><th>Notional</th>
   <th>Unrealized PnL</th><th>Lev.</th><th>Margin</th></tr></thead><tbody>`;
  for(const p of d.positions){const pc=p.upnl>=0?'pos':'neg';
   let liqCls='liq-far';
   if(p.dist_to_liq_pct!=null){
    if(p.dist_to_liq_pct<5)liqCls='liq-near';
    else if(p.dist_to_liq_pct<15)liqCls='liq-mid';
   }
   h+=`<tr><td class="mkt">${p.market}</td>
    <td><span class="pillside ${p.side}">${p.side}</span></td>
    <td>${(+p.size).toLocaleString('en-US',{maximumFractionDigits:4})}</td>
    <td>${P(p.entry)}</td><td>${P(p.mark)}</td>
    <td>${p.liq_price!=null?P(p.liq_price):'—'}</td>
    <td class="${liqCls}">${p.dist_to_liq_pct!=null?(p.dist_to_liq_pct>=0?p.dist_to_liq_pct.toFixed(2)+'%':'<span class="liq-near">LIQ</span>'):'—'}</td>
    <td>${U(p.notional)}</td>
    <td class="${pc}">${p.upnl>=0?'+':''}${U(p.upnl)} <span style="color:var(--muted)">(${p.upnl>=0?'+':''}${p.upnl_pct.toFixed(2)}%)</span></td>
    <td>${p.leverage?'x'+(+p.leverage).toFixed(0):'—'}</td><td>${p.margin_mode}</td></tr>`;}
  h+='</tbody></table></div>';
  h+='<div class="note" style="margin-top:6px">Liq. price for <b>Cross</b> positions accounts for the total account balance and the unrealized PnL of all other cross positions (so adding margin or other positions changes the liq price of all of them). For <b>Isolated</b> positions, it uses the dedicated isolated USDC balance. Mark and other positions\' uPnL are taken at their current value.</div>';
 } else {
  h+='<div class="empty">No open positions right now.</div>';
 }

 // Orders
 h+=`<div class="sectitle">Open orders</div>`;
 if(d.open_orders.length){
  h+=`<div class="panel" style="padding:0"><table><thead><tr><th>Market</th><th>Side</th><th>Price</th><th>Size</th><th>Notional</th></tr></thead><tbody>`;
  for(const o of d.open_orders) h+=`<tr><td class="mkt">${o.market}</td>
   <td><span class="pillside ${o.side}">${o.side}</span></td>
   <td>${P(o.price)}</td><td>${o.size}</td><td>${U(o.price*o.size)}</td></tr>`;
  h+='</tbody></table></div>';
 } else {
  h+='<div class="empty">No open orders.</div>';
 }

 // Trade history
 h+=`<div class="sectitle">Trade history · ${d.trades.length} most recent</div>`;
 if(d.trades.length){
  h+=`<div class="panel" style="padding:0;max-height:520px;overflow:auto"><table><thead><tr>
   <th>Time (UTC)</th><th>Market</th><th>Side</th><th>Role</th><th>Price</th><th>Size</th>
   <th>Notional</th><th>Fee</th><th>Realized PnL</th><th>%</th></tr></thead><tbody>`;
  for(const t of d.trades){
   const pc=t.realized_pnl>=0?'pos':'neg';
   const ppc=t.realized_pnl_pct>=0?'pos':'neg';
   const dt=new Date(t.ts*1000);
   const dts=dt.toISOString().slice(0,16).replace('T',' ');
   const role=t.role==='TAKER'?'<span class="role-T">TAKER</span>':'<span class="role-M">MAKER</span>';
   const liqTag=t.is_liq?' <span class="liq-near" style="font-size:10px">LIQ</span>':'';
   h+=`<tr><td style="font-family:ui-monospace,monospace;font-size:12px;color:var(--muted)">${dts}</td>
    <td class="mkt">${t.market}</td>
    <td><span class="pillside ${t.side}">${t.side}</span>${liqTag}</td>
    <td>${role}</td>
    <td>${P(t.price)}</td>
    <td>${(+t.size).toLocaleString('en-US',{maximumFractionDigits:4})}</td>
    <td>${U(t.notional)}</td>
    <td style="color:var(--muted)">${U(t.fee)}</td>
    <td class="${pc}">${t.realized_pnl!==0?(t.realized_pnl>=0?'+':'')+U(t.realized_pnl):'—'}</td>
    <td class="${ppc}">${t.realized_pnl_pct!==0?(t.realized_pnl_pct>=0?'+':'')+t.realized_pnl_pct.toFixed(2)+'%':'—'}</td>
   </tr>`;}
  h+='</tbody></table></div>';
 } else {
  h+='<div class="empty">No trade history.</div>';
 }

 return h;
}

// Routing por hash: #wallet=0x... o #market=ID
function handleHashRoute(){
 const h=location.hash||'';
 const w=h.match(/wallet=(0x[0-9a-fA-F]{40})/);
 const mk=h.match(/market=(\d+)/);
 if(w){
  document.querySelectorAll('.navitem').forEach(x=>x.classList.remove('active'));
  document.querySelectorAll('.view').forEach(x=>x.classList.remove('active'));
  document.getElementById('v_wallet').classList.add('active');
  loadWallet(w[1]);
 } else if(mk){
  document.querySelectorAll('.navitem').forEach(x=>x.classList.remove('active'));
  document.querySelectorAll('.view').forEach(x=>x.classList.remove('active'));
  document.getElementById('v_marketdetail').classList.add('active');
  loadMarketDetail(mk[1]);
 } else {
  // sin hash: si estamos en wallet o market detail, volver a Visión general
  const inWallet=document.getElementById('v_wallet').classList.contains('active');
  const inMarket=document.getElementById('v_marketdetail').classList.contains('active');
  if(inWallet)loadWallet(null);
  if(inWallet||inMarket){
   document.querySelectorAll('.navitem').forEach(x=>x.classList.remove('active'));
   document.querySelectorAll('.view').forEach(x=>x.classList.remove('active'));
   document.querySelector('.navitem[data-v="overview"]').classList.add('active');
   document.getElementById('v_overview').classList.add('active');
  }
 }
}
function goHome(){history.replaceState(null,'',location.pathname);handleHashRoute();}
window.addEventListener('hashchange', handleHashRoute);
// invocar al cargar
setTimeout(handleHashRoute, 50);

// Búsqueda global: enter en el input → wallet o mercado
const gsi=document.getElementById('gs_input');
if(gsi) gsi.addEventListener('keydown',ev=>{
 if(ev.key!=='Enter')return;
 const q=gsi.value.trim();if(!q)return;
 // si es 0x… 42 chars → wallet
 if(/^0x[0-9a-fA-F]{40}$/.test(q)){location.hash='wallet='+q;gsi.value='';return;}
 // si no, buscar en DATA.markets por símbolo
 if(DATA&&DATA.markets){
  const sym=q.toUpperCase().replace('/USDC','').replace('-USDC','').replace('/USD','');
  const m=DATA.markets.find(x=>(x.name||'').toUpperCase().startsWith(sym));
  if(m){location.hash='market='+m.market_id;gsi.value='';return;}
 }
 gsi.style.borderColor='var(--red)';setTimeout(()=>gsi.style.borderColor='',1500);
});

async function loadHistory(){
 try{const d=await (await fetch('/api/history')).json();const p=d.points||[];
  const lab=p.map(x=>new Date(x.t*1000).toLocaleString('es-ES',{day:'2-digit',hour:'2-digit',minute:'2-digit'}));
  lineChart('hVol',lab,p.map(x=>x.vol),'rgb(51,214,166)');
  lineChart('hOi',lab,p.map(x=>x.oi),'rgb(124,92,255)');}catch(e){}}

let RANK=null;
async function loadRanking(){
 try{const d=await (await fetch('/api/ranking')).json();RANK=d;
  let st='';
  if(d.phase==='scanning'){const pct=d.scan_total?Math.floor(100*d.scan_done/d.scan_total):0;
   st=`<b style="color:var(--amber)">Scanning history…</b> ${pct}% · ${d.accounts.toLocaleString('en-US')} accounts`;}
  else if(d.phase==='loading'){st=`<b style="color:var(--amber)">Loading positions for ${d.accounts.toLocaleString('en-US')} accounts…</b>`;}
  else{st=`<b style="color:var(--green)">Ranking ready</b> · ${d.accounts.toLocaleString('en-US')} accounts · ${d.positions_count.toLocaleString('en-US')} positions · ${agoStr(d.last_update)} ago`;}
  document.getElementById('rk_status').innerHTML=st;
  const sel=document.getElementById('rk_market');const cur=sel.value;
  const mkts=Object.keys(d.ranking||{}).sort((a,b)=>((d.ranking[b].oi_long+d.ranking[b].oi_short)-(d.ranking[a].oi_long+d.ranking[a].oi_short)));
  sel.innerHTML=mkts.map(m=>`<option ${m===cur?'selected':''}>${m}</option>`).join('')||'<option>—</option>';
  renderRanking();
 }catch(e){document.getElementById('rk_status').textContent='Error.';}}
document.getElementById('rk_market').onchange=renderRanking;
function rkRow(r,i){const pc=r.upnl>=0?'pos':'neg';
 return `<tr><td>${i+1}</td><td class="mkt"><a href="#wallet=${r.account}">${shortAddr(r.account)}</a></td>
  <td>${(+r.size).toLocaleString('en-US',{maximumFractionDigits:4})}</td><td>${U(r.notional)}</td><td>${P(r.entry)}</td>
  <td class="${pc}">${r.upnl>=0?'+':''}${U(r.upnl)}</td><td>x${(+r.leverage).toFixed(0)}</td></tr>`;}
function renderRanking(){if(!RANK)return;const m=document.getElementById('rk_market').value;
 const r=(RANK.ranking||{})[m];const L=document.getElementById('rk_long'),S=document.getElementById('rk_short');
 if(!r){L.innerHTML=S.innerHTML='<tr><td class="empty" colspan=7>No data.</td></tr>';document.getElementById('rk_oi').textContent='';return;}
 document.getElementById('rk_oi').innerHTML=`OI longs: <b class="pos">${U(r.oi_long)}</b> (${r.n_long||r.longs.length}) · shorts: <b class="neg">${U(r.oi_short)}</b> (${r.n_short||r.shorts.length})`;
 L.innerHTML=r.longs.map(rkRow).join('')||'<tr><td class="empty" colspan=7>—</td></tr>';
 S.innerHTML=r.shorts.map(rkRow).join('')||'<tr><td class="empty" colspan=7>—</td></tr>';}

async function loadAcctOi(){
 try{const d=await (await fetch('/api/account-oi-ranking')).json();
  let st='';
  if(d.phase!=='live')st=`<b style="color:var(--amber)">Indexer loading…</b>`;
  else st=`<b style="color:var(--green)">Current snapshot</b> · ${d.active_accounts} accounts with positions · ${d.positions_count} positions · ${agoStr(d.last_update)} ago`;
  document.getElementById('ao_status').innerHTML=st;
  document.getElementById('ao_totals').innerHTML=`Total aggregated OI: <b>${U(d.total_oi)}</b>`;
  const tot=d.total_oi||0;
  const rows=(d.ranking||[]).map((r,i)=>{const pc=r.upnl>=0?'pos':'neg';
   const ml=r.markets.slice(0,4).join(', ')+(r.markets.length>4?` +${r.markets.length-4}`:'');
   return `<tr><td>${i+1}</td>
    <td class="mkt"><a href="#wallet=${r.account}">${shortAddr(r.account)}</a></td>
    <td>${U(r.total_oi)}</td><td>${tot?((r.total_oi/tot*100).toFixed(2)+'%'):'—'}</td>
    <td class="pos">${U(r.long_oi)}</td><td class="neg">${U(r.short_oi)}</td>
    <td class="${pc}">${r.upnl>=0?'+':''}${U(r.upnl)}</td>
    <td>${r.positions}</td><td style="font-size:11.5px;color:var(--muted)">${ml}</td></tr>`;}).join('');
  document.getElementById('ao_body').innerHTML=rows||'<tr><td class="empty" colspan=9>No data yet.</td></tr>';
 }catch(e){document.getElementById('ao_status').textContent='Error.';}}

let VR_PERIOD='1d';
document.querySelectorAll('#vr_seg button').forEach(b=>b.onclick=()=>{
 document.querySelectorAll('#vr_seg button').forEach(x=>x.classList.remove('active'));
 b.classList.add('active');VR_PERIOD=b.dataset.p;loadVolRanking();});
async function loadVolRanking(){
 try{const d=await (await fetch('/api/volume-ranking?period='+VR_PERIOD)).json();
  let st='';
  if(d.phase==='waiting')st=`<b style="color:var(--amber)">Waiting for census…</b>`;
  else if(d.phase==='scanning')st=`<b style="color:var(--amber)">Calculating…</b> ${d.scanned}/${d.total_accounts}`;
  else st=`<b style="color:var(--green)">Ready</b> · ${d.count_with_volume} accounts with volume in ${d.period} · ${agoStr(d.last_update)} ago`;
  document.getElementById('vr_status').innerHTML=st;
  document.getElementById('vr_totals').innerHTML=`Total volume ${d.period}: <b>${U(d.total_volume)}</b>`;
  const tot=d.total_volume||0;
  const rows=(d.ranking||[]).map((r,i)=>`<tr><td>${i+1}</td>
   <td class="mkt"><a href="#wallet=${r.account}">${shortAddr(r.account)}</a></td>
   <td>${U(r.volume)}</td><td>${tot?((r.volume/tot*100).toFixed(2)+'%'):'—'}</td><td>${r.trades}</td></tr>`).join('');
  document.getElementById('vr_body').innerHTML=rows||'<tr><td class="empty" colspan=5>No data yet.</td></tr>';
 }catch(e){document.getElementById('vr_status').textContent='Error.';}}

let OIR_PERIOD='1d';
document.querySelectorAll('#oir_seg button').forEach(b=>b.onclick=()=>{
 document.querySelectorAll('#oir_seg button').forEach(x=>x.classList.remove('active'));
 b.classList.add('active');OIR_PERIOD=b.dataset.p;loadOiRanking();});
async function loadOiRanking(){
 try{const d=await (await fetch('/api/oi-ranking?period='+OIR_PERIOD)).json();
  let st='';
  if(d.phase==='waiting')st=`<b style="color:var(--amber)">Waiting for census…</b>`;
  else if(d.phase==='scanning')st=`<b style="color:var(--amber)">Reconstructing positions…</b> ${d.scanned}/${d.total_accounts}`;
  else st=`<b style="color:var(--green)">Ready</b> · ${d.count_with_oi} accounts with avg OI in ${d.period} · ${agoStr(d.last_update)} ago`;
  document.getElementById('oir_status').innerHTML=st;
  let totalsLine=`Sum of avg OI (${d.period}): <b>${U(d.total_avg_oi)}</b>`;
  if(d.since_ts){const dt=new Date(d.since_ts*1000);
   totalsLine+=` · window since <b>${dt.toUTCString().slice(5,22)} UTC</b>`;}
  document.getElementById('oir_totals').innerHTML=totalsLine;
  const tot=d.total_avg_oi||0;
  const rows=(d.ranking||[]).map((r,i)=>`<tr><td>${i+1}</td>
   <td class="mkt"><a href="#wallet=${r.account}">${shortAddr(r.account)}</a></td>
   <td>${U(r.avg_oi)}</td><td>${tot?((r.avg_oi/tot*100).toFixed(2)+'%'):'—'}</td><td>${r.trades}</td></tr>`).join('');
  document.getElementById('oir_body').innerHTML=rows||'<tr><td class="empty" colspan=5>No data yet.</td></tr>';
 }catch(e){document.getElementById('oir_status').textContent='Error.';}}

let FC_MODE='apr', FC_FILTER='risex', FC_DATA=null;
document.querySelectorAll('#fc_mode button').forEach(b=>b.onclick=()=>{
 document.querySelectorAll('#fc_mode button').forEach(x=>x.classList.remove('active'));
 b.classList.add('active');FC_MODE=b.dataset.m;renderFunding();});
document.querySelectorAll('#fc_filter button').forEach(b=>b.onclick=()=>{
 document.querySelectorAll('#fc_filter button').forEach(x=>x.classList.remove('active'));
 b.classList.add('active');FC_FILTER=b.dataset.f;renderFunding();});
async function loadFunding(){
 try{const d=await (await fetch('/api/funding-compare')).json();FC_DATA=d;
  document.getElementById('fc_status').innerHTML=`<span style="color:var(--green)">Live</span> · ${d.rows.length} markets`;
  renderFunding();
 }catch(e){document.getElementById('fc_status').textContent='Error.';}}
function fmtPct(x){if(x==null||isNaN(x))return'<span style="color:var(--muted)">—</span>';
 const cls=x>=0?'pos':'neg';return `<span class="${cls}">${x>=0?'+':''}${x.toFixed(FC_MODE==='apr'?2:5)}%</span>`;}
function renderFunding(){if(!FC_DATA)return;
 let rows=FC_DATA.rows.slice();
 if(FC_FILTER==='risex')rows=rows.filter(r=>r.in_risex);
 rows.sort((a,b)=>{const aa=Math.abs(a.diff_lighter_apr||a.diff_pacifica_apr||0);
  const bb=Math.abs(b.diff_lighter_apr||b.diff_pacifica_apr||0);return bb-aa;});
 const k=FC_MODE==='apr'?'_apr':'_h';
 const tb=document.getElementById('fc_body');
 tb.innerHTML=rows.map(r=>{
  const rx=r['risex'+k]!=null?r['risex'+k]*(FC_MODE==='apr'?1:100):null;
  const lt=r['lighter'+k]!=null?r['lighter'+k]*(FC_MODE==='apr'?1:100):null;
  const pc=r['pacifica'+k]!=null?r['pacifica'+k]*(FC_MODE==='apr'?1:100):null;
  const hl=r['hyperliquid'+k]!=null?r['hyperliquid'+k]*(FC_MODE==='apr'?1:100):null;
  const dL=r.diff_lighter_apr!=null?(FC_MODE==='apr'?r.diff_lighter_apr:r.diff_lighter_apr/24/365):null;
  const dP=r.diff_pacifica_apr!=null?(FC_MODE==='apr'?r.diff_pacifica_apr:r.diff_pacifica_apr/24/365):null;
  const dH=r.diff_hyperliquid_apr!=null?(FC_MODE==='apr'?r.diff_hyperliquid_apr:r.diff_hyperliquid_apr/24/365):null;
  const ltCell=r.lighter_default
   ?`<span style="color:var(--muted2)" title="Default value from Lighter's public API (NOT the actual rate shown on lighter.xyz for low-activity markets)">${fmtPct(lt)} *</span>`
   :fmtPct(lt);
  return `<tr><td class="mkt">${r.symbol}${r.in_risex?'':' <span class="note" style="font-size:10px">(not on RISEx)</span>'}</td>
   <td>${fmtPct(rx)}</td><td>${ltCell}</td><td>${fmtPct(pc)}</td><td>${fmtPct(hl)}</td>
   <td>${r.lighter_default?'<span style="color:var(--muted2)">—</span>':fmtPct(dL)}</td><td>${fmtPct(dP)}</td><td>${fmtPct(dH)}</td></tr>`;}).join('')||'<tr><td class="empty" colspan=8>No data.</td></tr>';}

// Top PnL
async function loadPnl(){
 try{const d=await (await fetch('/api/pnl-ranking')).json();
  document.getElementById('pn_status').innerHTML=d.phase==='live'
    ?`<b style="color:var(--green)">Ranking ready</b> · ${d.count} accounts with activity · ${agoStr(d.last_update)} ago`
    :`<b style="color:var(--amber)">Calculating…</b>`;
  const W=document.getElementById('pn_winners'),L=document.getElementById('pn_losers');
  W.innerHTML=(d.winners||[]).map((r,i)=>{const tc=r.total>=0?'pos':'neg',rc=r.realized>=0?'pos':'neg',uc=r.unrealized>=0?'pos':'neg';
   return `<tr><td>${i+1}</td>
   <td class="mkt"><a href="#wallet=${r.account}">${shortAddr(r.account)}</a></td>
   <td class="${tc}">${r.total>=0?'+':''}${U(r.total)}</td>
   <td class="${rc}">${r.realized>=0?'+':''}${U(r.realized)}</td>
   <td class="${uc}">${r.unrealized>=0?'+':''}${U(r.unrealized)}</td>
   <td>${r.trades}</td></tr>`;}).join('')||'<tr><td class="empty" colspan=6>—</td></tr>';
  L.innerHTML=(d.losers||[]).map((r,i)=>{const tc=r.total>=0?'pos':'neg',rc=r.realized>=0?'pos':'neg',uc=r.unrealized>=0?'pos':'neg';
   return `<tr><td>${i+1}</td>
   <td class="mkt"><a href="#wallet=${r.account}">${shortAddr(r.account)}</a></td>
   <td class="${tc}">${r.total>=0?'+':''}${U(r.total)}</td>
   <td class="${rc}">${r.realized>=0?'+':''}${U(r.realized)}</td>
   <td class="${uc}">${r.unrealized>=0?'+':''}${U(r.unrealized)}</td>
   <td>${r.n_liquidations||0}</td></tr>`;}).join('')||'<tr><td class="empty" colspan=6>—</td></tr>';
 }catch(e){document.getElementById('pn_status').textContent='Error.';}}

// Liquidaciones
async function loadLiq(){
 try{const d=await (await fetch('/api/liquidations')).json();
  document.getElementById('lq_status').innerHTML=`<b style="color:var(--green)">${d.count} liquidations in 24h</b> · ${agoStr(d.last_update)} ago`;
  document.getElementById('lq_totals').innerHTML=`Liquidated notional: <b>${U(d.total_notional)}</b> · total losses <b class="neg">−${U(d.total_liq_loss)}</b>`;
  const tb=document.getElementById('lq_body');
  tb.innerHTML=(d.entries||[]).map(e=>{const dt=new Date(e.ts*1000);
   const dts=dt.toISOString().slice(11,16)+' '+dt.toISOString().slice(5,10);
   return `<tr><td style="font-family:ui-monospace,monospace;font-size:12px;color:var(--muted)">${dts}</td>
    <td class="mkt"><a href="#wallet=${e.account}">${shortAddr(e.account)}</a></td>
    <td>${e.market}</td>
    <td><span class="pillside ${e.position_side}">${e.position_side}</span></td>
    <td>${(+e.size).toLocaleString('en-US',{maximumFractionDigits:4})}</td>
    <td>${P(e.price)}</td>
    <td>${U(e.notional)}</td>
    <td class="neg">−${U(Math.abs(e.realized_pnl))}</td></tr>`;}).join('')||'<tr><td class="empty" colspan=8>No liquidations detected.</td></tr>';
 }catch(e){document.getElementById('lq_status').textContent='Error.';}}

// Live activity feed
let FD_FILTER='all';
document.querySelectorAll('#fd_filter button').forEach(b=>b.onclick=()=>{
 document.querySelectorAll('#fd_filter button').forEach(x=>x.classList.remove('active'));
 b.classList.add('active');FD_FILTER=b.dataset.f;loadFeed();});
async function loadFeed(){
 try{const url='/api/live-activity'+(FD_FILTER==='liq'?'?only_liq=true':'');
  const d=await (await fetch(url)).json();
  let rows=d.entries||[];
  if(FD_FILTER==='big') rows=rows.filter(e=>!e.is_liq);
  document.getElementById('fd_status').innerHTML=`<b style="color:var(--green)">${rows.length} events</b> · ${agoStr(d.last_update)} ago`;
  document.getElementById('fd_totals').innerHTML=`Filtered total notional: <b>${U(rows.reduce((s,e)=>s+e.notional,0))}</b>`;
  const tb=document.getElementById('fd_body');
  tb.innerHTML=rows.map(e=>{const dt=new Date(e.ts*1000);
   const dts=dt.toISOString().slice(11,19);
   const tcls=e.is_liq?'liq-near':(e.role==='TAKER'?'role-T':'role-M');
   const tag=e.is_liq?'<span class="liq-near" style="font-size:11px;font-weight:700">LIQ</span>':`<span class="${tcls}">${e.role||'—'}</span>`;
   const pnl=e.realized_pnl;const pc=pnl>=0?'pos':'neg';
   return `<tr><td style="font-family:ui-monospace,monospace;font-size:12px;color:var(--muted)">${dts}</td>
    <td>${tag}</td>
    <td class="mkt"><a href="#wallet=${e.account}">${shortAddr(e.account)}</a></td>
    <td>${e.market}</td>
    <td><span class="pillside ${e.position_side}">${e.position_side}</span></td>
    <td>${e.role==='TAKER'?'<span class="role-T">T</span>':'<span class="role-M">M</span>'}</td>
    <td>${(+e.size).toLocaleString('en-US',{maximumFractionDigits:4})}</td>
    <td>${P(e.price)}</td>
    <td>${U(e.notional)}</td>
    <td class="${pc}">${pnl===0?'—':(pnl>=0?'+':'')+U(pnl)}</td></tr>`;}).join('')||'<tr><td class="empty" colspan=10>No data yet. The indexer fills this feed while scanning accounts.</td></tr>';
 }catch(e){document.getElementById('fd_status').textContent='Error.';}}

// Página por mercado
async function loadMarketDetail(mid){
 const out=document.getElementById('md_content');
 out.innerHTML='<div class="empty">Loading…</div>';
 try{const d=await (await fetch('/api/market-detail?market_id='+encodeURIComponent(mid))).json();
  const i=d.info||{};const ch=i.change_24h>=0?'pos':'neg';const fc=i.funding_8h>=0?'pos':'neg';
  let h=`<div class="whead"><div class="row1">
   <div class="avatar">${(i.name||'?').slice(0,1)}</div>
   <div><div class="addr" style="font-size:18px;font-weight:700">${i.name||mid}</div>
    <div class="note">market #${i.market_id||mid} · max leverage x${i.max_leverage||'—'}</div></div>
  </div></div>`;
  h+=`<div class="cards">
   <div class="card"><div class="lbl">Price</div><div class="val">${P(i.last_price||0)}</div><div class="meta ${ch}">${i.change_24h>=0?'+':''}${(i.change_24h||0).toFixed(2)}% 24h</div></div>
   <div class="card"><div class="lbl">24h Volume</div><div class="val">${U(i.volume_24h||0)}</div></div>
   <div class="card"><div class="lbl">Open Interest</div><div class="val">${U(i.oi_usd||0)}</div><div class="meta">${d.n_long||0} longs · ${d.n_short||0} shorts</div></div>
   <div class="card"><div class="lbl">Funding 8h</div><div class="val ${fc}">${i.funding_8h>=0?'+':''}${(i.funding_8h*100).toFixed(4)}%</div><div class="meta">APR ${i.funding_apr>=0?'+':''}${(i.funding_apr||0).toFixed(2)}%</div></div>
   <div class="card"><div class="lbl">Mark / Index</div><div class="val">${P(i.mark_price||0)}</div><div class="meta">basis ${(i.basis_pct||0).toFixed(3)}%</div></div>
  </div>`;
  // top longs/shorts for this market
  h+=`<div class="grid2">
   <div><h2 style="font-size:13px;color:var(--green)">▲ Top Longs (${d.n_long||0}, OI ${U(d.oi_long||0)})</h2>
    <div class="panel" style="padding:0"><table><thead><tr><th>#</th><th>Account</th><th>Size</th><th>Notional</th><th>Entry</th><th>PnL</th></tr></thead><tbody>`+
    (d.longs||[]).map((r,i)=>`<tr><td>${i+1}</td><td class="mkt"><a href="#wallet=${r.account}">${shortAddr(r.account)}</a></td><td>${(+r.size).toLocaleString('en-US',{maximumFractionDigits:4})}</td><td>${U(r.notional)}</td><td>${P(r.entry)}</td><td class="${r.upnl>=0?'pos':'neg'}">${r.upnl>=0?'+':''}${U(r.upnl)}</td></tr>`).join('')+`</tbody></table></div></div>
   <div><h2 style="font-size:13px;color:var(--red)">▼ Top Shorts (${d.n_short||0}, OI ${U(d.oi_short||0)})</h2>
    <div class="panel" style="padding:0"><table><thead><tr><th>#</th><th>Account</th><th>Size</th><th>Notional</th><th>Entry</th><th>PnL</th></tr></thead><tbody>`+
    (d.shorts||[]).map((r,i)=>`<tr><td>${i+1}</td><td class="mkt"><a href="#wallet=${r.account}">${shortAddr(r.account)}</a></td><td>${(+r.size).toLocaleString('en-US',{maximumFractionDigits:4})}</td><td>${U(r.notional)}</td><td>${P(r.entry)}</td><td class="${r.upnl>=0?'pos':'neg'}">${r.upnl>=0?'+':''}${U(r.upnl)}</td></tr>`).join('')+`</tbody></table></div></div>
  </div>`;
  // recent activity for this market
  h+=`<div class="sectitle">Recent activity · ${i.name||mid}</div>`;
  if(d.feed&&d.feed.length){
   h+=`<div class="panel" style="padding:0;max-height:400px;overflow:auto"><table><thead><tr><th>Time</th><th>Type</th><th>Account</th><th>Side</th><th>Size</th><th>Price</th><th>Notional</th></tr></thead><tbody>`+
   d.feed.map(e=>{const dt=new Date(e.ts*1000);const ts=dt.toISOString().slice(11,19);
    const tag=e.is_liq?'<span class="liq-near">LIQ</span>':(e.role==='TAKER'?'<span class="role-T">TAKER</span>':'<span class="role-M">MAKER</span>');
    return `<tr><td style="font-family:ui-monospace,monospace;font-size:12px;color:var(--muted)">${ts}</td>
     <td>${tag}</td><td class="mkt"><a href="#wallet=${e.account}">${shortAddr(e.account)}</a></td>
     <td><span class="pillside ${e.position_side}">${e.position_side}</span></td>
     <td>${(+e.size).toLocaleString('en-US',{maximumFractionDigits:4})}</td>
     <td>${P(e.price)}</td><td>${U(e.notional)}</td></tr>`;}).join('')+`</tbody></table></div>`;
  } else h+='<div class="empty">No recent activity detected for this market.</div>';
  out.innerHTML=h;
 }catch(e){out.innerHTML='<div class="empty">Error loading.</div>';}}

async function loadUsers(){
 try{const d=await (await fetch('/api/users')).json();
  document.getElementById('us_status').innerHTML=`<span style="color:var(--green)">Data from the official explorer</span>`;
  document.getElementById('us_total').textContent=(d.total_accounts!=null?d.total_accounts:0).toLocaleString('en-US');
  document.getElementById('us_addr').textContent=(d.total_addresses!=null?d.total_addresses:0).toLocaleString('en-US');
  document.getElementById('us_active').textContent=(d.active_with_position||0).toLocaleString('en-US');
  document.getElementById('us_today').textContent='+'+(d.new_today||0).toLocaleString('en-US');
  document.getElementById('us_7d').textContent='+'+(d.new_7d||0).toLocaleString('en-US');
  const s=d.series||[];const lab=s.map(x=>x.date.slice(5));
  intChart('usNew','bar',lab,s.map(x=>x.new),'#33d6a6');
  intChart('usCum','line',lab,s.map(x=>x.cum),'#7c5cff');
  intChart('usAct','bar',lab,s.map(x=>x.active),'#ffb454');
 }catch(e){document.getElementById('us_status').textContent='Error.';}}

document.getElementById('al_vol').value=alerts.vol||'';
document.getElementById('al_oi').value=alerts.oi||'';
document.getElementById('al_fund').value=alerts.fund||'';
function saveAlerts(){alerts.vol=document.getElementById('al_vol').value;alerts.oi=document.getElementById('al_oi').value;
 alerts.fund=document.getElementById('al_fund').value;localStorage.setItem('rise_alerts',JSON.stringify(alerts));
 document.getElementById('al_status').textContent='Saved ✓';if(DATA)renderOverview(DATA);}
function askNotif(){if('Notification'in window)Notification.requestPermission().then(p=>document.getElementById('al_status').textContent=p==='granted'?'Active ✓':'Permission denied');}
let lastNotif={};
function notify(msg){if(!('Notification'in window)||Notification.permission!=='granted')return;
 const now=Date.now();if(lastNotif[msg]&&now-lastNotif[msg]<300000)return;lastNotif[msg]=now;
 try{new Notification('RISEx',{body:msg});}catch(e){}}

async function loadAll(){const dot=document.getElementById('dot');
 try{const d=await (await fetch('/api/data',{cache:'no-store'})).json();DATA=d;
  renderOverview(d);renderMarkets();
  // refresca el histórico (vive en Visión general ahora)
  loadHistory();
  const dt=new Date((d.generated_at||0)*1000);
  document.getElementById('updated').textContent='updated '+dt.toLocaleTimeString('en-US');
  dot.style.background='#36d39c';
 }catch(e){dot.style.background='#ff5d6c';document.getElementById('updated').textContent='no connection';}}
loadAll();setInterval(loadAll,30000);
setInterval(()=>{
 if(document.getElementById('v_ranking').classList.contains('active'))loadRanking();
 if(document.getElementById('v_acctoi').classList.contains('active'))loadAcctOi();
 if(document.getElementById('v_users').classList.contains('active'))loadUsers();
 if(document.getElementById('v_volranking').classList.contains('active'))loadVolRanking();
 if(document.getElementById('v_oiranking').classList.contains('active'))loadOiRanking();
 if(document.getElementById('v_funding').classList.contains('active'))loadFunding();
 if(document.getElementById('v_pnl').classList.contains('active'))loadPnl();
 if(document.getElementById('v_liq').classList.contains('active'))loadLiq();
 if(document.getElementById('v_feed').classList.contains('active'))loadFeed();
 if(document.getElementById('v_longshort').classList.contains('active'))loadLongShort();
 if(document.getElementById('v_marketshare').classList.contains('active'))loadMarketShare();
},15000);
</script></body></html>"""


if __name__ == "__main__":
    main()
