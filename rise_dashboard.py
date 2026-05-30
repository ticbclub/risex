#!/usr/bin/env python3
"""
RISEx Live Stats Dashboard  ·  v3
=================================
Panel en tiempo real para RISEx (https://www.rise.trade) con:
  - Mercados, volumen, OI, funding, basis, spread (api.rise.trade)
  - TVL (DefiLlama) y Fees 24h REALES sumando eventos onchain (PerpsManager)
  - Tracker de billetera: balance, equity, posiciones, ordenes
  - Ranking de mayores posiciones long/short por mercado (indexer onchain)
  - Usuarios y crecimiento (mismo microservicio del explorer oficial)
  - NUEVO: Ranking de cuentas por VOLUMEN realizado (1d/7d/14d/30d)
  - Histórico auto-grabado, alertas con notificacion del navegador
  - Ordenes grandes vivas del orderbook

Uso:
    python3 rise_dashboard.py
Luego abre:  http://localhost:8787
Sin dependencias externas (solo libreria estandar de Python 3).
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

PORT = int(os.environ.get("PORT", 8787))
IS_CLOUD = bool(os.environ.get("PORT") or os.environ.get("RAILWAY_ENVIRONMENT"))
BIND = "0.0.0.0" if IS_CLOUD else "127.0.0.1"

# --- fuentes ---
RISEX = "https://api.rise.trade"
LLAMA_TVL = "https://api.llama.fi/protocol/risex"
LLAMA_OI = "https://api.llama.fi/summary/open-interest/risex"
STATS_BASE = "https://stats-explorer.risechain.com/api/v1"
RPC_URL = "https://rpc.risechain.com/"

# --- contratos onchain ---
ACCOUNT_REGISTRY = "0x1238991Cac4E65902C08213e79909A9c813Eebc3"
PERPS_MANAGER = "0x53f10fAcFC8965750494E6965F5d6dA39B41d852"
AR_DEPLOY_BLOCK = 7345365
EXPLORER_ADDR = "https://explorer.risechain.com/address/"

# topic0 (keccak) de eventos con fee en data word [128:192]
TOPIC_TAKE = "0x3e92827023687af833e2eb9abe60e0726acfc9f7f82839dec79cf9e138b983ff"
TOPIC_SETTLE = "0x572a85e40cc9183c961148c546e88431898ab9938b85992ea5f6577ea06d9888"

# fee schedule oficial (docs.risechain.com/docs/risex/trading/fees)
TAKER_BPS, MAKER_BPS = 3.00, 1.00
BLENDED_BPS = (TAKER_BPS + MAKER_BPS) / 2.0

WAD = 1e18
WINDOW_SECONDS = 86400
CHUNK = 5000
CACHE_TTL = 15
REFRESH_SECONDS = 180
def _history_path():
    # En cloud usamos /data si esta montado (volumen Railway), si no la carpeta del script,
    # y si nada es escribible, /tmp como ultimo recurso.
    for base in ("/data", os.path.dirname(os.path.abspath(__file__)), "/tmp"):
        try:
            if os.path.isdir(base) and os.access(base, os.W_OK):
                return os.path.join(base, "rise_history.json")
        except Exception:
            pass
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "rise_history.json")
HISTORY_FILE = _history_path()
HISTORY_MIN_GAP = 120
HISTORY_MAX_POINTS = 5000

VOL_MAX_PAGES = 12
VOL_REFRESH_S = 600

_CACHE = {"ts": 0, "data": None}
_HIST_LOCK = threading.Lock()


# ============================== utilidades ==============================
def fetch_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "rise-dashboard/3.0",
                                                "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def f(x, d=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return d


def rpc(method, params, retry=5):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    for i in range(retry):
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
            if e.code in (403, 429) and i < retry - 1:
                time.sleep(0.6 + i * 0.6); continue
            raise
        except Exception:
            if i < retry - 1:
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
        out["errors"].append(f"markets: {e}")
        md = []

    mk_by_id = {}
    for m in md:
        if not m.get("visible", True):
            continue
        mid = str(m.get("market_id"))
        mark = f(m.get("mark_price"))
        index = f(m.get("index_price"))
        oi_base = f(m.get("open_interest"))
        fund8h = f(m.get("funding_rate_8h"))
        row = {
            "market_id": mid,
            "name": m.get("display_name") or m.get("base_asset_symbol"),
            "last_price": f(m.get("last_price")),
            "mark_price": mark,
            "index_price": index,
            "change_24h": f(m.get("change_24h")),
            "volume_24h": f(m.get("quote_volume_24h")),
            "oi_base": oi_base,
            "oi_usd": oi_base * mark,
            "funding_8h": fund8h,
            "funding_apr": fund8h * 3 * 365 * 100,
            "basis_pct": ((mark - index) / index * 100) if index else 0.0,
            "max_leverage": f(m.get("config", {}).get("max_leverage")),
            "spread_bps": None, "best_bid": None, "best_ask": None,
        }
        markets.append(row)
        mk_by_id[mid] = row

    big = []
    if markets:
        with ThreadPoolExecutor(max_workers=8) as ex:
            for mid, ob in ex.map(lambda r: fetch_orderbook(r["market_id"]), markets):
                if not ob:
                    continue
                row = mk_by_id.get(mid)
                bids = ob.get("bids") or []
                asks = ob.get("asks") or []
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

    # TVL DefiLlama
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
        "volume_24h": total_vol,
        "open_interest_usd": total_oi,
        "open_interest_llama": oi_llama,
        "tvl": tvl_cur,
        "oi_tvl_ratio": (total_oi / tvl_cur) if tvl_cur else None,
        "fees_24h_est": total_vol * BLENDED_BPS / 10000.0,
        "fees_24h_low": total_vol * MAKER_BPS / 10000.0,
        "fees_24h_high": total_vol * TAKER_BPS / 10000.0,
        "num_markets": len(markets),
    }

    # fees reales (si el indexer onchain ya esta listo)
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
        h.append({"t": now,
                  "vol": round(totals.get("volume_24h") or 0),
                  "oi": round(totals.get("open_interest_usd") or 0),
                  "tvl": round(tvl or 0)})
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
           "summary": {}, "balance": None, "errors": []}
    if not (account.startswith("0x") and len(account) == 42):
        res["ok"] = False; res["errors"].append("Dirección no válida (0x + 40 hex).")
        return res

    mk = {}
    try:
        for m in fetch_json(f"{RISEX}/v1/markets")["data"]["markets"]:
            mk[str(m.get("market_id"))] = {"name": m.get("display_name"),
                                           "mark": f(m.get("mark_price"))}
    except Exception:
        pass

    q = urllib.parse.quote(account)

    # balance de margen cruzado (USDC) - viene en unidades humanas
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

    total_upnl = 0.0
    try:
        pd = fetch_json(f"{RISEX}/v1/positions?account={q}")["data"]
        for p in pd.get("positions", []):
            mid = str(p.get("market_id"))
            info = mk.get(mid, {})
            size = abs(f(p.get("size")) / WAD)
            entry = f(p.get("avg_entry_price")) / WAD
            mark = info.get("mark") or f(p.get("mark_price"))
            longp = is_long(p.get("side"), f(p.get("size")))
            sign = 1 if longp else -1
            upnl = sign * (mark - entry) * size
            total_upnl += upnl
            res["positions"].append({
                "market": info.get("name") or mid,
                "side": "Long" if longp else "Short",
                "size": size, "entry": entry, "mark": mark,
                "notional": size * mark,
                "upnl": upnl,
                "upnl_pct": (upnl / (size * entry) * 100) if (size and entry) else 0.0,
                "leverage": f(p.get("leverage")) / WAD,
                "margin_mode": "Isolated" if p.get("margin_mode") in (1, "1") else "Cross",
                "isolated_usdc": f(p.get("isolated_usdc_balance")) / WAD,
            })
    except Exception as e:
        res["errors"].append(f"positions: {e}")

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

    res["summary"] = {
        "num_positions": len(res["positions"]),
        "total_notional": sum(p["notional"] for p in res["positions"]),
        "total_upnl": total_upnl,
        "num_open_orders": len(res["open_orders"]),
    }
    return res


# ============================== indexer: cuentas + ranking de posiciones ==============================
_INDEX = {"started": int(time.time()), "phase": "scanning", "cursor": None,
          "accounts": set(), "reg_blocks": {}, "scan_done": 0, "scan_total": 0,
          "positions_count": 0, "active_accounts": 0, "ranking": {}, "last_update": 0}
_IDX_LOCK = threading.Lock()


def _accounts_from_logs(logs):
    """Solo topic1 = la cuenta registrada (topic2 es un id incremental)."""
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
    with ThreadPoolExecutor(max_workers=8) as ex:
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
    active = len(set(r["account"] for r in rows))
    with _IDX_LOCK:
        _INDEX["ranking"] = rank
        _INDEX["positions_count"] = len(rows)
        _INDEX["active_accounts"] = active
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


# ============================== usuarios (stats del explorer oficial) ==============================
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


# ============================== ranking por VOLUMEN realizado ==============================
_VOL = {"by_account": {}, "ready": False, "phase": "waiting",
        "scanned": 0, "total": 0, "last_update": 0}
_VOL_LOCK = threading.Lock()


def _account_metrics(account, now_s):
    """Para una cuenta calcula, en una sola pasada por su trade-history:
       - VOLUMEN realizado por ventana (sum price*size de cada trade dentro)
       - OI MEDIO (time-weighted): reconstruye la posicion por mercado tras cada trade
         y promedia |posicion| * precio sobre la duracion de la ventana.
       Necesitamos *todos* los trades disponibles (no solo los de 30d) para conocer
       la posicion al inicio de cada ventana."""
    out = {"trades": 0, "last_refresh": now_s,
           "1d": 0.0, "7d": 0.0, "14d": 0.0, "30d": 0.0,
           "oi_1d": 0.0, "oi_7d": 0.0, "oi_14d": 0.0, "oi_30d": 0.0}
    windows = [("1d", 86400), ("7d", 7 * 86400), ("14d", 14 * 86400), ("30d", 30 * 86400)]
    cutoffs = {k: now_s - d for k, d in windows}

    events = []   # (ts, mid, signed_size, price)
    page = 1
    while page <= VOL_MAX_PAGES:
        try:
            d = fetch_json(
                f"{RISEX}/v1/trade-history?account={account}&limit=1000&page={page}", timeout=15)["data"]
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
            # volumen por ventana
            if ts >= cutoffs["30d"]:
                notional = abs(size) * price
                out["trades"] += 1
                if ts >= cutoffs["1d"]: out["1d"] += notional
                if ts >= cutoffs["7d"]: out["7d"] += notional
                if ts >= cutoffs["14d"]: out["14d"] += notional
                if ts >= cutoffs["30d"]: out["30d"] += notional
        if not d.get("has_next_page"):
            break
        page += 1

    if not events:
        return out
    events.sort(key=lambda e: e[0])

    # OI medio time-weighted por ventana
    for label, dur in windows:
        start = now_s - dur
        # posicion al inicio de la ventana: suma de signed sizes de todos los trades anteriores
        pos = {}   # mid -> size signed
        last_px = {}  # mid -> ultimo precio conocido (para valorar la posicion)
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
        # cola: desde el ultimo trade hasta ahora
        dt = now_s - prev_t
        if dt > 0:
            notional_now = sum(abs(pos.get(m, 0.0)) * last_px.get(m, 0.0) for m in last_px)
            sum_notional_dt += notional_now * dt
        out["oi_" + label] = sum_notional_dt / dur if dur else 0.0
    return out


# alias para compatibilidad con el resto del codigo
_account_volume = _account_metrics


def volume_indexer_loop():
    # esperar a que tengamos cuentas del censo
    while True:
        with _IDX_LOCK:
            n = len(_INDEX["accounts"])
            ph = _INDEX["phase"]
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
            # priorizar cuentas con mas volumen conocido
            accts.sort(key=lambda a: known.get(a, 0), reverse=True)

            def one(a):
                return a, _account_volume(a, now_s)
            with ThreadPoolExecutor(max_workers=6) as ex:
                for a, v in ex.map(one, accts):
                    with _VOL_LOCK:
                        _VOL["by_account"][a] = v
                        _VOL["scanned"] += 1
            with _VOL_LOCK:
                _VOL["ready"] = True; _VOL["phase"] = "live"
                _VOL["last_update"] = int(time.time())
        except Exception:
            pass
        time.sleep(VOL_REFRESH_S)


def get_oi_ranking(period="1d", limit=200):
    if period not in ("1d", "7d", "14d", "30d"):
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
    return {"ok": True, "period": period, "ready": ready, "phase": ph,
            "scanned": scanned, "total_accounts": total, "last_update": last,
            "count_with_oi": len(items), "total_avg_oi": total_oi,
            "ranking": items[:limit]}


def get_volume_ranking(period="1d", limit=200):
    if period not in ("1d", "7d", "14d", "30d"):
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


# ============================== comparativa de funding entre perp DEXes ==============================
# Compara funding HORARIO (las 3 plataformas emiten cada hora) entre RISEx, Lighter y Pacifica.
LIGHTER_FUND = "https://mainnet.zklighter.elliot.ai/api/v1/funding-rates"
PACIFICA_PRICES = "https://api.pacifica.fi/api/v1/info/prices"
_FUND_CACHE = {"ts": 0, "data": None}


def _risex_base(name):
    """De 'BTC/USDC' a 'BTC'."""
    if not name: return ""
    return name.split("/")[0].split("-")[0].upper()


def get_funding_compare():
    now = time.time()
    if _FUND_CACHE["data"] and now - _FUND_CACHE["ts"] < 30:
        return _FUND_CACHE["data"]
    out = {"ok": True, "generated_at": int(now), "errors": [], "rows": []}

    # RISEx (usamos current_funding_rate horario)
    risex = {}
    try:
        for m in fetch_json(f"{RISEX}/v1/markets")["data"]["markets"]:
            if not m.get("visible", True): continue
            sym = _risex_base(m.get("display_name") or m.get("base_asset_symbol"))
            cf = f(m.get("current_funding_rate"))
            risex[sym] = {"hourly": cf, "mark": f(m.get("mark_price"))}
    except Exception as e:
        out["errors"].append(f"risex: {e}")

    # Lighter: filtrar solo entradas con exchange == "lighter"
    lighter = {}
    try:
        for e in fetch_json(LIGHTER_FUND).get("funding_rates", []):
            if e.get("exchange") == "lighter":
                lighter[e.get("symbol", "").upper()] = {"hourly": f(e.get("rate"))}
    except Exception as e:
        out["errors"].append(f"lighter: {e}")

    # Pacifica
    pacifica = {}
    try:
        for p in fetch_json(PACIFICA_PRICES).get("data", []):
            sym = (p.get("symbol") or "").upper()
            pacifica[sym] = {"hourly": f(p.get("funding")),
                              "next": f(p.get("next_funding"))}
    except Exception as e:
        out["errors"].append(f"pacifica: {e}")

    # union por simbolo, ordenando primero los de RISEx
    symbols_risex = list(risex.keys())
    others = sorted(set(list(lighter.keys()) + list(pacifica.keys())) - set(symbols_risex))
    all_syms = symbols_risex + others
    for s in all_syms:
        r = risex.get(s, {}).get("hourly")
        l = lighter.get(s, {}).get("hourly")
        p = pacifica.get(s, {}).get("hourly")
        # APR % = hourly * 24 * 365 * 100
        def apr(x):
            return x * 24 * 365 * 100 if x is not None else None
        row = {"symbol": s, "in_risex": s in risex,
               "risex_h": r, "lighter_h": l, "pacifica_h": p,
               "risex_apr": apr(r), "lighter_apr": apr(l), "pacifica_apr": apr(p)}
        # diferenciales (en pp anualizadas) si tenemos ambos
        if r is not None and l is not None: row["diff_lighter_apr"] = (r - l) * 24 * 365 * 100
        if r is not None and p is not None: row["diff_pacifica_apr"] = (r - p) * 24 * 365 * 100
        out["rows"].append(row)
    _FUND_CACHE["ts"] = now; _FUND_CACHE["data"] = out
    return out


# ============================== servidor ==============================
# Rate limiter simple por IP para endpoints que consultan apis externas (evita abusos en publico)
_RL = {}
_RL_LOCK = threading.Lock()
RL_LIMITS = {  # endpoint -> (max requests, ventana en segundos)
    "/api/wallet": (30, 60),         # max 30 llamadas / min / IP
    "/api/funding-compare": (60, 60),
}


def _client_ip(headers, fallback):
    # respeta cabeceras de proxy (Railway/CDN delante)
    for h in ("x-forwarded-for", "x-real-ip", "cf-connecting-ip"):
        v = headers.get(h)
        if v:
            return v.split(",")[0].strip()
    return fallback


def _rate_limit_ok(ip, endpoint):
    limit = RL_LIMITS.get(endpoint)
    if not limit:
        return True
    n, window = limit
    now = time.time()
    with _RL_LOCK:
        key = (ip, endpoint)
        hist = _RL.get(key, [])
        hist = [t for t in hist if t > now - window]
        if len(hist) >= n:
            _RL[key] = hist
            return False
        hist.append(now)
        _RL[key] = hist
    return True


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def _send(self, code, body, ctype):
        self.send_response(code); self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        # CORS abierto para que cualquiera pueda usar la API desde su navegador
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj, code=200):
        self._send(code, json.dumps(obj).encode("utf-8"), "application/json")

    def do_GET(self):
        path = urllib.parse.urlparse(self.path)
        ip = _client_ip({k.lower(): v for k, v in self.headers.items()}, self.client_address[0])
        if not _rate_limit_ok(ip, path.path):
            self._json({"ok": False, "error": "Rate limit excedido. Espera un momento."}, 429)
            return
        try:
            if path.path == "/api/data":
                self._json(get_overview())
            elif path.path == "/api/history":
                self._json({"ok": True, "points": load_history()})
            elif path.path == "/api/ranking":
                self._json(get_ranking())
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
            elif path.path in ("/", "/index.html"):
                self._send(200, HTML.encode("utf-8"), "text/html; charset=utf-8")
            else:
                self._send(404, b"not found", "text/plain")
        except Exception as e:
            self._json({"ok": False, "error": str(e)}, 500)


def main():
    url = f"http://localhost:{PORT}" if not IS_CLOUD else f"port {PORT}"
    print("=" * 56)
    print("  RISEx Live Stats Dashboard  ·  v3")
    if IS_CLOUD:
        print(f"  Modo cloud · escuchando en {BIND}:{PORT}")
        print(f"  Histórico en: {HISTORY_FILE}")
    else:
        print(f"  Abriendo el navegador en: {url}")
        print("  (deja esta ventana abierta; ciérrala para parar)")
    print("=" * 56)
    try:
        srv = ThreadingHTTPServer((BIND, PORT), Handler)
    except OSError as e:
        print(f"\nNo se pudo abrir el puerto {PORT}: {e}")
        try: input("\nPulsa ENTER para cerrar...")
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
        print("\nServidor detenido. Hasta luego!")


HTML = r"""<!DOCTYPE html>
<html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>RISEx · Live Stats</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.5.0/dist/chart.umd.js"></script>
<style>
:root{--bg:#0a0b0f;--panel:#14161d;--panel2:#1b1e27;--line:#262a35;--txt:#e9edf5;--muted:#8a91a3;
 --accent:#7c5cff;--accent2:#b69bff;--green:#33d6a6;--red:#ff5d6c;--amber:#ffb454;color-scheme:dark}
*{box-sizing:border-box}
body{margin:0;background:radial-gradient(1200px 600px at 80% -10%,#1a1430 0%,var(--bg) 55%);
 color:var(--txt);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Inter,Roboto,sans-serif;padding:26px}
.wrap{max-width:1200px;margin:0 auto}
header{display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap;margin-bottom:18px}
.brand{display:flex;align-items:center;gap:12px}
.logo{width:40px;height:40px;border-radius:11px;background:linear-gradient(135deg,var(--accent),var(--accent2));
 display:flex;align-items:center;justify-content:center;font-weight:800;color:#fff;font-size:19px;box-shadow:0 6px 20px rgba(124,92,255,.35)}
h1{font-size:20px;margin:0}.sub{color:var(--muted);font-size:12.5px;margin-top:2px}
.status{display:flex;align-items:center;gap:8px;font-size:12.5px;color:var(--muted)}
.dot{width:9px;height:9px;border-radius:50%;background:var(--green);box-shadow:0 0 0 4px rgba(51,214,166,.15)}
button.rf,.btn{background:#23202e;border:1px solid var(--line);color:var(--txt);border-radius:9px;padding:6px 12px;font-size:12.5px;cursor:pointer}
button.rf:hover,.btn:hover{border-color:var(--accent)}
.tabs{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:16px}
.tab{padding:8px 14px;border-radius:10px;background:var(--panel);border:1px solid var(--line);color:var(--muted);
 font-size:13px;cursor:pointer;font-weight:600}
.tab.active{color:#fff;border-color:var(--accent);background:linear-gradient(180deg,#221d36,#1a1726)}
.view{display:none}.view.active{display:block}
.cards{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:16px}
.card{background:linear-gradient(180deg,var(--panel),var(--panel2));border:1px solid var(--line);border-radius:15px;padding:14px 16px}
.card.alert{border-color:var(--amber);box-shadow:0 0 0 3px rgba(255,180,84,.12)}
.card .lbl{color:var(--muted);font-size:11.5px;text-transform:uppercase;letter-spacing:.5px}
.card .val{font-size:23px;font-weight:750;margin-top:7px}
.card .meta{font-size:11.5px;color:var(--muted);margin-top:5px}
.tag{display:inline-block;font-size:10px;padding:1px 6px;border-radius:999px;vertical-align:middle;margin-left:6px}
.tag.live{background:rgba(51,214,166,.16);color:var(--green)}.tag.snap{background:#2a2533;color:var(--accent2)}
.grid2{display:grid;grid-template-columns:1.4fr 1fr;gap:14px;margin-bottom:16px}
.panel{background:linear-gradient(180deg,var(--panel),var(--panel2));border:1px solid var(--line);border-radius:15px;padding:16px 18px}
.panel h2{font-size:14px;margin:0 0 12px;font-weight:650;color:#cfd5e3}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{padding:9px 8px;text-align:right;border-bottom:1px solid var(--line);white-space:nowrap}
th:first-child,td:first-child{text-align:left}
th{color:var(--muted);font-weight:600;font-size:11px;text-transform:uppercase;letter-spacing:.5px;cursor:pointer;user-select:none}
tbody tr:hover{background:#1c2030}.pos{color:var(--green)}.neg{color:var(--red)}.mkt{font-weight:600}
.bar{height:6px;border-radius:4px;background:#23202e;overflow:hidden;margin-top:5px}.bar>i{display:block;height:100%;background:linear-gradient(90deg,var(--accent),var(--accent2))}
.pill{font-size:11px;padding:2px 8px;border-radius:999px;font-weight:600}
.pill.bid{background:rgba(51,214,166,.15);color:var(--green)}.pill.ask{background:rgba(255,93,108,.15);color:var(--red)}
input,select{background:#0f1117;border:1px solid var(--line);color:var(--txt);border-radius:9px;padding:9px 11px;font-size:13px}
input{width:430px;max-width:100%}
.row{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-bottom:14px}
.note{color:var(--muted);font-size:12px;line-height:1.6}
.empty{color:var(--muted);font-size:13px;padding:18px 4px}
.alertgrid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}
label.fl{display:block;font-size:12px;color:var(--muted);margin-bottom:6px}
.seg{display:inline-flex;background:#0f1117;border:1px solid var(--line);border-radius:10px;overflow:hidden}
.seg button{background:none;border:none;color:var(--muted);padding:7px 12px;font-size:12.5px;cursor:pointer}
.seg button.active{background:linear-gradient(180deg,#221d36,#1a1726);color:#fff}
a{color:var(--accent2);text-decoration:none}
footer{color:var(--muted);font-size:11.5px;margin-top:18px;line-height:1.6}
@media(max-width:980px){.cards{grid-template-columns:repeat(2,1fr)}.grid2{grid-template-columns:1fr}.alertgrid{grid-template-columns:1fr}}
</style></head>
<body><div class="wrap">
<header>
 <div class="brand"><div class="logo">R</div>
  <div><h1>RISEx · Live Stats</h1><div class="sub">Perpetuos onchain · datos en vivo de api.rise.trade</div></div></div>
 <div class="status"><span class="dot" id="dot"></span><span id="updated">cargando…</span>
  <button class="rf" onclick="loadAll(true)">Actualizar</button></div>
</header>

<div class="tabs">
 <div class="tab active" data-v="overview">Visión general</div>
 <div class="tab" data-v="markets">Mercados</div>
 <div class="tab" data-v="ranking">Posiciones (ranking)</div>
 <div class="tab" data-v="volranking">Volumen (ranking)</div>
 <div class="tab" data-v="oiranking">OI medio (ranking)</div>
 <div class="tab" data-v="funding">Funding vs DEXes</div>
 <div class="tab" data-v="users">Usuarios</div>
 <div class="tab" data-v="orders">Órdenes grandes</div>
 <div class="tab" data-v="wallet">Billetera</div>
 <div class="tab" data-v="history">Histórico</div>
 <div class="tab" data-v="alerts">Alertas</div>
</div>

<div class="view active" id="v_overview">
 <div class="cards">
  <div class="card" id="k_vol"><div class="lbl">Volumen 24h<span class="tag live">live</span></div><div class="val" id="c_vol">—</div><div class="meta" id="c_vol_m"></div></div>
  <div class="card" id="k_oi"><div class="lbl">Open Interest<span class="tag live">live</span></div><div class="val" id="c_oi">—</div><div class="meta" id="c_oi_m"></div></div>
  <div class="card"><div class="lbl">TVL<span class="tag snap">DefiLlama</span></div><div class="val" id="c_tvl">—</div><div class="meta">total value locked</div></div>
  <div class="card"><div class="lbl">Ratio OI / TVL</div><div class="val" id="c_ratio">—</div><div class="meta">apalancamiento del protocolo</div></div>
  <div class="card"><div class="lbl">Fees 24h<span class="tag snap" id="fee_tag">est.</span></div><div class="val" id="c_fee">—</div><div class="meta" id="c_fee_m"></div></div>
 </div>
 <div class="grid2">
  <div class="panel"><h2>TVL · evolución</h2><canvas id="tvlChart" height="150"></canvas></div>
  <div class="panel"><h2>Volumen 24h por mercado</h2><canvas id="volChart" height="150"></canvas></div>
 </div>
</div>

<div class="view" id="v_markets">
 <div class="panel"><h2>Mercados</h2>
  <table id="tbl"><thead><tr>
   <th data-k="name">Mercado</th><th data-k="last_price">Precio</th><th data-k="change_24h">24h %</th>
   <th data-k="volume_24h">Volumen 24h</th><th data-k="oi_usd">Open Interest</th>
   <th data-k="funding_8h">Funding 8h</th><th data-k="funding_apr">Funding APR</th>
   <th data-k="basis_pct">Basis</th><th data-k="spread_bps">Spread</th><th data-k="max_leverage">Lev.</th>
  </tr></thead><tbody id="tbody"></tbody></table>
 </div>
</div>

<div class="view" id="v_ranking">
 <div class="panel">
  <h2>Mayores posiciones abiertas por mercado</h2>
  <div class="note" id="rk_status" style="margin-bottom:12px">Iniciando indexador onchain…</div>
  <div class="row"><label class="fl" style="margin:0">Mercado:</label>
   <select id="rk_market"></select><span class="note" id="rk_oi"></span></div>
  <div class="grid2">
   <div><h2 style="font-size:13px;color:var(--green)">▲ Longs</h2>
    <table><thead><tr><th>#</th><th>Cuenta</th><th>Tamaño</th><th>Notional</th><th>Entrada</th><th>PnL</th><th>Lev</th></tr></thead><tbody id="rk_long"></tbody></table></div>
   <div><h2 style="font-size:13px;color:var(--red)">▼ Shorts</h2>
    <table><thead><tr><th>#</th><th>Cuenta</th><th>Tamaño</th><th>Notional</th><th>Entrada</th><th>PnL</th><th>Lev</th></tr></thead><tbody id="rk_short"></tbody></table></div>
  </div>
 </div>
</div>

<div class="view" id="v_volranking">
 <div class="panel">
  <h2>Ranking de cuentas por volumen realizado</h2>
  <div class="note" id="vr_status" style="margin-bottom:12px">Calculando…</div>
  <div class="row">
   <span class="seg" id="vr_seg">
    <button data-p="1d" class="active">1 día</button>
    <button data-p="7d">7 días</button>
    <button data-p="14d">14 días</button>
    <button data-p="30d">30 días</button>
   </span>
   <span class="note" id="vr_totals"></span>
  </div>
  <table><thead><tr><th>#</th><th>Cuenta</th><th>Volumen</th><th>% del total</th><th>Trades</th></tr></thead>
   <tbody id="vr_body"></tbody></table>
  <div class="note" style="margin-top:10px">Reconstruido sumando price×size de cada trade real de cada cuenta (endpoint <code>trade-history</code>). Primera carga: unos minutos mientras escanea todas las cuentas; luego se refresca cada 10 min.</div>
 </div>
</div>

<div class="view" id="v_oiranking">
 <div class="panel">
  <h2>Ranking de cuentas por Open Interest medio</h2>
  <div class="note" id="oir_status" style="margin-bottom:12px">Calculando…</div>
  <div class="row">
   <span class="seg" id="oir_seg">
    <button data-p="1d" class="active">1 día</button>
    <button data-p="7d">7 días</button>
    <button data-p="14d">14 días</button>
    <button data-p="30d">30 días</button>
   </span>
   <span class="note" id="oir_totals"></span>
  </div>
  <table><thead><tr><th>#</th><th>Cuenta</th><th>OI medio</th><th>% del total</th><th>Trades en 30d</th></tr></thead>
   <tbody id="oir_body"></tbody></table>
  <div class="note" style="margin-top:10px">"OI medio" = promedio temporal (time-weighted) del notional de posición de cada cuenta en la ventana seleccionada. Reconstruido trade a trade: vamos actualizando la posición de cada cuenta y multiplicando |posición| × precio por el tiempo que estuvo viva. Refleja la <b>exposición media mantenida</b>, no solo la actual.</div>
 </div>
</div>

<div class="view" id="v_funding">
 <div class="panel">
  <h2>Funding rates · comparativa entre perp DEXes</h2>
  <div class="note" id="fc_status" style="margin-bottom:12px">Cargando…</div>
  <div class="row">
   <span class="seg" id="fc_mode">
    <button data-m="apr" class="active">APR (% anual)</button>
    <button data-m="hourly">% por hora</button>
   </span>
   <span class="seg" id="fc_filter">
    <button data-f="risex" class="active">Solo mercados RISEx</button>
    <button data-f="all">Todos</button>
   </span>
  </div>
  <table id="fc_tbl"><thead><tr>
   <th>Símbolo</th><th>RISEx</th><th>Lighter</th><th>Pacifica</th>
   <th>Δ RISEx − Lighter</th><th>Δ RISEx − Pacifica</th>
  </tr></thead><tbody id="fc_body"></tbody></table>
  <div class="note" style="margin-top:10px">Las tres plataformas pagan funding cada hora. Mostramos el rate horario actual (o anualizado = ×24×365). Una Δ positiva significa que los longs en RISEx pagan más (o cobran menos) que en la otra plataforma; útil para arbitraje. Fuentes: <code>api.rise.trade</code>, <code>mainnet.zklighter.elliot.ai</code> (filtrado por exchange=lighter), <code>api.pacifica.fi</code>. Refresco cada 30s.</div>
 </div>
</div>

<div class="view" id="v_users">
 <div class="panel">
  <h2>Adopción de la plataforma</h2>
  <div class="note" id="us_status" style="margin-bottom:12px">Cargando…</div>
  <div class="cards">
   <div class="card"><div class="lbl">Cuentas (≥1 tx)</div><div class="val" id="us_total">—</div><div class="meta">han operado al menos 1 vez</div></div>
   <div class="card"><div class="lbl">Direcciones totales</div><div class="val" id="us_addr">—</div><div class="meta">incluye smart-accounts</div></div>
   <div class="card"><div class="lbl">Con posición abierta</div><div class="val" id="us_active">—</div><div class="meta">ahora mismo (RISEx)</div></div>
   <div class="card"><div class="lbl">Nuevas hoy</div><div class="val" id="us_today">—</div><div class="meta">altas de hoy (UTC)</div></div>
   <div class="card"><div class="lbl">Nuevas (7 días)</div><div class="val" id="us_7d">—</div><div class="meta">última semana</div></div>
  </div>
  <div class="grid2">
   <div class="panel" style="border:none;background:none;padding:0"><h2>Nuevas cuentas por día</h2><canvas id="usNew" height="150"></canvas></div>
   <div class="panel" style="border:none;background:none;padding:0"><h2>Crecimiento acumulado</h2><canvas id="usCum" height="150"></canvas></div>
  </div>
  <div class="panel" style="border:none;background:none;padding:0;margin-top:14px"><h2>Cuentas activas por día</h2><canvas id="usAct" height="120"></canvas></div>
  <div class="note" style="margin-top:10px">Fuente: el mismo microservicio de estadísticas que usa el explorer oficial de RISE.</div>
 </div>
</div>

<div class="view" id="v_orders">
 <div class="panel"><h2>Órdenes vivas más grandes del libro</h2>
  <table><thead><tr><th>Mercado</th><th>Lado</th><th>Precio</th><th>Tamaño</th><th>Notional</th><th>#</th></tr></thead><tbody id="bigbody"></tbody></table>
 </div>
</div>

<div class="view" id="v_wallet">
 <div class="panel">
  <h2>Seguir una billetera</h2>
  <div class="row"><input id="addr" placeholder="0x… dirección de la cuenta de trading" />
   <button class="rf" onclick="loadWallet()">Buscar</button></div>
  <div id="walletOut"><div class="empty">Pega una dirección 0x… para ver balance, posiciones y órdenes.</div></div>
 </div>
</div>

<div class="view" id="v_history">
 <div class="grid2">
  <div class="panel"><h2>Volumen 24h · histórico (auto-grabado)</h2><canvas id="hVol" height="150"></canvas></div>
  <div class="panel"><h2>Open Interest · histórico</h2><canvas id="hOi" height="150"></canvas></div>
 </div>
 <div class="note">El servidor guarda un punto cada par de minutos en <code>rise_history.json</code>.</div>
</div>

<div class="view" id="v_alerts">
 <div class="panel"><h2>Umbrales de alerta</h2>
  <div class="note" style="margin-bottom:14px">Si una métrica supera el umbral, su tarjeta se resalta y (si lo permites) recibes notificación del navegador.</div>
  <div class="alertgrid">
   <div><label class="fl">Volumen 24h mayor que ($)</label><input id="al_vol" style="width:100%" type="number" placeholder="ej. 50000000"></div>
   <div><label class="fl">Open Interest mayor que ($)</label><input id="al_oi" style="width:100%" type="number" placeholder="ej. 5000000"></div>
   <div><label class="fl">Funding 8h (abs) mayor que (%)</label><input id="al_fund" style="width:100%" type="number" step="0.001" placeholder="ej. 0.05"></div>
  </div>
  <div class="row" style="margin-top:14px">
   <button class="rf" onclick="saveAlerts()">Guardar</button>
   <button class="rf" onclick="askNotif()">Activar notificaciones</button>
   <span class="note" id="al_status"></span>
  </div>
 </div>
</div>

<footer>Volumen, OI, precios, funding, orderbook, billeteras y fees: <b>en vivo y directo de RISEx</b>. TVL: DefiLlama. Usuarios: stats-explorer.risechain.com. Ranking de posiciones: indexador onchain del PerpsManager.</footer>
</div>

<script>
let DATA=null,sortK='volume_24h',sortDir=-1,charts={};
const U=n=>{if(n==null||isNaN(n))return'—';const a=Math.abs(n);
 if(a>=1e9)return'$'+(n/1e9).toFixed(2)+'B';if(a>=1e6)return'$'+(n/1e6).toFixed(2)+'M';
 if(a>=1e3)return'$'+(n/1e3).toFixed(1)+'K';return'$'+n.toFixed(2);};
const P=n=>n>=100?n.toLocaleString('en-US',{maximumFractionDigits:1}):n.toLocaleString('en-US',{maximumFractionDigits:4});
const shortAddr=a=>a.slice(0,6)+'…'+a.slice(-4);
const agoStr=ts=>{const s=Math.floor(Date.now()/1000)-ts;if(s<60)return s+'s';if(s<3600)return Math.floor(s/60)+'min';return Math.floor(s/3600)+'h'};
const alerts=JSON.parse(localStorage.getItem('rise_alerts')||'{}');

document.querySelectorAll('.tab').forEach(t=>t.onclick=()=>{
 document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));
 document.querySelectorAll('.view').forEach(x=>x.classList.remove('active'));
 t.classList.add('active');document.getElementById('v_'+t.dataset.v).classList.add('active');
 if(t.dataset.v==='history')loadHistory();
 if(t.dataset.v==='ranking')loadRanking();
 if(t.dataset.v==='users')loadUsers();
 if(t.dataset.v==='volranking')loadVolRanking();
 if(t.dataset.v==='oiranking')loadOiRanking();
 if(t.dataset.v==='funding')loadFunding();
});

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
 document.getElementById('c_vol_m').textContent=(t.num_markets||0)+' mercados';
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
  document.getElementById('c_fee_m').textContent='estimado (indexando reales…) · rango '+U(t.fees_24h_low)+' – '+U(t.fees_24h_high);
  feeTag.textContent='est.';feeTag.className='tag snap';
 }
 toggleAlert('k_vol',alerts.vol&&t.volume_24h>+alerts.vol,'Volumen 24h supera '+U(+alerts.vol));
 toggleAlert('k_oi',alerts.oi&&t.open_interest_usd>+alerts.oi,'OI supera '+U(+alerts.oi));
 lineChart('tvlChart',(d.tvl.series||[]).map(p=>new Date(p.date).toLocaleDateString('es-ES',{month:'short',day:'numeric'})),(d.tvl.series||[]).map(p=>p.tvl),'rgb(124,92,255)');
 const mv=[...(d.markets||[])].sort((a,b)=>b.volume_24h-a.volume_24h);
 barChart('volChart',mv.map(m=>m.name),mv.map(m=>m.volume_24h));
 const maxFund=Math.max(0,...(d.markets||[]).map(m=>Math.abs(m.funding_8h*100)));
 if(alerts.fund&&maxFund>+alerts.fund)notify('Funding alto: '+maxFund.toFixed(3)+'%');
}
function toggleAlert(id,on,msg){const el=document.getElementById(id);if(!el)return;
 el.classList.toggle('alert',!!on);if(on)notify(msg);}

function renderMarkets(){
 const m=[...(DATA.markets||[])];m.sort((a,b)=>{let x=a[sortK],y=b[sortK];
  if(typeof x==='string')return sortDir*x.localeCompare(y);return sortDir*((x||0)-(y||0));});
 const mx=Math.max(...m.map(r=>r.volume_24h||0),1);const tb=document.getElementById('tbody');tb.innerHTML='';
 for(const r of m){const cc=r.change_24h>=0?'pos':'neg',fd=r.funding_8h*100,fc=fd>=0?'pos':'neg',
  ap=r.funding_apr,ac=ap>=0?'pos':'neg',bc=r.basis_pct>=0?'pos':'neg';
  const tr=document.createElement('tr');
  tr.innerHTML=`<td class="mkt">${r.name}</td><td>${P(r.last_price)}</td>
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

function renderBig(){const tb=document.getElementById('bigbody');tb.innerHTML='';
 const b=DATA.big_orders||[];if(!b.length){tb.innerHTML='<tr><td class="empty" colspan=6>Sin datos.</td></tr>';return;}
 for(const o of b){const tr=document.createElement('tr');
  tr.innerHTML=`<td class="mkt">${o.market}</td><td><span class="pill ${o.side}">${o.side==='bid'?'Compra':'Venta'}</span></td>
   <td>${P(o.price)}</td><td>${o.qty}</td><td>${U(o.notional)}</td><td>${o.orders}</td>`;tb.appendChild(tr);}}

async function loadWallet(){
 const a=document.getElementById('addr').value.trim();const out=document.getElementById('walletOut');
 out.innerHTML='<div class="empty">Cargando…</div>';
 try{const d=await (await fetch('/api/wallet?account='+encodeURIComponent(a))).json();
  if(!d.ok){out.innerHTML='<div class="empty">'+(d.errors||['Error']).join(' ')+'</div>';return;}
  const s=d.summary||{};const upc=(s.total_upnl||0)>=0?'pos':'neg';
  const bal=d.balance;const equity=(bal!=null)?bal+(s.total_upnl||0):null;
  let h=`<div class="cards" style="grid-template-columns:repeat(5,1fr)">
   <div class="card"><div class="lbl">Balance (colateral)</div><div class="val">${bal!=null?U(bal):'—'}</div><div class="meta">${equity!=null?'equity '+U(equity):'margen cruzado USDC'}</div></div>
   <div class="card"><div class="lbl">Posiciones</div><div class="val">${s.num_positions||0}</div></div>
   <div class="card"><div class="lbl">Notional total</div><div class="val">${U(s.total_notional)}</div></div>
   <div class="card"><div class="lbl">PnL no realizado</div><div class="val ${upc}">${(s.total_upnl||0)>=0?'+':''}${U(s.total_upnl)}</div></div>
   <div class="card"><div class="lbl">Órdenes abiertas</div><div class="val">${s.num_open_orders||0}</div></div></div>`;
  if(d.positions.length){h+='<table><thead><tr><th>Mercado</th><th>Lado</th><th>Tamaño</th><th>Entrada</th><th>Mark</th><th>Notional</th><th>PnL no realiz.</th><th>Lev.</th><th>Margen</th></tr></thead><tbody>';
   for(const p of d.positions){const pc=p.upnl>=0?'pos':'neg';
    h+=`<tr><td class="mkt">${p.market}</td><td class="${p.side==='Long'?'pos':'neg'}">${p.side}</td>
    <td>${p.size}</td><td>${P(p.entry)}</td><td>${P(p.mark)}</td><td>${U(p.notional)}</td>
    <td class="${pc}">${p.upnl>=0?'+':''}${U(p.upnl)} <span style="color:var(--muted)">(${p.upnl>=0?'+':''}${p.upnl_pct.toFixed(2)}%)</span></td>
    <td>${p.leverage?'x'+(+p.leverage).toFixed(0):'—'}</td><td>${p.margin_mode}</td></tr>`;}
   h+='</tbody></table>';}
  else h+='<div class="empty">Sin posiciones abiertas para esta dirección.</div>';
  if(d.open_orders.length){h+='<h2 style="font-size:13px;margin:16px 0 8px;color:#cfd5e3">Órdenes abiertas</h2><table><thead><tr><th>Mercado</th><th>Lado</th><th>Precio</th><th>Tamaño</th></tr></thead><tbody>';
   for(const o of d.open_orders)h+=`<tr><td>${o.market}</td><td>${o.side}</td><td>${P(o.price)}</td><td>${o.size}</td></tr>`;h+='</tbody></table>';}
  out.innerHTML=h;
 }catch(e){out.innerHTML='<div class="empty">Error al consultar.</div>';}
}

async function loadHistory(){
 try{const d=await (await fetch('/api/history')).json();const p=d.points||[];
  const lab=p.map(x=>new Date(x.t*1000).toLocaleString('es-ES',{day:'2-digit',hour:'2-digit',minute:'2-digit'}));
  lineChart('hVol',lab,p.map(x=>x.vol),'rgb(51,214,166)');
  lineChart('hOi',lab,p.map(x=>x.oi),'rgb(124,92,255)');}catch(e){}}

// posiciones ranking
let RANK=null;
async function loadRanking(){
 try{const d=await (await fetch('/api/ranking')).json();RANK=d;
  let st='';
  if(d.phase==='scanning'){const pct=d.scan_total?Math.floor(100*d.scan_done/d.scan_total):0;
   st=`<b style="color:var(--amber)">Escaneando histórico onchain…</b> ${pct}% (${d.scan_done}/${d.scan_total} tramos) · <b>${d.accounts.toLocaleString('es-ES')}</b> cuentas`;}
  else if(d.phase==='loading'){st=`<b style="color:var(--amber)">Cargando posiciones de ${d.accounts.toLocaleString('es-ES')} cuentas…</b>`;}
  else{st=`<b style="color:var(--green)">Ranking completo</b> · ${d.accounts.toLocaleString('es-ES')} cuentas · ${d.positions_count.toLocaleString('es-ES')} posiciones · hace ${agoStr(d.last_update)}`;}
  document.getElementById('rk_status').innerHTML=st;
  const sel=document.getElementById('rk_market');const cur=sel.value;
  const mkts=Object.keys(d.ranking||{}).sort((a,b)=>((d.ranking[b].oi_long+d.ranking[b].oi_short)-(d.ranking[a].oi_long+d.ranking[a].oi_short)));
  sel.innerHTML=mkts.map(m=>`<option ${m===cur?'selected':''}>${m}</option>`).join('')||'<option>—</option>';
  renderRanking();
 }catch(e){document.getElementById('rk_status').textContent='Error al cargar.';}}
document.getElementById('rk_market').onchange=renderRanking;
function rkRow(r,i){const pc=r.upnl>=0?'pos':'neg';
 return `<tr><td>${i+1}</td><td class="mkt"><a href="${'https://explorer.risechain.com/address/'+r.account}" target="_blank">${shortAddr(r.account)}</a></td>
  <td>${(+r.size).toLocaleString('en-US',{maximumFractionDigits:4})}</td><td>${U(r.notional)}</td><td>${P(r.entry)}</td>
  <td class="${pc}">${r.upnl>=0?'+':''}${U(r.upnl)}</td><td>x${(+r.leverage).toFixed(0)}</td></tr>`;}
function renderRanking(){if(!RANK)return;const m=document.getElementById('rk_market').value;
 const r=(RANK.ranking||{})[m];const L=document.getElementById('rk_long'),S=document.getElementById('rk_short');
 if(!r){L.innerHTML=S.innerHTML='<tr><td class="empty" colspan=7>Sin posiciones detectadas.</td></tr>';document.getElementById('rk_oi').textContent='';return;}
 document.getElementById('rk_oi').innerHTML=`OI longs: <b class="pos">${U(r.oi_long)}</b> (${r.n_long||r.longs.length}) · shorts: <b class="neg">${U(r.oi_short)}</b> (${r.n_short||r.shorts.length})`;
 L.innerHTML=r.longs.map(rkRow).join('')||'<tr><td class="empty" colspan=7>—</td></tr>';
 S.innerHTML=r.shorts.map(rkRow).join('')||'<tr><td class="empty" colspan=7>—</td></tr>';}

// ranking volumen
let VR_PERIOD='1d';
document.querySelectorAll('#vr_seg button').forEach(b=>b.onclick=()=>{
 document.querySelectorAll('#vr_seg button').forEach(x=>x.classList.remove('active'));
 b.classList.add('active');VR_PERIOD=b.dataset.p;loadVolRanking();});
async function loadVolRanking(){
 try{const d=await (await fetch('/api/volume-ranking?period='+VR_PERIOD)).json();
  let st='';
  if(d.phase==='waiting')st=`<b style="color:var(--amber)">Esperando al censo de cuentas…</b>`;
  else if(d.phase==='scanning')st=`<b style="color:var(--amber)">Calculando volumen…</b> ${d.scanned}/${d.total_accounts} cuentas procesadas`;
  else st=`<b style="color:var(--green)">Ranking listo</b> · ${d.count_with_volume} cuentas con volumen en ${d.period} · actualizado hace ${agoStr(d.last_update)}`;
  document.getElementById('vr_status').innerHTML=st;
  document.getElementById('vr_totals').innerHTML=`Volumen total ${d.period}: <b>${U(d.total_volume)}</b>`;
  const tot=d.total_volume||0;
  const rows=(d.ranking||[]).map((r,i)=>`<tr><td>${i+1}</td>
   <td class="mkt"><a href="${'https://explorer.risechain.com/address/'+r.account}" target="_blank">${shortAddr(r.account)}</a></td>
   <td>${U(r.volume)}</td><td>${tot?((r.volume/tot*100).toFixed(2)+'%'):'—'}</td><td>${r.trades}</td></tr>`).join('');
  document.getElementById('vr_body').innerHTML=rows||'<tr><td class="empty" colspan=5>Aún no hay datos.</td></tr>';
 }catch(e){document.getElementById('vr_status').textContent='Error al cargar.';}}

// ranking OI medio
let OIR_PERIOD='1d';
document.querySelectorAll('#oir_seg button').forEach(b=>b.onclick=()=>{
 document.querySelectorAll('#oir_seg button').forEach(x=>x.classList.remove('active'));
 b.classList.add('active');OIR_PERIOD=b.dataset.p;loadOiRanking();});
async function loadOiRanking(){
 try{const d=await (await fetch('/api/oi-ranking?period='+OIR_PERIOD)).json();
  let st='';
  if(d.phase==='waiting')st=`<b style="color:var(--amber)">Esperando al censo de cuentas…</b>`;
  else if(d.phase==='scanning')st=`<b style="color:var(--amber)">Reconstruyendo posiciones…</b> ${d.scanned}/${d.total_accounts} cuentas procesadas`;
  else st=`<b style="color:var(--green)">Ranking listo</b> · ${d.count_with_oi} cuentas con OI medio en ${d.period} · actualizado hace ${agoStr(d.last_update)}`;
  document.getElementById('oir_status').innerHTML=st;
  document.getElementById('oir_totals').innerHTML=`Suma de OI medio (${d.period}): <b>${U(d.total_avg_oi)}</b>`;
  const tot=d.total_avg_oi||0;
  const rows=(d.ranking||[]).map((r,i)=>`<tr><td>${i+1}</td>
   <td class="mkt"><a href="${'https://explorer.risechain.com/address/'+r.account}" target="_blank">${shortAddr(r.account)}</a></td>
   <td>${U(r.avg_oi)}</td><td>${tot?((r.avg_oi/tot*100).toFixed(2)+'%'):'—'}</td><td>${r.trades}</td></tr>`).join('');
  document.getElementById('oir_body').innerHTML=rows||'<tr><td class="empty" colspan=5>Aún no hay datos.</td></tr>';
 }catch(e){document.getElementById('oir_status').textContent='Error al cargar.';}}

// funding compare
let FC_MODE='apr', FC_FILTER='risex', FC_DATA=null;
document.querySelectorAll('#fc_mode button').forEach(b=>b.onclick=()=>{
 document.querySelectorAll('#fc_mode button').forEach(x=>x.classList.remove('active'));
 b.classList.add('active');FC_MODE=b.dataset.m;renderFunding();});
document.querySelectorAll('#fc_filter button').forEach(b=>b.onclick=()=>{
 document.querySelectorAll('#fc_filter button').forEach(x=>x.classList.remove('active'));
 b.classList.add('active');FC_FILTER=b.dataset.f;renderFunding();});
async function loadFunding(){
 try{const d=await (await fetch('/api/funding-compare')).json();FC_DATA=d;
  const errs=(d.errors||[]).length?` · <span style="color:var(--red)">avisos: ${d.errors.join(' | ')}</span>`:'';
  document.getElementById('fc_status').innerHTML=`<span style="color:var(--green)">En vivo</span> · ${d.rows.length} mercados${errs}`;
  renderFunding();
 }catch(e){document.getElementById('fc_status').textContent='Error al cargar.';}}
function fmtPct(x){if(x==null||isNaN(x))return'<span style="color:var(--muted)">—</span>';
 const cls=x>=0?'pos':'neg';return `<span class="${cls}">${x>=0?'+':''}${x.toFixed(FC_MODE==='apr'?2:5)}%</span>`;}
function renderFunding(){if(!FC_DATA)return;
 let rows=FC_DATA.rows.slice();
 if(FC_FILTER==='risex')rows=rows.filter(r=>r.in_risex);
 // ordenar por |diff lighter| desc para ver oportunidades primero
 rows.sort((a,b)=>{const aa=Math.abs(a.diff_lighter_apr||a.diff_pacifica_apr||0);
  const bb=Math.abs(b.diff_lighter_apr||b.diff_pacifica_apr||0);return bb-aa;});
 const k=FC_MODE==='apr'?'_apr':'_h';const m=FC_MODE==='apr'?1:1;
 const tb=document.getElementById('fc_body');
 tb.innerHTML=rows.map(r=>{
  const rx=r['risex'+k]!=null?r['risex'+k]*(FC_MODE==='apr'?1:100):null;
  const lt=r['lighter'+k]!=null?r['lighter'+k]*(FC_MODE==='apr'?1:100):null;
  const pc=r['pacifica'+k]!=null?r['pacifica'+k]*(FC_MODE==='apr'?1:100):null;
  // en modo hourly mostramos rate*100 ya como %
  const dL=r.diff_lighter_apr!=null?(FC_MODE==='apr'?r.diff_lighter_apr:r.diff_lighter_apr/24/365):null;
  const dP=r.diff_pacifica_apr!=null?(FC_MODE==='apr'?r.diff_pacifica_apr:r.diff_pacifica_apr/24/365):null;
  return `<tr><td class="mkt">${r.symbol}${r.in_risex?'':' <span class="note" style="font-size:10px">(no en RISEx)</span>'}</td>
   <td>${fmtPct(rx)}</td><td>${fmtPct(lt)}</td><td>${fmtPct(pc)}</td>
   <td>${fmtPct(dL)}</td><td>${fmtPct(dP)}</td></tr>`;}).join('')||'<tr><td class="empty" colspan=6>Sin datos.</td></tr>';}

async function loadUsers(){
 try{const d=await (await fetch('/api/users')).json();
  document.getElementById('us_status').innerHTML=`<span style="color:var(--green)">Datos del explorer oficial de RISE</span>`;
  document.getElementById('us_total').textContent=(d.total_accounts!=null?d.total_accounts:0).toLocaleString('es-ES');
  document.getElementById('us_addr').textContent=(d.total_addresses!=null?d.total_addresses:0).toLocaleString('es-ES');
  document.getElementById('us_active').textContent=(d.active_with_position||0).toLocaleString('es-ES');
  document.getElementById('us_today').textContent='+'+(d.new_today||0).toLocaleString('es-ES');
  document.getElementById('us_7d').textContent='+'+(d.new_7d||0).toLocaleString('es-ES');
  const s=d.series||[];const lab=s.map(x=>x.date.slice(5));
  intChart('usNew','bar',lab,s.map(x=>x.new),'#33d6a6');
  intChart('usCum','line',lab,s.map(x=>x.cum),'#7c5cff');
  intChart('usAct','bar',lab,s.map(x=>x.active),'#ffb454');
 }catch(e){document.getElementById('us_status').textContent='Error al cargar.';}}

// alertas
document.getElementById('al_vol').value=alerts.vol||'';
document.getElementById('al_oi').value=alerts.oi||'';
document.getElementById('al_fund').value=alerts.fund||'';
function saveAlerts(){alerts.vol=document.getElementById('al_vol').value;alerts.oi=document.getElementById('al_oi').value;
 alerts.fund=document.getElementById('al_fund').value;localStorage.setItem('rise_alerts',JSON.stringify(alerts));
 document.getElementById('al_status').textContent='Guardado ✓';if(DATA)renderOverview(DATA);}
function askNotif(){if('Notification'in window)Notification.requestPermission().then(p=>document.getElementById('al_status').textContent=p==='granted'?'Activas ✓':'Permiso denegado');}
let lastNotif={};
function notify(msg){if(!('Notification'in window)||Notification.permission!=='granted')return;
 const now=Date.now();if(lastNotif[msg]&&now-lastNotif[msg]<300000)return;lastNotif[msg]=now;
 try{new Notification('RISEx',{body:msg});}catch(e){}}

async function loadAll(){const dot=document.getElementById('dot');
 try{const d=await (await fetch('/api/data',{cache:'no-store'})).json();DATA=d;
  renderOverview(d);renderMarkets();renderBig();
  const dt=new Date((d.generated_at||0)*1000);
  document.getElementById('updated').textContent='actualizado '+dt.toLocaleTimeString('es-ES');
  dot.style.background='#33d6a6';
 }catch(e){dot.style.background='#ff5d6c';document.getElementById('updated').textContent='sin conexión';}}
loadAll();setInterval(loadAll,30000);
setInterval(()=>{
 if(document.getElementById('v_ranking').classList.contains('active'))loadRanking();
 if(document.getElementById('v_users').classList.contains('active'))loadUsers();
 if(document.getElementById('v_volranking').classList.contains('active'))loadVolRanking();
 if(document.getElementById('v_oiranking').classList.contains('active'))loadOiRanking();
 if(document.getElementById('v_funding').classList.contains('active'))loadFunding();
},15000);
</script></body></html>"""


if __name__ == "__main__":
    main()
