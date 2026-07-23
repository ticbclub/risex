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
import calendar
import sqlite3
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
VOL_PER_ACCOUNT_TIMEOUT = 120
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
BACKFILL_FILE = HISTORY_FILE.replace("rise_history.json", "rise_backfill.json")
# Cutoff for "since launch" charts — user requested April 20 2026 onwards
BACKFILL_FROM_TS = 1776643200  # 2026-04-20 00:00 UTC

# ============================== SQLite persistent store ==============================
# Ground truth for per-account metrics. The indexer writes here once and reads are SELECTs.
DB_PATH = HISTORY_FILE.replace("rise_history.json", "risex.db")
_DB = None
_DB_LOCK = threading.RLock()


def _db():
    global _DB
    if _DB is not None:
        return _DB
    with _DB_LOCK:
        if _DB is not None:
            return _DB
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA cache_size=-32000")
        conn.row_factory = sqlite3.Row
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS accounts (
            address TEXT PRIMARY KEY,
            last_refresh INTEGER NOT NULL DEFAULT 0,
            trades INTEGER NOT NULL DEFAULT 0,
            vol_1d REAL NOT NULL DEFAULT 0,
            vol_7d REAL NOT NULL DEFAULT 0,
            vol_30d REAL NOT NULL DEFAULT 0,
            pnl_1d REAL NOT NULL DEFAULT 0,
            pnl_7d REAL NOT NULL DEFAULT 0,
            pnl_30d REAL NOT NULL DEFAULT 0,
            fees_30d REAL NOT NULL DEFAULT 0,
            fees_1d REAL NOT NULL DEFAULT 0,
            fees_7d REAL NOT NULL DEFAULT 0,
            trades_1d INTEGER NOT NULL DEFAULT 0,
            trades_7d INTEGER NOT NULL DEFAULT 0,
            n_liquidations INTEGER NOT NULL DEFAULT 0,
            wins_30d INTEGER NOT NULL DEFAULT 0,
            losses_30d INTEGER NOT NULL DEFAULT 0,
            max_dd_30d REAL NOT NULL DEFAULT 0,
            oi_1d REAL NOT NULL DEFAULT 0,
            oi_7d REAL NOT NULL DEFAULT 0,
            oi_30d REAL NOT NULL DEFAULT 0,
            timed_out INTEGER NOT NULL DEFAULT 0,
            raw TEXT NOT NULL DEFAULT '{}'
        );
        CREATE INDEX IF NOT EXISTS idx_acc_vol_30d ON accounts(vol_30d DESC);
        CREATE INDEX IF NOT EXISTS idx_acc_vol_7d  ON accounts(vol_7d DESC);
        CREATE INDEX IF NOT EXISTS idx_acc_vol_1d  ON accounts(vol_1d DESC);
        CREATE INDEX IF NOT EXISTS idx_acc_pnl_30d ON accounts(pnl_30d DESC);
        CREATE INDEX IF NOT EXISTS idx_acc_pnl_1d  ON accounts(pnl_1d DESC);
        CREATE INDEX IF NOT EXISTS idx_acc_oi_30d  ON accounts(oi_30d DESC);
        CREATE INDEX IF NOT EXISTS idx_acc_oi_7d   ON accounts(oi_7d DESC);
        CREATE INDEX IF NOT EXISTS idx_acc_oi_1d   ON accounts(oi_1d DESC);
        CREATE INDEX IF NOT EXISTS idx_acc_fees_30d ON accounts(fees_30d DESC);
        CREATE INDEX IF NOT EXISTS idx_acc_liq      ON accounts(n_liquidations DESC);
        CREATE INDEX IF NOT EXISTS idx_acc_refresh ON accounts(last_refresh DESC);
        CREATE TABLE IF NOT EXISTS transfers (
            tx_hash TEXT NOT NULL,
            log_index INTEGER NOT NULL,
            block_number INTEGER NOT NULL,
            ts INTEGER NOT NULL,
            kind TEXT NOT NULL,
            account TEXT NOT NULL,
            counterparty TEXT,
            token TEXT NOT NULL,
            amount REAL NOT NULL,
            PRIMARY KEY (tx_hash, log_index)
        );
        CREATE INDEX IF NOT EXISTS idx_tx_account ON transfers(account, ts DESC);
        CREATE INDEX IF NOT EXISTS idx_tx_block ON transfers(block_number DESC);
        CREATE TABLE IF NOT EXISTS indexer_state (
            key TEXT PRIMARY KEY,
            value INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS trades (
            tx_hash TEXT NOT NULL,
            log_index INTEGER NOT NULL,
            account TEXT NOT NULL,
            market_id INTEGER NOT NULL,
            ts INTEGER NOT NULL,
            side TEXT NOT NULL,
            position_side TEXT,
            role TEXT,
            size REAL NOT NULL,
            price REAL NOT NULL,
            fee REAL NOT NULL DEFAULT 0,
            realized_pnl REAL NOT NULL DEFAULT 0,
            order_id TEXT,
            is_liq INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (tx_hash, log_index, account)
        );
        CREATE INDEX IF NOT EXISTS idx_trades_acct_ts ON trades(account, ts DESC);
        CREATE INDEX IF NOT EXISTS idx_trades_market_ts ON trades(market_id, ts DESC);
        CREATE INDEX IF NOT EXISTS idx_trades_order ON trades(account, order_id);
        CREATE INDEX IF NOT EXISTS idx_trades_liq ON trades(is_liq, ts DESC) WHERE is_liq=1;
        """)
        conn.commit()
        _DB = conn
        try:
            n = conn.execute("SELECT COUNT(*) FROM accounts").fetchone()[0]
            print(f"[db] SQLite ready at {DB_PATH} — {n} accounts cached", flush=True)
        except Exception:
            pass
    return _DB


_DB_SORTABLE = {"vol_1d", "vol_7d", "vol_30d", "pnl_1d", "pnl_7d", "pnl_30d",
                "fees_30d", "fees_1d", "fees_7d", "n_liquidations", "trades",
                "oi_1d", "oi_7d", "oi_30d", "max_dd_30d", "last_refresh"}


def db_upsert_account(address, v):
    """Persist a _account_metrics result. Idempotent. Silent on failure."""
    if not address:
        return
    try:
        raw = json.dumps({"equity_curve": v.get("equity_curve") or []}, default=float)
    except Exception:
        raw = "{}"
    try:
        with _DB_LOCK:
            _db().execute("""
            INSERT OR REPLACE INTO accounts
            (address, last_refresh, trades, vol_1d, vol_7d, vol_30d,
             pnl_1d, pnl_7d, pnl_30d, fees_30d, fees_1d, fees_7d,
             trades_1d, trades_7d, n_liquidations, wins_30d, losses_30d, max_dd_30d,
             oi_1d, oi_7d, oi_30d, timed_out, raw)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                address.lower(), int(v.get("last_refresh") or 0),
                int(v.get("trades") or 0),
                float(v.get("1d") or 0), float(v.get("7d") or 0), float(v.get("30d") or 0),
                float(v.get("pnl_1d") or 0), float(v.get("pnl_7d") or 0),
                float(v.get("realized_pnl_30d") or 0),
                float(v.get("fees_30d") or 0), float(v.get("fees_1d") or 0),
                float(v.get("fees_7d") or 0),
                int(v.get("trades_1d") or 0), int(v.get("trades_7d") or 0),
                int(v.get("n_liquidations") or 0),
                int(v.get("wins_30d") or 0), int(v.get("losses_30d") or 0),
                float(v.get("max_dd_30d") or 0),
                float(v.get("oi_1d") or 0), float(v.get("oi_7d") or 0),
                float(v.get("oi_30d") or 0),
                int(1 if v.get("timed_out") else 0),
                raw,
            ))
            _DB.commit()
    except Exception as e:
        print(f"[db] upsert err for {address[:10]}: {e}", flush=True)


def db_get_account(address):
    if not address:
        return None
    try:
        with _DB_LOCK:
            r = _db().execute("SELECT * FROM accounts WHERE address=?",
                               (address.lower(),)).fetchone()
        return dict(r) if r else None
    except Exception:
        return None


def db_top_by(column, limit=200, where_gt=0, desc=True):
    if column not in _DB_SORTABLE:
        return []
    direction = "DESC" if desc else "ASC"
    try:
        with _DB_LOCK:
            rows = _db().execute(
                f"SELECT * FROM accounts WHERE {column} > ? ORDER BY {column} {direction} LIMIT ?",
                (where_gt, limit),
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


def db_count_with(column=None, where_gt=0):
    try:
        with _DB_LOCK:
            if column:
                return _db().execute(f"SELECT COUNT(*) FROM accounts WHERE {column} > ?",
                                      (where_gt,)).fetchone()[0]
            return _db().execute("SELECT COUNT(*) FROM accounts").fetchone()[0]
    except Exception:
        return 0


def db_sum(column):
    if column not in _DB_SORTABLE:
        return 0.0
    try:
        with _DB_LOCK:
            r = _db().execute(f"SELECT SUM({column}) FROM accounts").fetchone()
        return float(r[0] or 0)
    except Exception:
        return 0.0


def db_save_trades_bulk(account, trades_rows):
    """Insert many trade fills at once. Idempotent via PK (tx_hash, log_index, account)."""
    if not account or not trades_rows:
        return 0
    try:
        with _DB_LOCK:
            _db().executemany("""
            INSERT OR IGNORE INTO trades
            (tx_hash, log_index, account, market_id, ts, side, position_side, role,
             size, price, fee, realized_pnl, order_id, is_liq)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, trades_rows)
            _DB.commit()
        return len(trades_rows)
    except Exception as e:
        print(f"[db] trades insert err: {e}", flush=True)
        return 0


def db_max_trade_ts(account):
    if not account:
        return 0
    try:
        with _DB_LOCK:
            r = _db().execute("SELECT MAX(ts) FROM trades WHERE account=?",
                               (account.lower(),)).fetchone()
        return int(r[0]) if r and r[0] else 0
    except Exception:
        return 0


def db_min_trade_ts(account):
    if not account:
        return 0
    try:
        with _DB_LOCK:
            r = _db().execute("SELECT MIN(ts) FROM trades WHERE account=?",
                               (account.lower(),)).fetchone()
        return int(r[0]) if r and r[0] else 0
    except Exception:
        return 0


def db_trades_for_account(account, limit=200, since_ts=None, market_id=None, order_id=None):
    if not account:
        return []
    where = ["account=?"]; args = [account.lower()]
    if since_ts is not None:
        where.append("ts >= ?"); args.append(int(since_ts))
    if market_id is not None:
        where.append("market_id=?"); args.append(int(market_id))
    if order_id:
        where.append("order_id=?"); args.append(order_id)
    args.append(limit)
    try:
        with _DB_LOCK:
            rows = _db().execute(
                f"SELECT * FROM trades WHERE {' AND '.join(where)} ORDER BY ts DESC LIMIT ?",
                args
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


def db_trades_count(account):
    if not account:
        return 0
    try:
        with _DB_LOCK:
            r = _db().execute("SELECT COUNT(*) FROM trades WHERE account=?",
                               (account.lower(),)).fetchone()
        return int(r[0]) if r else 0
    except Exception:
        return 0


def db_orders_count(account):
    """Count UNIQUE order_id values for this account (= real orders, not fills)."""
    if not account:
        return 0
    try:
        with _DB_LOCK:
            r = _db().execute(
                "SELECT COUNT(DISTINCT order_id) FROM trades WHERE account=? AND order_id IS NOT NULL AND order_id != ''",
                (account.lower(),)
            ).fetchone()
        return int(r[0]) if r else 0
    except Exception:
        return 0


def db_account_window_metrics(account, since_ts, now_s=None):
    """Compute volume + trade count + PnL + fees from DB for an account in a time window.
    All from local SQLite — no API calls, instant. The source of truth once trades
    are persisted."""
    if not account:
        return {"vol": 0.0, "orders": 0, "fills": 0, "pnl": 0.0, "fees": 0.0,
                 "wins": 0, "losses": 0, "n_liq": 0}
    try:
        with _DB_LOCK:
            r = _db().execute("""
                SELECT
                  COALESCE(SUM(size*price), 0)       AS vol,
                  COUNT(*)                            AS fills,
                  COUNT(DISTINCT NULLIF(order_id, '')) AS orders,
                  COALESCE(SUM(realized_pnl), 0)     AS pnl,
                  COALESCE(SUM(fee), 0)              AS fees,
                  COALESCE(SUM(is_liq), 0)           AS n_liq
                FROM trades WHERE account=? AND ts >= ?
                """, (account.lower(), int(since_ts))).fetchone()
            # Wins/losses: group by order_id, sum realized_pnl, count signs
            wl = _db().execute("""
                SELECT
                  SUM(CASE WHEN s.s > 0 THEN 1 ELSE 0 END) AS wins,
                  SUM(CASE WHEN s.s < 0 THEN 1 ELSE 0 END) AS losses
                FROM (
                  SELECT SUM(realized_pnl) AS s FROM trades
                  WHERE account=? AND ts >= ? AND order_id IS NOT NULL AND order_id != ''
                  GROUP BY order_id
                ) AS s
                """, (account.lower(), int(since_ts))).fetchone()
        return {
            "vol":    float(r["vol"] or 0),
            "fills":  int(r["fills"] or 0),
            "orders": int(r["orders"] or 0),
            "pnl":    float(r["pnl"] or 0),
            "fees":   float(r["fees"] or 0),
            "n_liq":  int(r["n_liq"] or 0),
            "wins":   int(wl["wins"] or 0) if wl else 0,
            "losses": int(wl["losses"] or 0) if wl else 0,
        }
    except Exception as e:
        print(f"[db] window metrics err: {e}", flush=True)
        return {"vol": 0.0, "orders": 0, "fills": 0, "pnl": 0.0, "fees": 0.0,
                 "wins": 0, "losses": 0, "n_liq": 0}


def db_rank_for(column, my_val):
    """1-indexed rank for a value in `column`. None if no data."""
    if column not in _DB_SORTABLE or my_val is None or my_val <= 0:
        return None
    try:
        with _DB_LOCK:
            better = _db().execute(f"SELECT COUNT(*) FROM accounts WHERE {column} > ?",
                                    (my_val,)).fetchone()[0]
            total = _db().execute(f"SELECT COUNT(*) FROM accounts WHERE {column} > 0").fetchone()[0]
        return {"rank": better + 1, "of": total}
    except Exception:
        return None


_CACHE = {"ts": 0, "data": None}
_HIST_LOCK = threading.Lock()
_BACKFILL_LOCK = threading.Lock()
_BACKFILL_STATE = {"phase": "idle", "scanned_to": 0, "target": 0, "days_indexed": 0}


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


_FPAY = {"by_account": {}, "last_update": 0}
_FPAY_LOCK = threading.Lock()


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
        # cachear para el funding leaderboard
        with _FPAY_LOCK:
            _FPAY["by_account"][account.lower()] = {
                "funding": funding, "unsettled": unsettled,
                "portfolio": portfolio, "ts": int(time.time())}
        return {"portfolio": portfolio, "total_maint": total_maint,
                "funding": funding, "unsettled": unsettled,
                "cross_balance": cross_balance}
    except Exception:
        return None


def funding_indexer_loop():
    """Refresca funding/unsettled onchain para top accounts por OI cada 10 min."""
    while True:
        try:
            with _IDX_LOCK:
                tops = [r["account"] for r in _INDEX["account_oi_ranking"][:300]]
            for acc in tops:
                onchain_account_state(acc)
                time.sleep(0.2)
            with _FPAY_LOCK:
                _FPAY["last_update"] = int(time.time())
        except Exception:
            pass
        time.sleep(600)


def get_funding_ranking(limit=100):
    """Snapshot ranking de funding paid / received basado en _FPAY.
       funding < 0 → la cuenta debe pagar (le quitarán al settle).
       funding > 0 → la cuenta cobrará (le abonarán al settle).
    """
    with _FPAY_LOCK:
        items = []
        for acc, v in _FPAY["by_account"].items():
            f_ = v.get("funding", 0.0)
            u_ = v.get("unsettled", 0.0)
            if abs(f_) < 1.0 and abs(u_) < 1.0:
                continue
            items.append({"account": acc, "funding": f_, "unsettled": u_,
                          "portfolio": v.get("portfolio", 0.0), "ts": v.get("ts", 0)})
        last = _FPAY["last_update"]
    payers = sorted(items, key=lambda x: x["funding"])[:limit]   # más negativos primero
    receivers = sorted(items, key=lambda x: x["funding"], reverse=True)[:limit]
    total_paid = -sum(min(0, x["funding"]) for x in items)
    total_recv = sum(max(0, x["funding"]) for x in items)
    return {"ok": True, "last_update": last,
            "tracked_accounts": len(items),
            "total_paid": total_paid, "total_received": total_recv,
            "payers": payers, "receivers": receivers}


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

    record_history(out["totals"], tvl_cur, out["markets"])
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


def load_backfill():
    """Load onchain-backfilled daily perp volume + fees since BACKFILL_FROM_TS."""
    try:
        with open(BACKFILL_FILE) as fh:
            return json.load(fh)
    except Exception:
        return {"last_block": None, "daily": {}, "ref_ts": None, "ref_block": None}


def save_backfill(state):
    try:
        # Serialize: sets aren't JSON, so flush accts_set → accts (sorted list)
        out = dict(state)
        daily = dict(state.get("daily") or {})
        for k, rec in daily.items():
            if "accts_set" in rec:
                accts_list = sorted(rec["accts_set"])
                daily[k] = {**{kk: vv for kk, vv in rec.items() if kk != "accts_set"},
                            "accts": accts_list}
        out["daily"] = daily
        with _BACKFILL_LOCK:
            with open(BACKFILL_FILE, "w") as fh:
                json.dump(out, fh)
    except Exception:
        pass


# ============================== Transfers (deposits/withdrawals) indexer ==============================
TOPIC_DEPOSIT  = "0x1a52dc5f1a697e41465e09288950bab46daf62b3558244f71c7eee6ec1872a88"
TOPIC_WITHDRAW = "0xba06d99e13a05820f144f5b669e4cb8c1299da99a36cd44ab9e971975627c6a0"
TRANSFERS_FROM_BLOCK = 7345365   # start of perp activity
USDC_DECIMALS = 6


def db_save_transfer(tx_hash, log_index, block_number, ts, kind, account, counterparty, token, amount):
    try:
        with _DB_LOCK:
            _db().execute("""
            INSERT OR IGNORE INTO transfers
            (tx_hash, log_index, block_number, ts, kind, account, counterparty, token, amount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (tx_hash, log_index, block_number, ts, kind,
                  account.lower(), counterparty.lower() if counterparty else None,
                  token.lower(), float(amount)))
            _DB.commit()
    except Exception:
        pass


def db_get_transfers(account, limit=100):
    if not account:
        return []
    try:
        with _DB_LOCK:
            rows = _db().execute(
                "SELECT * FROM transfers WHERE account=? ORDER BY ts DESC LIMIT ?",
                (account.lower(), limit)
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


def db_set_state(key, value):
    try:
        with _DB_LOCK:
            _db().execute("INSERT OR REPLACE INTO indexer_state (key, value) VALUES (?, ?)",
                          (key, int(value)))
            _DB.commit()
    except Exception:
        pass


def db_get_state(key, default=0):
    try:
        with _DB_LOCK:
            r = _db().execute("SELECT value FROM indexer_state WHERE key=?", (key,)).fetchone()
        return int(r[0]) if r else default
    except Exception:
        return default


def transfers_indexer_loop():
    """Indexes CollateralManager Deposit/Withdraw events into SQLite. Runs on startup +
    incrementally every minute. Uses ~1s/block timestamp estimation for speed."""
    state_key = "transfers_last_block"
    last_block = db_get_state(state_key, TRANSFERS_FROM_BLOCK - 1)
    try:
        latest_now = int(rpc("eth_blockNumber", []), 16)
        ref_block  = max(TRANSFERS_FROM_BLOCK, last_block + 1)
        ref_ts     = block_ts(ref_block)
    except Exception:
        time.sleep(60); return

    while True:
        try:
            latest = int(rpc("eth_blockNumber", []), 16)
            if latest <= last_block:
                time.sleep(60); continue
            b = last_block + 1
            chunks_done = 0
            while b <= latest:
                to = min(b + CHUNK - 1, latest)
                try:
                    logs = get_logs(COLLATERAL_MANAGER, b, to,
                                     topics=[[TOPIC_DEPOSIT, TOPIC_WITHDRAW]])
                except Exception:
                    time.sleep(2); continue
                for l in logs:
                    try:
                        bn = int(l["blockNumber"], 16)
                        ts = ref_ts + (bn - ref_block)
                        kind = "deposit" if l["topics"][0] == TOPIC_DEPOSIT else "withdraw"
                        account = "0x" + l["topics"][1][26:]
                        counterparty = "0x" + l["topics"][2][26:] if len(l["topics"]) > 2 else None
                        token = "0x" + l["topics"][3][26:] if len(l["topics"]) > 3 else None
                        amount = int(l["data"][2:66], 16) / (10 ** USDC_DECIMALS) if len(l["data"]) >= 66 else 0
                        log_index = int(l.get("logIndex", "0x0"), 16)
                        db_save_transfer(l["transactionHash"], log_index, bn, ts,
                                          kind, account, counterparty, token or "", amount)
                    except Exception:
                        continue
                b = to + 1
                chunks_done += 1
                if chunks_done % 20 == 0:
                    db_set_state(state_key, b - 1)
            last_block = latest
            db_set_state(state_key, last_block)
            if chunks_done > 0:
                print(f"[transfers] indexed up to block {last_block}", flush=True)
        except Exception as e:
            print(f"[transfers] err: {e}", flush=True)
        time.sleep(60)


def historical_backfill_loop():
    """Walks PerpsManager TakerFee/MakerSettle events from BACKFILL_FROM_TS to now,
    bucketing by UTC day. Estimates volume from taker fee at 0.05% rate. Initial pass
    (~80s for 46 days on RISE), then incremental updates every hour."""
    bf = load_backfill()
    daily = bf.get("daily") or {}
    # Find the block at the cutoff timestamp (binary search via block_at_ts)
    try:
        latest_now = int(rpc("eth_blockNumber", []), 16)
    except Exception:
        time.sleep(60); return
    if bf.get("last_block") is not None:
        last_block = int(bf["last_block"])
        ref_ts = bf.get("ref_ts"); ref_block = bf.get("ref_block")
    else:
        try:
            start_block = block_at_ts(BACKFILL_FROM_TS, AR_DEPLOY_BLOCK, latest_now)
            ref_block = start_block
            ref_ts = block_ts(start_block)
            print(f"[backfill] cutoff 2026-04-20 → block {start_block} (ts {ref_ts})", flush=True)
        except Exception as e:
            print(f"[backfill] could not resolve cutoff block: {e}", flush=True)
            time.sleep(60); return
        last_block = start_block - 1

    while True:
        try:
            latest = int(rpc("eth_blockNumber", []), 16)
            if latest <= last_block:
                time.sleep(300); continue
            with _BACKFILL_LOCK:
                _BACKFILL_STATE["phase"] = "scanning"
                _BACKFILL_STATE["target"] = latest
                _BACKFILL_STATE["scanned_to"] = last_block
            b = last_block + 1
            chunks_done = 0
            while b <= latest:
                to = min(b + CHUNK - 1, latest)
                try:
                    logs = fetch_fee_logs(b, to)
                except Exception:
                    time.sleep(2); continue
                for l in logs:
                    bn = int(l["blockNumber"], 16)
                    # Estimate timestamp using ~1s block time on RISE
                    ts = ref_ts + (bn - ref_block)
                    try:
                        fee = _signed256(l["data"][2:][128:192]) / WAD
                    except Exception:
                        continue
                    date = time.strftime("%Y-%m-%d", time.gmtime(ts))
                    rec = daily.setdefault(date, {"fees": 0.0, "vol": 0.0, "accts": []})
                    rec["fees"] = rec.get("fees", 0.0) + fee
                    if l["topics"][0] == TOPIC_TAKE:
                        # Taker fee ≈ 0.05% of notional → notional = fee × 2000
                        rec["vol"] = rec.get("vol", 0.0) + fee * 2000.0
                    # Track unique account_id per day (topic[2] in the event = numeric account id)
                    try:
                        acct_id = int(l["topics"][2], 16)
                        if "accts_set" not in rec:
                            rec["accts_set"] = set(rec.get("accts") or [])
                        rec["accts_set"].add(acct_id)
                    except Exception:
                        pass
                b = to + 1
                chunks_done += 1
                with _BACKFILL_LOCK:
                    _BACKFILL_STATE["scanned_to"] = b - 1
                    _BACKFILL_STATE["days_indexed"] = len(daily)
                if chunks_done % 20 == 0:
                    save_backfill({"last_block": b - 1, "daily": daily,
                                    "ref_ts": ref_ts, "ref_block": ref_block})
            last_block = latest
            save_backfill({"last_block": last_block, "daily": daily,
                            "ref_ts": ref_ts, "ref_block": ref_block})
            with _BACKFILL_LOCK:
                _BACKFILL_STATE["phase"] = "ready"
                _BACKFILL_STATE["days_indexed"] = len(daily)
            print(f"[backfill] complete: {len(daily)} days indexed, last block {last_block}", flush=True)
        except Exception as e:
            print(f"[backfill] err: {e}", flush=True)
        time.sleep(3600)  # incremental refresh every hour


def get_daily_active_wallets(days_back=30):
    """Return unique active accounts per UTC day (last N days) from onchain PerpsManager events.
    Source: rise_backfill.json which tracks accts list per day via historical_backfill_loop."""
    bf = load_backfill()
    daily = bf.get("daily") or {}
    if not daily:
        return {"ok": True, "days": [], "note": "backfill not ready yet"}
    today = time.strftime("%Y-%m-%d", time.gmtime())
    cutoff = time.strftime("%Y-%m-%d", time.gmtime(time.time() - days_back * 86400))
    out_days = []
    for date in sorted(daily.keys()):
        if date < cutoff or date > today:
            continue
        rec = daily[date]
        accts = rec.get("accts") or []
        out_days.append({
            "date": date,
            "active_wallets": len(accts),
            "events": int((rec.get("vol", 0.0) or 0) / 2000.0 / 1000),  # rough event proxy from vol
        })
    return {"ok": True, "count": len(out_days),
            "days_requested": days_back, "days": out_days,
            "phase": _BACKFILL_STATE.get("phase", "unknown")}


def get_cumulative_growth():
    """Cumulative perp volume + fees + OI snapshots since contract launch.
    Sources: onchain backfill (daily totals from PerpsManager events) for vol+fees,
    live history file for OI snapshots."""
    daily = {}  # {"YYYY-MM-DD": {"vol": ..., "fees": ..., "oi": ...}}
    # 1) Onchain backfill: daily vol + fees from deployment onwards
    bf = load_backfill()
    for date, rec in (bf.get("daily") or {}).items():
        daily[date] = {"vol": rec.get("vol", 0.0), "fees": rec.get("fees", 0.0), "oi": 0}
    # 2) Overlay live history for OI snapshot per day (and as fallback for vol/fees if backfill empty)
    h = load_history()
    for p in h:
        date = time.strftime("%Y-%m-%d", time.gmtime(p["t"]))
        rec = daily.setdefault(date, {"vol": 0.0, "fees": 0.0, "oi": 0})
        # OI: last sample wins per day
        rec["oi"] = p.get("oi", 0) or rec.get("oi", 0)
        # If backfill missing this day, fall back to live vol/fees (less accurate but better than 0)
        if rec["vol"] == 0:
            rec["vol"] = p.get("vol", 0) or 0
        if rec["fees"] == 0:
            rec["fees"] = p.get("fees", 0) or 0
    cutoff_date = time.strftime("%Y-%m-%d", time.gmtime(BACKFILL_FROM_TS))
    dates = sorted(d for d in daily.keys() if d >= cutoff_date)
    cum_vol = 0.0; cum_fees = 0.0
    out = []
    for d in dates:
        r = daily[d]
        cum_vol += r["vol"]
        cum_fees += r["fees"]
        try:
            ts = int(calendar.timegm(time.strptime(d, "%Y-%m-%d"))) + 43200  # noon UTC
        except Exception:
            continue
        out.append({
            "t": ts,
            "cum_vol": round(cum_vol),
            "cum_fees": round(cum_fees, 2),
            "oi": round(r.get("oi") or 0),
        })
    with _BACKFILL_LOCK:
        bs = dict(_BACKFILL_STATE)
    return {"ok": True, "points": out, "days": len(out), "backfill": bs}


def record_history(totals, tvl, markets=None):
    """Guarda snapshot global + funding por mercado para series temporales."""
    with _HIST_LOCK:
        h = load_history()
        now = int(time.time())
        if h and now - h[-1]["t"] < HISTORY_MIN_GAP:
            return
        # snapshot global
        point = {"t": now, "vol": round(totals.get("volume_24h") or 0),
                  "oi": round(totals.get("open_interest_usd") or 0), "tvl": round(tvl or 0),
                  "fees": round(totals.get("fees_24h_real") or 0, 2)}
        # funding por mercado (anualizado %) y OI por mercado
        if markets:
            point["mk"] = {m["name"]: {"f": round(m.get("funding_apr", 0), 3),
                                        "oi": round(m.get("oi_usd", 0))} for m in markets}
        h.append(point)
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

    # historial de trades — agrupamos fills por order_id para mostrar 1 orden = 1 entrada.
    # Fetch up to 1000 fills, group by order_id, return last 200 consolidated orders.
    try:
        th = fetch_json(f"{RISEX}/v1/trade-history?account={q}&limit=1000")["data"]
        raw_fills = th.get("trades") or []
        # Group by order_id (each user-placed order can fill in many small executions)
        groups = {}
        for t in raw_fills:
            oid = t.get("order_id") or t.get("id")  # fallback if no order_id
            groups.setdefault(oid, []).append(t)
        # Build consolidated trade per order
        consolidated = []
        for oid, fills in groups.items():
            f0 = fills[0]
            mid = str(f0.get("market_id"))
            total_size = sum(f(t.get("size")) for t in fills)
            total_notional = sum(f(t.get("price")) * f(t.get("size")) for t in fills)
            total_fee = sum(f(t.get("fee")) for t in fills)
            total_pnl = sum(f(t.get("realized_pnl")) for t in fills)
            vwap = total_notional / total_size if total_size > 0 else f(f0.get("price"))
            # Earliest fill is the order entry time
            min_ts = min(int(t.get("time", 0)) for t in fills) // 1_000_000_000
            max_ts = max(int(t.get("time", 0)) for t in fills) // 1_000_000_000
            # Role: if ALL fills are MAKER → maker, if all TAKER → taker, else mixed
            roles = set(str(t.get("liquidity_indicator") or "").upper() for t in fills)
            role_lbl = list(roles)[0] if len(roles) == 1 else "MIXED"
            any_liq = any(bool(t.get("is_liquidation")) for t in fills)
            # realized_pnl % weighted by size
            consolidated.append({
                "order_id": oid,
                "market": mk.get(mid, {}).get("name") or mid,
                "ts": min_ts,
                "ts_end": max_ts,
                "side": "Buy" if str(f0.get("side")).upper() == "BUY" else "Sell",
                "position_side": "Long" if str(f0.get("position_side")).upper() == "BUY" else "Short",
                "price": vwap,
                "size": total_size,
                "notional": total_notional,
                "fee": total_fee,
                "realized_pnl": total_pnl,
                "realized_pnl_pct": f(f0.get("realized_pnl_percentage")),
                "role": role_lbl,
                "is_liq": any_liq,
                "leverage": f(f0.get("leverage")),
                "n_fills": len(fills),
            })
        consolidated.sort(key=lambda x: -x["ts"])
        res["trades"] = consolidated[:200]
    except Exception as e:
        res["errors"].append(f"trades: {e}")

    total_realized = sum(t["realized_pnl"] for t in res["trades"])
    total_fees = sum(t["fee"] for t in res["trades"])
    vol_summary = None
    realized_pnl_30d = None
    fees_30d = None
    n_liquidations = None
    # ----- Volume/PnL/stats from SQLite (instant); stale-while-revalidate in background -----
    equity_curve = None; max_dd_30d = None; smart = False
    pnl_1d = None; pnl_7d = None
    try:
        now_s = int(time.time())
        db_acct = db_get_account(account)
        # In-memory fast-path (for very recent writes that haven't been re-read from DB yet)
        with _VOL_LOCK:
            v_mem = _VOL["by_account"].get(account)
        # Prefer in-memory if it's the freshest, else use DB
        v = None
        if v_mem and v_mem.get("last_refresh", 0) > (db_acct["last_refresh"] if db_acct else 0):
            v = v_mem; source = "mem"
        elif db_acct:
            # Reconstruct a v-like dict from DB row
            v = {
                "trades": db_acct["trades"], "last_refresh": db_acct["last_refresh"],
                "1d": db_acct["vol_1d"], "7d": db_acct["vol_7d"], "30d": db_acct["vol_30d"],
                CUSTOM_LABEL: 0,
                "pnl_1d": db_acct["pnl_1d"], "pnl_7d": db_acct["pnl_7d"],
                "realized_pnl_30d": db_acct["pnl_30d"],
                "fees_1d": db_acct["fees_1d"], "fees_7d": db_acct["fees_7d"],
                "fees_30d": db_acct["fees_30d"],
                "trades_1d": db_acct["trades_1d"], "trades_7d": db_acct["trades_7d"],
                "wins_30d": db_acct["wins_30d"], "losses_30d": db_acct["losses_30d"],
                "max_dd_30d": db_acct["max_dd_30d"],
                "oi_1d": db_acct["oi_1d"], "oi_7d": db_acct["oi_7d"], "oi_30d": db_acct["oi_30d"],
                "n_liquidations": db_acct["n_liquidations"],
                "timed_out": bool(db_acct["timed_out"]),
            }
            try:
                raw = json.loads(db_acct.get("raw") or "{}")
                v["equity_curve"] = raw.get("equity_curve") or []
            except Exception:
                v["equity_curve"] = []
            source = "db"

        fresh = v and (now_s - v.get("last_refresh", 0) < 1800)  # 30min freshness window

        # Schedule background refresh if stale OR if we've never seen this wallet (timed_out)
        if v is None or not fresh:
            def _bg_scan(acct, ts):
                try:
                    nv = _account_metrics(acct, ts)
                    nv.pop("feed_entries", None)
                    with _VOL_LOCK:
                        _VOL["by_account"][acct] = nv
                    db_upsert_account(acct, nv)
                except Exception:
                    pass
            threading.Thread(target=_bg_scan, args=(account, now_s), daemon=True).start()

            if v is None:
                # Never seen: try VERY quick inline scan (1 page, 3s) just to have something
                v = _account_metrics(account, now_s, max_pages=1, max_seconds=3.0)
                v.pop("feed_entries", None)
                with _VOL_LOCK:
                    _VOL["by_account"][account] = v
                db_upsert_account(account, v)

        if v:
            vol_summary = {"1d": v.get("1d", 0), "7d": v.get("7d", 0),
                            "30d": v.get("30d", 0),
                            CUSTOM_LABEL: v.get(CUSTOM_LABEL, 0),
                            "trades_30d": v.get("trades", 0)}
            realized_pnl_30d = v.get("realized_pnl_30d", 0)
            fees_30d = v.get("fees_30d", 0)
            n_liquidations = v.get("n_liquidations", 0)
            # Ranks: pure SQL queries (indexed) — instant
            ranks = {}
            for period in ("1d", "7d", "30d", CUSTOM_LABEL):
                if period == CUSTOM_LABEL:
                    ranks[period] = None  # custom not in DB
                    continue
                col = f"vol_{period}"
                ranks[period] = db_rank_for(col, v.get(period, 0))
            vol_summary["ranks"] = ranks
            equity_curve = v.get("equity_curve") or None
            max_dd_30d = v.get("max_dd_30d")
            pnl_1d = v.get("pnl_1d"); pnl_7d = v.get("pnl_7d")
            smart = _is_smart_money(v) if not v.get("timed_out") else False
    except Exception as e:
        res["errors"].append(f"vol: {e}")

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
        "pnl_1d": pnl_1d, "pnl_7d": pnl_7d,
        "fees_30d": fees_30d,
        "n_liquidations": n_liquidations,
        "max_dd_30d": max_dd_30d,
        "smart_money": smart,
        "equity_curve": equity_curve,
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
    """Platform adoption stats — RISEx perp DEX SPECIFIC, not chain-wide.
    Sources: SQLite accounts table (trades per window) + AccountRegistry events for
    total registered traders + daily backfill for time series.
    """
    import datetime
    now = time.time()
    if _USERS_CACHE["data"] and now - _USERS_CACHE["ts"] < 60:
        return _USERS_CACHE["data"]
    out = {"ok": True, "errors": []}

    # 1) Total RISEx accounts ever registered (from PerpsManager AccountRegistry events)
    with _IDX_LOCK:
        out["total_traders"] = len(_INDEX.get("accounts") or set())
        out["active_with_position"] = _INDEX["active_accounts"]
        out["phase"] = _INDEX["phase"]

    # 2) Active traders per window (from SQLite — fast, indexed queries)
    out["active_1d"] = db_count_with("trades_1d", 0)
    out["active_7d"] = db_count_with("trades_7d", 0)
    out["active_30d"] = db_count_with("trades", 0)

    # 3) Time series (daily) — from rise_backfill.json if accts data is present
    series = []
    try:
        bf = load_backfill()
        daily = bf.get("daily") or {}
        days_sorted = sorted(daily.keys())
        cum_seen = set()
        for date in days_sorted:
            rec = daily[date]
            accts_today = set(rec.get("accts") or [])
            new_today = len(accts_today - cum_seen)
            cum_seen |= accts_today
            series.append({
                "date": date,
                "active": len(accts_today),       # unique RISEx traders that day
                "new":    new_today,              # first-time RISEx traders that day
                "cum":    len(cum_seen),          # cumulative unique traders ever
            })
    except Exception as e:
        out["errors"].append(f"series: {e}")

    out["series"] = series
    # Roll-ups from series
    if series:
        today_rec = series[-1] if series else None
        out["new_today"] = today_rec.get("new", 0) if today_rec else 0
        out["new_7d"]    = sum(s.get("new", 0) for s in series[-7:])
        out["new_30d"]   = sum(s.get("new", 0) for s in series[-30:])
        out["active_today_series"] = today_rec.get("active", 0) if today_rec else 0
    else:
        out["new_today"] = 0
        out["new_7d"] = 0
        out["new_30d"] = 0
        out["active_today_series"] = 0

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


def _account_metrics(account, now_s, max_pages=None, max_seconds=None):
    """Para cada cuenta, calcula en una sola pasada:
       - VOLUMEN realizado por ventana (sum price*size)
       - TWAP OI por ventana (time-weighted)
       - REALIZED PnL acumulado (suma de realized_pnl de cada trade en 30d)
       - Lista de eventos relevantes (trades grandes + liquidaciones) para el feed global
    """
    out = {"trades": 0, "last_refresh": 0,
           "1d": 0.0, "7d": 0.0, "30d": 0.0, CUSTOM_LABEL: 0.0,
           "oi_1d": 0.0, "oi_7d": 0.0, "oi_30d": 0.0,
           "oi_" + CUSTOM_LABEL: 0.0,
           "realized_pnl_30d": 0.0, "fees_30d": 0.0, "n_liquidations": 0,
           # nuevos: PnL/fees/trades por ventana corta + wins/losses + drawdown
           "pnl_1d": 0.0, "pnl_7d": 0.0,
           "fees_1d": 0.0, "fees_7d": 0.0,
           "trades_1d": 0, "trades_7d": 0,
           "wins_30d": 0, "losses_30d": 0,
           "max_dd_30d": 0.0,
           "equity_curve": [],  # [(ts, cumulative_pnl), ...] downsampled
           "feed_entries": []}
    windows = [("1d", 86400), ("7d", 7 * 86400),
               ("30d", 30 * 86400), (CUSTOM_LABEL, max(1, now_s - CUSTOM_SINCE_TS))]
    cutoffs = {k: now_s - d for k, d in windows}
    # cutoff mas antiguo para decidir cuando parar de paginar
    earliest_cutoff = min(cutoffs.values())

    events = []
    pnl_events = []  # (ts, realized_pnl) en 30d para equity curve + drawdown
    page = 1
    feed_cutoff = now_s - FEED_WINDOW_S
    # Para contar ORDENES (no fills): RISEx ejecuta cada orden en muchos fills.
    # Un trader manual que abre 1 posición genera 5-100 fills, pero es 1 orden real.
    # Agrupamos por order_id para contar correctamente y para wins/losses.
    orders_30d = {}  # order_id -> {"ts", "realized_pnl_sum", "is_liq_any"}
    orders_7d  = set()
    orders_1d  = set()

    # === INCREMENTAL SCAN with BACKFILL + PARALLEL PAGE FETCHING ===
    # We fetch pages in batches of 8 in parallel. Each batch then gets processed
    # in order. 6-8x throughput vs sequential. Critical for ultra-active wallets.
    max_known_ts = db_max_trade_ts(account)
    min_known_ts = db_min_trade_ts(account)
    have_full_window = (min_known_ts > 0 and min_known_ts <= (now_s - 30*86400))
    trade_rows_to_insert = []
    deadline = time.time() + (max_seconds if max_seconds is not None else VOL_PER_ACCOUNT_TIMEOUT)
    pages_cap = max_pages if max_pages is not None else VOL_MAX_PAGES
    PAGE_BATCH = 8   # pages to fetch in parallel per round

    def _fetch_page(p):
        try:
            return (p, fetch_json(
                f"{RISEX}/v1/trade-history?account={account}&limit=1000&page={p}",
                timeout=10).get("data", {}))
        except Exception:
            return (p, None)

    def _process_trade(t):
        """Process one fill: append to events, queue for DB insert, update aggregates."""
        nonlocal page  # not strictly needed, but flagged
        try:
            ts = int(t.get("time", 0)) / 1e9
        except Exception:
            return None
        side = str(t.get("side")).upper()
        sgn = 1 if side in ("BUY", "0", "LONG") else -1
        size = f(t.get("size")) * sgn
        price = f(t.get("price"))
        mid = str(t.get("market_id"))
        events.append((ts, mid, size, price))
        bd = t.get("blockchain_data", {}) or {}
        tx_hash = bd.get("tx_hash") or t.get("id", "")[:66]
        log_idx = int(bd.get("log_index", 0) or 0)
        trade_rows_to_insert.append((
            tx_hash, log_idx, account.lower(), int(mid), int(ts),
            "Buy" if side == "BUY" else "Sell",
            "Long" if str(t.get("position_side")).upper() == "BUY" else "Short",
            str(t.get("liquidity_indicator") or "").upper(),
            abs(size), price, f(t.get("fee")), f(t.get("realized_pnl")),
            t.get("order_id") or "", int(1 if t.get("is_liquidation") else 0)
        ))
        if ts >= earliest_cutoff:
            notional = abs(size) * price
            rp = f(t.get("realized_pnl"))
            fee = f(t.get("fee"))
            oid = t.get("order_id") or t.get("id")
            if ts >= cutoffs["30d"]:
                out["realized_pnl_30d"] += rp
                out["fees_30d"] += fee
                if bool(t.get("is_liquidation")):
                    out["n_liquidations"] += 1
                if rp != 0:
                    pnl_events.append((ts, rp))
                if oid:
                    rec = orders_30d.setdefault(oid, {"ts": ts, "pnl": 0.0, "is_liq": False})
                    rec["pnl"] += rp
                    if bool(t.get("is_liquidation")):
                        rec["is_liq"] = True
            if ts >= cutoffs["7d"]:
                out["pnl_7d"] += rp; out["fees_7d"] += fee
                if oid: orders_7d.add(oid)
            if ts >= cutoffs["1d"]:
                out["pnl_1d"] += rp; out["fees_1d"] += fee
                if oid: orders_1d.add(oid)
            for k in ("1d", "7d", "30d", CUSTOM_LABEL):
                if ts >= cutoffs[k]:
                    out[k] += notional
            if ts >= feed_cutoff and (notional >= LARGE_TRADE_USD or t.get("is_liquidation")):
                out["feed_entries"].append({
                    "ts": int(ts), "account": account, "market_id": mid,
                    "side": "Buy" if side == "BUY" else "Sell",
                    "position_side": "Long" if str(t.get("position_side")).upper() == "BUY" else "Short",
                    "price": price, "size": abs(size), "notional": notional,
                    "realized_pnl": rp, "fee": fee,
                    "role": str(t.get("liquidity_indicator") or "").upper(),
                    "is_liq": bool(t.get("is_liquidation")),
                })
        return ts

    should_stop = False
    while page <= pages_cap and not should_stop:
        if time.time() > deadline:
            out["timed_out"] = True
            break
        batch_pages = list(range(page, min(page + PAGE_BATCH, pages_cap + 1)))
        try:
            with ThreadPoolExecutor(max_workers=PAGE_BATCH) as ex:
                results = list(ex.map(_fetch_page, batch_pages))
        except Exception:
            break
        results.sort(key=lambda x: x[0])
        for p_num, d in results:
            if d is None:
                continue
            trades = d.get("trades") or []
            if not trades:
                should_stop = True
                break
            oldest_in_page = None
            for t in trades:
                ts = _process_trade(t)
                if ts is not None and (oldest_in_page is None or ts < oldest_in_page):
                    oldest_in_page = ts
            if not d.get("has_next_page"):
                out["last_refresh"] = now_s
                should_stop = True
                break
            if oldest_in_page is not None and oldest_in_page < earliest_cutoff:
                out["last_refresh"] = now_s
                should_stop = True
                break
            if (oldest_in_page is not None and max_known_ts > 0
                    and oldest_in_page <= max_known_ts and have_full_window):
                out["last_refresh"] = now_s
                should_stop = True
                break
        page = batch_pages[-1] + 1 if batch_pages else page + 1

    # If we exhausted pages_cap without stopping, mark as fully scanned
    if not should_stop and page > pages_cap:
        out["last_refresh"] = now_s

    # Bulk insert all new fills into trades table (persists for future incremental scans)
    if trade_rows_to_insert:
        db_save_trades_bulk(account, trade_rows_to_insert)

    # === Compute metrics from DB (source of truth) ===
    # The incremental scan only fetches NEW fills. So vol/trades from this call's
    # events would be wrong (only the few new fills). Query the persistent trades
    # table for the correct totals over each window.
    for label, dur in windows:
        cutoff_ts = now_s - dur
        m = db_account_window_metrics(account, cutoff_ts, now_s)
        out[label] = m["vol"]
        if label == "30d":
            out["trades"] = m["orders"]
            out["realized_pnl_30d"] = m["pnl"]
            out["fees_30d"] = m["fees"]
            out["wins_30d"] = m["wins"]
            out["losses_30d"] = m["losses"]
            out["n_liquidations"] = m["n_liq"]
        elif label == "7d":
            out["trades_7d"] = m["orders"]
            out["pnl_7d"] = m["pnl"]
            out["fees_7d"] = m["fees"]
        elif label == "1d":
            out["trades_1d"] = m["orders"]
            out["pnl_1d"] = m["pnl"]
            out["fees_1d"] = m["fees"]

    if not events:
        return out
    events.sort(key=lambda e: e[0])

    # ---- Calibration: fetch CURRENT positions snapshot so TWAP isn't poisoned by
    #      phantom shorts created when trade-history pagination misses an opening trade.
    #      Also use current mark prices for the trailing segment of the integral.
    cur_pos = {}   # mid -> signed size (positive long, negative short)
    cur_mark = {}  # mid -> current mark price
    try:
        pd = fetch_json(f"{RISEX}/v1/positions?account={urllib.parse.quote(account)}",
                          timeout=6).get("data", {})
        for p in pd.get("positions", []) or []:
            mid = str(p.get("market_id"))
            sz = f(p.get("size")) / WAD
            longp = str(p.get("side")).upper() in ("BUY", "0", "LONG") or sz >= 0
            cur_pos[mid] = abs(sz) if longp else -abs(sz)
            cur_mark[mid] = f(p.get("mark_price")) / WAD
    except Exception:
        pass
    # Backfill mark prices from markets endpoint if positions API missed them
    try:
        for m in fetch_json(f"{RISEX}/v1/markets", timeout=6)["data"]["markets"]:
            mid = str(m.get("market_id"))
            if mid not in cur_mark or not cur_mark.get(mid):
                cur_mark[mid] = f(m.get("mark_price"))
    except Exception:
        pass

    # TWAP OI por ventana — calibrado contra current positions
    for label, dur in windows:
        start = now_s - dur
        if dur <= 0:
            continue
        # Sum of signed sizes from IN-WINDOW trades per market
        delta_window = {}
        last_px = {}
        for ts, mid, size, price in events:
            if ts < start:
                last_px[mid] = price  # latest pre-window price (proxy for mark at window start)
            else:
                delta_window[mid] = delta_window.get(mid, 0.0) + size
                last_px[mid] = price
        # Initial position at window start = current_pos - in_window_delta
        # Falls back to event-replay if current snapshot is missing (better than nothing)
        if cur_pos:
            pos = {}
            all_mids = set(list(cur_pos.keys()) + list(delta_window.keys()) + list(last_px.keys()))
            for m in all_mids:
                pos[m] = cur_pos.get(m, 0.0) - delta_window.get(m, 0.0)
        else:
            pos = {}
            for ts, mid, size, price in events:
                if ts < start:
                    pos[mid] = pos.get(mid, 0.0) + size

        # Iterate through in-window events, accumulating time-weighted notional
        in_win = [(ts, mid, size, price) for ts, mid, size, price in events if ts >= start]
        prev_t = start
        sum_notional_dt = 0.0
        for ts, mid, size, price in in_win:
            dt = ts - prev_t
            if dt > 0:
                notional_now = sum(abs(pos.get(m, 0.0)) * last_px.get(m, cur_mark.get(m, 0.0))
                                    for m in set(list(pos.keys()) + list(last_px.keys())))
                sum_notional_dt += notional_now * dt
            pos[mid] = pos.get(mid, 0.0) + size
            last_px[mid] = price
            prev_t = ts
        # Final segment to now — use CURRENT mark prices (more accurate than stale last_px)
        dt = now_s - prev_t
        if dt > 0:
            notional_now = sum(abs(pos.get(m, 0.0)) * cur_mark.get(m, last_px.get(m, 0.0))
                                for m in set(list(pos.keys()) + list(cur_mark.keys())))
            sum_notional_dt += notional_now * dt
        out["oi_" + label] = sum_notional_dt / dur

    # Equity curve (downsampled) + max drawdown a partir de pnl_events (30d)
    if pnl_events:
        pnl_events.sort(key=lambda e: e[0])
        running = 0.0; peak = 0.0; max_dd = 0.0
        curve = []
        N = len(pnl_events)
        # downsample para no inflar el cache: ~120 puntos
        step = max(1, N // 120)
        for i, (ts, rp) in enumerate(pnl_events):
            running += rp
            if running > peak: peak = running
            dd = peak - running
            if dd > max_dd: max_dd = dd
            if i % step == 0 or i == N - 1:
                curve.append([int(ts), round(running, 2)])
        out["max_dd_30d"] = round(max_dd, 2)
        out["equity_curve"] = curve

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
            with ThreadPoolExecutor(max_workers=10) as ex:
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
                    # Persist to SQLite so data survives restarts and ranks are SQL queries
                    db_upsert_account(a, v)
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
                            # Liquidations always kept; large trades capped by remaining budget
                            liqs=[e for e in buf if e.get("is_liq")]
                            bigs=[e for e in buf if not e.get("is_liq")]
                            keep=liqs+bigs[:max(0,FEED_MAX-len(liqs))]
                            keep.sort(key=lambda e:e["ts"],reverse=True)
                            _FEED["entries"] = keep
                            _FEED["last_update"] = int(time.time())
            elapsed_total = time.time() - t_start
            print(f"[vol_indexer] PASS DONE: {len(accts)} accts in {elapsed_total:.0f}s, "
                  f"{n_timed_out} timed out", flush=True)

            # publicacion final del pase
            buf.sort(key=lambda e: e["ts"], reverse=True)
            with _FEED_LOCK:
                # Liquidations always kept; large trades capped by remaining budget
                liqs=[e for e in buf if e.get("is_liq")]
                bigs=[e for e in buf if not e.get("is_liq")]
                keep=liqs+bigs[:max(0,FEED_MAX-len(liqs))]
                keep.sort(key=lambda e:e["ts"],reverse=True)
                _FEED["entries"] = keep
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


def get_market_ticks(market_id, interval="1m", limit=240, big_min=50_000):
    """Para overlay CVD + trade markers en el candle chart.
       Pagina /v1/markets/id/{id}/trade-history hasta cubrir la ventana ['from','now'].
       Devuelve: cvd buckets (uno por candle), big_trades para markers.
    """
    INT_SEC = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}
    sec = INT_SEC.get(interval, 60)
    now = int(time.time())
    n_buckets = max(1, int(limit))
    frm = now - sec * n_buckets
    # bucket index → cvd
    buckets = {}
    big_trades = []
    cap_pages = 80  # tope de paginas para no irse en mercados muy activos
    deadline = time.time() + 6.0
    page = 1
    seen_min = float("inf")
    try:
        while page <= cap_pages and time.time() < deadline:
            r = fetch_json(
                f"{RISEX}/v1/markets/id/{market_id}/trade-history?limit=1000&page={page}",
                timeout=6)
            trades = ((r or {}).get("data") or {}).get("trades") or []
            if not trades:
                break
            for t in trades:
                try:
                    ts = int(int(t["time"]) / 1_000_000_000)
                except Exception:
                    continue
                if ts < seen_min: seen_min = ts
                if ts < frm:
                    continue
                price = f(t.get("price")); size = f(t.get("size"))
                notional = price * size
                if notional <= 0: continue
                # maker_side SELL → taker BUY → +; maker_side BUY → taker SELL → −
                sign = 1 if str(t.get("maker_side")).upper() == "SELL" else -1
                b = (ts - frm) // sec
                if b < 0 or b >= n_buckets: continue
                buckets[b] = buckets.get(b, 0.0) + sign * notional
                if notional >= big_min:
                    big_trades.append({
                        "t": ts, "price": price, "size": size,
                        "notional": notional, "side": "Buy" if sign > 0 else "Sell"})
            # if we've gone past the window, stop
            if seen_min < frm:
                break
            page += 1
    except Exception:
        pass
    # construir array ordenado por bucket
    cvd_delta = [round(buckets.get(i, 0.0), 2) for i in range(n_buckets)]
    cvd_cum = []
    running = 0.0
    for v in cvd_delta:
        running += v
        cvd_cum.append(round(running, 2))
    # ordenar markers por tiempo y limitar
    big_trades.sort(key=lambda x: x["notional"], reverse=True)
    big_trades = big_trades[:60]
    big_trades.sort(key=lambda x: x["t"])
    return {"ok": True, "market_id": str(market_id), "interval": interval,
            "n_buckets": n_buckets, "from_ts": frm, "bucket_sec": sec,
            "cvd_delta": cvd_delta, "cvd_cum": cvd_cum,
            "big_trades": big_trades, "big_min": big_min}


_SPARKS = {"by_market": {}, "ts": 0}
_SPARKS_LOCK = threading.Lock()

# ============================== Onchain Explorer ==============================
_EXP = {"blocks": [], "txs": [], "last_block": 0, "tps_window": [], "txs_24h": 0,
        "last_update": 0}
_EXP_LOCK = threading.Lock()

# SSE subscribers (list of queues for each connected client)
import queue as _queue_mod
_SSE_SUBS = []
_SSE_LOCK = threading.Lock()


def _sse_broadcast(event_type, payload):
    """Send event to all connected SSE subscribers (non-blocking)."""
    msg = {"type": event_type, "data": payload}
    with _SSE_LOCK:
        dead = []
        for q in _SSE_SUBS:
            try:
                q.put_nowait(msg)
            except Exception:
                dead.append(q)
        for q in dead:
            try: _SSE_SUBS.remove(q)
            except Exception: pass


def _hex_to_int(h):
    if h is None: return 0
    if isinstance(h, str) and h.startswith("0x"):
        try: return int(h, 16)
        except Exception: return 0
    return int(h) if h else 0


def _wei_to_eth(w):
    return _hex_to_int(w) / 1e18


RPC_WS_URL = "wss://rpc.risechain.com/ws"


def _ingest_block(b):
    """Ingest a full block (with transactions) into the explorer buffer."""
    if not b: return
    bn = _hex_to_int(b.get("number"))
    ts = _hex_to_int(b.get("timestamp"))
    gas_used = _hex_to_int(b.get("gasUsed"))
    gas_limit = _hex_to_int(b.get("gasLimit"))
    txs_raw = b.get("transactions") or []
    miner = b.get("miner", "0x0000000000000000000000000000000000000000")
    block_payload = {
        "number": bn, "hash": b.get("hash"), "ts": ts,
        "tx_count": len(txs_raw), "miner": miner,
        "gas_used": gas_used, "gas_limit": gas_limit,
        "gas_pct": (gas_used / gas_limit * 100) if gas_limit else 0,
        "size": _hex_to_int(b.get("size")),
    }
    new_txs = []
    for tx in txs_raw[:30]:
        if isinstance(tx, str): continue
        tx_p = {
            "hash": tx.get("hash"), "block": bn, "ts": ts,
            "from": tx.get("from"), "to": tx.get("to"),
            "value": _wei_to_eth(tx.get("value")),
            "gas_price": _hex_to_int(tx.get("gasPrice")),
            "gas": _hex_to_int(tx.get("gas")),
            "nonce": _hex_to_int(tx.get("nonce")),
            "input_size": len((tx.get("input") or "0x")) // 2,
        }
        new_txs.append(tx_p)
    with _EXP_LOCK:
        # Avoid duplicates: only ingest if new
        if any(blk["number"] == bn for blk in _EXP["blocks"][:3]):
            return
        _EXP["blocks"].insert(0, block_payload)
        if len(_EXP["blocks"]) > 60:
            _EXP["blocks"] = _EXP["blocks"][:60]
        for tx_p in new_txs:
            _EXP["txs"].insert(0, tx_p)
        if len(_EXP["txs"]) > 250:
            _EXP["txs"] = _EXP["txs"][:250]
        _EXP["tps_window"].append((ts, len(txs_raw)))
        cutoff = ts - 60
        _EXP["tps_window"] = [(t, n) for t, n in _EXP["tps_window"] if t >= cutoff]
        _EXP["last_block"] = bn
        _EXP["last_update"] = int(time.time())
        # compute fresh stats snapshot for the event
        if _EXP["tps_window"]:
            ts_pts = list(_EXP["tps_window"])
            w_start = min(t for t, _ in ts_pts)
            w_end = max(t for t, _ in ts_pts)
            dur = max(1, w_end - w_start)
            tps_now = sum(n for _, n in ts_pts) / dur
            tps_peak = max((n for _, n in ts_pts), default=0)
        else:
            tps_now = tps_peak = 0
        blocks_snapshot = list(_EXP["blocks"][:5])
        if len(blocks_snapshot) >= 2:
            diffs = [blocks_snapshot[i]["ts"] - blocks_snapshot[i+1]["ts"]
                     for i in range(len(blocks_snapshot)-1)
                     if blocks_snapshot[i]["ts"] > blocks_snapshot[i+1]["ts"]]
            avg_bt = sum(diffs) / len(diffs) if diffs else 1
        else:
            avg_bt = 1
        buffered_txs = len(_EXP["txs"])
    # Broadcast to all SSE subscribers
    _sse_broadcast("block", {
        "block": block_payload,
        "new_txs": new_txs,
        "stats": {
            "last_block": bn,
            "tps_now": round(tps_now, 2),
            "tps_peak": round(tps_peak, 2),
            "avg_block_time": round(avg_bt, 2),
            "buffered_txs": buffered_txs,
        }
    })


def explorer_indexer_loop():
    """Subscribes to newHeads via WebSocket, fetches full block + txs on each notification.
       Falls back to polling if WSS fails."""
    import json as _json
    while True:
        try:
            try:
                import websocket
            except ImportError:
                # fallback to polling if dep missing
                _explorer_polling_fallback()
                return
            ws = websocket.create_connection(RPC_WS_URL, timeout=10)
            sub_req = _json.dumps({"jsonrpc": "2.0", "id": 1,
                                   "method": "eth_subscribe", "params": ["newHeads"]})
            ws.send(sub_req)
            print("[explorer] subscribed to newHeads via WS", flush=True)
            # ack
            try: ws.recv()
            except Exception: pass
            while True:
                msg = ws.recv()
                d = _json.loads(msg)
                if d.get("method") != "eth_subscription":
                    continue
                head = d.get("params", {}).get("result") or {}
                bn = _hex_to_int(head.get("number"))
                if not bn: continue
                # fetch full block + txs via HTTP RPC (cheaper than overloading WS)
                try:
                    b = rpc("eth_getBlockByNumber", [hex(bn), True])
                    _ingest_block(b)
                except Exception:
                    pass
        except Exception as e:
            print(f"[explorer] WS error: {e}, retrying in 3s", flush=True)
            time.sleep(3)


def _explorer_polling_fallback():
    """Slow fallback when WS not available."""
    while True:
        try:
            tip = _hex_to_int(rpc("eth_blockNumber", []))
            with _EXP_LOCK:
                last_seen = _EXP["last_block"]
            start = max(last_seen + 1, tip - 4) if last_seen > 0 else tip
            for bn in range(start, tip + 1):
                try: _ingest_block(rpc("eth_getBlockByNumber", [hex(bn), True]))
                except Exception: pass
        except Exception:
            pass
        time.sleep(1.0)


def get_explorer_stats():
    with _EXP_LOCK:
        blocks = list(_EXP["blocks"][:5])
        txs_count = len(_EXP["txs"])
        last_block = _EXP["last_block"]
        last_update = _EXP["last_update"]
        tps_pts = list(_EXP["tps_window"])
    # tps = sum of tx in window / window duration
    if tps_pts:
        w_start = min(t for t, _ in tps_pts)
        w_end = max(t for t, _ in tps_pts)
        dur = max(1, w_end - w_start)
        total = sum(n for _, n in tps_pts)
        tps_now = total / dur
        # peak per single block
        tps_peak = max((n / 1.0 for _, n in tps_pts), default=0)  # blocks ~1s on RISE
    else:
        tps_now = tps_peak = 0
    # block time avg
    if len(blocks) >= 2:
        diffs = [blocks[i]["ts"] - blocks[i+1]["ts"] for i in range(len(blocks)-1)
                 if blocks[i]["ts"] > blocks[i+1]["ts"]]
        avg_bt = sum(diffs) / len(diffs) if diffs else 0
    else:
        avg_bt = 0
    return {"ok": True, "last_block": last_block, "last_update": last_update,
            "tps_now": round(tps_now, 2), "tps_peak": round(tps_peak, 2),
            "avg_block_time": round(avg_bt, 2),
            "buffered_txs": txs_count,
            "blocks_indexed": len(_EXP["blocks"])}


def get_explorer_blocks(limit=20):
    with _EXP_LOCK:
        return {"ok": True, "blocks": list(_EXP["blocks"][:limit]),
                "last_update": _EXP["last_update"]}


def get_explorer_txs(limit=30):
    with _EXP_LOCK:
        return {"ok": True, "txs": list(_EXP["txs"][:limit]),
                "last_update": _EXP["last_update"]}


def get_explorer_tx_detail(tx_hash):
    """Fetches a single tx with receipt."""
    try:
        tx = rpc("eth_getTransactionByHash", [tx_hash])
        if not tx:
            return {"ok": False, "error": "tx not found"}
        receipt = None
        try:
            receipt = rpc("eth_getTransactionReceipt", [tx_hash])
        except Exception:
            pass
        bn = _hex_to_int(tx.get("blockNumber"))
        block_ts = 0
        # Try to get block timestamp from buffer first
        with _EXP_LOCK:
            for b in _EXP["blocks"]:
                if b["number"] == bn:
                    block_ts = b["ts"]; break
        if not block_ts and bn:
            try:
                blk = rpc("eth_getBlockByNumber", [hex(bn), False])
                if blk: block_ts = _hex_to_int(blk.get("timestamp"))
            except Exception:
                pass
        gas_used = _hex_to_int(receipt.get("gasUsed")) if receipt else None
        status = _hex_to_int(receipt.get("status")) if receipt else None
        logs = receipt.get("logs") if receipt else []
        return {"ok": True, "tx": {
            "hash": tx.get("hash"),
            "block": bn,
            "block_hash": tx.get("blockHash"),
            "ts": block_ts,
            "from": tx.get("from"),
            "to": tx.get("to"),
            "value": _wei_to_eth(tx.get("value")),
            "value_wei_hex": tx.get("value"),
            "gas": _hex_to_int(tx.get("gas")),
            "gas_used": gas_used,
            "gas_price": _hex_to_int(tx.get("gasPrice")),
            "nonce": _hex_to_int(tx.get("nonce")),
            "input": tx.get("input", "0x"),
            "input_size": len((tx.get("input") or "0x")) // 2,
            "tx_index": _hex_to_int(tx.get("transactionIndex")),
            "type": _hex_to_int(tx.get("type")),
            "chain_id": _hex_to_int(tx.get("chainId")),
            "status": status,
            "contract_address": receipt.get("contractAddress") if receipt else None,
            "logs_count": len(logs or []),
            "logs": [{
                "address": l.get("address"),
                "topics": l.get("topics", []),
                "data": l.get("data", "0x"),
                "log_index": _hex_to_int(l.get("logIndex")),
            } for l in (logs or [])[:20]],
        }}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def get_explorer_block_detail(n):
    with _EXP_LOCK:
        for b in _EXP["blocks"]:
            if b["number"] == n:
                txs = [t for t in _EXP["txs"] if t["block"] == n]
                return {"ok": True, "block": b, "txs": txs}
    # fallback: fetch from RPC
    try:
        b = rpc("eth_getBlockByNumber", [hex(n), True])
        if not b: return {"ok": False}
        ts = _hex_to_int(b.get("timestamp"))
        gas_used = _hex_to_int(b.get("gasUsed"))
        gas_limit = _hex_to_int(b.get("gasLimit"))
        txs_raw = b.get("transactions") or []
        out_b = {"number": n, "hash": b.get("hash"), "ts": ts,
                 "tx_count": len(txs_raw),
                 "miner": b.get("miner"), "gas_used": gas_used,
                 "gas_limit": gas_limit,
                 "gas_pct": (gas_used / gas_limit * 100) if gas_limit else 0,
                 "size": _hex_to_int(b.get("size"))}
        out_txs = [{"hash": tx.get("hash"), "block": n, "ts": ts,
                    "from": tx.get("from"), "to": tx.get("to"),
                    "value": _wei_to_eth(tx.get("value")),
                    "gas": _hex_to_int(tx.get("gas")),
                    "gas_price": _hex_to_int(tx.get("gasPrice")),
                    "nonce": _hex_to_int(tx.get("nonce")),
                    "input_size": len((tx.get("input") or "0x")) // 2,
                    } for tx in txs_raw]
        return {"ok": True, "block": out_b, "txs": out_txs}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ============================== trending / view counters ==============================
_VIEWS = {"wallet": {}, "market": {}}  # {addr/id: [(ts, ...)]} - timestamps of views in last 1h
_VIEWS_LOCK = threading.Lock()


def track_view(kind, key):
    """Tracks a view (wallet or market). Auto-prunes entries older than 1h."""
    now = int(time.time())
    cutoff = now - 3600
    with _VIEWS_LOCK:
        d = _VIEWS.setdefault(kind, {})
        lst = d.setdefault(str(key).lower(), [])
        lst.append(now)
        # prune old
        d[str(key).lower()] = [t for t in lst if t >= cutoff]


def get_trending(kind="wallet", limit=10, window_min=60):
    cutoff = int(time.time()) - window_min * 60
    with _VIEWS_LOCK:
        items = []
        d = _VIEWS.get(kind, {})
        for key, ts_list in d.items():
            n = sum(1 for t in ts_list if t >= cutoff)
            if n > 0:
                items.append({"key": key, "views": n})
        # active viewers (last 60 sec)
        active_cutoff = int(time.time()) - 60
        n_active = sum(1 for ts_list in d.values() for t in ts_list if t >= active_cutoff)
    items.sort(key=lambda x: x["views"], reverse=True)
    return {"items": items[:limit], "active_viewers": n_active}


def get_viewing_now(kind, key):
    """How many distinct sessions viewed this key in last 60s (proxy for 'right now')."""
    cutoff = int(time.time()) - 60
    with _VIEWS_LOCK:
        lst = _VIEWS.get(kind, {}).get(str(key).lower(), [])
        n = sum(1 for t in lst if t >= cutoff)
    return n


# ============================== suggested / random whale ==============================
def get_random_whale():
    """Random account from top 100 by OI."""
    import random
    with _IDX_LOCK:
        top = list(_INDEX["account_oi_ranking"])[:100]
    if not top:
        return {"ok": False}
    pick = random.choice(top)
    return {"ok": True, "account": pick["account"], "total_oi": pick.get("total_oi", 0),
            "positions": pick.get("positions", 0)}


def get_suggested_wallets(account, limit=3):
    """Find similar wallets based on volume range (±30%)."""
    a = account.lower()
    with _VOL_LOCK:
        target = _VOL["by_account"].get(account) or _VOL["by_account"].get(a)
        items = list(_VOL["by_account"].items())
    if not target:
        return {"ok": True, "suggestions": []}
    tvol = target.get("30d", 0)
    if tvol <= 0:
        return {"ok": True, "suggestions": []}
    lo, hi = tvol * 0.5, tvol * 2.0
    sims = []
    for addr, v in items:
        if addr.lower() == a:
            continue
        vol = v.get("30d", 0)
        if lo <= vol <= hi:
            score = 1 - abs(vol - tvol) / tvol
            sims.append({"account": addr, "volume": vol,
                         "pnl_30d": v.get("realized_pnl_30d", 0),
                         "trades": v.get("trades", 0), "score": score})
    sims.sort(key=lambda x: x["score"], reverse=True)
    return {"ok": True, "suggestions": sims[:limit]}


def get_related_markets(market_id, limit=3):
    """Related markets by OI similarity."""
    try:
        ms = fetch_json(f"{RISEX}/v1/markets")["data"]["markets"]
        target = None
        for m in ms:
            if str(m.get("market_id")) == str(market_id):
                target = m; break
        if not target:
            return {"ok": True, "related": []}
        toi = f(target.get("open_interest")) * f(target.get("mark_price"))
        if toi <= 0:
            toi = 1
        rel = []
        for m in ms:
            if str(m.get("market_id")) == str(market_id):
                continue
            oi = f(m.get("open_interest")) * f(m.get("mark_price"))
            if oi <= 0:
                continue
            score = 1 - min(abs(oi - toi) / toi, 1)
            rel.append({"market_id": m.get("market_id"), "name": m.get("display_name"),
                        "last_price": f(m.get("last_price")),
                        "change_24h": f(m.get("change_24h")),
                        "oi_usd": oi, "score": score})
        rel.sort(key=lambda x: x["score"], reverse=True)
        return {"ok": True, "related": rel[:limit]}
    except Exception:
        return {"ok": True, "related": []}


# ============================== daily story ==============================
def get_daily_story():
    """Auto-curated highlights, returns list of headlines for rotation."""
    stories = []
    try:
        ms = fetch_json(f"{RISEX}/v1/markets")["data"]["markets"]
        if ms:
            top = max(ms, key=lambda m: f(m.get("quote_volume_24h")))
            stories.append({"icon": "🚀", "html": f"<b>{top.get('display_name')}</b> leads with <b>{_fmt_usd_compact(f(top.get('quote_volume_24h')))}</b> volume in 24h"})
            mover = max(ms, key=lambda m: abs(_chg_pct(m)))
            chg = _chg_pct(mover)
            cls = "pos" if chg >= 0 else "neg"
            stories.append({"icon": "📊", "html": f"<b>{mover.get('display_name')}</b> is the biggest mover at <span class='{cls}'>{('+' if chg>=0 else '')}{chg:.2f}%</span>"})
            extreme = max(ms, key=lambda m: abs(f(m.get("funding_rate_8h"))))
            apr = f(extreme.get("funding_rate_8h")) * 3 * 365 * 100
            cls = "pos" if apr >= 0 else "neg"
            stories.append({"icon": "💱", "html": f"Hottest funding on <b>{extreme.get('display_name')}</b>: <span class='{cls}'>{('+' if apr>=0 else '')}{apr:.1f}%</span> APR"})
    except Exception:
        pass
    try:
        with _IDX_LOCK:
            tot_oi = sum(r.get("total_oi", 0) for r in _INDEX["account_oi_ranking"])
            n_active = _INDEX.get("active_accounts", 0)
        if tot_oi > 0:
            stories.append({"icon": "💼", "html": f"<b>{_fmt_usd_compact(tot_oi)}</b> total OI across <b>{n_active}</b> active accounts right now"})
    except Exception:
        pass
    try:
        with _FEED_LOCK:
            big_loss = max((e for e in _FEED["entries"] if e.get("is_liq")), key=lambda e: abs(e.get("realized_pnl", 0)), default=None)
        if big_loss:
            stories.append({"icon": "💀", "html": f"Biggest wipe in 24h: <span class='neg'>−{_fmt_usd_compact(abs(big_loss.get('realized_pnl', 0)))}</span> on <b>{big_loss.get('market', '?')}</b>"})
    except Exception:
        pass
    if not stories:
        stories = [{"icon": "🌿", "html": "Live perp analytics for RISE chain · refreshed every few seconds"}]
    return {"ok": True, "stories": stories}


def get_market_sparks():
    """Sparklines de 24h (close por hora) para cada mercado. Cached 5min, fetch en paralelo."""
    with _SPARKS_LOCK:
        if time.time() - _SPARKS["ts"] < 300 and _SPARKS["by_market"]:
            return {"ok": True, "by_market": _SPARKS["by_market"], "ts": int(_SPARKS["ts"])}
    int_1h = 3600 * 1_000_000_000
    now_ns = int(time.time() * 1_000_000_000)
    from_ns = now_ns - 24 * 3600 * 1_000_000_000
    try:
        markets = fetch_json(f"{RISEX}/v1/markets")["data"]["markets"]
    except Exception:
        return {"ok": False, "by_market": {}}
    def one(m):
        mid = str(m.get("market_id"))
        try:
            r = fetch_json(
                f"{RISEX}/v1/markets/id/{mid}/trading-view-data"
                f"?interval={int_1h}&from={from_ns}&to={now_ns}", timeout=4)
            cs = ((r or {}).get("data") or {}).get("data") or []
            return mid, [f(c.get("close")) for c in cs if c.get("close") is not None]
        except Exception:
            return mid, []
    out = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        for mid, vals in ex.map(one, markets):
            if vals:
                out[mid] = vals
    with _SPARKS_LOCK:
        _SPARKS["by_market"] = out; _SPARKS["ts"] = time.time()
    return {"ok": True, "by_market": out, "ts": int(time.time())}


def get_candles(market_id, interval="1m", limit=240):
    """Lee OHLCV desde /v1/markets/id/{id}/trading-view-data.
       interval ∈ {1m,5m,15m,1h,4h,1d}. limit = max candles a devolver (=ventana atrás).
    """
    INT_SEC = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}
    sec = INT_SEC.get(interval, 60)
    now = int(time.time())
    frm = now - sec * max(1, int(limit))
    int_ns = sec * 1_000_000_000
    frm_ns = frm * 1_000_000_000
    to_ns = now * 1_000_000_000
    try:
        r = fetch_json(
            f"{RISEX}/v1/markets/id/{market_id}/trading-view-data"
            f"?interval={int_ns}&from={frm_ns}&to={to_ns}", timeout=8)
        raw = r.get("data", {}).get("data", []) if r else []
        candles = []
        for c in raw:
            try:
                candles.append({
                    "t": int(int(c["time"]) / 1_000_000_000),
                    "o": f(c.get("open")), "h": f(c.get("high")),
                    "l": f(c.get("low")), "c": f(c.get("close")),
                    "v": f(c.get("volume")),
                })
            except Exception:
                continue
        # nombre del mercado para el header
        name = None
        try:
            for m in fetch_json(f"{RISEX}/v1/markets")["data"]["markets"]:
                if str(m.get("market_id")) == str(market_id):
                    name = m.get("display_name"); break
        except Exception:
            pass
        return {"ok": True, "market_id": str(market_id), "name": name,
                "interval": interval, "count": len(candles),
                "candles": candles}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _fmt_usd_compact(n):
    a = abs(n)
    if a >= 1_000_000_000: return f"${n/1_000_000_000:.2f}B"
    if a >= 1_000_000: return f"${n/1_000_000:.2f}M"
    if a >= 1_000: return f"${n/1_000:.1f}k"
    return f"${n:.2f}"


def _svg_wallet_card(account):
    """Genera un SVG 1200x630 con stats clave de la wallet, para previsualizaciones OG."""
    acc = account.lower()
    short = acc[:6] + "…" + acc[-4:]
    pnl = 0.0; vol = 0.0; trades = 0; smart = False; win_rate = None
    with _VOL_LOCK:
        v = _VOL["by_account"].get(account) or _VOL["by_account"].get(acc) or _VOL["by_account"].get(account.lower())
    if not v:
        # intenta match case-insensitive
        with _VOL_LOCK:
            for a, val in _VOL["by_account"].items():
                if a.lower() == acc:
                    v = val; break
    if v:
        pnl = v.get("realized_pnl_30d", 0.0); vol = v.get("30d", 0.0)
        trades = v.get("trades", 0); smart = _is_smart_money(v)
        w = v.get("wins_30d", 0); l = v.get("losses_30d", 0)
        if w + l > 0: win_rate = w / (w + l) * 100
    pnl_color = "#36d39c" if pnl >= 0 else "#ff5466"
    pnl_sign = "+" if pnl >= 0 else "−"
    pnl_str = pnl_sign + _fmt_usd_compact(abs(pnl)).lstrip("$") if pnl != 0 else "0"
    wr_str = (f"{win_rate:.1f}%" if win_rate is not None else "—")
    smart_chip = ('<g transform="translate(880,80)">'
                  '<rect width="220" height="44" rx="6" fill="#97FCE4" fill-opacity="0.14" stroke="#97FCE4" stroke-width="1.5"/>'
                  '<text x="110" y="29" text-anchor="middle" font-family="-apple-system,Inter,sans-serif" '
                  'font-size="18" font-weight="700" fill="#97FCE4" letter-spacing="2">SMART MONEY</text></g>') if smart else ""

    return (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1200 630" width="1200" height="630">'
        '<defs>'
        '<radialGradient id="bg" cx="100%" cy="0%" r="80%"><stop offset="0%" stop-color="#0d1822"/><stop offset="100%" stop-color="#070a0d"/></radialGradient>'
        '<linearGradient id="line" x1="0%" y1="0%" x2="100%" y2="0%"><stop offset="0%" stop-color="#97FCE4" stop-opacity="0"/><stop offset="50%" stop-color="#97FCE4" stop-opacity="0.6"/><stop offset="100%" stop-color="#97FCE4" stop-opacity="0"/></linearGradient>'
        '</defs>'
        '<rect width="1200" height="630" fill="url(#bg)"/>'
        '<rect x="0" y="0" width="1200" height="2" fill="url(#line)"/>'
        # Brand
        '<g transform="translate(80,72)">'
        '<text font-family="-apple-system,Inter,sans-serif" font-size="22" font-weight="800" fill="#97FCE4" letter-spacing="3">RISEX · STATS</text>'
        '</g>'
        + smart_chip +
        # Address
        f'<text x="80" y="195" font-family="ui-monospace,Menlo,monospace" font-size="44" font-weight="700" fill="#e9edf2">{short}</text>'
        '<text x="80" y="232" font-family="-apple-system,Inter,sans-serif" font-size="15" fill="#6b7785" letter-spacing="2">WALLET · 30 DAYS</text>'
        # KPIs row
        '<g transform="translate(80,290)">'
        '<text font-family="-apple-system,Inter,sans-serif" font-size="14" font-weight="700" fill="#6b7785" letter-spacing="2">REALIZED PNL</text>'
        f'<text y="68" font-family="-apple-system,Inter,sans-serif" font-size="72" font-weight="700" fill="{pnl_color}">{pnl_str}</text>'
        '</g>'
        '<g transform="translate(80,440)">'
        '<text font-family="-apple-system,Inter,sans-serif" font-size="14" font-weight="700" fill="#6b7785" letter-spacing="2">VOLUME</text>'
        f'<text y="44" font-family="-apple-system,Inter,sans-serif" font-size="36" font-weight="600" fill="#e9edf2">{_fmt_usd_compact(vol)}</text>'
        '</g>'
        '<g transform="translate(440,440)">'
        '<text font-family="-apple-system,Inter,sans-serif" font-size="14" font-weight="700" fill="#6b7785" letter-spacing="2">WIN RATE</text>'
        f'<text y="44" font-family="-apple-system,Inter,sans-serif" font-size="36" font-weight="600" fill="#e9edf2">{wr_str}</text>'
        '</g>'
        '<g transform="translate(720,440)">'
        '<text font-family="-apple-system,Inter,sans-serif" font-size="14" font-weight="700" fill="#6b7785" letter-spacing="2">TRADES</text>'
        f'<text y="44" font-family="-apple-system,Inter,sans-serif" font-size="36" font-weight="600" fill="#e9edf2">{trades:,}</text>'
        '</g>'
        '<text x="80" y="585" font-family="-apple-system,Inter,sans-serif" font-size="14" fill="#6b7785">risexscan.io · live perp analytics on RISE chain</text>'
        '</svg>')


def _svg_home_card():
    """OG card 1200x630 para la home (/og/home.svg)."""
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1200 630" width="1200" height="630">'
        '<defs>'
        '<radialGradient id="bgh" cx="100%" cy="0%" r="80%"><stop offset="0%" stop-color="#0d1822"/><stop offset="100%" stop-color="#070a0d"/></radialGradient>'
        '<linearGradient id="lh" x1="0%" y1="0%" x2="100%" y2="0%"><stop offset="0%" stop-color="#97FCE4" stop-opacity="0"/><stop offset="50%" stop-color="#97FCE4" stop-opacity="0.6"/><stop offset="100%" stop-color="#97FCE4" stop-opacity="0"/></linearGradient>'
        '<linearGradient id="th" x1="0%" y1="0%" x2="0%" y2="100%"><stop offset="0%" stop-color="#00ffd4"/><stop offset="100%" stop-color="#0CD8B7"/></linearGradient>'
        '</defs>'
        '<rect width="1200" height="630" fill="url(#bgh)"/>'
        '<rect x="0" y="0" width="1200" height="2" fill="url(#lh)"/>'
        # Brand
        '<g transform="translate(80,76)">'
        '<rect x="0" y="0" width="56" height="56" rx="13" fill="#0d1418" stroke="#00ffd4" stroke-width="1.5"/>'
        '<g transform="translate(15,14)"><path d="M21.5 0H0.07V6.13H21.5C23.19 6.13 24.56 7.50 24.56 9.19V12.25H9.42C4.25 12.25 0.07 16.43 0.07 21.6V36.78H6.18V22.39L21.62 36.78H30.59L10.89 18.40H24.56V12.27H30.59V9.19C30.59 4.16 26.49 0 21.5 0Z" fill="#00ffd4"/></g>'
        '<text x="72" y="22" font-family="-apple-system,Inter,sans-serif" font-size="14" font-weight="800" fill="#00ffd4" letter-spacing="3">RISEX</text>'
        '<text x="72" y="46" font-family="-apple-system,Inter,sans-serif" font-size="22" font-weight="800" fill="#fff" letter-spacing="0">STATS</text>'
        '</g>'
        # Title
        '<text x="80" y="280" font-family="-apple-system,Inter,sans-serif" font-size="76" font-weight="800" fill="#ffffff" letter-spacing="-3">Live perp analytics</text>'
        '<text x="80" y="350" font-family="-apple-system,Inter,sans-serif" font-size="76" font-weight="800" fill="url(#th)" letter-spacing="-3">for RISE chain</text>'
        # Tags
        '<g transform="translate(80,420)" font-family="-apple-system,Inter,sans-serif" font-size="16" font-weight="600">'
        '<rect x="0" y="0" rx="20" width="170" height="36" fill="rgba(0,255,212,0.10)" stroke="rgba(0,255,212,0.30)" stroke-width="1"/><text x="22" y="23" fill="#00ffd4">📊 Real-time markets</text>'
        '<rect x="186" y="0" rx="20" width="180" height="36" fill="rgba(0,255,212,0.10)" stroke="rgba(0,255,212,0.30)" stroke-width="1"/><text x="208" y="23" fill="#00ffd4">🏆 Trader leaderboards</text>'
        '<rect x="382" y="0" rx="20" width="180" height="36" fill="rgba(0,255,212,0.10)" stroke="rgba(0,255,212,0.30)" stroke-width="1"/><text x="404" y="23" fill="#00ffd4">💎 Smart money tracker</text>'
        '<rect x="578" y="0" rx="20" width="170" height="36" fill="rgba(0,255,212,0.10)" stroke="rgba(0,255,212,0.30)" stroke-width="1"/><text x="600" y="23" fill="#00ffd4">⚡ Live liquidations</text>'
        '</g>'
        # URL
        '<text x="80" y="585" font-family="-apple-system,Inter,sans-serif" font-size="18" font-weight="700" fill="#00ffd4">risexscan.io</text>'
        '<text x="220" y="585" font-family="-apple-system,Inter,sans-serif" font-size="16" fill="#6b7785">· built on public RISE chain data · no accounts · open</text>'
        '</svg>')


def _sitemap_xml():
    """Sitemap.xml dinámico con markets actuales."""
    base = "https://risexscan.io"
    items = [
        ("/", "1.0", "always"),
        ("/about", "0.8", "monthly"),
        ("/methodology", "0.8", "monthly"),
    ]
    # markets dinámicos
    try:
        ms = fetch_json(f"{RISEX}/v1/markets")["data"]["markets"]
        for m in ms[:30]:
            items.append((f"/share/market/{m.get('market_id')}", "0.6", "hourly"))
    except Exception:
        pass
    lines = ['<?xml version="1.0" encoding="UTF-8"?>',
             '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    for path, prio, freq in items:
        lines.append(f"  <url><loc>{base}{path}</loc><priority>{prio}</priority><changefreq>{freq}</changefreq></url>")
    lines.append("</urlset>")
    return "\n".join(lines)


_ROBOTS_TXT = """User-agent: *
Allow: /
Disallow: /api/

Sitemap: https://risexscan.io/sitemap.xml
"""


def _chg_pct(m):
    """change_24h del API es $ absoluto, no %. Compute real percentage."""
    c = f(m.get("change_24h"))
    last = f(m.get("last_price"))
    open_p = last - c
    return (c / open_p * 100) if open_p else 0.0


def _svg_market_card(market_id):
    """SVG 1200x630 con stats clave del mercado."""
    info = {}
    try:
        for m in fetch_json(f"{RISEX}/v1/markets")["data"]["markets"]:
            if str(m.get("market_id")) == str(market_id):
                info = m; break
    except Exception:
        pass
    name = info.get("display_name") or f"MARKET {market_id}"
    last = f(info.get("last_price"))
    chg = _chg_pct(info) if info else 0.0
    vol = f(info.get("quote_volume_24h"))
    oi = f(info.get("open_interest")) * f(info.get("mark_price"))
    fund8h = f(info.get("funding_rate_8h"))
    apr = fund8h * 3 * 365 * 100
    chg_color = "#36d39c" if chg >= 0 else "#ff5466"
    chg_sign = "+" if chg >= 0 else ""
    apr_color = "#36d39c" if apr >= 0 else "#ff5466"
    apr_sign = "+" if apr >= 0 else ""
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1200 630" width="1200" height="630">'
        '<defs>'
        '<radialGradient id="bg2" cx="100%" cy="0%" r="80%"><stop offset="0%" stop-color="#0d1822"/><stop offset="100%" stop-color="#070a0d"/></radialGradient>'
        '<linearGradient id="line2" x1="0%" y1="0%" x2="100%" y2="0%"><stop offset="0%" stop-color="#97FCE4" stop-opacity="0"/><stop offset="50%" stop-color="#97FCE4" stop-opacity="0.6"/><stop offset="100%" stop-color="#97FCE4" stop-opacity="0"/></linearGradient>'
        '</defs>'
        '<rect width="1200" height="630" fill="url(#bg2)"/>'
        '<rect x="0" y="0" width="1200" height="2" fill="url(#line2)"/>'
        '<g transform="translate(80,72)">'
        '<text font-family="-apple-system,Inter,sans-serif" font-size="22" font-weight="800" fill="#97FCE4" letter-spacing="3">RISEX · MARKET</text>'
        '</g>'
        f'<text x="80" y="200" font-family="-apple-system,Inter,sans-serif" font-size="64" font-weight="800" fill="#e9edf2">{name}</text>'
        f'<text x="80" y="280" font-family="ui-monospace,Menlo,monospace" font-size="56" font-weight="700" fill="#97FCE4">${last:,.4g}</text>'
        f'<text x="500" y="280" font-family="-apple-system,Inter,sans-serif" font-size="42" font-weight="700" fill="{chg_color}">{chg_sign}{chg:.2f}%</text>'
        '<g transform="translate(80,400)">'
        '<text font-family="-apple-system,Inter,sans-serif" font-size="14" font-weight="700" fill="#6b7785" letter-spacing="2">24H VOLUME</text>'
        f'<text y="44" font-family="-apple-system,Inter,sans-serif" font-size="36" font-weight="600" fill="#e9edf2">{_fmt_usd_compact(vol)}</text>'
        '</g>'
        '<g transform="translate(440,400)">'
        '<text font-family="-apple-system,Inter,sans-serif" font-size="14" font-weight="700" fill="#6b7785" letter-spacing="2">OPEN INTEREST</text>'
        f'<text y="44" font-family="-apple-system,Inter,sans-serif" font-size="36" font-weight="600" fill="#e9edf2">{_fmt_usd_compact(oi)}</text>'
        '</g>'
        '<g transform="translate(800,400)">'
        '<text font-family="-apple-system,Inter,sans-serif" font-size="14" font-weight="700" fill="#6b7785" letter-spacing="2">FUNDING APR</text>'
        f'<text y="44" font-family="-apple-system,Inter,sans-serif" font-size="36" font-weight="600" fill="{apr_color}">{apr_sign}{apr:.1f}%</text>'
        '</g>'
        '<text x="80" y="585" font-family="-apple-system,Inter,sans-serif" font-size="14" fill="#6b7785">risexscan.io · live perp analytics on RISE chain</text>'
        '</svg>')


def _share_page_html(kind, ident, og_path, title, desc, deeplink):
    """HTML pequeño con meta OG + redirect JS al hash route."""
    esc = lambda s: str(s).replace("&", "&amp;").replace('"', "&quot;").replace("<", "&lt;")
    return (
        '<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">'
        f'<title>{esc(title)}</title>'
        f'<meta name="description" content="{esc(desc)}">'
        '<meta property="og:type" content="website">'
        f'<meta property="og:title" content="{esc(title)}">'
        f'<meta property="og:description" content="{esc(desc)}">'
        f'<meta property="og:image" content="{og_path}">'
        '<meta property="og:image:width" content="1200">'
        '<meta property="og:image:height" content="630">'
        '<meta name="twitter:card" content="summary_large_image">'
        f'<meta name="twitter:title" content="{esc(title)}">'
        f'<meta name="twitter:description" content="{esc(desc)}">'
        f'<meta name="twitter:image" content="{og_path}">'
        f'<meta http-equiv="refresh" content="0; url={deeplink}">'
        '<style>body{background:#070a0d;color:#e9edf2;font-family:-apple-system,Inter,sans-serif;'
        'display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}'
        'a{color:#97FCE4;text-decoration:none}</style></head><body>'
        f'<div style="text-align:center"><h1>Loading RISExscan…</h1>'
        f'<p>If you are not redirected, <a href="{deeplink}">click here</a>.</p></div>'
        '</body></html>')


def get_big_trades(limit=50, min_usd=100_000, sort_by="size"):
    """Feed dedicado de trades grandes (no liquidaciones), 24h.
       sort_by: 'size' (default, biggest first) o 'time' (most recent first).
    """
    with _FEED_LOCK:
        entries = [e for e in _FEED["entries"]
                   if (not e.get("is_liq")) and e.get("notional", 0) >= min_usd]
        last = _FEED["last_update"]
    if sort_by == "time":
        entries.sort(key=lambda e: e["ts"], reverse=True)
    else:
        entries.sort(key=lambda e: e["notional"], reverse=True)
    total = sum(e["notional"] for e in entries)
    by_side = {"Long": 0.0, "Short": 0.0}
    for e in entries:
        by_side[e.get("position_side", "Long")] = by_side.get(e.get("position_side", "Long"), 0.0) + e["notional"]
    return {"ok": True, "last_update": last, "count": len(entries),
            "total_notional": total, "by_side": by_side, "min_usd": min_usd,
            "entries": entries[:limit]}


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


def _is_smart_money(v):
    """Criterios para badge 'Smart Money':
       - >= 50 trades en 30d
       - volumen 30d >= $250k
       - win rate >= 55%
       - profit factor >= 1.5 (aprox: wins_pnl / |losses_pnl|, usamos avg como proxy via win rate × edge)
       - sin liquidaciones en 30d
       - PnL 30d positivo
       - max drawdown < 50% del PnL acumulado
    """
    trades = v.get("trades", 0)
    vol_30d = v.get("30d", 0.0)
    pnl_30d = v.get("realized_pnl_30d", 0.0)
    w = v.get("wins_30d", 0); l = v.get("losses_30d", 0)
    nl = v.get("n_liquidations", 0)
    dd = v.get("max_dd_30d", 0.0)
    if trades < 50 or vol_30d < 250_000 or pnl_30d <= 0 or nl > 0:
        return False
    decided = w + l
    if decided < 30:
        return False
    win_rate = w / decided
    if win_rate < 0.55:
        return False
    if dd > pnl_30d * 0.5 and dd > 1000:
        return False
    return True


def get_pnl_ranking(period="30d", limit=100):
    """Top traders by realized PnL. Pure SQL on indexed pnl_* columns + unrealized overlay."""
    if period not in ("1d", "7d", "30d", "all"):
        period = "30d"
    # unrealized snapshot from positions indexer
    unreal = {}
    with _IDX_LOCK:
        for r in _INDEX["account_oi_ranking"]:
            unreal[r["account"]] = r.get("upnl", 0.0)
    if period == "1d":
        pnl_col, vol_col, fee_col, tr_col = "pnl_1d", "vol_1d", "fees_1d", "trades_1d"
    elif period == "7d":
        pnl_col, vol_col, fee_col, tr_col = "pnl_7d", "vol_7d", "fees_7d", "trades_7d"
    else:
        pnl_col, vol_col, fee_col, tr_col = "pnl_30d", "vol_30d", "fees_30d", "trades"
    # Fetch enough candidates to cover winners + losers + a buffer
    fetch_n = max(limit * 4, 400)
    try:
        with _DB_LOCK:
            rows_raw = _db().execute(
                f"SELECT address, {pnl_col} AS pnl, {vol_col} AS vol, "
                f"{fee_col} AS fee, {tr_col} AS trades, n_liquidations, "
                f"trades AS total_trades, wins_30d, losses_30d, max_dd_30d, "
                f"vol_30d, pnl_30d "
                f"FROM accounts WHERE {pnl_col} != 0 ORDER BY {pnl_col} DESC LIMIT ?",
                (fetch_n,)
            ).fetchall()
            rows_raw_neg = _db().execute(
                f"SELECT address, {pnl_col} AS pnl, {vol_col} AS vol, "
                f"{fee_col} AS fee, {tr_col} AS trades, n_liquidations, "
                f"trades AS total_trades, wins_30d, losses_30d, max_dd_30d, "
                f"vol_30d, pnl_30d "
                f"FROM accounts WHERE {pnl_col} != 0 ORDER BY {pnl_col} ASC LIMIT ?",
                (fetch_n,)
            ).fetchall()
    except Exception:
        rows_raw, rows_raw_neg = [], []
    seen = {}
    for r in list(rows_raw) + list(rows_raw_neg):
        d = dict(r); seen[d["address"]] = d
    rows = []
    for a, d in seen.items():
        rp = float(d.get("pnl") or 0)
        vol = float(d.get("vol") or 0)
        up = unreal.get(a, 0.0) if period == "all" else 0.0
        total = rp + up
        if rp == 0.0 and up == 0.0:
            continue
        edge_bps = (rp / vol * 10000) if vol > 0 else 0.0
        # smart_money inline check using DB fields
        smart = (
            d.get("total_trades", 0) >= 50 and float(d.get("vol_30d", 0)) >= 250000
            and (d.get("wins_30d", 0) + d.get("losses_30d", 0) > 0)
            and (d.get("wins_30d", 0) / max(1, d.get("wins_30d", 0) + d.get("losses_30d", 0))) >= 0.55
            and int(d.get("n_liquidations") or 0) == 0
            and float(d.get("pnl_30d", 0)) > 0
            and float(d.get("max_dd_30d", 0)) < float(d.get("pnl_30d", 0)) / 2
        )
        rows.append({"account": a, "realized": rp, "unrealized": up, "total": total,
                     "volume": vol, "fees": float(d.get("fee") or 0),
                     "trades": int(d.get("trades") or 0),
                     "edge_bps": round(edge_bps, 1),
                     "smart": smart,
                     "n_liquidations": int(d.get("n_liquidations") or 0)})
    winners = sorted(rows, key=lambda x: x["total"], reverse=True)[:limit]
    losers = sorted(rows, key=lambda x: x["total"])[:limit]
    n_smart = sum(1 for r in rows if r["smart"])
    with _VOL_LOCK:
        last = _VOL["last_update"]; ph = _VOL["phase"]
    return {"ok": True, "phase": ph, "last_update": last, "period": period,
            "count": len(rows), "n_smart": n_smart,
            "winners": winners, "losers": losers}


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
    if period == CUSTOM_LABEL:
        # Custom period not in DB schema — fall back to in-memory
        with _VOL_LOCK:
            ph = _VOL["phase"]; ready = _VOL["ready"]
            scanned = _VOL["scanned"]; total = _VOL["total"]; last = _VOL["last_update"]
            items = [{"account": a, "volume": v.get(period, 0), "trades": v.get("trades", 0)}
                     for a, v in _VOL["by_account"].items() if v.get(period, 0) > 0]
        items.sort(key=lambda x: x["volume"], reverse=True)
        total_vol = sum(x["volume"] for x in items)
        return {"ok": True, "period": period, "ready": ready, "phase": ph,
                "scanned": scanned, "total_accounts": total, "last_update": last,
                "count_with_volume": len(items), "total_volume": total_vol,
                "ranking": items[:limit]}
    # Hot path: pure SQL query on indexed column — instant
    col = f"vol_{period}"
    rows = db_top_by(col, limit)
    total_vol = db_sum(col)
    count_with = db_count_with(col)
    with _VOL_LOCK:
        ph = _VOL["phase"]; ready = _VOL["ready"]
        scanned = _VOL["scanned"]; total = _VOL["total"]; last = _VOL["last_update"]
    return {"ok": True, "period": period, "ready": ready, "phase": ph,
            "scanned": scanned, "total_accounts": total, "last_update": last,
            "count_with_volume": count_with, "total_volume": total_vol,
            "ranking": [{"account": r["address"], "volume": r[col], "trades": r["trades"]}
                        for r in rows]}


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
    # Lighter excluded from "others" since its public API returns unreliable defaults for low-activity markets
    others = sorted(set(list(pacifica.keys()) + list(hyperliquid.keys())) - set(symbols_risex))
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
                                  "color": "#97FCE4"})
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
    deadline = time.time() + 8.0   # per-wallet 8s budget for stats
    cutoff_30d = time.time() - 30 * 86400
    while page <= 12:    # cap at 12k trades; stop early when we cross 30d window
        if time.time() > deadline:
            break
        try:
            d = fetch_json(f"{RISEX}/v1/trade-history?account={q}&limit=1000&page={page}", timeout=6)["data"]
        except Exception:
            break
        batch = d.get("trades") or []
        if not batch:
            break
        trades.extend(batch)
        if not d.get("has_next_page"):
            break
        # early break: if the oldest trade in this page is already older than 30 days,
        # we have enough for win-rate/drawdown/etc — older data isn't shown anyway
        try:
            oldest = min(int(t.get("time", 0)) / 1e9 for t in batch)
            if oldest < cutoff_30d:
                break
        except Exception:
            pass
        page += 1

    if not trades:
        return {"ok": True, "account": account, "trades_analyzed": 0}

    # parse trades (con market_id para per-market breakdown)
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
                            "is_liq": bool(t.get("is_liquidation")),
                            "market_id": str(t.get("market_id", ""))})
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

    # Per-market PnL breakdown
    mm = _market_map()
    per_market = {}
    for t in parsed:
        mid = t["market_id"]
        name = (mm.get(mid) or {}).get("name") or mid or "—"
        e = per_market.setdefault(name, {"market": name, "realized_pnl": 0.0,
                                          "fees": 0.0, "trades": 0, "volume": 0.0,
                                          "wins": 0, "losses": 0})
        e["realized_pnl"] += t["pnl"]; e["fees"] += t["fee"]; e["trades"] += 1
        e["volume"] += t["notional"]
        if t["pnl"] > 0: e["wins"] += 1
        elif t["pnl"] < 0: e["losses"] += 1
    market_pnl = sorted(per_market.values(), key=lambda x: abs(x["realized_pnl"]), reverse=True)

    # Activity heatmap (UTC): 7 days x 24 hours + daily activity for calendar
    import datetime
    heatmap = [[0] * 24 for _ in range(7)]   # heatmap[day_of_week][hour]
    daily_activity = {}                       # YYYY-MM-DD -> trade count
    for t in parsed:
        try:
            dt = datetime.datetime.utcfromtimestamp(t["ts"])
            heatmap[dt.weekday()][dt.hour] += 1
            key = dt.strftime("%Y-%m-%d")
            daily_activity[key] = daily_activity.get(key, 0) + 1
        except Exception:
            pass

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
            "trades_per_day": round(len(parsed) / days_active, 2),
            "market_pnl": market_pnl,
            "activity_heatmap": heatmap,
            "daily_activity": daily_activity}


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


# Tiny in-process memoization for hot endpoints. Smooths bursts of visitors hitting the same endpoint within TTL.
_MEMO = {}
_MEMO_LOCK = threading.Lock()
def _memo(key, ttl_s, fn):
    now = time.time()
    with _MEMO_LOCK:
        e = _MEMO.get(key)
        if e and now - e[0] < ttl_s:
            return e[1]
    val = fn()
    with _MEMO_LOCK:
        _MEMO[key] = (now, val)
    return val


# Stale-while-revalidate: NEVER block the request. If cache exists, return it instantly
# (even if "stale") and refresh in background. Only block on the very first call ever.
_SWR_LOCK = threading.Lock()
_SWR_INFLIGHT = set()


def _swr(key, fn, refresh_after_s=15):
    """Returns the cached value INSTANTLY if available. If older than refresh_after_s,
    schedules a background refresh. On first-ever call, blocks until first result.
    """
    now = time.time()
    with _MEMO_LOCK:
        cached = _MEMO.get(key)
    if cached is None:
        # First call ever — must block
        try:
            val = fn()
            with _MEMO_LOCK:
                _MEMO[key] = (time.time(), val)
            return val
        except Exception:
            with _MEMO_LOCK:
                _MEMO[key] = (time.time(), {"ok": False, "error": "initial fetch failed"})
            return _MEMO[key][1]
    age = now - cached[0]
    if age > refresh_after_s:
        # Refresh in background, but return current cached value NOW
        with _SWR_LOCK:
            if key not in _SWR_INFLIGHT:
                _SWR_INFLIGHT.add(key)
                def _refresh():
                    try:
                        v = fn()
                        with _MEMO_LOCK:
                            _MEMO[key] = (time.time(), v)
                    except Exception:
                        pass
                    finally:
                        with _SWR_LOCK:
                            _SWR_INFLIGHT.discard(key)
                threading.Thread(target=_refresh, daemon=True).start()
    return cached[1]


def bg_warm_loop():
    """Periodically pre-warms slow endpoints so their cache is always fresh.
    No user ever blocks on these — they're always served from cache."""
    # Wait until indexers have something
    time.sleep(20)
    while True:
        for key, fn in [
            ("data", get_overview),
            ("history", lambda: {"ok": True, "points": load_history()}),
            ("cumulative", get_cumulative_growth),
            ("funding_compare", get_funding_compare),
            ("users", get_users),
        ]:
            try:
                val = fn()
                with _MEMO_LOCK:
                    _MEMO[key] = (time.time(), val)
            except Exception:
                pass
        time.sleep(20)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def _send(self, code, body, ctype, cache=None):
        # HTML pages: short revalidatable cache (browser can 304). API: no-store (default).
        if cache is None:
            cache = "public, max-age=30, must-revalidate" if ctype.startswith("text/html") else "no-store"
        is_html = ctype.startswith("text/html") and code == 200
        etag = None
        if is_html:
            etag = f'W/"{len(body)}-{hash(body)&0xffffffff:08x}"'
            inm = self.headers.get("If-None-Match")
            if inm and inm.strip() == etag:
                # Browser already has this version — send 304 with no body
                self.send_response(304); self.send_header("ETag", etag)
                self.send_header("Cache-Control", cache); self.end_headers(); return
        self.send_response(code); self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", cache)
        if etag: self.send_header("ETag", etag)
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
                self._json(_swr("data", get_overview, refresh_after_s=15))
            elif path.path == "/api/history":
                self._json(_swr("history", lambda: {"ok": True, "points": load_history()[-1200:]}, refresh_after_s=60))
            elif path.path == "/api/cumulative":
                self._json(_swr("cumulative", get_cumulative_growth, refresh_after_s=60))
            elif path.path == "/api/daily-active-wallets":
                qs = urllib.parse.parse_qs(path.query)
                days_back = int((qs.get("days") or ["30"])[0])
                self._json(_memo(f"dactive:{days_back}", 60,
                                  lambda: get_daily_active_wallets(days_back)))
            elif path.path == "/api/ranking":
                self._json(get_ranking())
            elif path.path == "/api/account-oi-ranking":
                self._json(get_account_oi_ranking())
            elif path.path == "/api/users":
                self._json(_swr("users", get_users, refresh_after_s=60))
            elif path.path == "/api/wallet" and (urllib.parse.parse_qs(path.query).get("account") or [""])[0]:
                addr = (urllib.parse.parse_qs(path.query).get("account") or [""])[0]
                track_view("wallet", addr)
                self._json(_memo(f"w:{addr.lower()}", 45, lambda: get_wallet(addr)))
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
                self._json(_swr("funding_compare", get_funding_compare, refresh_after_s=120))
            elif path.path == "/api/live-activity":
                qs = urllib.parse.parse_qs(path.query)
                only_liq = (qs.get("only_liq") or ["false"])[0].lower() in ("true", "1", "yes")
                mkt = (qs.get("market") or [None])[0]
                self._json(get_live_activity(only_liq=only_liq, market=mkt))
            elif path.path == "/api/big-trades":
                qs = urllib.parse.parse_qs(path.query)
                m = int(float((qs.get("min_usd") or ["100000"])[0]))
                sb = (qs.get("sort") or ["size"])[0]
                self._json(get_big_trades(min_usd=m, sort_by=sb))
            elif path.path == "/api/liquidations":
                self._json(get_live_activity(only_liq=True))
            elif path.path == "/api/pnl-ranking":
                qs = urllib.parse.parse_qs(path.query)
                p = (qs.get("period") or ["30d"])[0]
                self._json(get_pnl_ranking(period=p))
            elif path.path == "/api/market-sparks":
                self._json(get_market_sparks())
            elif path.path == "/api/market-ticks":
                qs = urllib.parse.parse_qs(path.query)
                mid = (qs.get("market_id") or [""])[0]
                iv = (qs.get("interval") or ["1m"])[0]
                lim = int(float((qs.get("limit") or ["240"])[0]))
                if not mid.isdigit():
                    self._json({"ok": False, "error": "market_id required"}, 400); return
                self._json(get_market_ticks(mid, interval=iv, limit=lim))
            elif path.path == "/api/candles":
                qs = urllib.parse.parse_qs(path.query)
                mid = (qs.get("market_id") or [""])[0]
                iv = (qs.get("interval") or ["1m"])[0]
                lim = int(float((qs.get("limit") or ["240"])[0]))
                if not mid.isdigit():
                    self._json({"ok": False, "error": "market_id required"}, 400); return
                # Cache by (market, interval) — candles only need refresh every 10s
                self._json(_memo(f"c:{mid}:{iv}:{lim}", 10, lambda: get_candles(mid, interval=iv, limit=lim)))
            elif path.path == "/api/market-detail" and (urllib.parse.parse_qs(path.query).get("market_id") or [""])[0]:
                mid = (urllib.parse.parse_qs(path.query).get("market_id") or [""])[0]
                track_view("market", mid)
                self._json(get_market_detail(mid))
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
            elif path.path == "/api/funding-ranking":
                self._json(get_funding_ranking())
            elif path.path == "/api/trending":
                qs = urllib.parse.parse_qs(path.query)
                k = (qs.get("kind") or ["wallet"])[0]
                self._json({"ok": True, **get_trending(kind=k)})
            elif path.path == "/api/random-whale":
                self._json(get_random_whale())
            elif path.path == "/api/suggested-wallets":
                qs = urllib.parse.parse_qs(path.query)
                a = (qs.get("account") or [""])[0]
                self._json(get_suggested_wallets(a))
            elif path.path == "/api/related-markets":
                qs = urllib.parse.parse_qs(path.query)
                m = (qs.get("market_id") or [""])[0]
                self._json(get_related_markets(m))
            elif path.path == "/api/daily-story":
                self._json(get_daily_story())
            elif path.path == "/api/explorer/stream":
                # Server-Sent Events: push new blocks/txs in real time
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("X-Accel-Buffering", "no")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                q = _queue_mod.Queue(maxsize=200)
                with _SSE_LOCK:
                    _SSE_SUBS.append(q)
                try:
                    # send initial state immediately
                    with _EXP_LOCK:
                        init = {
                            "blocks": list(_EXP["blocks"][:18]),
                            "txs": list(_EXP["txs"][:24]),
                            "last_block": _EXP["last_block"],
                        }
                    self.wfile.write(("event: init\ndata: " + json.dumps(init) + "\n\n").encode("utf-8"))
                    self.wfile.flush()
                    last_hb = time.time()
                    while True:
                        try:
                            msg = q.get(timeout=12)
                            line = "event: " + msg["type"] + "\ndata: " + json.dumps(msg["data"]) + "\n\n"
                            self.wfile.write(line.encode("utf-8"))
                            self.wfile.flush()
                        except _queue_mod.Empty:
                            # heartbeat to keep connection alive through proxies
                            self.wfile.write(b": heartbeat\n\n")
                            self.wfile.flush()
                            last_hb = time.time()
                except Exception:
                    pass
                finally:
                    with _SSE_LOCK:
                        try: _SSE_SUBS.remove(q)
                        except Exception: pass
                return
            elif path.path == "/api/explorer/stats":
                self._json(get_explorer_stats())
            elif path.path == "/api/explorer/blocks":
                qs = urllib.parse.parse_qs(path.query)
                lim = int(float((qs.get("limit") or ["20"])[0]))
                self._json(get_explorer_blocks(limit=lim))
            elif path.path == "/api/explorer/txs":
                qs = urllib.parse.parse_qs(path.query)
                lim = int(float((qs.get("limit") or ["30"])[0]))
                self._json(get_explorer_txs(limit=lim))
            elif path.path.startswith("/api/explorer/tx/"):
                txh = path.path[len("/api/explorer/tx/"):]
                if not (txh.startswith("0x") and len(txh) == 66):
                    self._json({"ok": False, "error": "invalid hash"}, 400); return
                self._json(get_explorer_tx_detail(txh))
            elif path.path.startswith("/api/explorer/block/"):
                try:
                    n = int(path.path[len("/api/explorer/block/"):])
                    self._json(get_explorer_block_detail(n))
                except Exception as e:
                    self._json({"ok": False, "error": str(e)}, 400)
            elif path.path == "/api/wallet-preview":
                qs = urllib.parse.parse_qs(path.query)
                a = (qs.get("account") or [""])[0]
                if not (a.startswith("0x") and len(a) == 42):
                    self._json({"ok": False, "error": "invalid"}); return
                out = {"ok": True, "account": a}
                with _VOL_LOCK:
                    v = _VOL["by_account"].get(a) or _VOL["by_account"].get(a.lower())
                if v:
                    out["volume_30d"] = v.get("30d", 0)
                    out["pnl_30d"] = v.get("realized_pnl_30d", 0)
                    out["trades"] = v.get("trades", 0)
                    out["n_liquidations"] = v.get("n_liquidations", 0)
                    w = v.get("wins_30d", 0); l = v.get("losses_30d", 0)
                    out["win_rate"] = (w / (w + l) * 100) if (w + l) > 0 else None
                    out["equity_curve"] = (v.get("equity_curve") or [])[-30:]
                    out["smart"] = _is_smart_money(v)
                with _IDX_LOCK:
                    for r in _INDEX["account_oi_ranking"]:
                        if r["account"].lower() == a.lower():
                            out["current_oi"] = r.get("total_oi", 0)
                            out["positions"] = r.get("positions", 0)
                            break
                self._json(out)
            elif path.path == "/api/wallet-stats":
                qs = urllib.parse.parse_qs(path.query)
                a = (qs.get("account") or [""])[0]
                self._json(_memo(f"ws:{a.lower()}", 120, lambda: get_wallet_stats(a)))
            elif path.path == "/api/wallet-trades":
                qs = urllib.parse.parse_qs(path.query)
                a = (qs.get("account") or [""])[0]
                limit = int((qs.get("limit") or ["200"])[0])
                since = qs.get("since")
                since_ts = int(since[0]) if since else None
                if not (a.startswith("0x") and len(a) == 42):
                    self._json({"ok": False, "error": "invalid account"}, 400); return
                rows = db_trades_for_account(a, limit=limit, since_ts=since_ts)
                total_fills = db_trades_count(a)
                total_orders = db_orders_count(a)
                self._json({"ok": True, "account": a,
                             "total_fills_indexed": total_fills,
                             "total_orders_indexed": total_orders,
                             "returned": len(rows),
                             "trades": rows})
            elif path.path == "/api/wallet-transfers":
                qs = urllib.parse.parse_qs(path.query)
                a = (qs.get("account") or [""])[0]
                limit = int((qs.get("limit") or ["100"])[0])
                if not (a.startswith("0x") and len(a) == 42):
                    self._json({"ok": False, "error": "invalid account"}, 400); return
                rows = db_get_transfers(a, limit)
                deposits = sum(r["amount"] for r in rows if r["kind"] == "deposit")
                withdrawals = sum(r["amount"] for r in rows if r["kind"] == "withdraw")
                self._json({"ok": True, "account": a,
                             "count": len(rows),
                             "total_deposits": round(deposits, 2),
                             "total_withdrawals": round(withdrawals, 2),
                             "net": round(deposits - withdrawals, 2),
                             "transfers": rows})
            elif path.path == "/api/league":
                self._json(_memo("league", 60, get_league))
            elif path.path == "/league":
                self._send(200, LEAGUE_HTML.encode("utf-8"), "text/html; charset=utf-8")
            elif path.path == "/sitemap.xml":
                self._send(200, _sitemap_xml().encode("utf-8"), "application/xml; charset=utf-8")
            elif path.path == "/robots.txt":
                self._send(200, _ROBOTS_TXT.encode("utf-8"), "text/plain; charset=utf-8")
            elif path.path == "/og/home.svg":
                self._send(200, _svg_home_card().encode("utf-8"), "image/svg+xml")
            elif path.path == "/about":
                self._send(200, ABOUT_HTML.encode("utf-8"), "text/html; charset=utf-8")
            elif path.path == "/methodology":
                self._send(200, METHODOLOGY_HTML.encode("utf-8"), "text/html; charset=utf-8")
            elif path.path.startswith("/og/wallet/"):
                addr = path.path[len("/og/wallet/"):].split(".")[0]
                if not (addr.startswith("0x") and len(addr) == 42):
                    self._send(400, b"bad address", "text/plain"); return
                svg = _svg_wallet_card(addr)
                self._send(200, svg.encode("utf-8"), "image/svg+xml")
            elif path.path.startswith("/og/market/"):
                mid = path.path[len("/og/market/"):].split(".")[0]
                if not mid.isdigit():
                    self._send(400, b"bad market id", "text/plain"); return
                svg = _svg_market_card(mid)
                self._send(200, svg.encode("utf-8"), "image/svg+xml")
            elif path.path.startswith("/share/wallet/"):
                addr = path.path[len("/share/wallet/"):]
                if not (addr.startswith("0x") and len(addr) == 42):
                    self._send(404, b"not found", "text/plain"); return
                short = addr[:6] + "…" + addr[-4:]
                title = f"{short} · RISEx Trader Stats"
                desc = "Realized PnL, volume, win rate, trade history — live RISE chain perp DEX analytics."
                html_page = _share_page_html("wallet", addr, f"/og/wallet/{addr}.svg",
                                             title, desc, f"/#wallet={addr}")
                self._send(200, html_page.encode("utf-8"), "text/html; charset=utf-8")
            elif path.path.startswith("/share/market/"):
                mid = path.path[len("/share/market/"):]
                if not mid.isdigit():
                    self._send(404, b"not found", "text/plain"); return
                title = f"RISEx Market #{mid} · Live perp stats"
                desc = "Mark price, 24h change, OI, volume, funding APR — RISE chain perps."
                html_page = _share_page_html("market", mid, f"/og/market/{mid}.svg",
                                             title, desc, f"/#market={mid}")
                self._send(200, html_page.encode("utf-8"), "text/html; charset=utf-8")
            elif path.path in ("/", "/index.html"):
                self._send(200, HTML.encode("utf-8"), "text/html; charset=utf-8")
            else:
                self._send(404, NOT_FOUND_HTML.encode("utf-8"), "text/html; charset=utf-8")
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
    threading.Thread(target=historical_backfill_loop, daemon=True).start()
    threading.Thread(target=transfers_indexer_loop, daemon=True).start()
    threading.Thread(target=bg_warm_loop, daemon=True).start()
    threading.Thread(target=volume_indexer_loop, daemon=True).start()
    threading.Thread(target=funding_indexer_loop, daemon=True).start()
    threading.Thread(target=explorer_indexer_loop, daemon=True).start()
    threading.Thread(target=league_indexer_loop, daemon=True).start()
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


_INFO_PAGE_CSS = """*{box-sizing:border-box;margin:0;padding:0}
body{background:#06090c;color:#f0f4f8;font-family:-apple-system,BlinkMacSystemFont,'Inter','Segoe UI',Roboto,sans-serif;line-height:1.65;min-height:100vh;position:relative;overflow-x:hidden}
body::before{content:"";position:fixed;width:520px;height:520px;border-radius:50%;filter:blur(120px);top:-180px;right:-160px;background:radial-gradient(circle,rgba(0,255,212,.16),transparent 70%);pointer-events:none;z-index:0}
body::after{content:"";position:fixed;width:520px;height:520px;border-radius:50%;filter:blur(120px);bottom:-200px;left:-180px;background:radial-gradient(circle,rgba(26,238,179,.10),transparent 70%);pointer-events:none;z-index:0}
.wrap{max-width:760px;margin:0 auto;padding:60px 28px 80px;position:relative;z-index:1}
.brand{display:flex;align-items:center;gap:12px;margin-bottom:40px}
.brand a{text-decoration:none;display:flex;align-items:center;gap:12px}
.brand .logo{width:40px;height:40px;border-radius:11px;background:linear-gradient(135deg,#0e1418,#070b0f);border:1px solid rgba(0,255,212,.22);display:flex;align-items:center;justify-content:center;box-shadow:0 4px 16px rgba(0,255,212,.15)}
.brand .logo svg{width:22px;height:auto}
.brand .name{font-weight:800;font-size:15px;letter-spacing:.4px;color:#fff}
.brand .sub{color:#7a8694;font-size:10.5px;margin-top:3px;letter-spacing:.6px;text-transform:uppercase;font-weight:600}
.eyebrow{font-size:11px;font-weight:700;color:#00ffd4;text-transform:uppercase;letter-spacing:2px;margin-bottom:14px}
h1{font-size:44px;font-weight:800;letter-spacing:-1.5px;line-height:1.1;color:#fff;margin-bottom:24px}
h2{font-size:24px;font-weight:700;letter-spacing:-.5px;color:#fff;margin:48px 0 16px;display:flex;align-items:center;gap:10px}
h2::before{content:"";width:4px;height:20px;border-radius:2px;background:linear-gradient(180deg,#00ffd4,#0CD8B7);box-shadow:0 0 10px rgba(0,255,212,.5)}
h3{font-size:17px;font-weight:700;color:#fff;margin:28px 0 10px;letter-spacing:-.2px}
p{color:#c9d0d8;font-size:15px;margin-bottom:14px}
a{color:#00ffd4;text-decoration:none;border-bottom:1px solid rgba(0,255,212,.3);transition:border-color .15s}
a:hover{border-color:#00ffd4}
ul,ol{margin:14px 0 18px 24px;color:#c9d0d8}
ul li,ol li{margin-bottom:8px;font-size:14.5px}
ul li::marker{color:#00ffd4}
code{font-family:'JetBrains Mono',ui-monospace,Menlo,monospace;font-size:13px;background:rgba(0,255,212,.08);color:#00ffd4;padding:2px 6px;border-radius:4px;border:1px solid rgba(0,255,212,.2)}
.lead{font-size:18px;color:#c9d0d8;line-height:1.6;margin-bottom:32px}
.card{background:linear-gradient(180deg,#0d1217,#0a0e13);border:1px solid #1a2129;border-radius:12px;padding:24px 28px;margin:18px 0}
.card h3{margin-top:0}
.metricdef{display:grid;grid-template-columns:160px 1fr;gap:14px;padding:14px 0;border-bottom:1px solid #141a22}
.metricdef:last-child{border-bottom:none}
.metricdef .lbl{font-family:'JetBrains Mono',monospace;color:#00ffd4;font-size:13px;font-weight:700}
.metricdef .desc{color:#c9d0d8;font-size:14px}
.tag{display:inline-block;font-size:10.5px;font-weight:700;padding:3px 8px;border-radius:4px;background:rgba(0,255,212,.10);color:#00ffd4;border:1px solid rgba(0,255,212,.25);text-transform:uppercase;letter-spacing:.6px;margin-right:4px;vertical-align:middle}
.cta{display:inline-flex;align-items:center;gap:8px;background:linear-gradient(135deg,#00ffd4,#0CD8B7);color:#06090c;padding:11px 22px;border-radius:8px;font-weight:700;font-size:14px;border:none;text-decoration:none;margin-top:24px;box-shadow:0 6px 18px rgba(0,255,212,.22)}
.cta:hover{transform:translateY(-1px);border:none}
.footer{margin-top:60px;padding-top:24px;border-top:1px solid #141a22;color:#54616d;font-size:12px;text-align:center}
@media (max-width:600px){h1{font-size:32px}h2{font-size:20px}.wrap{padding:40px 20px 60px}.metricdef{grid-template-columns:1fr;gap:4px}}"""

_INFO_BRAND = """<div class="brand"><a href="/"><div class="logo"><svg viewBox="0 0 252 303"><path d="M176.12 0.39H0.59V50.72H176.12C189.97 50.72 201.20 61.98 201.20 75.88V101.04H77.36C34.96 101.04 0.59 135.41 0.59 177.81V302.34H50.74V184.00L177.66 302.33H251.36L89.42 151.37H201.20V101.29H251.36V75.88C251.36 34.19 217.66 0.39 176.12 0.39Z" fill="#fff"/></svg></div><div><div class="name">RISExscan</div><div class="sub">Live perp analytics</div></div></a></div>"""

# ---------------------------------------------------------------------------
# Volume League — competicion de la comunidad (22 jul -> 1 ago 2026)
# Reglas: cuenta TODO el volumen (taker + maker) de la lista fija de wallets;
# >= $200K de volumen => +5% de boost de puntos; TOP 5 => +20%.
# El volumen sale de la propia BD del indexador (tabla trades), calculado en
# el servidor y cacheado 60s — mismos numeros para todos, al instante.
# ---------------------------------------------------------------------------
LEAGUE_START_TS = calendar.timegm((2026, 7, 22, 0, 0, 0))
LEAGUE_END_TS   = calendar.timegm((2026, 8, 1, 0, 0, 0))
LEAGUE_MIN_VOL  = 200_000
LEAGUE_TOP_N    = 5
LEAGUE_WALLETS  = [
"0x01a4c3cbdc016a58adfd5dffc359724322d2a2f3",
"0x054ae303fcc8de33d6983681e03e6776ebe95ab2",
"0x061126af439c46c8c9f219d12fec4047248d36ee",
"0x0c43a1f690972823df171185865aadfc05cb726d",
"0x0c643306a5c176fc2122809447aeb9e080f489aa",
"0x0c94959001aa4f11bb8791b640b73e658522c8e0",
"0x0e183f408cbf40b7428adfd02fcac9a46a2b1961",
"0x10510290c3b39ba1366a683c7a1f9093063baa01",
"0x11a9249903d951723268e5eb37506f57a16ee17a",
"0x134be30b5b0b95367595d2f1c21a4e75e1c6e8f1",
"0x1642cc1b66f2e3bb1ad06b7261981ded9d77397b",
"0x1b06d8b2a64118028e9d3f2e99e4c2b7c4a45f54",
"0x1b58efe82ec954f60fed62b0e75eee66d5cbb108",
"0x1ccf424aee6d87de22353872a30cf96d0a320744",
"0x1ce291f3aa96c87ab10efd007d6cca4e6b090e84",
"0x1e3fa5fd341b1f1864793c4c6f126a2a02da6a37",
"0x1e81edff1658c2f2b83dc6b29e288c9116eef320",
"0x217d066699d87a0b61cf6bfc72d50ba34d82a987",
"0x254830cb8a45413237712499e838ecd657963114",
"0x295f87671b9c1f6a090b9a3bd86484608c27917c",
"0x2a63eb6780448379962339d88783445d3f770f5c",
"0x2a96e5f524bd25873dda57aa1ebf8c4a89ba26f4",
"0x2be9038e19e15a018c799d07ab3ad79f7a9baac4",
"0x2c662977da2f5f26b33832312c8c9086ee2d2344",
"0x2d403d80660674a2bfcc086b7e47c3de37f09ed4",
"0x2ec7491394bcbd30d0b2e78f54b1591d3f2ea8a4",
"0x2ee0597fe557daccda27e818f0e2cff1391daf8e",
"0x32c56448865100a9a9ff3aeb44b4d824bdb35fa4",
"0x341350a5bd357146fe339c0b44928a4ff5c67773",
"0x351cc81e65cf4cac78c61fc489ae37915711f73e",
"0x3589a7e9aff8e312c66ae5f51fe3de6ff95a9821",
"0x359665284b127fbb26ce97ad5d2aa114752d1c57",
"0x35bb17f553097ea1b4920cb8ab21a21aa4a66665",
"0x35dfc46ef53511f56c1f89126f45be24089a7c7f",
"0x36bd3da5ecf14b381a9edd542ea930445d5f0606",
"0x37bb22270e48bf1ce2ae4b6338c5c401c6de33aa",
"0x380fd1a31ca5e25a32e3b81d9f05ddcaba44bd71",
"0x3887b1ed6383ab3e0431f490f5f876f44dfa24bc",
"0x3c937d6d1458ccc9ccf6b9609a07e3d93d37a069",
"0x3d075154f5a275cd2ec254192ca98f2e9d553d90",
"0x3fc56aaab3de3c92cb5c3a36f2dce528231c1010",
"0x41bd10dd4986f16e05d493b9660da9cd1f1d9b0f",
"0x41d26da5afdcb84a90d47b9276840861759199de",
"0x43b95b9c5bf9c74e02d4b883a70c5af20b8a99e7",
"0x455a40c3f365ed4a097d1cf17f2600349acb6e9c",
"0x4693df65a63c4ed05597ef65ab9a77f75b2c3f95",
"0x4794baf61515a5fda60ed1342d26f8108a96340c",
"0x4832780bfa5511d2028e39b42689a520d170b664",
"0x4aab5d8fc6c6ecd7bc747a7cc8d5e61ea46dfc44",
"0x4b218e595ba61f389253163a733cca45cf9942c8",
"0x4c4f2b1dfea107832743bd848477d13a194a3ca6",
"0x4cdba91d62c3d7d669391a03f4069aa6693b54d7",
"0x4e66f55cc93fa8db3c263f7c03722a6dc8bcd5e8",
"0x4e67497559bdd97d8f1aefc9dd6e457923b1f101",
"0x4f2e6dea8ee08d62aa4128a00065ac8ed14658cd",
"0x4fe30bc714c0985c140a80faaf21a1e46fab43f3",
"0x52c09074eb2596796bf6ff6ae00a7bb0c7a7d1b9",
"0x536e51023e0189e41d2b6ebcde45034e092f2397",
"0x553d0f296ad69e9a52fa5c7b22509eac0c0bf111",
"0x5566c3d6acfff4d58f903bf75b2d7656518b7213",
"0x585a30611dac4da3b84c8da73468bef169dc3397",
"0x591f8bba6d9102b6626286c43c76ea3736767952",
"0x59d10adfac735978e7947a5432742dc400dc52aa",
"0x5a93f5b34820b49119f4c23b4429ce02af306757",
"0x5e2d26de7e7cc792d917571d7779dd90a3514675",
"0x5e9862ebe77eed1db11b4cfe1a223ff95dfb6632",
"0x6306224ddf6faaed738f44151247f9ec22579fbd",
"0x65d3f6794a83916c687921030dea698b17d589de",
"0x667cd32ca8b691eca59c2e556923eecdf69ca1a0",
"0x670aeabf52531e434d7dccf4c0cf1280e33e5832",
"0x6a3c1b0be8c53af0f351ee6efef18ae3892187e9",
"0x6baa62779e7591445f4dfe52ab999a933d7a804b",
"0x6c13a21dac9ac8eb47cf6296d6eea7a1c4c7e87c",
"0x6d975f91efc2fc9b1f65c1ca3ade8334635cec6d",
"0x70ec2ec3fe51929345f4ee7a3079ca4a30f42115",
"0x71199aa13c02f1e4c71bf9c3fec0b62a8a75bcb3",
"0x722ab179b05c937d9009a52ead3445671f2ac703",
"0x749e2629ad16cfaeccaecef4a4ea7cd12a5f7eaf",
"0x76c519ad2d3578d497bd82fe3a2e1216fa712d59",
"0x78645b860e153dc244716c928402cf712cb8a1bd",
"0x7a53ff085aad6ea9e394f35cc0204f04ad22bf47",
"0x7b36e27a59d6e6bd3258bf90314072ea866eb29e",
"0x7cf66d79195141018579e0b9f03053664a5475ba",
"0x7ea69a5d57ce4cdb766ddd205fa56a323cdf83d4",
"0x817901e33848bbb0fd6bcf6d804ba9a47f333885",
"0x8531a31c5f4e5e07d0faeaef7cb9900a79ff9315",
"0x85cec0bb0b38efaaf8217328488ec0374671593e",
"0x874ccd286e0590100a1b16bb846ce05650702218",
"0x87c2194f3155e0d93bb40533879e97c02c290937",
"0x88900216ccd96d6d6240dd5b7700b198499a7c6a",
"0x8aebf76afea865d80906df65cee9b1609d202333",
"0x8fe00fb53e7d12580a330e7702abc76d812fef4d",
"0x936fe5ea7533cc1ad401a304e51f25fdae96c7eb",
"0x9abeb8e8a45330ae4aea20764e9bc573a8651be1",
"0xa0d3a93bbce155187a7b78252d8298cc3f28e884",
"0xa1e022dad52c2e50134df21f4c8a1d5a86301c4f",
"0xa2f06229080a3b199f65720c0988af068c2d9e98",
"0xa358ca417d14754e1c64e08c7412a4806e896ec6",
"0xa8e9734b25f0c79be56337ef41dbee4d2cb55fe8",
"0xaa87ee03d91be06b794a239db8dbea966b6771e7",
"0xaac29e79761b140eb8745d57fd324af974416bbf",
"0xae549da7f76b62fd82c2945147cca5e09dabb474",
"0xaf96951d3d1c5dd6d676307cec7cecf242aafdbf",
"0xb061d61fb4a4031f7f528378aec7302d5fc6a498",
"0xb08359d37003ab61c38411c46ae647cbdb26db97",
"0xb093c98788a38aba169420a4fa3afe20eede1561",
"0xb47826a07a9e21923e57d70882157b5776e8ad1f",
"0xb4f7b3ea72d8f834612b370d323d3aa7ebc1d253",
"0xb50e5743e8b1eeb3afe08419843caa22e568203f",
"0xb6e02394def3f11c3a7f3ad0e91b747305d3f045",
"0xb6f12c211f5f039e9185238508c0a9be60a370af",
"0xb791e6efcfbde977263b59f6228dfe72c7d66a67",
"0xb7a10b3b00e1e757f8762efea9910105d909ee1c",
"0xb88e58077cb0e8ce59e2a0b31694d764460421f9",
"0xbe065ebba26a54809c2a692b9056b6be1cf19081",
"0xbf6266a545c232ffb08d3727ca47c15ce2062d10",
"0xbfd3c41de3cebfbbd5ba39325f69dfd2bf2a9f51",
"0xc02dd2d1403463b8d2f79cd5e114568af3775cbd",
"0xc12c81284a9b05d11520474cdfb817d115c942e3",
"0xc1d2d9aa00f09fd0392992a6421c3bea7d1b6c83",
"0xc2334a8945e142b31ae0e4162f7d2c480ba9baa3",
"0xc25bc6de6431cfe0cef0e4ea4abe84f046604504",
"0xc3159c0ec0f66ca8cc6d87abef9ef443497b4be6",
"0xc382d3f879a1161a562865c334d80d7a7359c80c",
"0xc4a84144a4743d161eb3a7713e28b6b7a652fb25",
"0xc5d4a46aec7f38d75a76404d9c5fe6c0fcb75653",
"0xc701bd6ded6f47188430eda8b2bb31d87637b513",
"0xcac2ef22949d92ccdbf5992c870f3b1625da8d9f",
"0xcca80fc8360d9024250a2e1959b9267c72efe87f",
"0xce5e4ddcdc98843569e4d8784ea38b1f9acb016d",
"0xceb0802ca5841ce28f708d75ed1de77c64cd06a9",
"0xd098dfc7b4737c19d5f9e6f04784b71fae0df07d",
"0xd1c8849b3afd1b55e2038d30f9f77f511a83e9e7",
"0xd1f86f3c3e14aa992cafb6941e3b6410fde77aa2",
"0xd4670a5af0e4a4851babdda4c35994b61f9759f7",
"0xd4b327e8405087ef70747a85c2d51a22a7c88061",
"0xd63a4e75c2faaeefe45d82e6f49bf7e9bedd37dd",
"0xd91fa7b82c6dcdf6cfc2cbdd0bf2ac19753c17c0",
"0xdc3116cfa2e2f2b66e67c25bf78b7e8abb10e6db",
"0xde83fc13e2f00d89d1e88d6444b53ed157900e68",
"0xe17d4646768a46f42509a6ba524b17ff568f0245",
"0xe4c26df9ac9718d23880e1b4900849e6e6c1630e",
"0xea1e5d8f06f505e6dd9adfa5d2004b48a6c4e6e8",
"0xeabaf2f4b5b1a755aab1ca44ebfd25e8a2b0037f",
"0xebdcc84ae6f22dc21b9794d2a42661613502cf5a",
"0xedbc6a2825a413da20cfdbb671c5beab32a3a97f",
"0xee235618ac8aa951e3108616c1aeb65bf5efd058",
"0xf06ab1b6394560607af0f61808b42a6368758bd7",
"0xf0941f06e586677dbad1015dfe4a54ea94c76475",
"0xf2d156db1bb92ed70746ab28cae9c689ec465ca2",
"0xf3dc78592499a62511953fda21ac63ff128e1a7d",
"0xf6c0b6caaf69d962bf24ec464c9453d97fa4cdc0",
"0xf8255aa69e99c2a3f79ff48436746aef0675d231",
"0xf95313b5a26f6939032faac03cc6b918900fe014",
"0xf9cbea7b64225431b0932f375abc73ce2f9bbfae",
"0xfa0a96d77da5b00127b3aebeb10cd8bb487249e5",
"0xfdfa268a2188738962ea2f351f77c95fe87f8cbc",
"0xfe307fa5ec586b8a2ff847c6694db0a2f2f9a24e",
"0xff6dfefa9349a050235d72d138088432c022db5a"
]


def get_league():
    agg = {}
    try:
        ph = ",".join("?" * len(LEAGUE_WALLETS))
        with _DB_LOCK:
            rows = _db().execute(
                "SELECT account, SUM(size*price) AS vol, COUNT(*) AS n, MAX(ts) AS last "
                "FROM trades WHERE ts >= ? AND ts < ? AND account IN (" + ph + ") "
                "GROUP BY account",
                [LEAGUE_START_TS, LEAGUE_END_TS] + LEAGUE_WALLETS,
            ).fetchall()
        for a, vol, cnt, last in rows:
            agg[a] = (vol or 0.0, cnt or 0, last or 0)
    except Exception as e:
        return {"ok": False, "error": str(e)}
    out = []
    for a in LEAGUE_WALLETS:
        vol, cnt, last = agg.get(a, (0.0, 0, 0))
        out.append({"account": a, "vol": round(vol, 2), "trades": cnt, "last": last})
    out.sort(key=lambda x: -x["vol"])
    total = round(sum(x["vol"] for x in out), 2)
    qualified = sum(1 for x in out if x["vol"] >= LEAGUE_MIN_VOL)
    return {"ok": True,
            "start": LEAGUE_START_TS, "end": LEAGUE_END_TS,
            "min_vol": LEAGUE_MIN_VOL, "top_n": LEAGUE_TOP_N,
            "total_vol": total, "participants": len(out), "qualified": qualified,
            "updated": int(time.time()), "rows": out}


def league_indexer_loop():
    """Garantiza que las wallets de la Volume League esten indexadas.

    Tras un arranque con BD vacia (p.ej. deploy sin volumen persistente),
    fuerza el escaneo del historial de cada wallet de la liga (~30 dias,
    cubre la ventana completa de la competicion) aunque nadie las visite.
    Despues las mantiene frescas en rotacion. Se apaga solo al acabar la liga.
    INSERT OR IGNORE hace que solaparse con los demas indexadores sea inocuo."""
    time.sleep(25)  # dejar levantar el servidor y el resto de hilos
    while True:
        now_s = int(time.time())
        if now_s > LEAGUE_END_TS + 12 * 3600:
            print("[league] competicion terminada — indexador de liga parado", flush=True)
            return
        t0 = time.time()
        done = 0
        for a in LEAGUE_WALLETS:
            try:
                _account_metrics(a, int(time.time()))
                done += 1
            except Exception as e:
                print(f"[league] err {a[:10]}: {e}", flush=True)
            time.sleep(0.6)  # suave con la API de RISE
        dt = time.time() - t0
        print(f"[league] pasada completa: {done}/{len(LEAGUE_WALLETS)} wallets en {dt:.0f}s", flush=True)
        time.sleep(max(120.0, 300.0 - dt))


LEAGUE_HTML = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Volume League · RISEx</title>
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect fill='%23080d00' width='32' height='32' rx='7'/%3E%3Cpath d='M8 24V14M16 24V8M24 24V17' stroke='%238df885' stroke-width='3.5' fill='none' stroke-linecap='round'/%3E%3C/svg%3E">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&family=IBM+Plex+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<style>
:root{
  --bg:#080d00; --surface:#0d1406; --surface2:#121a0a; --line:#1c261c; --line2:#2a3a2a;
  --green:#8df885; --cyan:#22ffe2; --up:#5bf055; --down:#ff5c6c; --amber:#ffc908;
  --text:#e8f5e6; --muted:#7d8b7d; --faint:#4a564a;
  --mono:'IBM Plex Mono',ui-monospace,monospace; --sans:'Space Grotesk',system-ui,sans-serif;
  --r:6px;
}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:var(--sans);font-size:14px;
     background-image:radial-gradient(ellipse 80% 50% at 50% -10%,rgba(141,248,133,.07),transparent)}
.wrap{max-width:980px;margin:0 auto;padding:0 20px}
header{border-bottom:1px solid var(--line);padding:14px 0;position:sticky;top:0;background:rgba(8,13,0,.92);
       backdrop-filter:blur(8px);z-index:10}
.hbar{display:flex;align-items:center;gap:14px;flex-wrap:wrap}
.logo{font-size:17px;font-weight:700}.logo b{color:var(--green)}
.logo span{color:var(--muted);font-size:10px;letter-spacing:.14em;text-transform:uppercase;margin-left:8px}
.hmeta{margin-left:auto;display:flex;align-items:center;gap:14px;font-family:var(--mono);font-size:12px;color:var(--muted)}
.dot{width:7px;height:7px;border-radius:50%;background:var(--green);display:inline-block;margin-right:6px;
     box-shadow:0 0 8px var(--green);animation:pulse 2s infinite}
@keyframes pulse{50%{opacity:.4}}
button{font-family:var(--mono);font-size:12px;color:var(--green);background:var(--surface2);
       border:1px solid var(--line2);border-radius:var(--r);padding:7px 14px;cursor:pointer;transition:all .15s}
button:hover{border-color:var(--green)}
main{padding:26px 0 60px}
.card{background:var(--surface);border:1px solid var(--line);border-radius:var(--r)}
.count-hero{padding:34px 24px;text-align:center;margin-bottom:16px}
.count-hero .ch-lbl{font-size:11px;letter-spacing:.2em;text-transform:uppercase;color:var(--muted);margin-bottom:12px}
.count-hero .ch-big{font-family:var(--mono);font-size:46px;font-weight:700;letter-spacing:-.02em;
                    color:var(--green);text-shadow:0 0 24px rgba(141,248,133,.4)}
.count-hero .ch-sub{font-family:var(--mono);font-size:13px;color:var(--muted);margin-top:12px}
.count-hero.live .ch-big{color:var(--text);font-size:34px}
.count-hero.live .ch-lbl{color:var(--green)}
.note{background:var(--surface2);border:1px solid var(--line2);border-left:3px solid var(--amber);
      border-radius:var(--r);padding:11px 15px;font-family:var(--mono);font-size:12px;
      color:var(--text);margin-bottom:20px;line-height:1.7}
.note b{color:var(--amber)}
.kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:22px}
@media(max-width:800px){.kpis{grid-template-columns:repeat(2,1fr)}}
.kpi{padding:15px 17px}
.kpi .k{font-size:10px;letter-spacing:.14em;text-transform:uppercase;color:var(--muted);margin-bottom:6px}
.kpi .v{font-family:var(--mono);font-size:21px;font-weight:700;letter-spacing:-.02em}
.kpi .d{font-family:var(--mono);font-size:11px;color:var(--muted);margin-top:4px}
.big{color:var(--green);text-shadow:0 0 18px rgba(141,248,133,.35)}
.sec-head{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:11px}
.sec-title{font-size:11px;letter-spacing:.16em;text-transform:uppercase;color:var(--muted)}
.sec-title::before{content:'—';color:var(--green);margin-right:8px}
.rk{display:grid;grid-template-columns:52px 1fr auto;gap:0 16px;align-items:center;
    padding:11px 16px;border-bottom:1px solid rgba(28,38,28,.55);position:relative;overflow:hidden}
.rk:last-child{border-bottom:none}
.rk .bar{position:absolute;left:0;top:0;bottom:0;background:linear-gradient(90deg,rgba(141,248,133,.10),rgba(141,248,133,.02));
         border-right:1px solid rgba(141,248,133,.18);transition:width .8s ease;z-index:0}
.rk>*{position:relative;z-index:1}
.rk .pos{font-family:var(--mono);font-size:17px;font-weight:700;color:var(--muted);text-align:center}
.rk.p1 .pos{color:var(--amber);font-size:20px}
.rk.p2 .pos{color:#cfd8cf;font-size:19px}
.rk.p3 .pos{color:#d0925a;font-size:18px}
.rk.top5{background:rgba(255,201,8,.03)}
.rk .who .addr{font-family:var(--mono);font-size:14px;color:var(--green);font-weight:500}
.rk .who .meta{font-family:var(--mono);font-size:11px;color:var(--muted);margin-top:2px}
.rk .num{text-align:right}
.rk .num .vol{font-family:var(--mono);font-size:17px;font-weight:700;letter-spacing:-.01em}
.rk.p1 .num .vol{color:var(--amber)}
.badge{display:inline-block;font-family:var(--mono);font-size:11px;font-weight:700;
       padding:2px 9px;border-radius:20px;margin-top:4px}
.b20{background:rgba(255,201,8,.12);color:var(--amber);border:1px solid rgba(255,201,8,.35)}
.b5{background:rgba(91,240,85,.10);color:var(--up);border:1px solid rgba(91,240,85,.3)}
.bfalta{font-family:var(--mono);font-size:11px;color:var(--muted);margin-top:4px}
.bfalta b{color:var(--amber)}
.empty{padding:30px;text-align:center;color:var(--muted);font-family:var(--mono);font-size:12.5px}
footer{border-top:1px solid var(--line);padding:16px 0;font-size:11.5px;color:var(--faint);font-family:var(--mono)}
</style>
</head>
<body>

<header><div class="wrap hbar">
  <div class="logo">volume<b>league</b><span>RISEx · comunidad</span></div>
  <div class="hmeta">
    <span><span class="dot"></span><span id="upd">conectando…</span></span>
    <button id="btn-scan">↻ actualizar</button>
  </div>
</div></header>

<main class="wrap">

  <div class="card count-hero" id="hero">
    <div class="ch-lbl" id="hero-lbl">LA COMPETICIÓN EMPIEZA EN</div>
    <div class="ch-big" id="hero-big">—</div>
    <div class="ch-sub">del <b style="color:var(--text)">22 jul</b> al <b style="color:var(--text)">1 ago · 00:00 UTC</b> — 10 días</div>
  </div>

  <div class="note">🏆 <b>Premios en boost de puntos:</b> con <b>$200K+</b> de volumen te llevas <b>+5%</b> de boost ·
    el <b>TOP 5</b> se lleva <b>+20%</b>. Aquí cuenta TODO el volumen: taker y maker.</div>

  <div class="kpis">
    <div class="card kpi"><div class="k">Volumen total</div><div class="v big" id="k-vol">—</div><div class="d">desde el inicio</div></div>
    <div class="card kpi"><div class="k">Con +5% asegurado</div><div class="v" id="k-q" style="color:var(--up)">—</div><div class="d" id="k-q-d">≥ $200K de volumen</div></div>
    <div class="card kpi"><div class="k">Corte del TOP 5</div><div class="v" id="k-cut" style="color:var(--amber)">—</div><div class="d">volumen del 5º puesto</div></div>
    <div class="card kpi"><div class="k">Líder</div><div class="v" id="k-lead" style="font-size:16px">—</div><div class="d" id="k-lead-d"></div></div>
  </div>

  <section>
    <div class="sec-head"><div class="sec-title">Clasificación · boost por volumen</div>
      <div class="sec-title" style="text-transform:none;letter-spacing:0" id="rk-meta"></div></div>
    <div class="card" id="ranking"><div class="empty">cargando clasificación…</div></div>
  </section>

</main>

<footer><div class="wrap">volume league · RISEx — volumen taker + maker · calculado en el servidor sobre la base de datos
  del indexador de risexscan · se actualiza solo cada minuto</div></footer>

<script>
(() => {
  const $ = s => document.querySelector(s);
  const usd = n => {
    const a=Math.abs(n||0), s=n<0?'-':'';
    if(a>=1e9) return s+'$'+(a/1e9).toFixed(2)+'B';
    if(a>=1e6) return s+'$'+(a/1e6).toFixed(2)+'M';
    if(a>=1e3) return s+'$'+(a/1e3).toFixed(1)+'K';
    return s+'$'+a.toFixed(2);
  };
  const short = a => a ? a.slice(0,6)+'…'+a.slice(-4) : '—';
  const hhmm = ms => new Date(ms).toLocaleTimeString('es-ES',{hour:'2-digit',minute:'2-digit'});
  const pad = n => String(n).padStart(2,'0');

  let data = null;
  let START_MS = Date.UTC(2026, 6, 22, 0, 0, 0);
  let END_MS   = Date.UTC(2026, 7, 1, 0, 0, 0);
  let MIN_VOL  = 200000;
  let TOP_N    = 5;

  function tick(){
    const now = Date.now();
    if(now < START_MS){
      const s = Math.floor((START_MS-now)/1000);
      const h = Math.floor(s/3600), m = Math.floor((s%3600)/60), ss = s%60;
      $('#hero-lbl').textContent = 'LA COMPETICIÓN EMPIEZA EN';
      $('#hero-big').textContent = pad(h)+':'+pad(m)+':'+pad(ss);
      $('#hero').classList.remove('live');
    } else if(now < END_MS){
      const s = Math.floor((END_MS-now)/1000);
      const d = Math.floor(s/86400), h = Math.floor((s%86400)/3600), m = Math.floor((s%3600)/60);
      $('#hero-lbl').textContent = '● EN MARCHA — TERMINA EN';
      $('#hero-big').textContent = (d>0? d+'d ' : '')+h+'h '+m+'m';
      $('#hero').classList.add('live');
    } else {
      $('#hero-lbl').textContent = '🏁 COMPETICIÓN FINALIZADA';
      $('#hero-big').textContent = 'resultados finales';
      $('#hero').classList.add('live');
    }
  }

  async function load(){
    try{
      const r = await fetch('/api/league', {cache:'no-store'});
      if(!r.ok) throw new Error('HTTP '+r.status);
      const d = await r.json();
      if(!d.ok) throw new Error(d.error || 'error');
      data = d;
      START_MS = d.start * 1000;
      END_MS   = d.end * 1000;
      MIN_VOL  = d.min_vol;
      TOP_N    = d.top_n;
      $('#upd').textContent = 'actualizado ' + hhmm((d.updated||0)*1000 || Date.now());
      render();
    }catch(e){
      $('#upd').textContent = 'error — reintentando…';
    }
  }

  function render(){
    if(!data) return;
    const started = Date.now() >= START_MS;
    const rows = data.rows || [];
    const tot = data.total_vol || 0;
    const q = data.qualified || 0;
    if(started && tot === 0) $('#upd').textContent = 'reindexando histórico…';

    $('#k-vol').textContent = started && rows.length ? usd(tot) : '—';
    $('#k-q').textContent = started && rows.length ? q : '—';
    $('#k-q-d').textContent = '≥ '+usd(MIN_VOL)+' de volumen · de '+rows.length;
    const cut = rows[TOP_N-1];
    $('#k-cut').textContent = started && cut && (cut.vol||0)>0 ? usd(cut.vol) : '—';
    const lead = rows[0];
    if(started && lead && (lead.vol||0)>0){
      $('#k-lead').textContent = short(lead.account);
      $('#k-lead-d').textContent = usd(lead.vol)+' · '+(100*(lead.vol||0)/(tot||1)).toFixed(0)+'% del total';
    } else { $('#k-lead').textContent='—'; $('#k-lead-d').textContent=''; }

    $('#rk-meta').textContent = rows.length + ' participantes';
    if(!started){
      $('#ranking').innerHTML = '<div class="empty">⏳ el marcador arranca a las 00:00 UTC del 22 de julio<br><br>'+
        rows.length+' wallets inscritas y listas</div>';
      return;
    }
    if(!rows.length){
      $('#ranking').innerHTML = '<div class="empty">sin datos aún…</div>';
      return;
    }
    const max = Math.max(...rows.map(r=>r.vol||0), 1);
    $('#ranking').innerHTML = rows.map((r,i)=>{
      const v = r.vol||0;
      const barw = 100*v/max;
      const cls = (i===0?'p1':i===1?'p2':i===2?'p3':'') + (i<TOP_N && v>0 ? ' top5' : '');
      const medal = i===0?'🥇':i===1?'🥈':i===2?'🥉':(i+1);
      let badge;
      if(i < TOP_N && v > 0) badge = '<div class="badge b20">🚀 +20% BOOST</div>';
      else if(v >= MIN_VOL)  badge = '<div class="badge b5">✅ +5% BOOST</div>';
      else if(v > 0)         badge = '<div class="bfalta">faltan <b>'+usd(MIN_VOL - v)+'</b> para +5%</div>';
      else                   badge = '<div class="bfalta">sin actividad</div>';
      const meta = v>0 ? (r.trades||0)+' trades'+(r.last?' · último '+hhmm(r.last*1000):'') : '—';
      return '<div class="rk '+cls+'">'+
        '<div class="bar" style="width:'+barw.toFixed(1)+'%"></div>'+
        '<div class="pos">'+medal+'</div>'+
        '<div class="who"><div class="addr" title="'+r.account+'">'+short(r.account)+'</div>'+
          '<div class="meta">'+meta+'</div></div>'+
        '<div class="num"><div class="vol">'+usd(v)+'</div>'+badge+'</div>'+
      '</div>';
    }).join('');
  }

  $('#btn-scan').addEventListener('click', load);
  tick(); setInterval(tick, 1000);
  load(); setInterval(load, 60000);
})();
</script>
</body>
</html>
"""


ABOUT_HTML = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>About · RISExscan</title>
<meta name="description" content="About RISExscan — the community-built real-time analytics dashboard for RISE chain perpetual futures.">
<link rel="canonical" href="https://risexscan.io/about">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Inter:wght@400;600;700;800&display=swap" rel="stylesheet">
<style>""" + _INFO_PAGE_CSS + """</style></head><body><div class="wrap">""" + _INFO_BRAND + """
<div class="eyebrow">About</div>
<h1>The reference dashboard for RISE chain perps</h1>
<p class="lead">RISExscan (<code>risexscan.io</code>) is a community-built real-time analytics dashboard for RISEx, the perpetual futures DEX on RISE chain. Built from public data only — no accounts, no tracking, no premium tiers.</p>

<h2>What it does</h2>
<p>Everything a serious perp trader, analyst, or researcher needs in one place — refreshed every few seconds, free, with no signup:</p>
<ul>
 <li><b>Market data</b> — live prices, OI, volume, funding rates, candle charts with CVD overlay and whale trade markers</li>
 <li><b>Trader leaderboards</b> — top by volume, OI, realized PnL with periods (1d/7d/30d/all), edge in bps, smart-money flags</li>
 <li><b>Wallet deep-dive</b> — equity curve, win rate, profit factor, drawdown, per-market PnL, activity heatmap, calendar view</li>
 <li><b>Live activity</b> — liquidations feed, whale trades feed, real-time funding payments leaderboard</li>
 <li><b>Cross-DEX comparison</b> — funding rates vs Pacifica and Hyperliquid for every market</li>
 <li><b>Tools</b> — position simulator with liquidation price calculator, funding cost projector</li>
 <li><b>Discoverability</b> — ⌘K command palette, shareable OG cards for wallets and markets, full data export to CSV</li>
</ul>

<h2>How it works</h2>
<p>All data comes from <b>public sources</b>:</p>
<ul>
 <li><code>api.rise.trade</code> — the official RISEx REST API for markets, trade history, positions, orderbook</li>
 <li><b>RISE chain RPC</b> — direct onchain calls to <code>PerpsManager</code> and <code>CollateralManager</code> contracts for accurate cross-margin states, liquidation prices, funding</li>
 <li><b>RISE chain explorer</b> — for protocol-wide user counts and adoption metrics</li>
 <li><b>External DEX APIs</b> — Pacifica and Hyperliquid public endpoints for funding rate comparison</li>
</ul>
<p>Background indexers scan accounts and events continuously. The frontend polls every 15–30 seconds for live data. Nothing is stored about you — your IP, browser, behavior, none of it leaves your device.</p>

<h2>Why it exists</h2>
<p>RISE chain is a new ecosystem and RISEx has the data, but nobody was visualizing it in a way that lets traders make sense of what's happening minute-to-minute. The Hyperliquid ecosystem has <a href="https://hypurrscan.io" target="_blank">hypurrscan</a> and similar. RISE chain deserved the same.</p>
<p>The goal is to be the canonical source of perp analytics for RISE chain — the page everyone bookmarks, the link everyone shares.</p>

<h2>Open & transparent</h2>
<p>Every metric on the dashboard has a method behind it. Read the <a href="/methodology">methodology</a> page for exact formulas and data sources. The codebase is in Python and JavaScript, served from a single file — no obfuscation, no hidden trackers.</p>

<h2>Support the project</h2>
<p>If RISExscan has been useful for you, the best support is to use the <b>"Trade on RISEx" button</b> when you next open a position. It uses the creator's referral link and helps cover hosting + dev costs. Zero added cost for you, fully transparent.</p>
<a class="cta" href="https://www.rise.trade/invite/ticb" target="_blank">Trade on RISEx →</a>

<h2>Contact</h2>
<p>Found a bug? Have a feature request? Want to collaborate? The fastest way is to ping <code>@ticb</code> on Twitter/X or open the in-app feedback flow.</p>

<div class="footer">RISExscan · risexscan.io · Built on public RISE chain data · Open source · No accounts, no tracking.</div>
</div></body></html>"""


METHODOLOGY_HTML = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Methodology · RISExscan</title>
<meta name="description" content="How RISExscan computes every metric — sources, formulas, edge cases.">
<link rel="canonical" href="https://risexscan.io/methodology">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Inter:wght@400;600;700;800&display=swap" rel="stylesheet">
<style>""" + _INFO_PAGE_CSS + """</style></head><body><div class="wrap">""" + _INFO_BRAND + """
<div class="eyebrow">Methodology</div>
<h1>How every metric is computed</h1>
<p class="lead">Full transparency on data sources, formulas, and edge cases. If a number on the dashboard surprises you, this page is where you check what it means.</p>

<h2>Sources</h2>
<div class="card">
 <h3>Primary</h3>
 <div class="metricdef"><div class="lbl">api.rise.trade</div><div class="desc">Markets, trade history, positions, orderbook, candles (trading-view-data), market-specific trade tape. Official REST.</div></div>
 <div class="metricdef"><div class="lbl">RISE chain RPC</div><div class="desc">Direct eth_call to <code>PerpsManager</code> for cross-margin maintenance, funding, unsettled USDC. <code>CollateralManager</code> for portfolio balance (including non-USDC collateral).</div></div>
 <div class="metricdef"><div class="lbl">Event logs</div><div class="desc">PerpsManager <code>TakerFee</code> + <code>MakerSettle</code> events for real 24h fees. AccountRegistry events for the universe of accounts.</div></div>
 <div class="metricdef"><div class="lbl">RISE explorer</div><div class="desc">Aggregated protocol-wide stats (total accounts, daily new accounts, growth charts).</div></div>
 <div class="metricdef"><div class="lbl">External DEXes</div><div class="desc">Pacifica + Hyperliquid public APIs for cross-DEX funding rate comparison.</div></div>
</div>

<h2>Volume metrics</h2>
<div class="card">
 <div class="metricdef"><div class="lbl">24h Volume</div><div class="desc">Sum of <code>quote_volume_24h</code> across all markets from RISEx's market list endpoint.</div></div>
 <div class="metricdef"><div class="lbl">Account volume</div><div class="desc">For each account, paginate <code>/v1/trade-history</code> and sum <code>price × size</code> per trade. Bucketed by 1d / 7d / 30d / custom-since periods.</div></div>
 <div class="metricdef"><div class="lbl">Trades counter</div><div class="desc">Count of distinct trade entries in the last 30 days for that account.</div></div>
</div>

<h2>Open interest</h2>
<div class="card">
 <div class="metricdef"><div class="lbl">Current OI</div><div class="desc">Sum of <code>|size| × mark_price</code> across all currently open positions. Snapshot, not time-weighted.</div></div>
 <div class="metricdef"><div class="lbl">TWAP OI</div><div class="desc">Time-Weighted Average OI: integral of OI over the window divided by duration. Reconstructed from trade events (signed size changes per market per timestamp), with mark price at each event.</div></div>
 <div class="metricdef"><div class="lbl">Long / Short ratio</div><div class="desc">Sum of long-side notional vs short-side notional across all positions in a market. Skew = long% − 50%.</div></div>
</div>

<h2>PnL & performance</h2>
<div class="card">
 <div class="metricdef"><div class="lbl">Realized PnL</div><div class="desc">Sum of <code>realized_pnl</code> field on each trade in the trade-history. Aggregated per window (1d/7d/30d).</div></div>
 <div class="metricdef"><div class="lbl">Unrealized PnL</div><div class="desc">Per open position: <code>size × (mark − entry) × side</code>, where side is +1 long, −1 short. Aggregated across positions for the account total.</div></div>
 <div class="metricdef"><div class="lbl">Win rate</div><div class="desc">Wins ÷ (Wins + Losses), where wins are trades with realized_pnl &gt; 0, losses with &lt; 0. Zero-PnL trades ignored.</div></div>
 <div class="metricdef"><div class="lbl">Profit factor</div><div class="desc">Sum of all winning PnL ÷ |sum of all losing PnL|. &gt;1.5 strong, &gt;2 excellent, &lt;1 net losing.</div></div>
 <div class="metricdef"><div class="lbl">Max drawdown</div><div class="desc">Largest peak-to-trough drop in cumulative realized PnL during the period. Reconstructed by walking trades chronologically.</div></div>
 <div class="metricdef"><div class="lbl">Edge (bps)</div><div class="desc"><code>(PnL ÷ Volume) × 10000</code>. The trader's average per-dollar margin in basis points. Pros sustain 5–20 bps; degens swing widely.</div></div>
 <div class="metricdef"><div class="lbl">Smart Money</div><div class="desc">Composite flag. Requires: ≥50 trades in 30d, ≥$250k volume, ≥55% win rate, 0 liquidations, drawdown &lt; ½ of PnL.</div></div>
</div>

<h2>Funding rates</h2>
<div class="card">
 <div class="metricdef"><div class="lbl">Funding 8h</div><div class="desc">Raw <code>funding_rate_8h</code> from RISEx markets endpoint. Long pays short if positive.</div></div>
 <div class="metricdef"><div class="lbl">Funding APR</div><div class="desc">8h rate × 3 × 365 × 100 (percent). Naïve annualization assuming the rate stays constant.</div></div>
 <div class="metricdef"><div class="lbl">Funding payments (snapshot)</div><div class="desc">Onchain <code>getTotalCrossFunding</code> per account. Represents unsettled funding accumulated since the account's last settlement — not historical totals.</div></div>
 <div class="metricdef"><div class="lbl">Cross-DEX deltas</div><div class="desc">Difference of APRs between RISEx and each external DEX, positive = longs pay more on RISEx.</div></div>
</div>

<h2>Liquidations</h2>
<div class="card">
 <div class="metricdef"><div class="lbl">Liquidation price (isolated)</div><div class="desc"><code>entry × (1 ∓ 1/L + dir × MMR)</code>. MMR (maintenance margin ratio) read from the market config.</div></div>
 <div class="metricdef"><div class="lbl">Liquidation price (cross)</div><div class="desc">Considers total cross-margin balance + uPnL of all other cross positions. Solved as: <code>liq = (entry × size − totEq) ÷ (size × (1 − MMR))</code> for longs, mirror for shorts.</div></div>
 <div class="metricdef"><div class="lbl">Liquidation feed</div><div class="desc">Trade events with <code>is_liquidation = true</code> in trade-history, captured by the indexer scanning all active accounts.</div></div>
 <div class="metricdef"><div class="lbl">Liquidation heatmap</div><div class="desc">Bucketed sum of position notional that would be liquidated if the price moved to each bin. Built from open positions snapshot.</div></div>
</div>

<h2>CVD overlay</h2>
<div class="card">
 <p>CVD = <b>Cumulative Volume Delta</b>. Running sum of <code>taker-buy notional − taker-sell notional</code>, where taker side is inferred from <code>maker_side</code> in market trade-history (if maker is SELL, taker bought; positive delta).</p>
 <p>Bucketed at the same resolution as the candles (1m/5m/15m/1h/4h/1d). Divergences between CVD and price often signal hidden buying or selling pressure.</p>
</div>

<h2>Fees</h2>
<div class="card">
 <div class="metricdef"><div class="lbl">24h Fees (real)</div><div class="desc">Sum of fee amount from <code>TakerFee</code> and <code>MakerSettle</code> events on the PerpsManager contract, anchored to a 24h window by block timestamp.</div></div>
 <div class="metricdef"><div class="lbl">Account fees</div><div class="desc">Sum of <code>fee</code> field on each trade in the 30d trade-history.</div></div>
</div>

<h2>Caching & freshness</h2>
<p>Live data (markets, positions, balances): refreshed every <b>15-30 seconds</b>.</p>
<p>Indexed data (volume/PnL leaderboards, OI rankings): full re-scan every <b>10 minutes</b>. Individual account stats refresh on-demand if older than 15 min.</p>
<p>Cross-DEX funding compare: cached <b>30 seconds</b>.</p>
<p>Sparklines and historical series: refreshed every <b>5 minutes</b>.</p>

<h2>Limitations</h2>
<p>The trade-history endpoint is paginated and capped — for very high-volume accounts with &gt;10000 trades in 30d, only the most recent N are aggregated. This affects the precision of PnL and volume for the top 1% most active wallets but doesn't change rankings materially.</p>
<p>Onchain calls are subject to RPC rate limits. If the RPC is slow, balances and liquidation prices may be a few seconds stale.</p>
<p>Cross-DEX funding for low-activity markets on Lighter was found to be unreliable and is excluded from comparison.</p>

<a class="cta" href="/">Back to dashboard →</a>
<div class="footer">RISExscan · risexscan.io · Built on public RISE chain data · Open source · No accounts, no tracking.</div>
</div></body></html>"""


NOT_FOUND_HTML = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><title>404 · RISExscan</title>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@600;700&family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#06090c;color:#f0f4f8;font-family:-apple-system,Inter,sans-serif;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:24px;position:relative;overflow:hidden}
body::before{content:"";position:fixed;width:520px;height:520px;border-radius:50%;filter:blur(120px);top:-180px;right:-160px;background:radial-gradient(circle,rgba(0,255,212,.18),transparent 70%)}
.box{text-align:center;max-width:520px;position:relative;z-index:1}
.code{font-family:'JetBrains Mono',monospace;font-size:120px;font-weight:700;background:linear-gradient(180deg,#00ffd4,#0CD8B7);-webkit-background-clip:text;background-clip:text;color:transparent;letter-spacing:-4px;line-height:1;margin-bottom:12px;text-shadow:0 0 40px rgba(0,255,212,.3)}
h1{font-size:24px;font-weight:700;margin-bottom:14px}
p{color:#7a8694;margin-bottom:28px;font-size:14px;line-height:1.5}
code{font-family:'JetBrains Mono',monospace;font-size:13px;color:#00ffd4;background:rgba(0,255,212,.08);padding:2px 8px;border-radius:4px}
a{color:#00ffd4;text-decoration:none;padding:11px 22px;background:rgba(0,255,212,.06);border:1px solid rgba(0,255,212,.3);border-radius:8px;font-weight:600;font-size:13.5px;display:inline-block;transition:all .15s}
a:hover{background:rgba(0,255,212,.12);transform:translateY(-1px)}
</style></head><body>
<div class="box">
 <div class="code">404</div>
 <h1>This route doesn't exist on RISE chain (yet).</h1>
 <p>The path you requested wasn't found. Maybe it's coming next deploy.</p>
 <a href="/">← Back to stats</a>
</div>
</body></html>"""

HTML = r"""<!DOCTYPE html>
<html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>RISExscan · Live perp DEX analytics on RISE chain</title>
<meta name="description" content="Real-time analytics for RISEx, the perpetual futures DEX on RISE chain. Track markets, traders, liquidations, funding rates, smart-money wallets and more. No accounts, all public data.">
<meta name="keywords" content="RISExscan, RISEx, RISE chain, perp DEX, perpetual futures, crypto analytics, trading dashboard, smart money, liquidations, funding rates, on-chain analytics, DeFi, perpetuals">
<meta name="author" content="risexscan.io">
<meta name="theme-color" content="#04DF83">
<link rel="canonical" href="https://risexscan.io/">
<!-- Open Graph / Social cards -->
<meta property="og:type" content="website">
<meta property="og:title" content="RISExscan · Live perp DEX analytics on RISE chain">
<meta property="og:description" content="Real-time analytics for RISEx — markets, traders, smart money, liquidations, funding. The reference dashboard for RISE chain perps.">
<meta property="og:url" content="https://risexscan.io/">
<meta property="og:site_name" content="RISExscan">
<meta property="og:image" content="https://risexscan.io/og/home.svg">
<meta property="og:image:width" content="1200">
<meta property="og:image:height" content="630">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="RISExscan · Live perp analytics">
<meta name="twitter:description" content="Real-time analytics for RISEx. Markets, traders, smart money, liquidations, funding rates. Built on public RISE chain data.">
<meta name="twitter:image" content="https://risexscan.io/og/home.svg">
<!-- Structured data -->
<script type="application/ld+json">
{
 "@context":"https://schema.org",
 "@type":"WebApplication",
 "name":"RISExscan",
 "alternateName":"risexscan.io",
 "url":"https://risexscan.io",
 "description":"Real-time analytics dashboard for RISEx, the perpetual futures DEX on RISE chain.",
 "applicationCategory":"FinanceApplication",
 "operatingSystem":"Any",
 "offers":{"@type":"Offer","price":"0","priceCurrency":"USD"},
 "creator":{"@type":"Organization","name":"RISExscan","url":"https://risexscan.io"}
}
</script>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='555 -10 210 200'><rect x='555' y='-10' width='210' height='200' fill='%23080809'/><path d='M675.798 83.41C690.4 71.16 712.646 76.09 720.7 93.37L762.031 182H733.687L698.974 107.56C693.603 96.04 678.77 92.76 669.036 100.92L572.632 181.82H558.466L675.798 83.41ZM585.345 0L620.058 74.44C625.428 85.96 640.261 89.24 649.995 81.08L746.399 0.18H760.565L643.233 98.59C628.631 110.84 606.386 105.91 598.331 88.63L557 0H585.345Z' fill='%2304DF83'/></svg>">
<link rel="preconnect" href="https://cdn.jsdelivr.net" crossorigin>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="dns-prefetch" href="https://api.rise.trade">
<!-- Preload Chart.js so it downloads in parallel with HTML parse -->
<link rel="preload" as="script" href="https://cdn.jsdelivr.net/npm/chart.js@4.5.0/dist/chart.umd.js" crossorigin>
<!-- Prefetch critical overview APIs (browser starts them during HTML parse) -->
<link rel="prefetch" as="fetch" href="/api/data" crossorigin>
<link rel="prefetch" as="fetch" href="/api/history" crossorigin>
<link rel="prefetch" as="fetch" href="/api/users" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.5.0/dist/chart.umd.js" defer></script>
<!-- Kick off the slow API fetches NOW (in parallel with HTML parsing). The main JS reuses these via window._earlyFetch. -->
<script>
window._earlyFetch = {
 data:    fetch('/api/data',    {cache:'no-store'}).then(r=>r.json()).catch(()=>null),
 history: fetch('/api/history').then(r=>r.json()).catch(()=>null),
 users:   fetch('/api/users').then(r=>r.json()).catch(()=>null)
};
</script>
<style>
:root{
 --bg:#06090c;--bg2:#080b0f;--panel:#0d1217;--panel2:#101620;--line:#1a2129;--line2:#141a22;
 --txt:#f0f4f8;--muted:#7a8694;--muted2:#3f4856;
 /* Saturated mint accent */
 --accent:#00ffd4;--accent-lo:#0CD8B7;--accent2:#1aeeb3;--accent-glow:rgba(0,255,212,.22);
 /* Vivid PnL */
 --green:#1aeeaa;--green-lo:#0db883;--red:#ff3b6e;--red-lo:#c92850;--amber:#ffb854;
 --mono:'JetBrains Mono',ui-monospace,Menlo,monospace;
 color-scheme:dark}
[data-theme="light"]{--bg:#f6f8fa;--bg2:#eef1f4;--panel:#ffffff;--panel2:#f7f9fb;
 --line:#dfe4ea;--line2:#e6eaee;--txt:#11181f;--muted:#54616d;--muted2:#9aa3ad;
 --accent:#0d1117;--accent2:#0CD8B7;--accent-glow:rgba(12,216,183,.10);color-scheme:light}
*{box-sizing:border-box}
html,body{margin:0;padding:0;background:var(--bg);color:var(--txt);font-family:-apple-system,BlinkMacSystemFont,"Inter","Segoe UI",Roboto,sans-serif;-webkit-font-smoothing:antialiased;text-rendering:optimizeLegibility}
body{position:relative;overflow-x:hidden}
body::before,body::after{content:"";position:fixed;width:520px;height:520px;border-radius:50%;filter:blur(120px);pointer-events:none;z-index:0;will-change:transform;animation:floatOrb 38s ease-in-out infinite}
body::before{top:-180px;right:-160px;background:radial-gradient(circle,rgba(0,255,212,.20),transparent 70%)}
body::after{bottom:-200px;left:-180px;background:radial-gradient(circle,rgba(26,238,179,.14),transparent 70%);animation-delay:-19s}
@keyframes floatOrb{0%,100%{transform:translate(0,0) scale(1)}50%{transform:translate(40px,-30px) scale(1.06)}}
.app{position:relative;z-index:1}

/* Section divider with gradient */
.divider{height:1px;border:0;margin:24px 0;background:linear-gradient(90deg,transparent 0%,rgba(0,255,212,.28) 50%,transparent 100%)}

/* Hero section */
.hero{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:18px}
.hero-card{position:relative;padding:22px 26px;border:1px solid var(--line);border-radius:14px;background:linear-gradient(135deg,rgba(0,255,212,.04) 0%,var(--panel) 45%,#0b1015 100%);overflow:hidden;box-shadow:inset 0 1px 0 rgba(0,255,212,.08),0 6px 18px rgba(0,0,0,.4)}
.hero-card::before{content:"";position:absolute;top:-50%;right:-30%;width:80%;height:200%;background:radial-gradient(closest-side,rgba(0,255,212,.10),transparent 70%);pointer-events:none}
.hero-card::after{content:"";position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,transparent,var(--accent),transparent);background-size:200% 100%;animation:slideHi 4s linear infinite;opacity:.55}
.hero-lbl{position:relative;font-size:11px;font-weight:700;letter-spacing:1.2px;color:var(--accent);text-transform:uppercase;display:flex;align-items:center;gap:8px}
.hero-val{position:relative;font-family:var(--mono);font-size:48px;font-weight:700;letter-spacing:-1.5px;color:#fff;line-height:1.02;margin-top:8px;text-shadow:0 0 32px rgba(0,255,212,.18)}
.hero-meta{position:relative;font-size:12.5px;color:var(--muted);margin-top:8px;display:flex;align-items:center;gap:10px;font-weight:500}
.hero-spark{position:absolute;bottom:0;right:0;left:0;width:100%;height:58px;opacity:.65;pointer-events:none}
.livedot{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--accent);box-shadow:0 0 0 0 rgba(0,255,212,.5);animation:pulse 2.2s ease-out infinite}

/* Cursor-following glow on hero (Apple/Linear-style) */
.hero{position:relative}
.hero::after{content:"";position:absolute;width:300px;height:300px;border-radius:50%;background:radial-gradient(circle,rgba(0,255,212,.13),transparent 65%);pointer-events:none;left:var(--mx,50%);top:var(--my,50%);transform:translate(-50%,-50%);transition:opacity .3s;opacity:0;z-index:2}
.hero:hover::after{opacity:1}

/* Splash loader (covers screen for first 700ms) */
#splash{position:fixed;inset:0;background:#06090c;z-index:9999;display:flex;align-items:center;justify-content:center;flex-direction:column;gap:18px;transition:opacity .35s ease;animation:splashOut .9s 1.4s forwards}
@keyframes splashOut{0%{opacity:1;pointer-events:auto}90%{opacity:0;pointer-events:none}100%{opacity:0;visibility:hidden;pointer-events:none}}
#splash .splash-logo{width:64px;height:64px;border-radius:14px;background:linear-gradient(135deg,#0d141a,#070b0f);border:1px solid rgba(0,255,212,.25);display:flex;align-items:center;justify-content:center;box-shadow:0 0 40px rgba(0,255,212,.18);position:relative;overflow:hidden}
#splash .splash-logo::before{content:"";position:absolute;inset:-2px;background:linear-gradient(90deg,transparent,rgba(0,255,212,.5),transparent);background-size:200% 100%;animation:shimSplash 1.2s linear infinite}
#splash .splash-logo svg{position:relative;z-index:2;width:36px;height:auto}
#splash .splash-text{font-family:-apple-system,Inter,sans-serif;color:var(--accent);font-size:11px;font-weight:700;letter-spacing:3px;text-transform:uppercase}
@keyframes shimSplash{0%{background-position:200% 0}100%{background-position:-200% 0}}

/* Price ticker band — fixed at bottom of viewport, always visible */
.priceticker{position:fixed;bottom:0;left:0;right:0;z-index:80;display:flex;align-items:center;height:28px;background:rgba(7,10,13,.92);backdrop-filter:saturate(160%) blur(12px);-webkit-backdrop-filter:saturate(160%) blur(12px);border-top:1px solid var(--line);overflow:hidden;opacity:.95;box-shadow:0 -4px 20px rgba(0,0,0,.4)}
body{padding-bottom:28px}
.priceticker:hover{opacity:1}
.priceticker .pt-track{display:flex;gap:36px;padding:0 20px;animation:pttick 95s linear infinite;white-space:nowrap;font-family:var(--mono);font-size:11px;font-weight:600}
.pt-asset{display:inline-flex;align-items:center;gap:7px;color:var(--muted2)}
.pt-asset .pt-sym{color:var(--muted);font-weight:700;letter-spacing:.3px}
.pt-asset .pt-px{color:var(--accent2)}
.pt-asset svg.pt-sk{width:38px;height:12px;display:inline-block;vertical-align:middle;opacity:.7}
@keyframes pttick{from{transform:translateX(0)}to{transform:translateX(-50%)}}
.priceticker:hover .pt-track{animation-play-state:paused}

/* ===== Command palette ⌘K ===== */
.cmdk-back{position:fixed;inset:0;background:rgba(6,9,12,.78);backdrop-filter:blur(6px);-webkit-backdrop-filter:blur(6px);z-index:9000;display:none;align-items:flex-start;justify-content:center;padding-top:14vh;animation:cmdkIn .18s ease}
.cmdk-back.open{display:flex}
@keyframes cmdkIn{from{opacity:0}to{opacity:1}}
.cmdk{width:min(620px,92vw);background:linear-gradient(180deg,#0e1218,#0a0e13);border:1px solid var(--line);border-radius:12px;box-shadow:0 24px 80px rgba(0,0,0,.7),0 0 60px rgba(0,255,212,.06);overflow:hidden}
.cmdk-input{width:100%;padding:16px 18px;background:transparent;border:none;border-bottom:1px solid var(--line2);color:var(--txt);font-size:15px;font-family:-apple-system,Inter,sans-serif}
.cmdk-input:focus{outline:none}
.cmdk-list{max-height:54vh;overflow-y:auto;padding:6px 0}
.cmdk-list::-webkit-scrollbar{width:6px}.cmdk-list::-webkit-scrollbar-thumb{background:var(--line);border-radius:3px}
.cmdk-sec{padding:6px 18px 4px;font-size:9.5px;font-weight:700;color:var(--muted2);text-transform:uppercase;letter-spacing:1.2px}
.cmdk-item{display:flex;align-items:center;gap:11px;padding:10px 18px;cursor:pointer;color:var(--txt);font-size:13.5px}
.cmdk-item:hover,.cmdk-item.sel{background:rgba(0,255,212,.07)}
.cmdk-item .ic{width:20px;font-size:14px;color:var(--accent2)}
.cmdk-item .lab{flex:1}
.cmdk-item .sub{color:var(--muted);font-size:11.5px;font-family:var(--mono)}
.cmdk-foot{padding:9px 18px;border-top:1px solid var(--line2);font-size:10.5px;color:var(--muted);display:flex;gap:14px}
.cmdk-foot kbd{background:var(--panel2);border:1px solid var(--line);border-radius:3px;padding:1px 5px;font-family:var(--mono);font-size:10px;color:var(--accent2)}

/* ===== Right-click context menu ===== */
.ctxmenu{position:fixed;background:linear-gradient(180deg,#0e1218,#0a0e13);border:1px solid var(--line);border-radius:8px;box-shadow:0 12px 40px rgba(0,0,0,.6);min-width:200px;z-index:8000;padding:5px;animation:cmdkIn .12s ease}
.ctxmenu .it{display:flex;align-items:center;gap:10px;padding:8px 12px;cursor:pointer;border-radius:5px;font-size:12.5px;color:var(--txt);font-family:-apple-system,Inter,sans-serif}
.ctxmenu .it:hover{background:rgba(0,255,212,.08);color:var(--accent)}
.ctxmenu .it.sep{height:1px;background:var(--line2);padding:0;margin:4px 0;cursor:default}
.ctxmenu .it.sep:hover{background:var(--line2)}
.ctxmenu .ic{width:14px;color:var(--accent2)}

/* ===== Sticky table headers ===== */
.panel table thead{position:sticky;top:0;background:#0e1218;z-index:5;box-shadow:0 1px 0 var(--line)}
.panel table thead th{background:#0e1218}

/* ===== Breadcrumbs ===== */
.crumbs{display:flex;align-items:center;gap:8px;padding:10px 28px 0;font-size:12.5px;color:var(--muted);font-family:-apple-system,Inter,sans-serif}
.crumbs a{color:var(--accent2)}.crumbs .sep{color:var(--muted2)}
.crumbs .cur{color:var(--txt);font-weight:600}

/* ===== Color themes (4 variants) ===== */
[data-theme="magenta"]{--accent:#ff66e0;--accent-lo:#cc52b3;--accent2:#ff8aee;--accent-glow:rgba(255,102,224,.22);--green:#1aeeaa;--red:#ff3b6e}
[data-theme="solar"]{--accent:#ffbf3d;--accent-lo:#e89e1f;--accent2:#ffd070;--accent-glow:rgba(255,191,61,.22);--green:#1aeeaa;--red:#ff3b6e}
[data-theme="mono"]{--accent:#ffffff;--accent-lo:#c0c0c0;--accent2:#dddddd;--accent-glow:rgba(255,255,255,.10);--green:#ffffff;--red:#888888}
[data-theme="magenta"] body::before{background:radial-gradient(circle,rgba(255,102,224,.20),transparent 70%)}
[data-theme="magenta"] body::after{background:radial-gradient(circle,rgba(255,138,238,.14),transparent 70%)}
[data-theme="solar"] body::before{background:radial-gradient(circle,rgba(255,191,61,.20),transparent 70%)}
[data-theme="solar"] body::after{background:radial-gradient(circle,rgba(255,208,112,.14),transparent 70%)}
[data-theme="mono"] body::before,[data-theme="mono"] body::after{background:radial-gradient(circle,rgba(255,255,255,.05),transparent 70%)}

/* Theme picker popover */
.themepicker{position:relative;display:inline-block}
.themepicker .swatches{position:absolute;right:0;top:calc(100% + 8px);background:linear-gradient(180deg,#0e1218,#0a0e13);border:1px solid var(--line);border-radius:10px;padding:8px;display:none;gap:6px;flex-direction:column;min-width:160px;z-index:50;box-shadow:0 12px 36px rgba(0,0,0,.6)}
.themepicker.open .swatches{display:flex}
.swatch{display:flex;align-items:center;gap:9px;padding:7px 9px;border-radius:6px;cursor:pointer;font-size:12px;color:var(--txt);font-family:-apple-system,Inter,sans-serif;border:1px solid transparent}
.swatch:hover{background:rgba(0,255,212,.05)}
.swatch.active{background:rgba(0,255,212,.08);border-color:rgba(0,255,212,.20)}
.swatch .dotc{width:14px;height:14px;border-radius:50%;flex:0 0 14px;box-shadow:0 0 8px currentColor}
.swatch[data-th="mint"] .dotc{background:#00ffd4;color:#00ffd4}
.swatch[data-th="magenta"] .dotc{background:#ff66e0;color:#ff66e0}
.swatch[data-th="solar"] .dotc{background:#ffbf3d;color:#ffbf3d}
.swatch[data-th="mono"] .dotc{background:#ffffff;color:#ffffff}
.swatch[data-th="light"] .dotc{background:#f6f8fa;color:#f6f8fa;box-shadow:0 0 0 1px rgba(0,0,0,.1)}

/* ===== Identicon (deterministic) ===== */
.identicon{display:inline-block;width:42px;height:42px;border-radius:8px;overflow:hidden;background:#0a0e13;border:1px solid rgba(0,255,212,.2);position:relative}
.identicon svg{display:block;width:100%;height:100%}

/* ===== Market detail background watermark ===== */
.market-watermark{position:absolute;top:30px;right:30px;font-size:200px;font-weight:900;color:rgba(0,255,212,.025);font-family:-apple-system,Inter,sans-serif;pointer-events:none;line-height:1;letter-spacing:-12px;user-select:none;z-index:0}

/* ===== Footer ===== */
.footer{margin-top:48px;padding:28px 0 20px;border-top:1px solid var(--line2);text-align:center;color:var(--muted);font-size:11.5px;font-family:-apple-system,Inter,sans-serif}
.footer .row{display:flex;gap:18px;justify-content:center;align-items:center;flex-wrap:wrap;margin-bottom:10px}
.footer a{color:var(--accent2)}
.footer .built{color:var(--accent);font-weight:600;letter-spacing:.3px}
.footer .uptime{font-family:var(--mono);color:var(--muted)}

/* ===== Toast notifications ===== */
.toasts{position:fixed;top:24px;right:24px;z-index:8500;display:flex;flex-direction:column;gap:8px;pointer-events:none}
.toast{pointer-events:auto;background:linear-gradient(180deg,#0e1218,#0a0e13);border:1px solid var(--line);border-left:3px solid var(--accent);border-radius:8px;padding:11px 16px 11px 14px;font-family:-apple-system,Inter,sans-serif;font-size:12.5px;color:var(--txt);box-shadow:0 12px 30px rgba(0,0,0,.6);min-width:240px;max-width:340px;animation:toastIn .25s cubic-bezier(.3,1.3,.4,1) both;display:flex;align-items:center;gap:9px}
.toast.warn{border-left-color:var(--amber)}
.toast.error{border-left-color:var(--red)}
.toast.ok{border-left-color:var(--green)}
.toast.dismiss{animation:toastOut .25s ease both}
@keyframes toastIn{from{opacity:0;transform:translateX(20px)}to{opacity:1;transform:none}}
@keyframes toastOut{from{opacity:1;transform:none}to{opacity:0;transform:translateX(20px)}}
.toast .ic{font-size:14px}
.toast .x{margin-left:auto;cursor:pointer;color:var(--muted);font-size:14px}
.toast .x:hover{color:var(--txt)}

/* ===== Nav-item ping (for sidebar notifications) ===== */
.navitem .ping{position:absolute;right:10px;top:50%;transform:translateY(-50%);width:6px;height:6px;border-radius:50%;background:var(--red);box-shadow:0 0 0 0 rgba(255,59,110,.6);animation:pulse 2s ease-out infinite}
.navitem .ping.ok{background:var(--green);box-shadow:0 0 0 0 rgba(26,238,170,.6)}

/* ===== What's interesting feed ===== */
.wif-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px;margin:18px 0}
.wif{position:relative;padding:14px 16px;background:linear-gradient(135deg,rgba(0,255,212,.04),var(--panel));border:1px solid var(--line);border-radius:9px;transition:all .15s;cursor:pointer;overflow:hidden}
.wif:hover{border-color:rgba(0,255,212,.30);transform:translateY(-1px)}
.wif::before{content:"";position:absolute;top:0;left:0;width:3px;height:100%;background:var(--accent);opacity:.7}
.wif .lbl{font-size:10px;font-weight:700;color:var(--accent);letter-spacing:1px;text-transform:uppercase}
.wif .val{font-size:15.5px;font-weight:600;color:#fff;margin-top:5px;line-height:1.3;font-family:-apple-system,Inter,sans-serif}
.wif .sub{font-size:11px;color:var(--muted);margin-top:5px;font-family:var(--mono)}

/* ===== Position cards (alt view in wallet) ===== */
.poscards{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:14px}
.poscard{position:relative;padding:14px 16px;background:linear-gradient(180deg,var(--panel),#0b0f14);border:1px solid var(--line);border-radius:11px;overflow:hidden;transition:all .15s}
.poscard:hover{border-color:rgba(0,255,212,.25);transform:translateY(-1px)}
.poscard .pc-top{display:flex;align-items:center;gap:10px;margin-bottom:10px}
.poscard .pc-mkt{font-weight:700;color:#fff;font-size:14px;flex:1}
.poscard .pc-lev{font-family:var(--mono);font-size:11.5px;font-weight:600;color:var(--muted);padding:2px 7px;background:rgba(255,255,255,.04);border-radius:4px}
.poscard .pc-pnl{font-family:var(--mono);font-size:22px;font-weight:700;letter-spacing:-.4px;line-height:1.1}
.poscard .pc-pnl.pos{color:var(--green);text-shadow:0 0 14px rgba(26,238,170,.3)}
.poscard .pc-pnl.neg{color:var(--red);text-shadow:0 0 14px rgba(255,59,110,.3)}
.poscard .pc-meta{font-family:var(--mono);font-size:11.5px;color:var(--muted);margin-top:3px}
.poscard .pc-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:12px;font-family:var(--mono);font-size:11.5px}
.poscard .pc-grid div{display:flex;flex-direction:column;gap:2px}
.poscard .pc-grid .lab{color:var(--muted2);font-size:9.5px;text-transform:uppercase;letter-spacing:.8px;font-family:-apple-system,Inter,sans-serif;font-weight:700}
.poscard .pc-liqbar{margin-top:12px}
.poscard .pc-liqbar .lab{display:flex;justify-content:space-between;font-size:10px;color:var(--muted);margin-bottom:4px}
.poscard .pc-liqbar .track{height:6px;border-radius:3px;background:rgba(255,255,255,.05);overflow:hidden;position:relative}
.poscard .pc-liqbar .track::before{content:"";position:absolute;left:0;top:0;bottom:0;width:var(--pct,50%);background:linear-gradient(90deg,var(--green),var(--accent));border-radius:3px;transition:width .3s}
.poscard.danger .pc-liqbar .track::before{background:linear-gradient(90deg,var(--red),var(--amber))}

/* ===== Calendar heatmap (GitHub-style) ===== */
.calmap{display:grid;grid-template-columns:repeat(53,1fr);gap:2px;margin-top:14px}
.calmap .cell{aspect-ratio:1;border-radius:2px;background:#13181f;border:1px solid rgba(255,255,255,.02);position:relative}
.calmap .cell.l1{background:rgba(0,255,212,.18)}
.calmap .cell.l2{background:rgba(0,255,212,.36)}
.calmap .cell.l3{background:rgba(0,255,212,.56)}
.calmap .cell.l4{background:rgba(0,255,212,.85);box-shadow:0 0 6px rgba(0,255,212,.4)}
.calmap-legend{display:flex;align-items:center;gap:8px;font-size:10.5px;color:var(--muted);margin-top:10px;justify-content:flex-end}
.calmap-legend .swc{width:10px;height:10px;border-radius:2px}

/* ===== Treemap ===== */
.treemap{position:relative;width:100%;height:300px;border:1px solid var(--line);border-radius:9px;overflow:hidden;background:#0a0e13}
.treemap .tcell{position:absolute;border:1px solid rgba(0,0,0,.4);display:flex;align-items:center;justify-content:center;flex-direction:column;padding:6px;cursor:pointer;transition:all .14s;overflow:hidden}
.treemap .tcell:hover{border-color:var(--accent);z-index:5}
.treemap .tcell .nm{font-weight:700;color:#fff;font-size:13px;font-family:-apple-system,Inter,sans-serif}
.treemap .tcell .vl{font-family:var(--mono);font-size:11.5px;color:rgba(255,255,255,.85);margin-top:2px}

/* ===== View toggle row (for treemap/cards alt views) ===== */
.viewtoggle{display:inline-flex;gap:4px;padding:3px;background:rgba(0,255,212,.04);border:1px solid var(--line);border-radius:7px}
.viewtoggle button{background:transparent;border:none;color:var(--muted);font-size:11.5px;font-weight:600;padding:5px 12px;border-radius:5px;cursor:pointer;font-family:-apple-system,Inter,sans-serif;text-transform:uppercase;letter-spacing:.5px}
.viewtoggle button.active{background:rgba(0,255,212,.12);color:var(--accent)}

/* ===== Density modes ===== */
[data-density="compact"] th,[data-density="compact"] td{padding:5px 9px;font-size:12px}
[data-density="compact"] .card{padding:10px 12px}
[data-density="compact"] .card .val{font-size:19px}
[data-density="compact"] .wrap{padding:14px 18px}
[data-density="compact"] .panel{padding:13px 15px}
[data-density="spacious"] th,[data-density="spacious"] td{padding:15px 16px;font-size:14px}
[data-density="spacious"] .card{padding:18px 22px}
[data-density="spacious"] .card .val{font-size:28px}
[data-density="spacious"] .panel{padding:24px 26px}

/* ===== Pinned row star ===== */
.pinstar{display:inline-block;cursor:pointer;color:var(--muted2);font-size:14px;margin-right:8px;transition:transform .14s,color .14s;user-select:none}
.pinstar:hover{transform:scale(1.2);color:var(--amber)}
.pinstar.pinned{color:var(--amber)}
tbody tr.pinned{background:rgba(255,184,84,.04) !important;box-shadow:inset 3px 0 0 var(--amber)}

/* ===== Empty state with illustration ===== */
.emptystate{padding:40px 20px;text-align:center;color:var(--muted);display:flex;flex-direction:column;align-items:center;gap:14px}
.emptystate svg{width:80px;height:80px;opacity:.55}
.emptystate .ttl{color:var(--txt);font-size:14px;font-weight:600;font-family:-apple-system,Inter,sans-serif}
.emptystate .sub{font-size:12px;color:var(--muted);max-width:380px;line-height:1.5}
.empty{padding:30px;text-align:center;color:var(--muted2);font-style:italic;font-family:-apple-system,Inter,sans-serif}

/* ===== Welcome tour overlay ===== */
.tour-back{position:fixed;inset:0;background:rgba(6,9,12,.82);backdrop-filter:blur(4px);z-index:8800;display:none;align-items:center;justify-content:center;padding:24px}
.tour-back.open{display:flex}
.tour{max-width:500px;background:linear-gradient(180deg,#0e1218,#0a0e13);border:1px solid var(--line);border-radius:14px;padding:28px;box-shadow:0 24px 80px rgba(0,0,0,.7),0 0 50px rgba(0,255,212,.10);font-family:-apple-system,Inter,sans-serif}
.tour h3{margin:0 0 12px;color:var(--accent);font-size:11.5px;letter-spacing:1.5px;text-transform:uppercase}
.tour h2{margin:0 0 14px;font-size:22px;color:#fff;font-weight:700;letter-spacing:-.3px}
.tour p{color:var(--muted);font-size:13.5px;line-height:1.55;margin:0 0 18px}
.tour .acts{display:flex;gap:10px;justify-content:flex-end}
.tour .dots{display:flex;gap:6px;margin-bottom:18px}
.tour .dot{width:6px;height:6px;border-radius:50%;background:var(--line)}
.tour .dot.active{background:var(--accent);box-shadow:0 0 8px rgba(0,255,212,.5)}

/* ===== What's new badge ===== */
.whatsnew{position:fixed;bottom:42px;right:24px;background:linear-gradient(180deg,#0e1218,#0a0e13);border:1px solid var(--line);border-left:3px solid var(--accent);border-radius:9px;padding:14px 18px;box-shadow:0 12px 36px rgba(0,0,0,.6);z-index:7000;display:flex;align-items:center;gap:12px;max-width:380px;animation:toastIn .3s ease;font-family:-apple-system,Inter,sans-serif}
.whatsnew .ic{font-size:18px}
.whatsnew .ttl{font-weight:700;color:var(--txt);font-size:12.5px;letter-spacing:.3px;margin-bottom:2px}
.whatsnew .desc{color:var(--muted);font-size:11.5px;line-height:1.4}
.whatsnew .x{cursor:pointer;color:var(--muted);font-size:16px;margin-left:auto;align-self:flex-start}

/* ===== Skeleton tuned to actual shape ===== */
.skel-card{display:inline-block;width:100%;min-height:78px;border-radius:11px;background:linear-gradient(180deg,#10141a,#0a0e13);border:1px solid var(--line);padding:14px 16px;animation:shim 1.6s ease-in-out infinite;background-size:200% 100%;background-image:linear-gradient(90deg,#0f1418 0%,#171c24 50%,#0f1418 100%)}
.skel-row{display:block;height:42px;margin:5px 0;border-radius:7px;background:linear-gradient(90deg,#0f1418 0%,#181d24 50%,#0f1418 100%);background-size:200% 100%;animation:shim 1.6s ease-in-out infinite}
.skel-bar{display:inline-block;height:1.3em;border-radius:4px;background:linear-gradient(90deg,#0f1418 0%,#181d24 50%,#0f1418 100%);background-size:200% 100%;animation:shim 1.6s ease-in-out infinite;min-width:80px;vertical-align:middle}

/* ===== Copy trading ===== */
.copy-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:4px}
/* ===== Copy trading · dashboard ===== */
.cpd-stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin:14px 0}
.cpd-stat{background:var(--panel2);border:1px solid var(--line);border-radius:12px;padding:14px 16px}
.cpd-stat .lbl{font-size:11px;letter-spacing:.07em;text-transform:uppercase;color:var(--muted);margin-bottom:6px}
.cpd-stat .val{font-size:22px;font-weight:600;font-family:var(--mono);line-height:1.1}
.cpd-stat .sub{font-size:11px;color:var(--muted);margin-top:4px}
.cpd-sect{margin-top:22px}
.cpd-sect h3{font-size:13px;letter-spacing:.04em;color:var(--txt);margin:0 0 4px;display:flex;align-items:center;gap:8px}
.cpd-sect .hint{font-size:11px;color:var(--muted);margin:0 0 10px}
.cpd-live{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--green);box-shadow:0 0 8px var(--green);animation:cpdPulse 2s ease-in-out infinite}
@keyframes cpdPulse{0%,100%{opacity:1}50%{opacity:.35}}
.cpd-table{width:100%;border-collapse:collapse;font-size:13px}
.cpd-table th{text-align:left;font-size:10px;letter-spacing:.06em;text-transform:uppercase;color:var(--muted);font-weight:500;padding:8px 10px;border-bottom:1px solid var(--line)}
.cpd-table td{padding:9px 10px;border-bottom:1px solid var(--line2);font-family:var(--mono)}
.cpd-table tr:last-child td{border-bottom:0}
.cpd-side-long{color:var(--green)}.cpd-side-short{color:var(--red)}
.cpd-pos{color:var(--green)}.cpd-neg{color:var(--red)}
.cpd-empty{padding:22px;text-align:center;color:var(--muted);font-size:13px;background:var(--panel2);border:1px dashed var(--line);border-radius:12px}
.cpd-mkt-row{display:grid;grid-template-columns:1fr auto;gap:10px;align-items:center;padding:9px 0;border-bottom:1px solid var(--line2)}
.cpd-mkt-row:last-child{border-bottom:0}
.cpd-bar{height:5px;border-radius:3px;background:var(--line);overflow:hidden;margin-top:5px}
.cpd-bar>i{display:block;height:100%;border-radius:3px}
@media (max-width:860px){.copy-grid{grid-template-columns:1fr}}
.copy-card{background:var(--panel2);border:1px solid var(--line);border-radius:12px;padding:14px}
.copy-k{font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);margin-bottom:6px}
.copy-step{border-top:1px solid var(--line2);padding-top:14px;margin-top:16px}
.copy-step h3{margin:0 0 10px;font-size:14px;color:var(--txt)}
.copy-field{display:flex;flex-direction:column;gap:4px;min-width:150px}
.copy-field label{font-size:11px;color:var(--muted)}
.mono{font-family:var(--mono)}
.cp-dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--muted2);margin-right:6px;vertical-align:middle}
.cp-dot.on{background:var(--green);box-shadow:0 0 8px var(--green)}
.cp-chip{display:inline-block;padding:2px 9px;border-radius:999px;border:1px solid var(--line);font-size:11px;color:var(--muted);margin-left:8px}
.cp-chip.live{border-color:var(--green);color:var(--green)}
.cp-chip.warn{border-color:var(--amber);color:var(--amber)}

/* ===== Reserve space - min-heights ===== */
.view{min-height:60vh}
#walletOut,#md_content{min-height:50vh}
.cards{min-height:80px}
.poscards{min-height:200px}
.hero{min-height:140px}

/* ===== Smoother transitions ===== */
.card,.panel,.navitem,.walltab,.chgpill,.viewtoggle button,.swatch{transition:all .18s cubic-bezier(.2,.85,.3,1)}

/* ===== Custom scrollbar ===== */
::-webkit-scrollbar{width:8px;height:8px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:rgba(0,255,212,.12);border-radius:4px}
::-webkit-scrollbar-thumb:hover{background:rgba(0,255,212,.22)}

/* ===== Text selection ===== */
::selection{background:rgba(0,255,212,.25);color:#fff}

/* ===== Drawing tools toolbar on candle chart ===== */
.draw-tools{display:inline-flex;gap:3px;padding:3px;background:rgba(0,255,212,.04);border:1px solid var(--line);border-radius:7px;margin-left:8px}
.draw-tools button{background:transparent;border:1px solid transparent;color:var(--muted);width:30px;height:26px;border-radius:5px;cursor:pointer;display:flex;align-items:center;justify-content:center;font-size:13px;transition:all .14s;font-family:-apple-system,Inter,sans-serif}
.draw-tools button:hover{color:var(--accent);background:rgba(0,255,212,.06)}
.draw-tools button.active{color:var(--accent);background:rgba(0,255,212,.12);border-color:rgba(0,255,212,.25)}
.draw-tools button svg{width:14px;height:14px;stroke-width:1.8}

/* ===== Custom select dropdown ===== */
.cselect{position:relative;display:inline-block;min-width:140px}
.cselect .csel-btn{width:100%;padding:8px 28px 8px 12px;background:rgba(15,20,24,.7);border:1px solid var(--line);border-radius:7px;color:var(--txt);font-size:12.5px;cursor:pointer;text-align:left;font-family:-apple-system,Inter,sans-serif;transition:all .15s;font-weight:500}
.cselect .csel-btn::after{content:"";position:absolute;right:10px;top:50%;transform:translateY(-50%);border-left:5px solid transparent;border-right:5px solid transparent;border-top:5px solid var(--muted)}
.cselect .csel-btn:hover{border-color:var(--accent2)}
.cselect.open .csel-btn{border-color:var(--accent2);box-shadow:0 0 0 3px rgba(0,255,212,.08)}
.cselect .csel-list{position:absolute;top:calc(100% + 4px);left:0;right:0;background:linear-gradient(180deg,#0e1218,#0a0e13);border:1px solid var(--line);border-radius:8px;box-shadow:0 12px 32px rgba(0,0,0,.55);max-height:0;overflow:hidden;transition:max-height .2s,opacity .15s,transform .15s;opacity:0;transform:translateY(-4px);z-index:80}
.cselect.open .csel-list{max-height:320px;opacity:1;transform:translateY(0);overflow-y:auto}
.cselect .csel-opt{padding:9px 12px;color:var(--txt);font-size:12.5px;cursor:pointer;display:flex;align-items:center;gap:8px;font-family:-apple-system,Inter,sans-serif}
.cselect .csel-opt:hover{background:rgba(0,255,212,.06);color:var(--accent)}
.cselect .csel-opt.sel{background:rgba(0,255,212,.10);color:var(--accent);font-weight:600}

/* ===== Custom range slider ===== */
input[type="range"]{-webkit-appearance:none;appearance:none;width:100%;height:6px;background:transparent;cursor:pointer;padding:0}
input[type="range"]::-webkit-slider-runnable-track{height:6px;background:linear-gradient(90deg,var(--accent) 0%,var(--accent) var(--rngP,50%),rgba(255,255,255,.05) var(--rngP,50%),rgba(255,255,255,.05) 100%);border-radius:3px;border:1px solid var(--line2)}
input[type="range"]::-moz-range-track{height:6px;background:rgba(255,255,255,.05);border-radius:3px;border:1px solid var(--line2)}
input[type="range"]::-moz-range-progress{height:6px;background:var(--accent);border-radius:3px}
input[type="range"]::-webkit-slider-thumb{-webkit-appearance:none;appearance:none;width:16px;height:16px;border-radius:50%;background:#fff;border:2px solid var(--accent);margin-top:-6px;cursor:grab;box-shadow:0 0 0 0 rgba(0,255,212,.4);transition:box-shadow .14s,transform .14s}
input[type="range"]::-webkit-slider-thumb:hover{box-shadow:0 0 0 6px rgba(0,255,212,.18);transform:scale(1.1)}
input[type="range"]::-webkit-slider-thumb:active{cursor:grabbing;transform:scale(.95);box-shadow:0 0 0 8px rgba(0,255,212,.25)}
input[type="range"]::-moz-range-thumb{width:16px;height:16px;border-radius:50%;background:#fff;border:2px solid var(--accent);cursor:grab}

/* ===== Custom checkbox ===== */
input[type="checkbox"]{-webkit-appearance:none;appearance:none;width:16px;height:16px;background:rgba(15,20,24,.8);border:1.5px solid var(--line);border-radius:4px;cursor:pointer;position:relative;transition:all .15s;flex:0 0 16px;vertical-align:middle;margin:0 4px 0 0}
input[type="checkbox"]:hover{border-color:var(--accent2)}
input[type="checkbox"]:checked{background:linear-gradient(135deg,var(--accent),var(--accent-lo));border-color:var(--accent);box-shadow:0 0 8px rgba(0,255,212,.4)}
input[type="checkbox"]:checked::after{content:"";position:absolute;left:4px;top:.5px;width:5px;height:9px;border:solid #06090c;border-width:0 2px 2px 0;transform:rotate(45deg);animation:checkPop .18s ease-out}
@keyframes checkPop{from{opacity:0;transform:rotate(45deg) scale(.5)}to{opacity:1;transform:rotate(45deg) scale(1)}}

/* ===== Particle hero (canvas behind) ===== */
.hero{position:relative}
.hero canvas.particles{position:absolute;inset:0;width:100%;height:100%;pointer-events:none;z-index:0;opacity:.5}
.hero>*{position:relative;z-index:1}

/* Glass morphism reinforced */
.topbar{background:rgba(7,10,13,.65);backdrop-filter:saturate(180%) blur(18px);-webkit-backdrop-filter:saturate(180%) blur(18px)}
.lbhero,.hero-card,.dailystory{backdrop-filter:saturate(140%) blur(4px);-webkit-backdrop-filter:saturate(140%) blur(4px)}

/* ===== Notification center 🔔 ===== */
.notif-btn{position:relative;background:rgba(15,20,24,.85);border:1px solid var(--line);color:var(--muted);width:34px;height:30px;border-radius:7px;cursor:pointer;display:flex;align-items:center;justify-content:center;transition:all .15s}
.notif-btn:hover{color:var(--accent);border-color:var(--accent2)}
.notif-btn svg{width:15px;height:15px;stroke-width:1.8}
.notif-btn .notif-dot{position:absolute;top:5px;right:5px;width:8px;height:8px;border-radius:50%;background:var(--red);box-shadow:0 0 0 2px var(--bg2),0 0 8px rgba(255,59,110,.6);animation:pulse 2s ease-out infinite;display:none}
.notif-btn.has-new .notif-dot{display:block}
.notif-panel{position:absolute;top:calc(100% + 8px);right:0;width:360px;background:linear-gradient(180deg,#0e1218,#0a0e13);border:1px solid var(--line);border-radius:10px;box-shadow:0 18px 50px rgba(0,0,0,.7),0 0 0 1px rgba(0,255,212,.08);display:none;z-index:200;overflow:hidden}
.notif-panel.open{display:block;animation:cmdkIn .18s ease}
.notif-head{display:flex;justify-content:space-between;align-items:center;padding:13px 16px;border-bottom:1px solid var(--line2)}
.notif-head h4{font-size:12.5px;font-weight:700;color:#fff;letter-spacing:.3px;margin:0;font-family:-apple-system,Inter,sans-serif}
.notif-head a{font-size:10.5px;color:var(--accent2);cursor:pointer;font-family:-apple-system,Inter,sans-serif}
.notif-list{max-height:420px;overflow-y:auto}
.notif-item{padding:11px 16px;border-bottom:1px solid var(--line2);display:flex;gap:10px;font-family:-apple-system,Inter,sans-serif;cursor:pointer;transition:background .14s}
.notif-item:hover{background:rgba(0,255,212,.04)}
.notif-item.new{background:rgba(0,255,212,.04)}
.notif-item:last-child{border-bottom:none}
.notif-item .ni-ic{width:30px;height:30px;border-radius:7px;display:flex;align-items:center;justify-content:center;font-size:15px;flex:0 0 30px;background:rgba(0,255,212,.08);border:1px solid rgba(0,255,212,.18)}
.notif-item .ni-c .ni-t{font-size:12.5px;color:#fff;font-weight:600;line-height:1.3}
.notif-item .ni-c .ni-d{font-size:11px;color:var(--muted);margin-top:2px;line-height:1.4}
.notif-item .ni-c .ni-ts{font-size:10px;color:var(--muted2);margin-top:4px;font-family:var(--mono)}
.notif-empty{padding:30px 20px;text-align:center;color:var(--muted);font-size:12.5px;font-family:-apple-system,Inter,sans-serif}

/* ===== Activity news ticker ===== */
.newsticker{display:flex;align-items:center;height:26px;background:linear-gradient(90deg,rgba(0,255,212,.06),rgba(0,255,212,.02));border-bottom:1px solid var(--line2);overflow:hidden;position:relative}
.newsticker .nt-label{padding:0 12px;font-family:-apple-system,Inter,sans-serif;font-size:10px;font-weight:800;color:var(--accent);letter-spacing:1.5px;text-transform:uppercase;flex:0 0 auto;border-right:1px solid var(--line2);height:100%;display:flex;align-items:center;gap:6px;background:rgba(0,255,212,.08)}
.newsticker .nt-label::before{content:"";width:6px;height:6px;border-radius:50%;background:var(--accent);box-shadow:0 0 6px rgba(0,255,212,.6);animation:pulse 1.6s ease-out infinite}
.newsticker .nt-feed{flex:1;overflow:hidden;position:relative;display:flex}
.newsticker .nt-track{display:flex;gap:36px;padding:0 24px;animation:ntScroll 70s linear infinite;white-space:nowrap;font-family:-apple-system,Inter,sans-serif;font-size:11.5px;color:var(--txt);align-items:center}
.newsticker .nt-item{display:inline-flex;align-items:center;gap:6px}
.newsticker .nt-item .nt-i{font-size:13px}
.newsticker .nt-item b{color:var(--accent2);font-weight:700;font-family:var(--mono)}
.newsticker .nt-item .pos{color:var(--green);font-weight:700}
.newsticker .nt-item .neg{color:var(--red);font-weight:700}
@keyframes ntScroll{from{transform:translateX(0)}to{transform:translateX(-50%)}}
.newsticker:hover .nt-track{animation-play-state:paused}

/* ===== Trader Card (downloadable) ===== */
.tcard-modal{position:fixed;inset:0;background:rgba(6,9,12,.84);backdrop-filter:blur(8px);z-index:9200;display:none;align-items:center;justify-content:center;padding:24px}
.tcard-modal.open{display:flex}
.tcard-wrap{position:relative;display:flex;flex-direction:column;align-items:center;gap:14px}
.tcard{position:relative;width:380px;height:520px;background:linear-gradient(180deg,#0c1015 0%,#0a0e13 100%);border-radius:22px;padding:0;overflow:hidden;box-shadow:0 24px 80px rgba(0,255,212,.18),0 0 60px rgba(0,255,212,.12),0 0 0 2px var(--accent),inset 0 1px 0 rgba(0,255,212,.2)}
.tcard::before{content:"";position:absolute;inset:-2px;border-radius:24px;background:linear-gradient(135deg,var(--accent),var(--accent-lo),var(--accent));background-size:300% 300%;animation:tcardBg 6s ease infinite;z-index:-1;filter:blur(8px);opacity:.6}
@keyframes tcardBg{0%,100%{background-position:0% 50%}50%{background-position:100% 50%}}
.tcard .tc-header{padding:18px 22px;background:linear-gradient(135deg,rgba(0,255,212,.15),rgba(0,255,212,.04));border-bottom:1px solid rgba(0,255,212,.18);display:flex;justify-content:space-between;align-items:center}
.tcard .tc-tier{font-size:9.5px;font-weight:800;color:var(--accent);letter-spacing:1.8px;text-transform:uppercase}
.tcard .tc-rarity{font-size:18px}
.tcard .tc-avatar{width:120px;height:120px;border-radius:14px;overflow:hidden;border:2px solid var(--accent);margin:18px auto 14px;box-shadow:0 0 28px rgba(0,255,212,.35)}
.tcard .tc-name{text-align:center;font-family:var(--mono);font-size:16px;font-weight:700;color:#fff;letter-spacing:.5px}
.tcard .tc-sub{text-align:center;font-size:10.5px;color:var(--muted);margin-top:2px;font-family:-apple-system,Inter,sans-serif;letter-spacing:.5px;text-transform:uppercase}
.tcard .tc-stats{padding:14px 22px;display:grid;grid-template-columns:1fr 1fr;gap:8px 12px}
.tcard .tc-stat{display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px dashed rgba(255,255,255,.06)}
.tcard .tc-stat-lbl{font-family:-apple-system,Inter,sans-serif;font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.7px;font-weight:600}
.tcard .tc-stat-val{font-family:var(--mono);font-size:11.5px;color:#fff;font-weight:700}
.tcard .tc-stat-val.pos{color:var(--green)}
.tcard .tc-stat-val.neg{color:var(--red)}
.tcard .tc-curve{margin:8px 22px;height:50px}
.tcard .tc-footer{position:absolute;bottom:14px;left:22px;right:22px;display:flex;justify-content:space-between;font-family:-apple-system,Inter,sans-serif;font-size:9px;color:var(--muted)}
.tcard .tc-footer .tc-url{color:var(--accent2);font-weight:700;letter-spacing:.5px}
.tcard-actions{display:flex;gap:10px}
.tcard-actions button{background:linear-gradient(135deg,var(--accent),var(--accent-lo));color:#06090c;border:none;padding:10px 18px;border-radius:8px;font-weight:700;font-family:-apple-system,Inter,sans-serif;font-size:12.5px;cursor:pointer;box-shadow:0 4px 14px rgba(0,255,212,.3)}
.tcard-actions button.secondary{background:rgba(255,255,255,.06);color:var(--txt)}

/* ===== Hover preview popup ===== */
.hover-prev{position:fixed;z-index:9500;background:linear-gradient(180deg,#0e1218,#0a0e13);border:1px solid var(--line);border-left:3px solid var(--accent);border-radius:10px;padding:14px 16px;box-shadow:0 18px 50px rgba(0,0,0,.75),0 0 0 1px rgba(0,255,212,.10);min-width:260px;max-width:320px;pointer-events:none;opacity:0;transform:translateY(4px);transition:opacity .14s,transform .14s;font-family:-apple-system,Inter,sans-serif}
.hover-prev.show{opacity:1;transform:translateY(0)}
.hover-prev .hp-head{display:flex;align-items:center;gap:9px;margin-bottom:10px}
.hover-prev .hp-avatar{width:28px;height:28px;border-radius:6px;overflow:hidden;border:1px solid rgba(255,255,255,.08);flex:0 0 28px}
.hover-prev .hp-name{font-family:var(--mono);font-size:12.5px;font-weight:700;color:#fff;flex:1}
.hover-prev .hp-row{display:flex;justify-content:space-between;padding:5px 0;font-size:11.5px;color:var(--muted);border-bottom:1px dashed rgba(255,255,255,.04)}
.hover-prev .hp-row:last-child{border-bottom:none}
.hover-prev .hp-row b{font-family:var(--mono);color:#fff;font-weight:600}
.hover-prev .hp-loading{color:var(--muted);font-size:11.5px;font-style:italic;padding:8px 0}
.hover-prev .hp-spark{margin-top:8px;height:32px;width:100%}

/* ===== Today on RISEx daily story banner ===== */
.dailystory{position:relative;display:flex;gap:14px;align-items:center;padding:14px 18px;margin-bottom:18px;background:linear-gradient(120deg,rgba(0,255,212,.06) 0%,rgba(0,255,212,.02) 50%,#0a0e13 100%);border:1px solid var(--line);border-left:3px solid var(--accent);border-radius:10px;overflow:hidden}
.dailystory::before{content:"";position:absolute;top:-50%;right:-15%;width:50%;height:200%;background:radial-gradient(closest-side,rgba(0,255,212,.10),transparent 70%);pointer-events:none}
.dailystory .ds-icon{width:38px;height:38px;border-radius:9px;background:linear-gradient(135deg,rgba(0,255,212,.18),rgba(0,255,212,.04));border:1px solid rgba(0,255,212,.25);display:flex;align-items:center;justify-content:center;font-size:18px;flex:0 0 38px;position:relative}
.dailystory .ds-content{position:relative;flex:1;min-width:0}
.dailystory .ds-eyebrow{font-size:9.5px;font-weight:700;color:var(--accent);text-transform:uppercase;letter-spacing:1.5px;margin-bottom:2px}
.dailystory .ds-text{color:#fff;font-size:13.5px;line-height:1.4;font-family:-apple-system,Inter,sans-serif;font-weight:500}
.dailystory .ds-text b{color:var(--accent);font-weight:700}
.dailystory .ds-text .pos{color:var(--green);font-weight:700}
.dailystory .ds-text .neg{color:var(--red);font-weight:700}
.dailystory .ds-cycle{position:relative;cursor:pointer;color:var(--muted);font-size:11px;display:flex;align-items:center;gap:5px;padding:6px 9px;border-radius:6px;border:1px solid var(--line);font-family:-apple-system,Inter,sans-serif}
.dailystory .ds-cycle:hover{color:var(--accent);border-color:rgba(0,255,212,.3)}

/* ===== Random whale button ===== */
.randwhale{position:fixed;bottom:42px;right:24px;z-index:60;width:56px;height:56px;border-radius:50%;background:linear-gradient(135deg,#00ffd4,#0CD8B7);color:#06090c;border:none;cursor:pointer;font-size:24px;display:flex;align-items:center;justify-content:center;box-shadow:0 8px 28px rgba(0,255,212,.45),inset 0 1px 0 rgba(255,255,255,.3);transition:transform .2s cubic-bezier(.3,1.4,.4,1);font-family:-apple-system,Inter,sans-serif}
.randwhale:hover{transform:scale(1.08) rotate(-8deg)}
.randwhale:active{transform:scale(.96)}
.randwhale::before{content:"";position:absolute;inset:-4px;border-radius:50%;background:radial-gradient(circle,rgba(0,255,212,.4),transparent 70%);z-index:-1;animation:pulse 2.4s ease-out infinite}

/* ===== You might like / Related ===== */
.suggested{margin-top:22px}
.suggested h3{font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:1.2px;margin-bottom:10px;display:flex;align-items:center;gap:8px}
.suggested h3::before{content:"";width:3px;height:11px;border-radius:2px;background:var(--accent)}
.suggested-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:10px}
.sug-card{padding:12px 14px;background:linear-gradient(180deg,var(--panel),#0a0e13);border:1px solid var(--line);border-radius:9px;text-decoration:none;color:inherit;transition:all .15s;display:flex;align-items:center;gap:10px}
.sug-card:hover{transform:translateY(-2px);border-color:rgba(0,255,212,.3)}
.sug-card .sc-avatar{width:30px;height:30px;border-radius:6px;overflow:hidden;border:1px solid rgba(255,255,255,.08);flex:0 0 30px}
.sug-card .sc-name{font-family:var(--mono);font-size:12px;font-weight:700;color:#fff}
.sug-card .sc-meta{font-size:10.5px;color:var(--muted);margin-top:1px}

/* ===== Trending widget ===== */
.trending{padding:13px 16px;background:linear-gradient(180deg,rgba(0,255,212,.03),var(--panel));border:1px solid var(--line);border-radius:10px;margin-bottom:14px}
.trending .tr-head{display:flex;align-items:center;gap:7px;margin-bottom:8px;font-size:10.5px;font-weight:700;color:var(--accent);text-transform:uppercase;letter-spacing:1.2px}
.trending .tr-head::before{content:"🔥"}
.trending .tr-list{display:flex;gap:8px;flex-wrap:wrap}
.tr-chip{display:inline-flex;align-items:center;gap:6px;padding:5px 9px;background:rgba(255,255,255,.03);border:1px solid var(--line2);border-radius:6px;font-size:11.5px;text-decoration:none;color:var(--txt);transition:all .15s}
.tr-chip:hover{border-color:rgba(0,255,212,.3);background:rgba(0,255,212,.04)}
.tr-chip .tr-num{font-family:var(--mono);font-size:10px;color:var(--muted)}
.tr-chip .tr-id{width:14px;height:14px;border-radius:3px;overflow:hidden;border:1px solid rgba(255,255,255,.05);flex:0 0 14px}
.viewing-now{display:inline-flex;align-items:center;gap:5px;padding:3px 8px;background:rgba(0,255,212,.06);border:1px solid rgba(0,255,212,.20);border-radius:5px;font-size:10.5px;color:var(--accent);font-family:-apple-system,Inter,sans-serif;font-weight:600;margin-left:8px;vertical-align:middle}
.viewing-now::before{content:"";width:5px;height:5px;border-radius:50%;background:var(--accent);box-shadow:0 0 6px rgba(0,255,212,.7);animation:pulse 2s ease-out infinite}

/* ===== Streak / personal stats footer ===== */
.streak{display:inline-flex;align-items:center;gap:5px;padding:3px 9px;background:linear-gradient(135deg,rgba(255,107,53,.10),rgba(255,184,84,.05));border:1px solid rgba(255,107,53,.30);border-radius:5px;font-size:10.5px;font-weight:700;color:#ff8a4c;letter-spacing:.3px;font-family:-apple-system,Inter,sans-serif}
.streak-fire{animation:fireWiggle 1.4s ease-in-out infinite}
@keyframes fireWiggle{0%,100%{transform:rotate(-2deg)}50%{transform:rotate(2deg) scale(1.05)}}
.anon-id{display:inline-flex;align-items:center;gap:6px;padding:4px 8px;background:rgba(0,255,212,.04);border:1px solid var(--line);border-radius:5px;font-size:10.5px;color:var(--muted);font-family:-apple-system,Inter,sans-serif;cursor:pointer}
.anon-id .anon-av{width:14px;height:14px;border-radius:3px;overflow:hidden}
.anon-id b{color:var(--accent2);font-family:var(--mono)}

/* ===== 3D tilt cards ===== */
.tiltable{transform-style:preserve-3d;transition:transform .25s cubic-bezier(.2,.85,.3,1);will-change:transform}

/* ===== Confetti container ===== */
#confetti-root{position:fixed;inset:0;pointer-events:none;z-index:9000;overflow:hidden}
.confetti-piece{position:absolute;top:-20px;width:8px;height:14px;border-radius:1px;animation:confettiFall 3s linear forwards}
@keyframes confettiFall{
 0%{transform:translateY(0) rotate(0deg);opacity:1}
 100%{transform:translateY(110vh) rotate(720deg);opacity:.3}
}

/* ===== Achievements popup ===== */
.achievement{position:fixed;bottom:24px;left:50%;transform:translateX(-50%) translateY(20px);background:linear-gradient(135deg,#0e1218 0%,#0a1015 100%);border:1px solid var(--accent);border-radius:11px;padding:14px 20px;box-shadow:0 12px 36px rgba(0,255,212,.30),0 0 60px rgba(0,255,212,.15),inset 0 1px 0 rgba(0,255,212,.15);min-width:280px;display:flex;align-items:center;gap:12px;z-index:9100;opacity:0;animation:achIn .4s cubic-bezier(.3,1.4,.4,1) forwards,achOut .4s 4.5s forwards;font-family:-apple-system,Inter,sans-serif}
@keyframes achIn{from{opacity:0;transform:translateX(-50%) translateY(20px)}to{opacity:1;transform:translateX(-50%) translateY(0)}}
@keyframes achOut{from{opacity:1;transform:translateX(-50%) translateY(0)}to{opacity:0;transform:translateX(-50%) translateY(20px)}}
.achievement .ach-ic{font-size:30px;filter:drop-shadow(0 0 12px rgba(0,255,212,.6))}
.achievement .ach-lbl{font-size:9px;font-weight:700;color:var(--accent);letter-spacing:2px;text-transform:uppercase}
.achievement .ach-ttl{font-size:14px;font-weight:700;color:#fff;margin-top:2px}
.achievement .ach-desc{font-size:11.5px;color:var(--muted);margin-top:2px}

/* ===== Mobile responsive polish ===== */
@media (max-width:980px){
 .app{grid-template-columns:1fr}
 .sidebar{position:fixed;top:0;left:0;height:100vh;width:280px;transform:translateX(-100%);transition:transform .25s ease;z-index:200;box-shadow:0 0 60px rgba(0,0,0,.7)}
 .sidebar.open{transform:translateX(0)}
 .topbar .menu-btn{display:block !important}
 .hero{grid-template-columns:1fr}
 .grid2{grid-template-columns:1fr}
 .cards{grid-template-columns:repeat(2,1fr) !important}
 .wrap{padding:16px 14px}
 .topbar{padding:10px 14px;gap:8px}
 .topbar .gsearch{flex:1;min-width:0;max-width:none}
 .topbar .gsearch input{font-size:13px}
 .topbar .live{gap:6px}
 .topbar .live #updated{display:none}
 .panel{padding:14px 13px;border-radius:9px}
 .panel h2{font-size:12.5px}
 .lbhero{grid-template-columns:1fr;gap:10px;padding:14px 16px}
 .lbhero .lh-icon{width:38px;height:38px;font-size:18px}
 .lbhero .lh-val{font-size:24px}
 .lbhero .lh-side{text-align:left;font-size:10.5px}
 .podium{grid-template-columns:1fr;gap:8px}
 .podium .p1,.podium .p2,.podium .p3,.podium .ppos{min-height:0;padding:14px 16px}
 .podium .pmedal{font-size:22px;top:10px;right:10px}
 .podium .pval{font-size:20px}
 .hero-card{padding:18px 20px}
 .hero-val{font-size:34px;letter-spacing:-1px}
 .hero-spark{height:46px}
 .priceticker{height:26px}
 .priceticker .pt-track{gap:18px;font-size:10.5px}
 .pt-asset svg.pt-sk{width:32px;height:11px}
 .walltabs{margin:12px -2px 10px;padding:4px;border-radius:9px;gap:2px}
 .walltab{padding:7px 10px;font-size:11.5px}
 .walltab .count{font-size:9.5px;padding:1px 5px}
 .whead{padding:16px 14px}
 .whead .row1{gap:10px}
 .whead .addr{font-size:11px;word-break:break-all}
 .whead .actions{flex-basis:100%;margin-left:0;margin-top:8px}
 .whead .actions .chip{font-size:11px;padding:4px 9px}
 .treemap{height:220px}
 .calmap{grid-template-columns:repeat(53,1fr);gap:1px}
 .lbfilter{padding:8px 10px;gap:8px}
 .lbfilter input[type="search"]{width:100%;flex-basis:100%}
 .poscards{grid-template-columns:1fr;gap:10px}
 .poscard{padding:12px 14px}
 .poscard .pc-pnl{font-size:20px}
 .wif-grid{grid-template-columns:1fr 1fr;gap:8px}
 .wif{padding:11px 13px}
 .wif .val{font-size:13.5px}
 .footer{padding:18px 16px 14px}
 .footer .row{gap:10px;font-size:10.5px}
 /* Tables: smaller padding + hide low-priority cols on tight screens */
 th,td{padding:7px 8px;font-size:11.5px}
 .spark-cell{width:60px;padding:5px !important}
 .spark-cell svg{width:56px;height:24px}
 .fbar{width:60px}
 .chgpill{font-size:11px;padding:2px 6px}
 .lsbar{width:90px;height:14px}
 .lsbar .ls-long,.lsbar .ls-short{font-size:8.5px}
 .idsm{width:18px;height:18px;flex:0 0 18px;margin-right:5px}
 .rankpill{min-width:24px;height:20px;padding:0 6px;font-size:10px}
 .tier{font-size:8.5px;padding:1px 4px;margin-left:3px}
 .smart-badge{font-size:8.5px;padding:1px 5px}
 .cmdk{width:94vw;max-width:560px}
 .cmdk-input{padding:13px 14px;font-size:14px}
 .cmdk-item{padding:9px 14px;font-size:12.5px}
 .ctxmenu{min-width:170px}
 .toasts{top:14px;right:14px;left:14px}
 .toast{min-width:0;max-width:none}
 .tour{padding:22px 20px}
 .tour h2{font-size:18px}
 .whatsnew{left:14px;right:14px;bottom:14px;max-width:none}
 .market-watermark{font-size:120px;letter-spacing:-6px;top:14px;right:10px}
 .hero-card .hero-lbl{font-size:10px;letter-spacing:1px}
 .candles canvas{width:100% !important}
 #ch_canvas{height:280px !important}
}
@media (max-width:580px){
 .cards{grid-template-columns:1fr !important}
 .wif-grid{grid-template-columns:1fr}
 h2{font-size:14px}
 .hero-val{font-size:28px}
 .lbhero .lh-val{font-size:20px}
 .topbar .gsearch input{padding:8px 12px 8px 32px}
 .topbar .gsearch::before{left:11px;font-size:13px}
 .topbar .rf,.topbar .btn{padding:5px 8px;font-size:11px}
 .priceticker{height:24px}
 .footer .row{font-size:10px}
 #ch_canvas{height:220px !important}
}

/* ===== Leaderboard gamification ===== */
/* Hero summary banner above each ranking */
.lbhero{position:relative;display:grid;grid-template-columns:auto 1fr auto;gap:18px;align-items:center;padding:18px 22px;margin-bottom:14px;background:linear-gradient(135deg,rgba(0,255,212,.05),var(--panel) 50%,#0a0e13);border:1px solid var(--line);border-radius:11px;overflow:hidden;box-shadow:inset 0 1px 0 rgba(0,255,212,.07)}
.lbhero::before{content:"";position:absolute;top:-40%;right:-15%;width:50%;height:180%;background:radial-gradient(closest-side,rgba(0,255,212,.10),transparent 70%);pointer-events:none}
.lbhero .lh-icon{position:relative;width:48px;height:48px;border-radius:11px;display:flex;align-items:center;justify-content:center;background:linear-gradient(135deg,rgba(0,255,212,.18),rgba(0,255,212,.04));border:1px solid rgba(0,255,212,.25);font-size:22px}
.lbhero .lh-info{position:relative}
.lbhero .lh-lbl{font-size:10.5px;font-weight:700;color:var(--accent);text-transform:uppercase;letter-spacing:1.5px}
.lbhero .lh-val{font-family:var(--mono);font-size:34px;font-weight:700;color:#fff;line-height:1.05;margin-top:4px;letter-spacing:-1px;text-shadow:0 0 20px rgba(0,255,212,.15)}
.lbhero .lh-meta{font-size:12px;color:var(--muted);margin-top:4px}
.lbhero .lh-side{position:relative;font-family:var(--mono);font-size:11px;color:var(--muted);text-align:right;line-height:1.6}
.lbhero .lh-side b{color:var(--txt);font-size:14px;font-weight:700}

/* Top-3 podium cards */
.podium{display:grid;grid-template-columns:1fr 1.15fr 1fr;gap:14px;margin-bottom:18px;align-items:end}
.podium .ppos{position:relative;padding:18px 18px 16px;border-radius:11px;border:1px solid var(--line);overflow:hidden;cursor:pointer;transition:transform .18s,box-shadow .18s;text-decoration:none;color:inherit;display:block}
.podium .ppos:hover{transform:translateY(-3px)}
.podium .p1{background:linear-gradient(180deg,rgba(255,200,80,.12),rgba(255,200,80,.02) 60%,#0c1015);border-color:rgba(255,200,80,.40);box-shadow:0 12px 32px rgba(255,200,80,.10),inset 0 1px 0 rgba(255,200,80,.18);min-height:170px}
.podium .p2{background:linear-gradient(180deg,rgba(180,200,210,.10),rgba(180,200,210,.02) 60%,#0c1015);border-color:rgba(180,200,210,.32);min-height:150px}
.podium .p3{background:linear-gradient(180deg,rgba(205,127,50,.10),rgba(205,127,50,.02) 60%,#0c1015);border-color:rgba(205,127,50,.32);min-height:150px}
.podium .p1::before{content:"";position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,transparent,#ffc850,transparent);background-size:200% 100%;animation:slideHi 3s linear infinite}
.podium .pmedal{position:absolute;top:14px;right:14px;font-size:26px;filter:drop-shadow(0 0 8px currentColor);line-height:1}
.podium .p1 .pmedal{color:#ffc850}.podium .p2 .pmedal{color:#cfd8dc}.podium .p3 .pmedal{color:#cd7f32}
.podium .pavatar{width:38px;height:38px;border-radius:7px;overflow:hidden;border:1px solid rgba(255,255,255,.10);margin-bottom:10px}
.podium .paddr{font-family:var(--mono);font-size:13px;font-weight:700;color:#fff;letter-spacing:.2px}
.podium .plbl{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:1.2px;font-weight:700;margin-top:10px}
.podium .pval{font-family:var(--mono);font-size:24px;font-weight:700;color:#fff;line-height:1.1;margin-top:2px;letter-spacing:-.5px}
.podium .pval.pos{color:var(--green);text-shadow:0 0 14px rgba(26,238,170,.3)}
.podium .pval.neg{color:var(--red);text-shadow:0 0 14px rgba(255,59,110,.3)}
.podium .psub{font-family:var(--mono);font-size:11px;color:var(--muted);margin-top:5px}

/* Rank pill (medal for top 3) */
.rankpill{display:inline-flex;align-items:center;justify-content:center;min-width:32px;height:24px;padding:0 9px;border-radius:6px;font-family:var(--mono);font-weight:700;font-size:11.5px;background:rgba(255,255,255,.04);color:var(--muted);letter-spacing:.3px}
.rankpill.r1{background:linear-gradient(135deg,rgba(255,200,80,.25),rgba(255,200,80,.10));color:#ffc850;border:1px solid rgba(255,200,80,.40);box-shadow:0 0 10px rgba(255,200,80,.18)}
.rankpill.r2{background:linear-gradient(135deg,rgba(207,216,220,.20),rgba(207,216,220,.08));color:#cfd8dc;border:1px solid rgba(207,216,220,.32)}
.rankpill.r3{background:linear-gradient(135deg,rgba(205,127,50,.20),rgba(205,127,50,.08));color:#cd7f32;border:1px solid rgba(205,127,50,.32)}

/* Identicon inline (small in tables) */
.idsm{display:inline-block;width:22px;height:22px;border-radius:5px;overflow:hidden;border:1px solid rgba(255,255,255,.06);vertical-align:middle;margin-right:8px;flex:0 0 22px}
.idsm svg{display:block;width:100%;height:100%}
td .addrcell{display:inline-flex;align-items:center;gap:0}
td .addrcell a{color:var(--txt);font-weight:600;font-family:var(--mono);font-size:12.5px}

/* Tier badges */
.tier{display:inline-block;font-size:9.5px;font-weight:700;padding:1.5px 5px;border-radius:3px;letter-spacing:.4px;margin-left:6px;text-transform:uppercase;vertical-align:middle;border:1px solid transparent}
.tier.whale{background:rgba(80,140,255,.10);color:#7aaaff;border-color:rgba(80,140,255,.25)}
.tier.pro{background:rgba(255,184,84,.10);color:var(--amber);border-color:rgba(255,184,84,.25)}
.tier.active{background:rgba(0,255,212,.06);color:var(--accent2);border-color:rgba(0,255,212,.18)}

/* Inline value-bar fill (gradient that fills the cell from left) */
.barfill{position:relative;display:block}
.barfill::before{content:"";position:absolute;left:-12px;top:50%;transform:translateY(-50%);height:24px;width:var(--w,0%);background:linear-gradient(90deg,rgba(0,255,212,.18),rgba(0,255,212,.04));border-radius:3px;z-index:0;pointer-events:none;transition:width .35s ease-out}
.barfill.pos::before{background:linear-gradient(90deg,rgba(26,238,170,.22),rgba(26,238,170,.04))}
.barfill.neg::before{background:linear-gradient(90deg,rgba(255,59,110,.22),rgba(255,59,110,.04))}
.barfill>span{position:relative;z-index:1}

/* Heat tint for top/bottom 10% values */
td.heat-top{background:rgba(26,238,170,.05) !important;border-left:2px solid rgba(26,238,170,.4)}
td.heat-bot{background:rgba(255,59,110,.05) !important;border-left:2px solid rgba(255,59,110,.4)}

/* Filter bar (search + min-volume + smart-only) */
.lbfilter{display:flex;gap:10px;align-items:center;padding:10px 14px;margin-bottom:12px;background:linear-gradient(180deg,rgba(0,255,212,.02),transparent);border:1px solid var(--line);border-radius:9px;flex-wrap:wrap}
.lbfilter input[type="search"]{background:rgba(15,20,24,.7);border:1px solid var(--line);border-radius:6px;padding:7px 11px;font-size:12px;color:var(--txt);width:200px;font-family:var(--mono)}
.lbfilter input:focus{outline:none;border-color:var(--accent2)}
.lbfilter label{font-size:11.5px;color:var(--muted);display:inline-flex;align-items:center;gap:6px;cursor:pointer;font-family:-apple-system,Inter,sans-serif}
.lbfilter .pickv{font-size:11px;color:var(--muted);font-family:var(--mono)}

/* Staggered row fade-in */
@keyframes rowIn{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:none}}
tbody tr.lbrow{animation:rowIn .26s ease both}

/* Block / Tx detail pages */
.detail-head{display:flex;align-items:center;gap:14px;flex-wrap:wrap;padding:18px 22px;background:linear-gradient(135deg,rgba(0,255,212,.04),var(--panel) 60%,#0a0e13);border:1px solid var(--line);border-radius:11px;margin-bottom:14px;position:relative;overflow:hidden}
.detail-head::before{content:"";position:absolute;top:-40%;right:-15%;width:50%;height:180%;background:radial-gradient(closest-side,rgba(0,255,212,.10),transparent 70%);pointer-events:none}
.detail-head .dh-icon{position:relative;width:54px;height:54px;border-radius:11px;background:linear-gradient(135deg,rgba(0,255,212,.16),rgba(0,255,212,.04));border:1px solid rgba(0,255,212,.25);display:flex;align-items:center;justify-content:center;font-size:24px;flex:0 0 54px}
.detail-head .dh-info{position:relative;flex:1;min-width:0}
.detail-head .dh-lbl{font-size:10.5px;font-weight:700;color:var(--accent);text-transform:uppercase;letter-spacing:1.5px}
.detail-head .dh-val{font-family:var(--mono);font-size:24px;font-weight:700;color:#fff;line-height:1.1;margin-top:4px;letter-spacing:-.3px;word-break:break-all}
.detail-head .dh-meta{font-size:12px;color:var(--muted);margin-top:5px;font-family:-apple-system,Inter,sans-serif}
.detail-head .dh-actions{position:relative;display:flex;gap:6px;flex-wrap:wrap}
.detail-head .chip{cursor:pointer;font-size:11.5px;padding:6px 11px;border-radius:6px;background:rgba(255,255,255,.04);border:1px solid var(--line2);color:var(--txt);text-decoration:none;display:inline-flex;align-items:center;gap:5px;font-family:-apple-system,Inter,sans-serif;font-weight:600;transition:all .15s}
.detail-head .chip:hover{border-color:var(--accent2);color:var(--accent)}
.detail-head .dh-status{display:inline-flex;align-items:center;gap:6px;padding:4px 9px;border-radius:5px;font-size:11px;font-weight:700;font-family:-apple-system,Inter,sans-serif;letter-spacing:.4px;text-transform:uppercase}
.dh-status.ok{background:rgba(26,238,170,.10);color:var(--green);border:1px solid rgba(26,238,170,.30)}
.dh-status.fail{background:rgba(255,59,110,.10);color:var(--red);border:1px solid rgba(255,59,110,.30)}
.dh-status::before{content:"";width:6px;height:6px;border-radius:50%;background:currentColor;box-shadow:0 0 6px currentColor}

/* KV table for detail rows */
.kv{display:grid;grid-template-columns:200px 1fr;border:1px solid var(--line);border-radius:10px;background:linear-gradient(180deg,var(--panel),#0a0e13);overflow:hidden}
.kv .k{padding:11px 16px;font-family:-apple-system,Inter,sans-serif;font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:1px;background:rgba(0,255,212,.02);border-right:1px solid var(--line);border-bottom:1px solid var(--line2);display:flex;align-items:center}
.kv .v{padding:11px 16px;font-family:var(--mono);font-size:13px;color:#fff;border-bottom:1px solid var(--line2);word-break:break-all;display:flex;align-items:center;gap:8px}
.kv .k:last-of-type,.kv .v:last-of-type{border-bottom:none}
.kv .v .copy{cursor:pointer;color:var(--muted);font-size:13px;padding:2px 5px;border-radius:3px;transition:all .15s;font-family:-apple-system,Inter,sans-serif}
.kv .v .copy:hover{background:rgba(0,255,212,.08);color:var(--accent)}
.kv .v a{color:var(--accent2);text-decoration:none}
.kv .v a:hover{color:var(--accent)}
.kv .v .tag-contract{font-size:9.5px;font-weight:800;padding:1.5px 6px;background:rgba(255,180,84,.10);color:var(--amber);border:1px solid rgba(255,180,84,.3);border-radius:3px;letter-spacing:.5px;font-family:-apple-system,Inter,sans-serif;text-transform:uppercase}
.kv .v .micro-id{width:18px;height:18px;border-radius:4px;overflow:hidden;flex:0 0 18px;border:1px solid rgba(255,255,255,.05)}
@media (max-width:680px){.kv{grid-template-columns:1fr}.kv .k{border-right:none;padding:8px 14px 4px}.kv .v{padding:0 14px 10px}}

/* Logs / events list */
.logitem{padding:12px 16px;background:linear-gradient(180deg,var(--panel),#0a0e13);border:1px solid var(--line);border-radius:9px;margin-bottom:8px}
.logitem .log-h{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.logitem .log-addr{font-family:var(--mono);font-size:12.5px;font-weight:700;color:var(--accent2)}
.logitem .log-idx{font-size:10px;color:var(--muted2);font-family:var(--mono)}
.logitem .log-topic{font-family:var(--mono);font-size:10.5px;color:var(--muted);padding:3px 0;word-break:break-all}
.logitem .log-topic b{color:var(--txt)}
.logitem .log-data{font-family:var(--mono);font-size:10.5px;color:var(--muted);background:rgba(0,0,0,.25);padding:7px 9px;border-radius:5px;margin-top:6px;word-break:break-all;max-height:80px;overflow:auto}

/* Input data display */
.inputdata{font-family:var(--mono);font-size:11.5px;background:#0a0e13;border:1px solid var(--line2);padding:12px 14px;border-radius:8px;color:var(--muted);max-height:160px;overflow:auto;word-break:break-all}
.inputdata .selector{color:var(--accent);font-weight:700}

/* Mini tx row in block detail */
.mini-tx-row td{padding:7px 10px !important;font-size:11.5px !important}

/* Explorer streaming animation — subtle slide + green flash */
@keyframes rowStream{
 0%{opacity:0;transform:translateY(-6px);background:rgba(0,255,212,.08)}
 60%{opacity:1;transform:translateY(0);background:rgba(0,255,212,.04)}
 100%{opacity:1;transform:none;background:transparent}
}
#v_explorer tbody tr.lbrow{animation:rowStream .45s cubic-bezier(.25,1,.5,1) both;will-change:transform,opacity}

/* ===== Long/Short bipolar bar (full width, shows balance) ===== */
.lsbar{display:flex;width:140px;height:18px;border-radius:4px;overflow:hidden;background:rgba(255,255,255,.04);border:1px solid var(--line2);position:relative}
.lsbar .ls-long{background:linear-gradient(90deg,var(--green),rgba(26,238,170,.4));height:100%;display:flex;align-items:center;justify-content:flex-start;padding-left:5px;color:#06090c;font-family:var(--mono);font-size:9.5px;font-weight:700;letter-spacing:-.2px}
.lsbar .ls-short{background:linear-gradient(270deg,var(--red),rgba(255,59,110,.4));height:100%;display:flex;align-items:center;justify-content:flex-end;padding-right:5px;color:#06090c;font-family:var(--mono);font-size:9.5px;font-weight:700;letter-spacing:-.2px}
.lsbar .ls-long:empty,.lsbar .ls-short:empty{color:transparent}

/* ===== Delta arrow indicator ===== */
.deltarr{display:inline-flex;align-items:center;gap:3px;font-family:var(--mono);font-weight:700;padding:2px 6px;border-radius:4px;font-size:11.5px}
.deltarr.pos{background:rgba(26,238,170,.10);color:var(--green);box-shadow:inset 0 0 0 1px rgba(26,238,170,.22)}
.deltarr.neg{background:rgba(255,59,110,.10);color:var(--red);box-shadow:inset 0 0 0 1px rgba(255,59,110,.22)}
.deltarr.zero{background:rgba(255,255,255,.03);color:var(--muted)}
.deltarr .ar{font-size:9px}

/* ===== Pain podium (red gradient for losers/liquidations) ===== */
.podium .ppain1{background:linear-gradient(180deg,rgba(255,59,110,.14),rgba(255,59,110,.02) 60%,#0c1015);border-color:rgba(255,59,110,.40);box-shadow:0 12px 32px rgba(255,59,110,.12),inset 0 1px 0 rgba(255,59,110,.18);min-height:170px}
.podium .ppain2{background:linear-gradient(180deg,rgba(255,100,140,.10),rgba(255,100,140,.02) 60%,#0c1015);border-color:rgba(255,100,140,.32);min-height:150px}
.podium .ppain3{background:linear-gradient(180deg,rgba(255,150,170,.08),rgba(255,150,170,.02) 60%,#0c1015);border-color:rgba(255,150,170,.28);min-height:150px}
.podium .ppain1::before{content:"";position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,transparent,var(--red),transparent);background-size:200% 100%;animation:slideHi 3s linear infinite}
.podium .ppain1 .pmedal{color:var(--red)}.podium .ppain2 .pmedal{color:#ff7a96}.podium .ppain3 .pmedal{color:#ff96aa}
a{color:var(--accent2);text-decoration:none;transition:color .15s}
a:hover{color:#97fce4}
/* numeros tabulares + mono font para valores */
.val,td,th{font-variant-numeric:tabular-nums}
.val{font-family:var(--mono);letter-spacing:-.01em}
td,th{font-family:var(--mono);font-feature-settings:"tnum","calt" 0}
.mkt,a,.lbl,.note,.pillside,.pill,.tag,.smart-badge,h1,h2,h3,button,label,select,option,.empty{font-family:-apple-system,BlinkMacSystemFont,"Inter","Segoe UI",Roboto,sans-serif}

/* ===== Layout: sidebar + main with slim topbar ===== */
.app{display:grid;grid-template-columns:248px 1fr;min-height:100vh}
.sidebar{display:flex !important;flex-direction:column}
.main{min-width:0}

/* ===== Sidebar accordion groups ===== */
.ngroup{position:relative;margin:1px 0}
.ngroup .ghead{position:relative;display:flex;align-items:center;gap:11px;padding:9px 12px;border-radius:8px;color:var(--muted);cursor:pointer;font-size:13px;font-weight:600;font-family:-apple-system,Inter,sans-serif;letter-spacing:.2px;user-select:none;transition:all .15s;border:1px solid transparent}
.ngroup .ghead:hover{background:rgba(0,255,212,.04);color:var(--txt)}
.ngroup .ghead:hover .ico{color:var(--accent);transform:scale(1.08)}
.ngroup .ghead .ico{width:16px;height:16px;display:flex;align-items:center;justify-content:center;color:var(--muted2);flex:0 0 16px;transition:all .18s}
.ngroup .ghead .ico svg{width:16px;height:16px;stroke-width:1.7}
.ngroup .ghead .chev{margin-left:auto;font-size:10px;color:var(--muted2);transition:transform .22s ease,color .15s}
.ngroup.open .ghead .chev{transform:rotate(180deg);color:var(--accent)}
.ngroup.parent-active > .ghead{color:var(--accent)}
.ngroup.parent-active > .ghead .ico{color:var(--accent)}
.ngroup .gchildren{display:none;padding:3px 0 6px 0;margin:2px 0 4px 22px;border-left:1px dashed rgba(0,255,212,.16)}
.ngroup.open .gchildren{display:block;animation:gOpen .2s ease}
@keyframes gOpen{from{opacity:0;transform:translateY(-3px)}to{opacity:1;transform:translateY(0)}}
.ngroup .gchildren .navitem{font-size:12.5px;padding:7px 11px;margin:1px 0 1px 6px;font-weight:500;gap:9px}
.ngroup .gchildren .navitem .ico{width:14px;height:14px;flex:0 0 14px}
.ngroup .gchildren .navitem .ico svg{width:14px;height:14px;stroke-width:1.7}
.ngroup .gchildren .navitem.active::before{left:-9px;top:7px;bottom:7px}
.ngroup .gchildren .navitem.active::after{right:8px}
.ngroup .ghead.navitem.active{background:linear-gradient(90deg,rgba(0,255,212,.13) 0%,rgba(0,255,212,.04) 100%);color:var(--accent);border-color:rgba(0,255,212,.20);box-shadow:inset 0 1px 0 rgba(0,255,212,.10),0 0 16px rgba(0,255,212,.06)}
.ngroup .ghead.navitem.active::before{content:"";position:absolute;left:-11px;top:9px;bottom:9px;width:3px;background:linear-gradient(180deg,var(--accent),var(--accent-lo));border-radius:0 3px 3px 0;box-shadow:0 0 12px rgba(0,255,212,.7)}
.ngroup .ghead.navitem.active::after{content:"";position:absolute;right:10px;top:50%;transform:translateY(-50%);width:5px;height:5px;border-radius:50%;background:var(--accent);box-shadow:0 0 8px rgba(0,255,212,.8);animation:pulse 2.6s ease-in-out infinite}
.ngroup .ghead.navitem.active .ico{color:var(--accent)}

/* ===== Slim Topbar (search + actions only — nav lives in sidebar) ===== */
.topnav{position:sticky;top:0;z-index:100;display:flex;align-items:center;gap:10px;padding:10px 22px;background:rgba(7,10,13,.78);backdrop-filter:saturate(180%) blur(18px);-webkit-backdrop-filter:saturate(180%) blur(18px);border-bottom:1px solid var(--line)}
.topnav .tn-brand{display:none !important}
.topnav .mainnav{display:none !important}
.topnav{justify-content:flex-end}
.topnav .gsearch{margin-left:auto;margin-right:0;flex:0 0 320px}
.topnav .tn-actions{flex:0 0 auto;margin-left:0}
.subnav{display:none !important}
.topnav .__legacy_brand_hidden{display:flex;align-items:center;gap:6px;text-decoration:none;color:inherit;flex:0 0 auto;padding-right:14px;border-right:1px solid var(--line2);transition:opacity .15s}
.topnav .tn-brand:hover{opacity:.85}
.topnav .tn-brand .tn-wordmark{height:22px;width:auto;display:block;filter:drop-shadow(0 0 8px rgba(4,223,131,.18))}
.topnav .tn-brand .tn-suffix{font-family:-apple-system,Inter,sans-serif;font-weight:700;font-size:17px;color:#fff;letter-spacing:-.3px;line-height:1;margin-left:-2px;text-transform:lowercase}
.topnav .mainnav{display:flex;gap:2px;flex:1;min-width:0;overflow-x:auto;scrollbar-width:none}
.topnav .mainnav::-webkit-scrollbar{display:none}
.topnav .mainnav a{position:relative;padding:8px 13px;border-radius:7px;color:var(--muted);font-size:13px;font-weight:600;text-decoration:none;font-family:-apple-system,Inter,sans-serif;letter-spacing:.2px;transition:all .15s;cursor:pointer;white-space:nowrap;display:inline-flex;align-items:center;gap:6px}
.topnav .mainnav a:hover{color:var(--txt);background:rgba(0,255,212,.04)}
.topnav .mainnav a.active{color:var(--accent);background:rgba(0,255,212,.08);box-shadow:inset 0 0 0 1px rgba(0,255,212,.18)}
.topnav .mainnav a.active::after{content:"";position:absolute;left:50%;bottom:-12px;transform:translateX(-50%);width:6px;height:6px;background:var(--accent);border-radius:50%;box-shadow:0 0 8px rgba(0,255,212,.6)}
.topnav .mainnav a .chev{font-size:9px;opacity:.6}
.topnav .tn-actions{display:flex;align-items:center;gap:8px;flex:0 0 auto}
.topnav .gsearch{flex:0 1 240px;position:relative}
.topnav .gsearch input{width:100%;padding:7px 12px 7px 32px;background:rgba(15,20,24,.85);border:1px solid var(--line);border-radius:7px;color:var(--txt);font-size:12.5px;transition:all .15s}
.topnav .gsearch input:focus{outline:none;border-color:var(--accent2);box-shadow:0 0 0 3px rgba(80,221,194,.08)}
.topnav .gsearch::before{content:"⌕";position:absolute;left:11px;top:50%;transform:translateY(-50%);font-size:13px;opacity:.5;pointer-events:none;color:var(--accent2)}
.topnav .live-status{display:inline-flex;align-items:center;gap:6px;font-size:11px;color:var(--muted);font-family:var(--mono)}

/* Sub navigation - pill-tab style, clearly visible */
.subnav{position:sticky;top:55px;z-index:99;display:none;align-items:center;gap:4px;padding:8px 22px;background:linear-gradient(180deg,rgba(7,10,13,.85),rgba(10,14,19,.92));backdrop-filter:saturate(160%) blur(14px);-webkit-backdrop-filter:saturate(160%) blur(14px);border-bottom:1px solid var(--line);overflow-x:auto;scrollbar-width:none}
.subnav::-webkit-scrollbar{display:none}
.subnav.show{display:flex}
.subnav a{position:relative;padding:7px 14px;color:#c9d0d8;font-size:12.5px;font-weight:600;text-decoration:none;font-family:-apple-system,Inter,sans-serif;cursor:pointer;white-space:nowrap;transition:all .15s;border-radius:7px;border:1px solid transparent;letter-spacing:.2px}
.subnav a:hover{color:#fff;background:rgba(255,255,255,.04);border-color:var(--line2)}
.subnav a.active{color:#06090c;background:linear-gradient(135deg,var(--accent),var(--accent-lo));border-color:var(--accent);box-shadow:0 4px 14px rgba(0,255,212,.25),inset 0 1px 0 rgba(255,255,255,.2);font-weight:700}
.subnav .sn-ic{display:none}

/* Mobile menu button (when top nav collapses) */
@media (max-width:880px){
 .topnav{padding:9px 14px;gap:8px}
 .topnav .tn-brand{padding-right:8px}
 .topnav .tn-brand .name{font-size:12.5px}
 .topnav .mainnav a{padding:7px 10px;font-size:12px}
 .topnav .gsearch{flex:1;min-width:0}
 .topnav .gsearch input{font-size:12px}
 .topnav .cta-trade .lbl{display:none}
}
@media (max-width:680px){
 .topnav{flex-wrap:wrap}
 .topnav .mainnav{order:3;flex-basis:100%;border-top:1px solid var(--line2);padding-top:6px;margin-top:4px}
 .topnav .gsearch{min-width:0}
 .subnav{top:auto;position:relative;padding:6px 14px}
}
.sidebar{background:linear-gradient(180deg,#080b0f 0%,#070a0d 100%);border-right:1px solid var(--line);position:sticky;top:0;height:100vh;display:flex;flex-direction:column;overflow:hidden}
.sidebar::after{content:"";position:absolute;top:0;right:0;width:1px;height:100%;background:linear-gradient(180deg,transparent,rgba(0,255,212,.12) 30%,rgba(0,255,212,.12) 70%,transparent);pointer-events:none}
.sidebar nav{flex:1;overflow-y:auto;padding:10px 0 6px;scrollbar-gutter:stable}
.sidebar nav::-webkit-scrollbar{width:5px}.sidebar nav::-webkit-scrollbar-thumb{background:var(--line);border-radius:3px}
.sidebar nav::-webkit-scrollbar-thumb:hover{background:rgba(0,255,212,.18)}

/* Brand area */
.brand{position:relative;padding:18px 18px 16px;display:flex;align-items:center;gap:12px;border-bottom:1px solid var(--line2);overflow:hidden;background:linear-gradient(135deg,rgba(0,255,212,.025),transparent 70%)}
.brand::before{content:"";position:absolute;top:0;left:0;right:0;height:1px;background:linear-gradient(90deg,transparent,rgba(0,255,212,.45),transparent);background-size:200% 100%;animation:slideHi 4s linear infinite}
.brand .logo{position:relative;width:40px;height:40px;border-radius:11px;background:linear-gradient(135deg,#0e1418,#070b0f);border:1px solid rgba(0,255,212,.22);display:flex;align-items:center;justify-content:center;box-shadow:0 4px 16px rgba(0,255,212,.15),inset 0 1px 0 rgba(0,255,212,.12)}
.brand .logo::after{content:"";position:absolute;inset:0;border-radius:11px;background:radial-gradient(circle at 30% 30%,rgba(0,255,212,.18),transparent 60%);pointer-events:none}
.brand .logo svg{position:relative;z-index:1;width:22px;height:auto;display:block}
.brand .name{font-family:-apple-system,Inter,sans-serif;font-weight:800;font-size:14.5px;letter-spacing:.4px;color:#fff;line-height:1.15;display:flex;align-items:center;gap:7px}
.brand .name .vbadge{font-family:var(--mono);font-size:9.5px;font-weight:700;color:var(--accent);background:rgba(0,255,212,.10);padding:1.5px 5px;border-radius:3px;border:1px solid rgba(0,255,212,.25);letter-spacing:0;line-height:1}
.brand .name .rscan{color:#04DF83;font-weight:700;text-shadow:0 0 12px rgba(4,223,131,.35);margin-left:1px}
.brand .sub{color:var(--muted);font-size:10.5px;margin-top:3px;letter-spacing:.6px;text-transform:uppercase;font-weight:600;font-family:-apple-system,Inter,sans-serif}

/* Nav groups */
.navgroup{padding:6px 10px 8px;margin:0;position:relative}
.navgroup+.navgroup::before{content:"";position:absolute;top:0;left:18px;right:18px;height:1px;background:linear-gradient(90deg,transparent,var(--line2) 30%,var(--line2) 70%,transparent)}
.navtitle{color:var(--muted2);font-size:9.5px;text-transform:uppercase;letter-spacing:1.3px;padding:8px 14px 6px;font-weight:700;font-family:-apple-system,Inter,sans-serif;display:flex;align-items:center;gap:7px}
.navtitle::before{content:"";width:3px;height:3px;border-radius:50%;background:var(--accent2);opacity:.5;flex:0 0 3px}

/* Nav items */
.navitem{position:relative;display:flex;align-items:center;gap:11px;padding:9px 12px;margin:1px 0;border-radius:8px;color:var(--muted);cursor:pointer;font-size:13px;font-weight:500;transition:all .15s cubic-bezier(.3,.85,.3,1);user-select:none;border:1px solid transparent;font-family:-apple-system,Inter,sans-serif}
.navitem:hover{background:rgba(0,255,212,.04);color:var(--txt)}
.navitem:hover .ico{color:var(--accent);transform:scale(1.08)}
.navitem.active{background:linear-gradient(90deg,rgba(0,255,212,.13) 0%,rgba(0,255,212,.04) 100%);color:var(--accent);border-color:rgba(0,255,212,.20);font-weight:600;box-shadow:inset 0 1px 0 rgba(0,255,212,.10),0 0 16px rgba(0,255,212,.06)}
.navitem.active::before{content:"";position:absolute;left:-11px;top:9px;bottom:9px;width:3px;background:linear-gradient(180deg,var(--accent),var(--accent-lo));border-radius:0 3px 3px 0;box-shadow:0 0 12px rgba(0,255,212,.7)}
.navitem.active::after{content:"";position:absolute;right:10px;top:50%;transform:translateY(-50%);width:5px;height:5px;border-radius:50%;background:var(--accent);box-shadow:0 0 8px rgba(0,255,212,.8);animation:pulse 2.6s ease-in-out infinite}
.navitem.active .ico{color:var(--accent)}
.navitem .ico{width:16px;height:16px;display:flex;align-items:center;justify-content:center;color:var(--muted2);transition:all .18s;flex:0 0 16px}
.navitem .ico svg{width:16px;height:16px;display:block;stroke-width:1.7}

/* Mini live stats card at bottom */
.sbstats{margin:6px 12px 8px;padding:11px 13px;border-radius:9px;background:linear-gradient(180deg,rgba(0,255,212,.04),rgba(0,255,212,.01));border:1px solid var(--line);position:relative;overflow:hidden;flex:0 0 auto}
.sbstats::before{content:"";position:absolute;top:0;left:0;right:0;height:1px;background:linear-gradient(90deg,transparent,rgba(0,255,212,.4),transparent);background-size:200% 100%;animation:slideHi 5s linear infinite}
.sbstats .sbs-row{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:6px}
.sbstats .sbs-row:last-child{margin-bottom:0}
.sbstats .sbs-lbl{font-size:9.5px;font-weight:700;color:var(--muted2);text-transform:uppercase;letter-spacing:1.2px;font-family:-apple-system,Inter,sans-serif}
.sbstats .sbs-val{font-family:var(--mono);font-size:13px;font-weight:700;color:#fff;letter-spacing:-.3px}
.sbstats .sbs-pulse{display:inline-block;width:5px;height:5px;border-radius:50%;background:var(--accent);box-shadow:0 0 6px rgba(0,255,212,.7);animation:pulse 2.2s ease-out infinite;margin-right:6px;vertical-align:middle}

/* Sidebar bottom toolbar */
.sbtool{flex:0 0 auto;display:flex;gap:4px;padding:8px 12px 14px;border-top:1px solid var(--line2);background:rgba(0,0,0,.18)}
.sbtool button{flex:1;background:transparent;border:1px solid var(--line);color:var(--muted);padding:7px 6px;border-radius:7px;font-size:11px;font-weight:600;cursor:pointer;transition:all .15s;display:flex;align-items:center;justify-content:center;gap:5px;font-family:-apple-system,Inter,sans-serif}
.sbtool button:hover{border-color:var(--accent2);color:var(--accent);background:rgba(0,255,212,.04)}
.sbtool button svg{width:13px;height:13px;stroke-width:1.7}

/* ===== Top bar ===== */
.main{min-width:0}
.topbar{display:flex;gap:12px;align-items:center;padding:13px 28px;border-bottom:1px solid var(--line2);position:sticky;top:0;background:rgba(7,10,13,.78);backdrop-filter:saturate(160%) blur(10px);-webkit-backdrop-filter:saturate(160%) blur(10px);z-index:10}
.topbar .menu-btn{display:none;background:none;border:none;color:var(--txt);font-size:20px;cursor:pointer;padding:4px 8px}
.topbar .gsearch{flex:1;max-width:540px;position:relative}
.topbar .gsearch input{width:100%;padding:9px 14px 9px 36px;background:rgba(15,20,24,.85);border:1px solid var(--line);border-radius:8px;color:var(--txt);font-size:13.5px;transition:all .15s}
.topbar .gsearch input:focus{outline:none;border-color:var(--accent2);box-shadow:0 0 0 3px rgba(80,221,194,.08)}
.topbar .gsearch::before{content:"⌕";position:absolute;left:13px;top:50%;transform:translateY(-50%);font-size:15px;opacity:.5;pointer-events:none;color:var(--accent2)}
.topbar .live{margin-left:auto;display:flex;gap:10px;align-items:center;font-size:12.5px;color:var(--muted);font-variant-numeric:tabular-nums}
.dot{width:7px;height:7px;border-radius:50%;background:var(--accent);box-shadow:0 0 0 4px rgba(151,252,228,.10);animation:pulse 2.4s ease-in-out infinite}
@keyframes pulse{0%,100%{box-shadow:0 0 0 0 rgba(151,252,228,.4)}50%{box-shadow:0 0 0 8px rgba(151,252,228,0)}}
.rf,.btn{background:rgba(15,20,24,.85);border:1px solid var(--line);color:var(--txt);border-radius:7px;padding:6px 11px;font-size:12.5px;cursor:pointer;transition:all .12s;font-weight:500}
.rf:hover,.btn:hover{border-color:var(--accent2);color:var(--accent)}

/* ===== CTA Trade button (referral) ===== */
.cta-trade{position:relative;display:inline-flex;align-items:center;gap:8px;padding:9px 16px;border-radius:8px;background:linear-gradient(135deg,#00ffd4 0%,#1aeeb3 50%,#0CD8B7 100%);background-size:200% 200%;color:#06090c !important;font-family:-apple-system,Inter,sans-serif;font-size:13px;font-weight:700;letter-spacing:.2px;text-decoration:none;border:1px solid rgba(0,255,212,.6);box-shadow:0 0 0 0 rgba(0,255,212,.4),0 6px 18px rgba(0,255,212,.22),inset 0 1px 0 rgba(255,255,255,.25);overflow:hidden;transition:transform .15s cubic-bezier(.3,1.4,.4,1),box-shadow .2s;cursor:pointer;animation:ctaShine 4s linear infinite;white-space:nowrap}
@keyframes ctaShine{0%{background-position:0% 50%}50%{background-position:100% 50%}100%{background-position:0% 50%}}
.cta-trade::before{content:"";position:absolute;top:0;left:-100%;width:60%;height:100%;background:linear-gradient(90deg,transparent,rgba(255,255,255,.45),transparent);transform:skewX(-20deg);animation:ctaSheen 3.5s ease-in-out infinite}
@keyframes ctaSheen{0%{left:-100%}55%{left:140%}100%{left:140%}}
.cta-trade:hover{transform:translateY(-1px) scale(1.02);box-shadow:0 0 0 4px rgba(0,255,212,.18),0 10px 28px rgba(0,255,212,.35),inset 0 1px 0 rgba(255,255,255,.35);color:#06090c}
.cta-trade:active{transform:translateY(0) scale(.99)}
.cta-trade .arr{font-size:14px;transition:transform .18s}
.cta-trade:hover .arr{transform:translateX(3px)}
.cta-trade svg{width:14px;height:14px;stroke-width:2.2;stroke:#06090c;flex:0 0 14px}

/* Sidebar CTA variant (full width) */
.cta-trade-sb{margin:0 12px 10px;padding:11px 14px;display:flex;justify-content:center;font-size:12.5px}
.cta-trade-sb svg{width:13px;height:13px}

/* Mobile: hide text, keep icon */
@media (max-width:680px){
 .cta-trade .lbl{display:none}
 .cta-trade{padding:9px 11px}
}

.wrap{padding:22px 28px;max-width:1400px;margin:0 auto}
.view{display:none;animation:fade .25s ease}.view.active{display:block}
@keyframes fade{from{opacity:0;transform:translateY(2px)}to{opacity:1;transform:none}}

/* compat: la .tab antigua redirige a .navitem (por si queda algun selector) */
.tabs{display:none}
.cards{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:18px}
.card{position:relative;background:linear-gradient(180deg,var(--panel),#0a0e13 100%);border:1px solid var(--line);border-radius:11px;padding:14px 16px;transition:transform .18s cubic-bezier(.2,.8,.2,1),border-color .18s,box-shadow .18s,background .18s;overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.025),0 1px 3px rgba(0,0,0,.3)}
.card::before{content:"";position:absolute;top:0;left:0;right:0;height:1px;background:linear-gradient(90deg,transparent 15%,rgba(0,255,212,.6) 50%,transparent 85%);opacity:0;transition:opacity .2s}
.card::after{content:"";position:absolute;inset:0;background:radial-gradient(circle at 50% 0%,rgba(0,255,212,.05),transparent 60%);opacity:0;transition:opacity .2s;pointer-events:none}
.card:hover{transform:translateY(-2px);border-color:rgba(0,255,212,.30);box-shadow:inset 0 1px 0 rgba(0,255,212,.07),0 8px 24px rgba(0,255,212,.06),0 2px 8px rgba(0,0,0,.4)}
.card:hover::before{opacity:1}
.card:hover::after{opacity:1}
.card.alert{border-color:var(--amber);box-shadow:0 0 0 2px rgba(255,180,84,.18),0 0 20px rgba(255,180,84,.15)}
.card.featured{border-color:rgba(0,255,212,.30);background:linear-gradient(180deg,var(--panel),rgba(0,255,212,.025) 100%)}
.card.featured::before{opacity:1;background:linear-gradient(90deg,transparent,var(--accent),transparent);background-size:200% 100%;animation:slideHi 3.2s linear infinite}
@keyframes slideHi{0%{background-position:100% 0}100%{background-position:-100% 0}}
.card .lbl{color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.85px;font-weight:700;display:flex;align-items:center;gap:6px}
.card .val{font-size:24px;font-weight:700;margin-top:6px;letter-spacing:-.5px;color:#ffffff;line-height:1.1;font-variant-numeric:tabular-nums}
.card .val.pos{color:var(--green);text-shadow:0 0 18px rgba(26,238,170,.32)}
.card .val.neg{color:var(--red);text-shadow:0 0 18px rgba(255,59,110,.30)}
.card .meta{font-size:11.5px;color:var(--muted);margin-top:6px;font-weight:500}

/* Value-changed flash animation */
@keyframes flashUp{0%{background:rgba(26,238,170,.30)}100%{background:transparent}}
@keyframes flashDn{0%{background:rgba(255,59,110,.30)}100%{background:transparent}}
.flash-up{animation:flashUp .55s ease-out}
.flash-dn{animation:flashDn .55s ease-out}
/* .tag definida mas abajo con estilo refinado */
.grid2{display:grid;grid-template-columns:1.4fr 1fr;gap:14px;margin-bottom:16px}
.grid3{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:14px}
@media (max-width:1000px){.grid3{grid-template-columns:1fr}}
.compact-charts .panel{padding:12px 14px}
.compact-charts .panel h2{font-size:13px;margin:0 0 6px;color:var(--muted);text-transform:uppercase;letter-spacing:.6px;font-weight:600}
.compact-charts .note.mini{font-size:11px;color:var(--muted);margin-top:6px;font-family:var(--mono);text-align:center}
.panel{background:linear-gradient(180deg,var(--panel),#0c1015 100%);border:1px solid var(--line);border-radius:11px;padding:18px 20px;transition:border-color .18s,box-shadow .18s;box-shadow:inset 0 1px 0 rgba(255,255,255,.02)}
.panel:hover{border-color:rgba(0,255,212,.14)}
.panel h2{font-size:13px;margin:0 0 14px;font-weight:700;color:var(--txt);display:flex;align-items:center;gap:9px;letter-spacing:.2px;text-transform:uppercase}
.panel h2::before{content:"";display:inline-block;width:3px;height:14px;border-radius:2px;background:linear-gradient(180deg,var(--accent),var(--accent-lo));box-shadow:0 0 10px rgba(0,255,212,.5)}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{padding:10px 12px;text-align:right;border-bottom:1px solid var(--line2);white-space:nowrap}
th:first-child,td:first-child{text-align:left}
th{color:var(--muted);font-weight:600;font-size:10.5px;text-transform:uppercase;letter-spacing:.8px;cursor:pointer;user-select:none;background:rgba(0,255,212,.018);border-bottom:1px solid var(--line);position:relative}
th:hover{color:var(--accent)}
tbody tr{transition:background .14s,border-color .14s;position:relative}
tbody tr:nth-child(even){background:rgba(0,255,212,.008)}
tbody tr:hover{background:rgba(0,255,212,.05);box-shadow:inset 2px 0 0 var(--accent)}
tbody tr:hover td:first-child{padding-left:14px}
td{font-variant-numeric:tabular-nums}
.pos{color:var(--green)}.neg{color:var(--red)}.mkt{font-weight:600;color:var(--txt)}
.bar{height:5px;border-radius:3px;background:rgba(255,255,255,.05);overflow:hidden;margin-top:6px}
.bar>i{display:block;height:100%;background:linear-gradient(90deg,var(--accent2),var(--accent));border-radius:3px;box-shadow:0 0 8px rgba(0,255,212,.4)}

/* Inline change-24h pill */
.chgpill{display:inline-block;font-family:var(--mono);font-weight:700;font-size:12.5px;padding:3px 9px;border-radius:5px;letter-spacing:-.2px}
.chgpill.pos{background:rgba(26,238,170,.10);color:var(--green);box-shadow:inset 0 0 0 1px rgba(26,238,170,.25),0 0 12px rgba(26,238,170,.10)}
.chgpill.neg{background:rgba(255,59,110,.10);color:var(--red);box-shadow:inset 0 0 0 1px rgba(255,59,110,.25),0 0 12px rgba(255,59,110,.10)}

/* Bipolar funding bar (centered at zero) */
.fbar{display:inline-block;position:relative;width:96px;height:6px;border-radius:3px;background:rgba(255,255,255,.04);margin-left:8px;vertical-align:middle;overflow:hidden}
.fbar::before{content:"";position:absolute;left:50%;top:0;bottom:0;width:1px;background:rgba(255,255,255,.18)}
.fbar>i{position:absolute;top:0;bottom:0;border-radius:3px}
.fbar>i.pos{left:50%;background:linear-gradient(90deg,rgba(26,238,170,.4),var(--green));box-shadow:0 0 8px rgba(26,238,170,.5)}
.fbar>i.neg{right:50%;background:linear-gradient(270deg,rgba(255,59,110,.4),var(--red));box-shadow:0 0 8px rgba(255,59,110,.5)}

/* Sparkline cell in markets table */
.spark-cell{padding:6px 8px !important;width:84px}
.spark-cell svg{display:block;width:80px;height:30px;overflow:visible}

/* ===== Wallet tabs (hypurrscan-style) ===== */
.walltabs{display:flex;gap:4px;margin:18px 0 14px;padding:5px;background:linear-gradient(180deg,var(--panel),#0a0e13);border:1px solid var(--line);border-radius:10px;overflow-x:auto;scrollbar-width:thin}
.walltabs::-webkit-scrollbar{height:4px}
.walltabs::-webkit-scrollbar-thumb{background:var(--line);border-radius:2px}
.walltab{flex:0 0 auto;display:inline-flex;align-items:center;gap:7px;padding:9px 14px;border-radius:7px;color:var(--muted);font-size:12.5px;font-weight:600;letter-spacing:.3px;cursor:pointer;border:1px solid transparent;background:transparent;transition:all .14s;white-space:nowrap;font-family:-apple-system,Inter,sans-serif;text-transform:uppercase}
.walltab:hover{background:rgba(0,255,212,.04);color:var(--txt)}
.walltab.active{background:linear-gradient(180deg,rgba(0,255,212,.10),rgba(0,255,212,.04));color:var(--accent);border-color:rgba(0,255,212,.22);box-shadow:inset 0 1px 0 rgba(0,255,212,.10),0 0 12px rgba(0,255,212,.10)}
.walltab .count{background:rgba(0,255,212,.10);color:var(--accent);font-family:var(--mono);font-size:10.5px;font-weight:700;padding:1px 7px;border-radius:10px;letter-spacing:0;text-transform:none;border:1px solid rgba(0,255,212,.18)}
.walltab .ico{font-size:13px;opacity:.85}
.wallsec{display:none}
.wallsec.active{display:block;animation:fadein .22s ease}
@keyframes fadein{from{opacity:0;transform:translateY(3px)}to{opacity:1;transform:none}}
.pill{font-size:10.5px;padding:2px 9px;border-radius:4px;font-weight:600;letter-spacing:.3px;text-transform:uppercase}
.pill.bid{background:rgba(54,211,156,.10);color:var(--green);border:1px solid rgba(54,211,156,.22)}
.pill.ask{background:rgba(255,84,102,.10);color:var(--red);border:1px solid rgba(255,84,102,.22)}
input,select{background:rgba(15,20,24,.7);border:1px solid var(--line);color:var(--txt);border-radius:7px;padding:9px 12px;font-size:13px;transition:all .15s}
input:focus,select:focus{outline:none;border-color:var(--accent2);box-shadow:0 0 0 3px rgba(80,221,194,.08)}
input{width:430px;max-width:100%}
.row{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-bottom:14px}
.note{color:var(--muted);font-size:12px;line-height:1.6}
.empty{color:var(--muted);font-size:13px;padding:18px 4px}
.alertgrid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}
label.fl{display:block;font-size:12px;color:var(--muted);margin-bottom:6px}
.seg{display:inline-flex;background:rgba(15,20,24,.6);border:1px solid var(--line);border-radius:8px;overflow:hidden;padding:3px}
.seg button{background:none;border:none;color:var(--muted);padding:5px 12px;font-size:12px;cursor:pointer;border-radius:5px;font-weight:500;transition:all .12s}
.seg button:hover{color:var(--txt)}
.seg button.active{background:rgba(151,252,228,.08);color:var(--accent);font-weight:600}
a{color:var(--accent2);text-decoration:none}
footer{color:var(--muted);font-size:11.5px;margin-top:18px;line-height:1.6}
/* Wallet detail page */
.whead{background:linear-gradient(135deg,#0d1318,#0a0d11 70%);border:1px solid var(--line);border-radius:12px;padding:22px 24px;margin-bottom:18px;position:relative;overflow:hidden}
.whead::before{content:"";position:absolute;right:-100px;top:-100px;width:280px;height:280px;background:radial-gradient(closest-side,rgba(151,252,228,.10),transparent);pointer-events:none}
.whead::after{content:"";position:absolute;top:0;left:0;right:0;height:1px;background:linear-gradient(90deg,transparent,rgba(151,252,228,.3),transparent)}
.whead .row1{display:flex;align-items:center;gap:14px;flex-wrap:wrap;position:relative}
.whead .avatar{width:42px;height:42px;border-radius:8px;background:linear-gradient(135deg,#0d1418,#070a0d);border:1px solid rgba(151,252,228,.2);display:flex;align-items:center;justify-content:center;color:var(--accent);font-weight:700;font-size:15px;letter-spacing:.5px}
.whead .addr{font-family:ui-monospace,Menlo,monospace;font-size:13.5px;letter-spacing:.2px;color:var(--txt)}
.whead .actions{margin-left:auto;display:flex;gap:8px;flex-wrap:wrap}
.chip{display:inline-flex;align-items:center;gap:5px;background:rgba(15,20,24,.85);border:1px solid var(--line);color:var(--txt);padding:6px 11px;border-radius:7px;font-size:12px;cursor:pointer;text-decoration:none;transition:all .12s;font-weight:500}
.chip:hover{border-color:rgba(151,252,228,.3);color:var(--accent);background:rgba(151,252,228,.04)}
.sectitle{font-size:11.5px;font-weight:600;color:var(--muted);margin:22px 0 12px;letter-spacing:.7px;text-transform:uppercase;display:flex;align-items:center;gap:9px}
.sectitle::before{content:"";display:inline-block;width:2px;height:12px;border-radius:1px;background:var(--accent)}
.pillside{display:inline-block;font-size:10px;font-weight:600;padding:2px 8px;border-radius:4px;letter-spacing:.4px;text-transform:uppercase}
.pillside.Long,.pillside.Buy{background:rgba(54,211,156,.10);color:var(--green);border:1px solid rgba(54,211,156,.22)}
.pillside.Short,.pillside.Sell{background:rgba(255,84,102,.10);color:var(--red);border:1px solid rgba(255,84,102,.22)}
.tag{display:inline-flex;align-items:center;gap:4px;font-size:9.5px;padding:2px 6px;border-radius:4px;vertical-align:middle;margin-left:6px;font-weight:600;letter-spacing:.4px;text-transform:uppercase}
.tag.live{background:rgba(151,252,228,.10);color:var(--accent);border:1px solid rgba(151,252,228,.25)}
.tag.snap{background:rgba(107,119,133,.10);color:var(--muted);border:1px solid var(--line)}
.liq-near{color:var(--red);font-weight:600}
.liq-mid{color:var(--amber);font-weight:600}
.liq-far{color:var(--green)}
.role-T{color:var(--amber);font-size:11px;font-weight:600}
.role-M{color:var(--accent2);font-size:11px;font-weight:600}
.smart-badge{display:inline-block;font-size:9px;font-weight:700;padding:2px 6px;border-radius:3px;letter-spacing:.6px;background:linear-gradient(135deg,rgba(151,252,228,.18),rgba(80,221,194,.10));color:var(--accent);border:1px solid rgba(151,252,228,.35);vertical-align:middle;margin-left:6px;text-transform:uppercase;text-shadow:0 0 8px rgba(151,252,228,.4)}

/* ===== Info tooltips ===== */
.info{display:inline-block;width:13px;height:13px;border-radius:50%;background:rgba(151,252,228,.08);color:var(--accent2);font-size:9px;font-weight:700;text-align:center;line-height:12px;cursor:help;margin-left:5px;position:relative;vertical-align:middle;border:1px solid rgba(151,252,228,.22);font-family:Georgia,serif;font-style:italic}
.info::before{content:"i"}
.info[data-tip]:hover::after{content:attr(data-tip);position:absolute;left:50%;transform:translateX(-50%);bottom:calc(100% + 8px);background:#0a0d11;color:#e9edf2;font-size:11.5px;font-weight:400;padding:9px 11px;border-radius:6px;white-space:normal;width:260px;line-height:1.45;text-transform:none;letter-spacing:.1px;z-index:1000;border:1px solid var(--line);box-shadow:0 8px 24px rgba(0,0,0,.55);text-align:left;font-style:normal;pointer-events:none}
.info[data-tip]:hover::before{content:"i";position:relative}
.info[data-tip]:hover{background:rgba(151,252,228,.18);color:var(--accent)}

/* ===== Skeleton loading ===== */
.skel{display:inline-block;background:linear-gradient(90deg,#0f1418 0%,#1a2129 50%,#0f1418 100%);background-size:200% 100%;animation:shim 1.4s ease-in-out infinite;border-radius:4px;height:1.1em;min-width:80px;vertical-align:middle;color:transparent}
.skel-line{display:block;height:14px;margin:6px 0;border-radius:3px}
.skel-row{display:block;height:36px;margin:4px 0;border-radius:6px;background:linear-gradient(90deg,#0f1418 0%,#171c22 50%,#0f1418 100%);background-size:200% 100%;animation:shim 1.4s ease-in-out infinite}
@keyframes shim{0%{background-position:200% 0}100%{background-position:-200% 0}}
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
<body>
<!-- Command palette ⌘K -->
<div class="cmdk-back" id="cmdk_back" onclick="if(event.target===this)closeCmdK()">
 <div class="cmdk">
  <input class="cmdk-input" id="cmdk_input" placeholder="Search wallet (0x…), market, view, action…" autocomplete="off" autocorrect="off" spellcheck="false">
  <div class="cmdk-list" id="cmdk_list"></div>
  <div class="cmdk-foot"><span><kbd>↑↓</kbd> navigate</span><span><kbd>↵</kbd> select</span><span><kbd>esc</kbd> close</span><span style="margin-left:auto"><kbd>⌘K</kbd> to open</span></div>
 </div>
</div>
<div id="ctx_root"></div>
<div class="toasts" id="toasts"></div>
<div id="confetti-root"></div>
<button class="randwhale" onclick="randomWhale()" title="🐋 Discover a random whale wallet">🐋</button>

<!-- Trader Card modal -->
<div class="tcard-modal" id="tcard_modal" onclick="if(event.target===this)closeTcard()">
 <div class="tcard-wrap">
  <div id="tcard_render"></div>
  <div class="tcard-actions">
   <button onclick="downloadTcard()">⬇ Download PNG</button>
   <button class="secondary" onclick="closeTcard()">Close</button>
  </div>
 </div>
</div>
<!-- Welcome tour -->
<div class="tour-back" id="tour_back">
 <div class="tour" id="tour_box">
  <div class="dots" id="tour_dots"></div>
  <h3 id="tour_step">Step 1 of 5</h3>
  <h2 id="tour_title">Welcome to RISExscan</h2>
  <p id="tour_body">The real-time analytics dashboard for RISE chain perps. Built on public data. No accounts, no tracking.</p>
  <div class="acts">
   <button class="rf" onclick="closeTour()">Skip</button>
   <button class="rf" onclick="tourPrev()" id="tour_prev" style="display:none">← Back</button>
   <button class="rf" onclick="tourNext()" id="tour_next" style="border-color:var(--accent);color:var(--accent)">Next →</button>
  </div>
 </div>
</div>

<div id="splash">
 <div class="splash-logo"><svg viewBox="555 0 210 182" xmlns="http://www.w3.org/2000/svg"><path d="M675.798 83.41C690.4 71.16 712.646 76.09 720.7 93.37L762.031 182H733.687L698.974 107.56C693.603 96.04 678.77 92.76 669.036 100.92L572.632 181.82H558.466L675.798 83.41ZM585.345 0L620.058 74.44C625.428 85.96 640.261 89.24 649.995 81.08L746.399 0.18H760.565L643.233 98.59C628.631 110.84 606.386 105.91 598.331 88.63L557 0H585.345Z" fill="#04DF83"/></svg></div>
 <div class="splash-text">RISEXSCAN</div>
</div>
<div class="app">
<aside class="sidebar" id="sidebar">
 <div class="brand">
  <a href="/" class="brand-link" style="display:flex;align-items:center;gap:4px;text-decoration:none;color:inherit">
   <svg class="sb-wordmark" viewBox="0 0 762 182" xmlns="http://www.w3.org/2000/svg" style="height:24px;width:auto;display:block;filter:drop-shadow(0 0 10px rgba(4,223,131,.25))">
    <path d="M105.684 0H0V30.31H105.684C114.016 30.31 120.781 37.09 120.781 45.45V60.59H46.234C20.698 60.61 0 81.29 0 106.83V181.82H30.195V110.57L106.624 181.82H151L53.49 90.9H120.804V60.75H151V45.45C151 20.34 130.705 0 105.684 0Z" fill="#fff"/>
    <path d="M203.371 0H173.041V181.82H203.371V0Z" fill="#fff"/>
    <path d="M225.592 60.26C225.592 85.37 245.886 105.71 270.907 105.71H322.047C330.379 105.71 337.144 112.54 337.144 120.92V136.37C337.144 144.75 330.379 151.51 322.047 151.51H225.592V181.82H322.047C347.068 181.82 367.362 161.48 367.362 136.37V120.92C367.362 95.81 347.068 75.47 322.069 75.47H270.93C262.597 75.47 255.832 68.63 255.832 60.26V45.47C255.832 37.09 262.597 30.33 270.93 30.33H358.133V0H270.93C245.909 0 225.614 20.34 225.614 45.45V60.23L225.592 60.26Z" fill="#fff"/>
    <path d="M384.252 45.45V60.75H414.447V45.45C414.447 37.07 421.212 30.31 429.545 30.31H527.052V0H429.545C404.524 0 384.229 20.34 384.229 45.45H384.252Z" fill="#fff"/>
    <path d="M430.598 75.44H516.771V105.71L430.598 105.75C422.265 105.75 415.5 112.54 415.5 120.89V136.35C415.5 144.73 422.265 151.49 430.598 151.49H527.052V181.8H430.598C405.577 181.8 385.282 161.44 385.282 136.35V120.89C385.282 95.78 405.577 75.44 430.575 75.44H430.598Z" fill="#fff"/>
    <path d="M675.798 83.41C690.4 71.16 712.646 76.09 720.7 93.37L762.031 182H733.687L698.974 107.56C693.603 96.04 678.77 92.76 669.036 100.92L572.632 181.82H558.466L675.798 83.41ZM585.345 0L620.058 74.44C625.428 85.96 640.261 89.24 649.995 81.08L746.399 0.18H760.565L643.233 98.59C628.631 110.84 606.386 105.91 598.331 88.63L557 0H585.345Z" fill="#04DF83"/>
   </svg>
   <span class="sb-suffix" style="font-family:-apple-system,Inter,sans-serif;font-weight:700;font-size:18px;color:#04DF83;letter-spacing:-.3px;line-height:1;text-transform:lowercase;text-shadow:0 0 12px rgba(4,223,131,.35)">scan</span>
  </a>
 </div>
 <nav>
  <!-- 1. Overview (leaf) -->
  <div class="ngroup" data-sec="overview">
   <div class="ghead navitem active" data-v="overview"><span class="ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="9" rx="1"/><rect x="14" y="3" width="7" height="5" rx="1"/><rect x="14" y="12" width="7" height="9" rx="1"/><rect x="3" y="16" width="7" height="5" rx="1"/></svg></span>Overview</div>
  </div>
  <!-- 2. Markets (leaf) -->
  <div class="ngroup" data-sec="markets">
   <div class="ghead navitem" data-v="markets"><span class="ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 17 9 11 13 15 21 7"/><polyline points="14 7 21 7 21 14"/></svg></span>Markets</div>
  </div>
  <!-- 3. Traders (group) -->
  <div class="ngroup expandable" data-sec="traders">
   <div class="ghead" onclick="toggleNgroup('traders')"><span class="ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><path d="M8 21h8"/><path d="M12 17v4"/><path d="M6 4h12v5a6 6 0 0 1-12 0V4z"/><path d="M18 6h2a2 2 0 0 1 0 4h-1"/><path d="M6 6H4a2 2 0 0 0 0 4h1"/></svg></span>Traders<span class="chev">▾</span></div>
   <div class="gchildren">
    <div class="navitem" data-v="pnl"><span class="ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M9 9l6 6"/><path d="M15 9l-6 6"/></svg></span>Top PnL</div>
    <div class="navitem" data-v="volranking"><span class="ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="13" width="4" height="8" rx="1"/><rect x="10" y="9" width="4" height="12" rx="1"/><rect x="17" y="5" width="4" height="16" rx="1"/></svg></span>Volume</div>
    <div class="navitem" data-v="acctoi"><span class="ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="7" width="18" height="13" rx="2"/><path d="M8 7V5a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/><path d="M12 12v4"/></svg></span>Current OI</div>
    <div class="navitem" data-v="oiranking"><span class="ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="13" r="8"/><path d="M12 9v4l2.5 2.5"/><path d="M9 2h6"/></svg></span>Avg OI</div>
    <div class="navitem" data-v="ranking"><span class="ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3v18"/><path d="M5 7l7-4 7 4"/><circle cx="5" cy="7" r="1.5"/><circle cx="19" cy="7" r="1.5"/><path d="M3 11l2 4 2-4"/><path d="M17 11l2 4 2-4"/></svg></span>Positions by market</div>
    <div class="navitem" data-v="funded"><span class="ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 7v10"/><path d="M9 10c0-1.5 1.5-2 3-2s3 .5 3 2-1.5 2-3 2-3 .5-3 2 1.5 2 3 2 3-.5 3-2"/></svg></span>Funding payments</div>
    <div class="navitem" data-v="liq"><span class="ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><path d="M13 2L4 14h7l-1 8 9-12h-7l1-8z"/></svg></span>Liquidations</div>
    <div class="navitem" data-v="feed"><span class="ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg></span>Live activity</div>
   </div>
  </div>
  <!-- 4. Insights (group) -->
  <div class="ngroup expandable" data-sec="insights">
   <div class="ghead" onclick="toggleNgroup('insights')"><span class="ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><path d="M9 18h6"/><path d="M10 22h4"/><path d="M12 2a7 7 0 0 0-4 12.7c1 .7 1.5 1.5 1.5 2.3v1h5v-1c0-.8.5-1.6 1.5-2.3A7 7 0 0 0 12 2z"/></svg></span>Insights<span class="chev">▾</span></div>
   <div class="gchildren">
    <div class="navitem" data-v="longshort"><span class="ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><path d="M3 12h18"/><path d="M12 3v18"/><path d="M5 7l7 5 7-5"/><path d="M5 17l7-5 7 5"/></svg></span>Long / Short</div>
    <div class="navitem" data-v="heatmap"><span class="ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2c2 4 5 6 5 11a5 5 0 0 1-10 0c0-3 2-5 3-8 1 2 2 3 2 5"/></svg></span>Liq. heatmap</div>
    <div class="navitem" data-v="marketshare"><span class="ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><path d="M21.21 15.89A10 10 0 1 1 8 2.83"/><path d="M22 12A10 10 0 0 0 12 2v10z"/></svg></span>Market share</div>
    <div class="navitem" data-v="funding"><span class="ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><path d="M3 7h13"/><path d="M16 4l3 3-3 3"/><path d="M21 17H8"/><path d="M8 20l-3-3 3-3"/></svg></span>Funding vs DEXes</div>
   </div>
  </div>
  <!-- 5. Explorer (leaf) -->
  <div class="ngroup" data-sec="explorer">
   <div class="ghead navitem" data-v="explorer"><span class="ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><path d="M14 17.5h7"/><path d="M17.5 14v7"/></svg></span>Explorer</div>
  </div>
  <!-- 6. Tools (group) -->
  <div class="ngroup expandable" data-sec="tools">
   <div class="ghead" onclick="toggleNgroup('tools')"><span class="ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><path d="M14.7 6.3a3 3 0 0 0 4.2 4.2L21 12.4a8 8 0 0 1-11.3 9.3L3 15l3-3 6.7 6.7 1.7-1.7L8 10l4-4 6.7 6.7 1.7-1.7L13 4l1.7-1.7"/></svg></span>Tools<span class="chev">▾</span></div>
   <div class="gchildren">
    <div class="navitem" data-v="tools"><span class="ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="3" width="16" height="18" rx="2"/><path d="M8 7h8"/><path d="M8 11h8"/><path d="M8 15h5"/></svg></span>Simulator &amp; Calculator</div>
    <div class="navitem" data-v="compare"><span class="ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><path d="M4 6h6"/><path d="M14 6h6"/><path d="M4 12h6"/><path d="M14 12h6"/><path d="M4 18h6"/><path d="M14 18h6"/></svg></span>Compare wallets</div>
    <div class="navitem" data-v="watchlist"><span class="ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><polygon points="12 2 15 8.5 22 9.3 17 14.1 18.2 21 12 17.8 5.8 21 7 14.1 2 9.3 9 8.5 12 2"/></svg></span>Watchlist</div>
    <div class="navitem" data-v="alerts"><span class="ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><path d="M18 8a6 6 0 0 0-12 0c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.7 21a2 2 0 0 1-3.4 0"/></svg></span>Alerts</div>
    <div class="navitem" data-v="copy"><span class="ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="12" height="12" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg></span>Copy trading</div>
   </div>
  </div>
  <!-- Hidden but reachable via cmdK -->
  <div class="navitem" data-v="users" style="display:none"></div>
 </nav>
 <a class="cta-trade cta-trade-sb" href="https://www.rise.trade/invite/ticb" target="_blank" rel="noopener">
  <svg viewBox="0 0 24 24" fill="none"><polyline points="3 17 9 11 13 15 21 7" stroke-linecap="round" stroke-linejoin="round"/><polyline points="14 7 21 7 21 14" stroke-linecap="round" stroke-linejoin="round"/></svg>
  <span class="lbl">Trade on RISEx</span><span class="arr">→</span>
 </a>
 <div class="sbstats">
  <div class="sbs-row"><span class="sbs-lbl"><span class="sbs-pulse"></span>24h Vol</span><span class="sbs-val" id="sb_vol">—</span></div>
  <div class="sbs-row"><span class="sbs-lbl">Open Interest</span><span class="sbs-val" id="sb_oi">—</span></div>
 </div>
 <div class="sbtool">
  <button title="Command palette ⌘K" onclick="openCmdK()"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="7"/><path d="m20 20-3-3"/></svg> ⌘K</button>
  <button title="Themes" onclick="document.getElementById('themepicker').classList.toggle('open')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M2 12h20"/><path d="M12 2a15 15 0 0 1 0 20"/><path d="M12 2a15 15 0 0 0 0 20"/></svg></button>
  <button title="Sound FX" onclick="toggleSound();this.firstChild.textContent=window._soundOn?'🔊':'🔇'"><span style="font-size:13px">🔇</span></button>
 </div>
</aside>
<main class="main">
<header class="topnav">
 <a class="tn-brand" href="/">
  <svg class="tn-wordmark" viewBox="0 0 762 182" xmlns="http://www.w3.org/2000/svg">
   <path d="M105.684 0H0V30.31H105.684C114.016 30.31 120.781 37.09 120.781 45.45V60.59H46.234C20.698 60.61 0 81.29 0 106.83V181.82H30.195V110.57L106.624 181.82H151L53.49 90.9H120.804V60.75H151V45.45C151 20.34 130.705 0 105.684 0Z" fill="#fff"/>
   <path d="M203.371 0H173.041V181.82H203.371V0Z" fill="#fff"/>
   <path d="M225.592 60.26C225.592 85.37 245.886 105.71 270.907 105.71H322.047C330.379 105.71 337.144 112.54 337.144 120.92V136.37C337.144 144.75 330.379 151.51 322.047 151.51H225.592V181.82H322.047C347.068 181.82 367.362 161.48 367.362 136.37V120.92C367.362 95.81 347.068 75.47 322.069 75.47H270.93C262.597 75.47 255.832 68.63 255.832 60.26V45.47C255.832 37.09 262.597 30.33 270.93 30.33H358.133V0H270.93C245.909 0 225.614 20.34 225.614 45.45V60.23L225.592 60.26Z" fill="#fff"/>
   <path d="M384.252 45.45V60.75H414.447V45.45C414.447 37.07 421.212 30.31 429.545 30.31H527.052V0H429.545C404.524 0 384.229 20.34 384.229 45.45H384.252Z" fill="#fff"/>
   <path d="M430.598 75.44H516.771V105.71L430.598 105.75C422.265 105.75 415.5 112.54 415.5 120.89V136.35C415.5 144.73 422.265 151.49 430.598 151.49H527.052V181.8H430.598C405.577 181.8 385.282 161.44 385.282 136.35V120.89C385.282 95.78 405.577 75.44 430.575 75.44H430.598Z" fill="#fff"/>
   <path d="M675.798 83.41C690.4 71.16 712.646 76.09 720.7 93.37L762.031 182H733.687L698.974 107.56C693.603 96.04 678.77 92.76 669.036 100.92L572.632 181.82H558.466L675.798 83.41ZM585.345 0L620.058 74.44C625.428 85.96 640.261 89.24 649.995 81.08L746.399 0.18H760.565L643.233 98.59C628.631 110.84 606.386 105.91 598.331 88.63L557 0H585.345Z" fill="#04DF83"/>
  </svg>
  <span class="tn-suffix">scan</span>
 </a>
 <nav class="mainnav">
  <a data-section="overview" class="active">Overview</a>
  <a data-section="markets">Markets</a>
  <a data-section="traders">Traders <span class="chev">▾</span></a>
  <a data-section="insights">Insights <span class="chev">▾</span></a>
  <a data-section="explorer">Explorer</a>
  <a data-section="tools">Tools <span class="chev">▾</span></a>
 </nav>
 <div class="tn-actions">
  <div class="gsearch"><input id="gs_input" placeholder="Search wallet (0x…) or market…" autocomplete="off" /></div>
  <button class="rf" onclick="openCmdK()" title="⌘K — Command palette">⌘K</button>
  <div style="position:relative">
   <button class="notif-btn" id="notif_btn" onclick="toggleNotif()" title="Notifications">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><path d="M18 8a6 6 0 0 0-12 0c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.7 21a2 2 0 0 1-3.4 0"/></svg>
    <span class="notif-dot"></span>
   </button>
   <div class="notif-panel" id="notif_panel">
    <div class="notif-head"><h4>Notifications</h4><a onclick="clearNotifs()">Clear all</a></div>
    <div class="notif-list" id="notif_list"></div>
   </div>
  </div>
  <span class="themepicker" id="themepicker"><button class="rf" onclick="document.getElementById('themepicker').classList.toggle('open')" id="theme_btn" title="Theme">🎨</button>
   <div class="swatches">
    <div class="swatch" data-th="mint" onclick="setColorTheme('mint')"><span class="dotc"></span> Mint <span style="margin-left:auto;color:var(--muted);font-size:10px">⌘1</span></div>
    <div class="swatch" data-th="magenta" onclick="setColorTheme('magenta')"><span class="dotc"></span> Magenta <span style="margin-left:auto;color:var(--muted);font-size:10px">⌘2</span></div>
    <div class="swatch" data-th="solar" onclick="setColorTheme('solar')"><span class="dotc"></span> Solar <span style="margin-left:auto;color:var(--muted);font-size:10px">⌘3</span></div>
    <div class="swatch" data-th="mono" onclick="setColorTheme('mono')"><span class="dotc"></span> Mono <span style="margin-left:auto;color:var(--muted);font-size:10px">⌘4</span></div>
    <div class="swatch" data-th="light" onclick="setColorTheme('light')"><span class="dotc"></span> Light <span style="margin-left:auto;color:var(--muted);font-size:10px">⌘5</span></div>
    <div class="swatch" onclick="toggleSound();this.querySelector('.dotc').textContent=window._soundOn?'🔊':'🔇'" style="margin-top:6px;border-top:1px solid var(--line2);padding-top:9px"><span class="dotc" style="background:none;box-shadow:none">🔇</span> Sound FX</div>
    <div style="margin-top:6px;border-top:1px solid var(--line2);padding-top:9px">
     <div style="font-size:10px;color:var(--muted2);letter-spacing:1px;text-transform:uppercase;font-weight:700;padding:0 9px 6px">Density</div>
     <div style="display:flex;gap:4px;padding:0 6px">
      <button class="rf" style="flex:1;font-size:11px" onclick="setDensity('compact')">Compact</button>
      <button class="rf" style="flex:1;font-size:11px" onclick="setDensity('cozy')">Cozy</button>
      <button class="rf" style="flex:1;font-size:11px" onclick="setDensity('spacious')">Spacious</button>
     </div>
    </div>
   </div>
  </span>
  <a class="cta-trade" href="https://www.rise.trade/invite/ticb" target="_blank" rel="noopener" title="Open RISEx with referral discount">
   <svg viewBox="0 0 24 24" fill="none"><polyline points="3 17 9 11 13 15 21 7" stroke-linecap="round" stroke-linejoin="round"/><polyline points="14 7 21 7 21 14" stroke-linecap="round" stroke-linejoin="round"/></svg>
   <span class="lbl">Trade</span><span class="arr">→</span>
  </a>
 </div>
</header>
<nav class="subnav" id="subnav"></nav>
<div style="display:none"><span class="dot" id="dot"></span><span id="updated"></span></div>
<div class="wrap">

<div class="view active" id="v_overview">
 <div class="hero">
  <canvas class="particles" id="hero_particles"></canvas>
  <div class="hero-card" id="hero_vol">
   <div class="hero-lbl">24h VOLUME <span class="livedot"></span></div>
   <div class="hero-val" id="hv_vol">—</div>
   <div class="hero-meta" id="hv_vol_m"><span class="skel" style="min-width:160px"></span></div>
   <svg class="hero-spark" id="hv_vol_spark" viewBox="0 0 320 60" preserveAspectRatio="none"></svg>
  </div>
  <div class="hero-card" id="hero_oi">
   <div class="hero-lbl">OPEN INTEREST <span class="livedot"></span></div>
   <div class="hero-val" id="hv_oi">—</div>
   <div class="hero-meta" id="hv_oi_m"><span class="skel" style="min-width:160px"></span></div>
   <svg class="hero-spark" id="hv_oi_spark" viewBox="0 0 320 60" preserveAspectRatio="none"></svg>
  </div>
 </div>
 <div class="cards">
  <div class="card featured" id="k_vol"><div class="lbl">24h Volume<span class="tag live">live</span></div><div class="val" id="c_vol">—</div><div class="meta" id="c_vol_m"></div></div>
  <div class="card featured" id="k_oi"><div class="lbl">Open Interest<span class="tag live">live</span></div><div class="val" id="c_oi">—</div><div class="meta" id="c_oi_m"></div></div>
  <div class="card"><div class="lbl">TVL<span class="tag snap">DefiLlama</span></div><div class="val" id="c_tvl">—</div><div class="meta">total value locked</div></div>
  <div class="card"><div class="lbl">OI / TVL ratio</div><div class="val" id="c_ratio">—</div><div class="meta">protocol leverage</div></div>
  <div class="card"><div class="lbl">24h Fees<span class="tag snap" id="fee_tag">est.</span></div><div class="val" id="c_fee">—</div><div class="meta" id="c_fee_m"></div></div>
 </div>
 <div class="grid2 compact-charts">
  <div class="panel"><h2>TVL · evolution</h2><canvas id="tvlChart" height="130"></canvas></div>
  <div class="panel"><h2>24h Volume by market</h2><canvas id="volChart" height="130"></canvas></div>
 </div>
 <div class="grid3 compact-charts">
  <div class="panel"><h2>Total volume · since launch</h2><canvas id="hCumVol" height="140"></canvas><div class="note mini" id="hCumVol_n">—</div></div>
  <div class="panel"><h2>Total fees · since launch</h2><canvas id="hCumFees" height="140"></canvas><div class="note mini" id="hCumFees_n">—</div></div>
  <div class="panel"><h2>Open Interest · over time</h2><canvas id="hOiTL" height="140"></canvas><div class="note mini" id="hOiTL_n">—</div></div>
 </div>
 <div class="panel" style="margin-top:14px"><h2>👥 Platform adoption</h2>
  <div class="note" id="us_status" style="margin-bottom:12px">Loading…</div>
  <div class="cards">
   <div class="card"><div class="lbl">Total traders</div><div class="val" id="us_total">—</div><div class="meta">accounts registered onchain</div></div>
   <div class="card"><div class="lbl">Active 24h</div><div class="val" id="us_active_1d">—</div><div class="meta">traded in last day</div></div>
   <div class="card"><div class="lbl">Active 7d</div><div class="val" id="us_active_7d">—</div><div class="meta">traded in last week</div></div>
   <div class="card"><div class="lbl">Active 30d</div><div class="val" id="us_active_30d">—</div><div class="meta">traded in last month</div></div>
   <div class="card"><div class="lbl">With open position</div><div class="val" id="us_active">—</div><div class="meta">right now</div></div>
  </div>
  <div class="grid2">
   <div class="panel" style="border:none;background:none;padding:0"><h2>New accounts per day</h2><canvas id="usNew" height="150"></canvas></div>
   <div class="panel" style="border:none;background:none;padding:0"><h2>Cumulative growth</h2><canvas id="usCum" height="150"></canvas></div>
  </div>
  <div class="panel" style="border:none;background:none;padding:0;margin-top:14px"><h2>Active accounts per day</h2><canvas id="usAct" height="120"></canvas></div>
 </div>
</div>

<div class="view" id="v_markets">
 <div class="panel" style="margin-bottom:14px"><h2>📊 Market dominance · volume treemap</h2>
  <div id="treemap_vol" class="treemap"></div>
  <div class="note" style="margin-top:8px">Rectangles sized by 24h volume, color hue from funding rate. Click to open the market.</div>
 </div>
 <div class="panel"><h2>Markets</h2>
  <table id="tbl"><thead><tr>
   <th data-k="name">Market</th><th data-k="last_price">Price</th><th data-k="change_24h">24h %</th>
   <th>24h trend</th>
   <th data-k="volume_24h">24h Volume</th><th data-k="oi_usd">Open Interest</th>
   <th data-k="funding_8h">Funding 8h</th><th data-k="funding_apr">Funding APR</th>
   <th data-k="basis_pct">Basis</th><th data-k="spread_bps">Spread</th><th data-k="max_leverage">Lev.</th>
  </tr></thead><tbody id="tbody"></tbody></table>
 </div>
 <div class="panel" style="margin-top:14px"><h2>Funding APR · history per market</h2>
  <div class="note" id="fh_status" style="margin-bottom:10px">Recorded by the dashboard every couple of minutes.</div>
  <canvas id="fhChart" height="160"></canvas>
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
  <h2>Top Traders by PnL</h2>
  <div class="note" id="pn_status" style="margin-bottom:12px">Calculating…</div>
  <div class="row" style="gap:14px;align-items:center">
   <span class="seg" id="pn_period">
    <button data-p="1d">1d</button>
    <button data-p="7d">7d</button>
    <button data-p="30d" class="active">30d</button>
    <button data-p="all">All (+ unreal)</button>
   </span>
   <label style="display:flex;gap:6px;align-items:center;cursor:pointer;font-size:12.5px;color:var(--muted)">
    <input type="checkbox" id="pn_only_smart" style="accent-color:var(--accent2)"> Only Smart Money
   </label>
   <span class="note" id="pn_totals" style="margin-left:auto"></span>
  </div>
  <div class="grid2" style="margin-top:14px">
   <div><h2 style="font-size:13px;color:var(--green)">▲ Winners</h2>
    <table><thead><tr><th>#</th><th>Account</th><th>PnL</th><th>Volume</th><th>Edge</th><th>Trades</th></tr></thead><tbody id="pn_winners"></tbody></table></div>
   <div><h2 style="font-size:13px;color:var(--red)">▼ Losers</h2>
    <table><thead><tr><th>#</th><th>Account</th><th>PnL</th><th>Volume</th><th>Edge</th><th>Liq.</th></tr></thead><tbody id="pn_losers"></tbody></table></div>
  </div>
  <div class="note" style="margin-top:10px">PnL summed from realized_pnl of each trade in the selected window. <b>Edge</b> = PnL / Volume × 10000, in basis points — the trader's average per-dollar margin. A pro typically shows 5–20 bps edge sustained; degens swing wildly. <span class="smart-badge" style="margin:0 4px">SMART</span> tag marks consistently profitable wallets: ≥50 trades, ≥$250k volume, ≥55% win rate, no liquidations, drawdown &lt; ½ PnL. "All" window adds current unrealized PnL.</div>
 </div>
</div>

<div class="view" id="v_funded">
 <div class="panel">
  <h2>💸 Funding payments · current snapshot</h2>
  <div class="note" id="fp_status" style="margin-bottom:12px">Loading…</div>
  <span class="note" id="fp_totals" style="display:block;margin-bottom:14px"></span>
  <div class="grid2">
   <div><h2 style="font-size:13px;color:var(--red)">💸 Owes (will be deducted on settle)</h2>
    <table><thead><tr><th>#</th><th>Account</th><th>Funding owed</th><th>Unsettled</th><th>Collateral</th></tr></thead><tbody id="fp_payers"></tbody></table></div>
   <div><h2 style="font-size:13px;color:var(--green)">💰 Owed (will be credited on settle)</h2>
    <table><thead><tr><th>#</th><th>Account</th><th>Funding due</th><th>Unsettled</th><th>Collateral</th></tr></thead><tbody id="fp_receivers"></tbody></table></div>
  </div>
  <div class="note" style="margin-top:10px">Reads <code>getTotalCrossFunding</code> and <code>getTotalCrossUnsettled</code> onchain for the top ~300 accounts by OI every 10 min. This is a snapshot of unrealized funding right now — the amount each account is about to pay or receive at the next settlement. Negative means the account is on the wrong side of the funding rate.</div>
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
   <th>Market</th><th>OI Long</th><th>OI Short</th><th>L / S balance</th>
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

<div class="view" id="v_copy">
 <div class="panel">
  <h2>Copy trading · mirror the house trader</h2>
  <div class="note" style="margin-bottom:14px">Your funds never leave your own RISEx account. One signature authorizes the copy engine to <b>trade</b> on your account — it can never withdraw, and you can pause or revoke at any time. Positions mirror the leader proportionally to your equity. <b>Leveraged perps can liquidate your account; past performance guarantees nothing.</b></div>

  <div class="copy-grid">
   <div class="copy-card">
    <div class="copy-k">Leader</div>
    <div id="cp_leader" class="mono">—</div>
    <div id="cp_leader_stats" class="note" style="margin-top:8px">Connect the engine to load the leader.</div>
   </div>
   <div class="copy-card">
    <div class="copy-k">Copy engine</div>
    <div class="row" style="margin:0">
     <input id="cp_api" placeholder="http://localhost:8790" style="flex:1;min-width:180px"/>
     <button class="rf" onclick="cpConnectEngine()">Connect</button>
    </div>
    <div id="cp_engine_status" class="note" style="margin-top:8px"><span class="cp-dot"></span>Not connected. Start the copy bot, then connect.</div>
   </div>
  </div>

  <div class="copy-step" id="cp_step_wallet" style="display:none">
   <h3>1 · Your wallet</h3>
   <div class="row">
    <button class="rf" onclick="cpConnectWallet()">Connect wallet</button>
    <span id="cp_acct" class="mono" style="color:var(--muted)"></span>
   </div>
  </div>

  <div class="copy-step" id="cp_step_settings" style="display:none">
   <h3>2 · Risk limits</h3>
   <div class="row" style="align-items:flex-end;flex-wrap:wrap;gap:14px">
    <div class="copy-field"><label>Size multiplier (1 = proportional)</label><input id="cp_mult" type="number" step="0.1" min="0.1" max="10"/></div>
    <div class="copy-field"><label>Max leverage</label><input id="cp_lev" type="number" step="1" min="1" max="50"/></div>
    <div class="copy-field"><label>Stop · max drawdown %</label><input id="cp_dd" type="number" step="1" min="1" max="90"/></div>
   </div>
   <div class="note" style="margin-top:10px">Markets to copy: <span id="cp_mkts"></span></div>
  </div>

  <div class="copy-step" id="cp_step_sign" style="display:none">
   <h3>3 · Authorize &amp; start</h3>
   <div class="note" style="margin-bottom:10px">The signature registers the engine as a <b>trade-only session key</b> on your account (expires in <span id="cp_expdays">30</span> days, revocable anytime). It cannot move funds.</div>
   <button class="rf" id="cp_sign_btn" onclick="cpSign()" style="border-color:var(--accent);color:var(--accent)">Sign authorization &amp; start copying</button>
  </div>

  <div class="copy-step" id="cp_status_panel" style="display:none">
   <h3>Your copy status</h3>
   <div id="cp_status_body" class="note">—</div>
   <div class="row" style="margin-top:10px">
    <button class="rf" id="cp_pause_btn" onclick="cpControl('pause')" style="display:none">Pause copying</button>
    <button class="rf" id="cp_resume_btn" onclick="cpControl('resume')" style="display:none;border-color:var(--green);color:var(--green)">Resume copying</button>
   </div>
   <div class="note" style="margin-top:10px;font-size:11px">Pause stops new copies (open positions stay yours to manage). To fully revoke the session key on-chain, use revokeSigner on the RISEx authorization contract — instructions in the bot README.</div>
  </div>

  <!-- ===== Dashboard del seguidor (demo hasta conectar wallet) ===== -->
  <div id="cpd_dash">
   <div class="divider"></div>
   <div id="cpd_demo_banner" class="note" style="background:var(--panel2);border:1px solid var(--line);border-radius:10px;padding:10px 14px;margin-bottom:6px">
    👁️ <b>Preview</b> — showing a sample wallet so you can see what your dashboard looks like. Connect your wallet above to see your own live positions and stats.
   </div>

   <div class="cpd-stats" id="cpd_kpis">
    <div class="cpd-stat"><div class="lbl">Account equity</div><div class="val" id="cpd_equity">—</div><div class="sub">collateral in your account</div></div>
    <div class="cpd-stat"><div class="lbl">Open positions</div><div class="val" id="cpd_openn">—</div><div class="sub" id="cpd_openoi">—</div></div>
    <div class="cpd-stat"><div class="lbl">Realized PnL · 30d</div><div class="val" id="cpd_pnl">—</div><div class="sub" id="cpd_pnlsub">copying performance</div></div>
    <div class="cpd-stat"><div class="lbl">Win rate</div><div class="val" id="cpd_wr">—</div><div class="sub" id="cpd_wrsub">—</div></div>
    <div class="cpd-stat"><div class="lbl">Volume · 30d</div><div class="val" id="cpd_vol">—</div><div class="sub" id="cpd_voltrades">—</div></div>
    <div class="cpd-stat"><div class="lbl">Profit factor</div><div class="val" id="cpd_pf">—</div><div class="sub">wins ÷ losses</div></div>
   </div>

   <div class="cpd-sect">
    <h3><span class="cpd-live"></span> Open positions <span class="cp-chip" id="cpd_pos_count" style="margin-left:auto"></span></h3>
    <p class="hint">Mirrored from the leader, sized to your equity. Refreshes automatically.</p>
    <div id="cpd_positions"><div class="cpd-empty">Connect to load your positions.</div></div>
   </div>

   <div class="cpd-sect">
    <h3>Performance by market · 30d</h3>
    <div id="cpd_markets"><div class="cpd-empty">No market activity yet.</div></div>
   </div>

   <div class="cpd-sect">
    <h3>Recent trades</h3>
    <p class="hint">Your last fills on this account.</p>
    <div id="cpd_trades"><div class="cpd-empty">No trades yet.</div></div>
   </div>

   <div class="cpd-sect">
    <h3>Highlights · 30d</h3>
    <div class="cpd-stats">
     <div class="cpd-stat"><div class="lbl">Best trade</div><div class="val cpd-pos" id="cpd_best">—</div></div>
     <div class="cpd-stat"><div class="lbl">Worst trade</div><div class="val cpd-neg" id="cpd_worst">—</div></div>
     <div class="cpd-stat"><div class="lbl">Max drawdown</div><div class="val" id="cpd_dd">—</div></div>
     <div class="cpd-stat"><div class="lbl">Fees paid</div><div class="val" id="cpd_fees">—</div></div>
     <div class="cpd-stat"><div class="lbl">Liquidations</div><div class="val" id="cpd_liq">—</div></div>
     <div class="cpd-stat"><div class="lbl">Avg trade size</div><div class="val" id="cpd_avg">—</div></div>
    </div>
   </div>
  </div>
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
   <th>Symbol</th><th>RISEx</th><th>Pacifica</th><th>Hyperliquid</th>
   <th>Δ vs Pacifica</th><th>Δ vs HL</th>
  </tr></thead><tbody id="fc_body"></tbody></table>
  <div class="note" style="margin-top:10px">All platforms pay funding every hour. Positive Δ = longs on RISEx pay more than on the other. Sources: <code>api.rise.trade</code>, <code>api.pacifica.fi</code>, Hyperliquid public API.</div>
 </div>
</div>

<!-- v_users moved into v_overview -->

<div class="view" id="v_explorer">
 <div class="cards" style="grid-template-columns:repeat(4,1fr);margin-bottom:14px">
  <div class="card"><div class="lbl">Latest Block <span class="livedot"></span></div><div class="val" id="ex_block">—</div><div class="meta" id="ex_block_m">indexing…</div></div>
  <div class="card"><div class="lbl">TPS <span class="livedot"></span></div><div class="val" id="ex_tps">—</div><div class="meta">tx / second · 60s window</div></div>
  <div class="card"><div class="lbl">Avg Block Time</div><div class="val" id="ex_bt">—</div><div class="meta">seconds between blocks</div></div>
  <div class="card"><div class="lbl">Buffered Tx</div><div class="val" id="ex_buf">—</div><div class="meta" id="ex_buf_m">in memory</div></div>
 </div>
 <div class="grid2" style="grid-template-columns:1fr 1fr">
  <div class="panel" style="padding:0">
   <h2 style="padding:14px 18px 0;margin-bottom:8px">📦 Latest Blocks</h2>
   <div style="max-height:560px;overflow:auto">
    <table>
     <thead><tr><th>Block</th><th>Age</th><th>Txs</th><th>Gas used</th><th>Validator</th></tr></thead>
     <tbody id="ex_blocks"></tbody>
    </table>
   </div>
  </div>
  <div class="panel" style="padding:0">
   <h2 style="padding:14px 18px 0;margin-bottom:8px">📜 Latest Transactions</h2>
   <div style="max-height:560px;overflow:auto">
    <table>
     <thead><tr><th>Hash</th><th>Age</th><th>From → To</th><th>Value</th></tr></thead>
     <tbody id="ex_txs"></tbody>
    </table>
   </div>
  </div>
 </div>
 <div class="note" style="margin-top:10px;text-align:center">Indexed from <code>rpc.risechain.com</code> · refreshing every 1.5s · all data is public.</div>
</div>

<div class="view" id="v_blockdetail">
 <div id="blockOut"><div class="empty">Loading block…</div></div>
</div>

<div class="view" id="v_txdetail">
 <div id="txOut"><div class="empty">Loading transaction…</div></div>
</div>

<div class="view" id="v_wallet">
 <div id="walletOut">
  <div class="emptystate">
   <svg viewBox="0 0 80 80" xmlns="http://www.w3.org/2000/svg"><rect x="14" y="22" width="52" height="40" rx="6" fill="none" stroke="var(--accent)" stroke-width="2"/><rect x="14" y="30" width="52" height="8" fill="var(--accent)" opacity=".15"/><circle cx="56" cy="48" r="4" fill="var(--accent)"/></svg>
   <div class="ttl">Search a wallet to see its stats</div>
   <div class="sub">Paste any 0x… address into the search bar above, press ⌘K to open the command palette, or load a demo wallet to see what's possible.</div>
   <div style="display:flex;gap:10px;margin-top:8px">
    <button class="rf" onclick="location.hash='wallet=0x69Be108a2aaA3e06Df75F854DF08d215ACf0Ca7A'" style="border-color:var(--accent);color:var(--accent)">Try a demo wallet ✨</button>
    <button class="rf" onclick="openCmdK()">⌘K Command palette</button>
   </div>
  </div>
 </div>
</div>

<div class="view" id="v_compare">
 <div class="panel">
  <h2>⚖ Compare wallets</h2>
  <div class="note" style="margin-bottom:14px">Add up to 4 wallets side by side. Compare PnL, win rate, volume, drawdown, equity curves — everything at a glance.</div>
  <div class="row" id="cmp_inputs" style="gap:8px;flex-wrap:wrap;margin-bottom:14px">
   <input id="cmp_add" placeholder="0x… address to add" style="flex:1;min-width:280px" />
   <button class="rf" onclick="cmpAdd()" style="border-color:var(--accent);color:var(--accent)">+ Add wallet</button>
   <button class="rf" onclick="cmpClear()">Clear all</button>
  </div>
  <div id="cmp_out"></div>
 </div>
</div>

<div class="view" id="v_tools">
 <div class="panel">
  <h2>🧰 Position simulator</h2>
  <div class="note" style="margin-bottom:14px">What-if any trade. Calculates liquidation price, current PnL at mark, and a PnL ladder ±20% from entry.</div>
  <div style="display:grid;grid-template-columns:1.1fr 1.4fr;gap:18px;align-items:start">
   <div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
     <div><label class="fl">Market</label><select id="sim_mkt" style="width:100%"></select></div>
     <div><label class="fl">Direction</label>
      <span class="seg" id="sim_side" style="width:100%;display:flex">
       <button data-d="long" class="active" style="flex:1;color:var(--green)">▲ Long</button>
       <button data-d="short" style="flex:1;color:var(--red)">▼ Short</button>
      </span></div>
     <div><label class="fl">Entry price ($)</label><input id="sim_entry" type="number" step="any" style="width:100%"></div>
     <div><label class="fl">Size (base)</label><input id="sim_size" type="number" step="any" style="width:100%"></div>
     <div><label class="fl">Leverage</label><input id="sim_lev" type="number" step="0.5" min="1" max="50" value="10" style="width:100%"></div>
     <div><label class="fl">Margin mode</label>
      <span class="seg" id="sim_mm" style="width:100%;display:flex">
       <button data-m="isolated" class="active" style="flex:1">Isolated</button>
       <button data-m="cross" style="flex:1">Cross</button>
      </span></div>
     <div><label class="fl">Cross collateral ($)</label><input id="sim_coll" type="number" step="any" style="width:100%" placeholder="ignored if isolated"></div>
     <div><label class="fl">Mark price ($)</label><input id="sim_mark" type="number" step="any" style="width:100%"></div>
    </div>
    <div class="row" style="margin-top:14px">
     <button class="rf" onclick="useCurrentMark()">Use current mark</button>
     <button class="rf" onclick="autofillSize()">From notional…</button>
    </div>
   </div>
   <div>
    <div class="cards" style="grid-template-columns:repeat(2,1fr);margin-bottom:14px">
     <div class="card"><div class="lbl">Notional</div><div class="val" id="sim_not">—</div><div class="meta">size × entry</div></div>
     <div class="card"><div class="lbl">Initial margin</div><div class="val" id="sim_im">—</div><div class="meta">notional ÷ leverage</div></div>
     <div class="card"><div class="lbl">Liq price</div><div class="val" id="sim_liq">—</div><div class="meta" id="sim_liq_meta">— from entry</div></div>
     <div class="card"><div class="lbl">Current PnL</div><div class="val" id="sim_pnl">—</div><div class="meta" id="sim_pnl_meta">at mark</div></div>
    </div>
    <div class="panel" style="padding:0;max-height:380px;overflow:auto">
     <table><thead><tr><th>Price</th><th>Move %</th><th>PnL ($)</th><th>ROI on margin</th><th>Status</th></tr></thead>
     <tbody id="sim_ladder"></tbody></table>
    </div>
   </div>
  </div>
  <div class="note" style="margin-top:12px">Isolated liquidation uses the dedicated margin: <code>liq = entry × (1 ∓ 1/L + MMR)</code>. Cross uses available collateral + entry margin against maintenance margin requirement. MMR is read from the market config — auto-prefilled from current market.</div>
 </div>

 <div class="panel" style="margin-top:14px">
  <h2>💱 Funding cost calculator</h2>
  <div class="note" style="margin-bottom:14px">Estimates funding paid (or received) at the current APR over a given holding period.</div>
  <div style="display:grid;grid-template-columns:1.1fr 1.4fr;gap:18px;align-items:start">
   <div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
     <div><label class="fl">Market</label><select id="fc_mkt" style="width:100%"></select></div>
     <div><label class="fl">Direction</label>
      <span class="seg" id="fc_side" style="width:100%;display:flex">
       <button data-d="long" class="active" style="flex:1;color:var(--green)">▲ Long</button>
       <button data-d="short" style="flex:1;color:var(--red)">▼ Short</button>
      </span></div>
     <div><label class="fl">Notional ($)</label><input id="fc_not" type="number" step="any" value="10000" style="width:100%"></div>
     <div><label class="fl">Days held</label><input id="fc_days" type="number" step="0.5" value="7" style="width:100%"></div>
    </div>
   </div>
   <div>
    <div class="cards" style="grid-template-columns:repeat(2,1fr)">
     <div class="card"><div class="lbl">Funding APR</div><div class="val" id="fc_apr">—</div><div class="meta" id="fc_apr_meta">current rate</div></div>
     <div class="card"><div class="lbl">Per 8h (per $1k notional)</div><div class="val" id="fc_per8">—</div><div class="meta">paid every funding cycle</div></div>
     <div class="card"><div class="lbl">Total over period</div><div class="val" id="fc_total">—</div><div class="meta" id="fc_total_meta">paid (−) / received (+)</div></div>
     <div class="card"><div class="lbl">Breakeven price move</div><div class="val" id="fc_be">—</div><div class="meta">needed to cover funding</div></div>
    </div>
   </div>
  </div>
  <div class="note" style="margin-top:12px">Funding is positive when the position is paying (long when funding > 0, short when funding < 0). Calculation assumes the current 8h funding rate stays constant during the holding period.</div>
 </div>
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
   <button class="rf" onclick="askNotif()">Enable browser notifications</button>
   <span class="note" id="al_status"></span>
  </div>
 </div>
 <div class="panel" style="margin-top:14px"><h2>📨 Telegram alerts</h2>
  <div class="note" style="margin-bottom:12px">Get alerts pushed to your Telegram. Create a bot with <a href="https://t.me/BotFather" target="_blank">@BotFather</a>, get its token, then start a chat with your bot and use <a href="https://t.me/userinfobot" target="_blank">@userinfobot</a> to find your chat ID. Token stays in your browser only (localStorage), never sent to our server.</div>
  <div class="alertgrid">
   <div><label class="fl">Bot token</label><input id="tg_token" style="width:100%" type="password" placeholder="123456:ABCdef…" autocomplete="new-password"></div>
   <div><label class="fl">Chat ID (your numeric ID)</label><input id="tg_chat" style="width:100%" type="text" placeholder="e.g. 123456789"></div>
   <div><label class="fl">&nbsp;</label>
    <button class="rf" onclick="saveTg()" style="width:100%">Save</button></div>
  </div>
  <div class="row" style="margin-top:14px">
   <button class="rf" onclick="testTg()">Send test message</button>
   <span class="note" id="tg_status"></span>
  </div>
 </div>
</div>


<div class="priceticker" id="priceticker"><div class="pt-track" id="pt_track">Loading prices…</div></div>

<footer class="footer">
 <div class="row">
  <span class="built">RISEx · STATS</span>
  <span>·</span>
  <span>Built on <a href="https://risechain.com" target="_blank">RISE chain</a></span>
  <span>·</span>
  <span class="uptime" id="footer_uptime">uptime —</span>
  <span>·</span>
  <span>v5</span>
  <span>·</span>
  <span><a href="javascript:openCmdK()">⌘K search</a></span>
 </div>
 <div class="row" style="margin-bottom:0;color:var(--muted2);font-size:10.5px">All data is read directly from <code>api.rise.trade</code> and the RISE chain RPC. No accounts, no tracking. Open source.</div>
</footer>

</div>
</main></div>

<script>
let DATA=null,sortK='volume_24h',sortDir=-1,charts={};
const U=n=>{if(n==null||isNaN(n))return'—';const a=Math.abs(n);
 if(a>=1e9)return'$'+(n/1e9).toFixed(2)+'B';if(a>=1e6)return'$'+(n/1e6).toFixed(2)+'M';
 if(a>=1e3)return'$'+(n/1e3).toFixed(1)+'K';return'$'+n.toFixed(2);};
const P=n=>n>=100?n.toLocaleString('en-US',{maximumFractionDigits:1}):n.toLocaleString('en-US',{maximumFractionDigits:4});
const shortAddr=a=>a.slice(0,6)+'…'+a.slice(-4);
// change_24h from API is absolute $ change, not %. compute real percentage.
const chgPct=m=>{const c=+m.change_24h||0;const last=+m.last_price||0;const open=last-c;return open!==0?(c/open*100):0;};
const agoStr=ts=>{const s=Math.floor(Date.now()/1000)-ts;if(s<60)return s+'s';if(s<3600)return Math.floor(s/60)+'min';return Math.floor(s/3600)+'h'};
const alerts=JSON.parse(localStorage.getItem('rise_alerts')||'{}');

// ====== Count-up animation: animate from 0 to target on first paint ======
function countUp(id,target,fmt,dur){
 const el=document.getElementById(id);if(!el||!isFinite(target))return;
 const fn=fmt||(x=>x);
 const D=dur||650;const start=performance.now();
 const init=_prevVals[id]!=null?_prevVals[id]:0;
 function step(t){const p=Math.min(1,(t-start)/D);
  const e=1-Math.pow(1-p,3); // easeOutCubic
  const v=init+(target-init)*e;
  el.innerHTML=fn(v);
  if(p<1)requestAnimationFrame(step);else{el.innerHTML=fn(target);_prevVals[id]=target;}}
 requestAnimationFrame(step);
}

// ====== Price ticker (uses RISEx market data + Hyperliquid if available) ======
function renderTicker(){
 if(!DATA||!DATA.markets)return;
 const wanted=['BTC','ETH','SOL','HYPE','XRP','DOGE','AVAX','BNB'];
 const items=[];
 for(const sym of wanted){
  const m=DATA.markets.find(x=>(x.name||'').toUpperCase().startsWith(sym));
  if(!m)continue;
  items.push({sym:sym,price:m.last_price,chg:chgPct(m),market_id:m.market_id});
 }
 if(!items.length)return;
 const sparks=(window._SPARKS||{}).by_market||{};
 const renderOne=it=>{
  const chgCls=it.chg>=0?'pos':'neg';const sign=it.chg>=0?'+':'';
  const closes=sparks[it.market_id];
  let svg='';
  if(closes&&closes.length>2){
   let lo=Infinity,hi=-Infinity;for(const v of closes){if(v<lo)lo=v;if(v>hi)hi=v;}
   if(lo===hi){lo-=1;hi+=1;}
   const n=closes.length;
   const x=i=>(42*i/(n-1)).toFixed(1);
   const y=v=>(14-(v-lo)/(hi-lo)*14).toFixed(1);
   let d='M '+x(0)+' '+y(closes[0]);
   for(let i=1;i<n;i++)d+=' L '+x(i)+' '+y(closes[i]);
   const c=closes[n-1]>=closes[0]?'var(--green)':'var(--red)';
   svg=`<svg class="pt-sk" viewBox="0 0 42 14" preserveAspectRatio="none"><path d="${d}" stroke="${c}" stroke-width="1.3" fill="none" stroke-linejoin="round" stroke-linecap="round" vector-effect="non-scaling-stroke"/></svg>`;
  }
  return `<a href="#market=${it.market_id}" class="pt-asset"><span class="pt-sym">${it.sym}</span> <span class="pt-px">$${it.price>=1000?it.price.toLocaleString('en-US',{maximumFractionDigits:1}):it.price.toLocaleString('en-US',{maximumFractionDigits:4})}</span> <span class="${chgCls}">${sign}${it.chg.toFixed(2)}%</span> ${svg}</a>`;
 };
 const html=items.map(renderOne).join('');
 // duplicate for seamless scroll
 document.getElementById('pt_track').innerHTML=html+html;
}

// ====== Command palette ⌘K ======
const CMDK_VIEWS=[
 {key:'overview',name:'Overview',ic:'📊'},
 {key:'markets',name:'Markets',ic:'📈'},
 {key:'ranking',name:'Positions ranking',ic:'⚖️'},
 {key:'acctoi',name:'Current OI ranking',ic:'💼'},
 {key:'volranking',name:'Volume ranking',ic:'💹'},
 {key:'oiranking',name:'Avg OI ranking',ic:'⏱️'},
 {key:'pnl',name:'Top traders by PnL',ic:'🏆'},
 {key:'funded',name:'Funding payments',ic:'💸'},
 {key:'liq',name:'Liquidations',ic:'💀'},
 {key:'feed',name:'Live activity',ic:'⚡'},
 {key:'longshort',name:'Long / Short ratio',ic:'⚖️'},
 {key:'heatmap',name:'Liquidation heatmap',ic:'🔥'},
 {key:'marketshare',name:'Market share',ic:'🥧'},
 {key:'funding',name:'Funding vs DEXes',ic:'💱'},
 {key:'users',name:'Users / adoption',ic:'👥'},
 {key:'watchlist',name:'Watchlist',ic:'⭐'},
 {key:'tools',name:'Tools (simulator & calculator)',ic:'🧰'},
 {key:'alerts',name:'Alerts',ic:'🔔'},
 {key:'copy',name:'Copy trading',ic:'🪞'}];
/* ===== Copy trading ===== */
// Set this to your deployed engine URL (e.g. https://xxxx.up.railway.app) so visitors don't have to type it.
const CP_DEFAULT_API='https://risex-copy-production.up.railway.app';
let CP={info:null,acct:null};
function cpApiBase(){return (localStorage.getItem('cp_api')||CP_DEFAULT_API).replace(/\/+$/,'');}
async function cpFetch(path,opts){
 const r=await fetch(cpApiBase()+path,opts);
 const d=await r.json().catch(()=>({ok:false,error:'bad response'}));
 if(!d.ok) throw new Error(d.error||('HTTP '+r.status));
 return d;
}
function cpShort(a){return a?a.slice(0,6)+'…'+a.slice(-4):'—';}
function initCopy(){
 const inp=document.getElementById('cp_api');
 if(inp&&!inp.value) inp.value=localStorage.getItem('cp_api')||CP_DEFAULT_API;
 if(localStorage.getItem('cp_api_ok')==='1'&&!CP.info) cpConnectEngine(true);
 if(!window._cpdInit){ window._cpdInit=true; cpdRefreshAll(); cpdStartAutoRefresh(); }
}
async function cpConnectEngine(quiet){
 const inp=document.getElementById('cp_api');
 localStorage.setItem('cp_api',inp.value.trim()||'http://localhost:8790');
 const st=document.getElementById('cp_engine_status');
 st.innerHTML='<span class="cp-dot"></span>Connecting…';
 try{
  const d=await cpFetch('/api/copy/info');
  CP.info=d;localStorage.setItem('cp_api_ok','1');
  st.innerHTML='<span class="cp-dot on"></span>Connected'
   +'<span class="cp-chip '+(d.network==='mainnet'?'live':'warn')+'">'+d.network+'</span>'
   +(d.dryRun?'<span class="cp-chip warn">dry-run</span>':'')
   +'<br>Engine signer: <span class="mono">'+cpShort(d.botSigner)+'</span>';
  const L=document.getElementById('cp_leader');
  L.innerHTML='<a href="#wallet='+d.leader+'" style="color:var(--accent)">'+cpShort(d.leader)+'</a>';
  document.getElementById('cp_expdays').textContent=d.expirationDays;
  document.getElementById('cp_mult').value=d.defaults.sizeMultiplier;
  document.getElementById('cp_lev').value=d.defaults.maxLeverage;
  document.getElementById('cp_dd').value=Math.round(d.defaults.maxDrawdownPct*100);
  document.getElementById('cp_mkts').innerHTML=d.markets.map(m=>
   '<label style="margin-right:14px"><input type="checkbox" class="cp_mkt" value="'+m.id+'" checked> '+m.name+'</label>').join('');
  document.getElementById('cp_step_wallet').style.display='block';
  cpLeaderStats(d.leader);
  if(CP.acct) cpRefreshStatus();
 }catch(e){
  CP.info=null;localStorage.removeItem('cp_api_ok');
  st.innerHTML='<span class="cp-dot"></span>Can\u2019t reach the engine at <span class="mono">'+cpApiBase()+'</span> \u2014 '+e.message+'. Start the bot (npm run dev) and connect again.';
  if(!quiet) toast('Copy engine unreachable','err');
 }
}
async function cpLeaderStats(addr){
 const el=document.getElementById('cp_leader_stats');
 try{
  const d=await (await fetch('/api/wallet-preview?account='+addr)).json();
  if(!d.ok) throw 0;
  const wr=(d.win_rate==null?'—':d.win_rate.toFixed(0)+'%');
  el.innerHTML='30d volume <b>'+U(d.volume_30d||0)+'</b> · 30d PnL <b style="color:'+((d.pnl_30d||0)>=0?'var(--green)':'var(--red)')+'">'+U(d.pnl_30d||0)+'</b> · win rate <b>'+wr+'</b>'
   +(d.smart?' · <span class="cp-chip live">SMART</span>':'')
   +' · <a href="#wallet='+addr+'" style="color:var(--accent)">full stats →</a>';
 }catch(_){el.textContent='Leader stats appear here once this wallet has indexed activity.';}
}
async function cpConnectWallet(){
 if(!window.ethereum){toast('No wallet found \u2014 install MetaMask or Rabby','err');return;}
 try{
  const accs=await window.ethereum.request({method:'eth_requestAccounts'});
  CP.acct=(accs[0]||'').toLowerCase();
  if(!CP.acct) return;
  if(CP.info&&CP.acct===CP.info.leader.toLowerCase()){
   document.getElementById('cp_acct').textContent=cpShort(CP.acct)+' \u2014 this is the leader account; it can\u2019t copy itself.';
   return;
  }
  document.getElementById('cp_acct').textContent=cpShort(CP.acct);
  document.getElementById('cp_step_settings').style.display='block';
  document.getElementById('cp_step_sign').style.display='block';
  document.getElementById('cpd_dash').style.display='block';
  cpdSyncBanner();
  cpRefreshStatus();
  cpdRefreshAll();
  cpdStartAutoRefresh();
 }catch(e){toast('Wallet connection rejected','err');}
}
async function cpRefreshStatus(){
 if(!CP.acct||!CP.info) return;
 try{
  const d=await cpFetch('/api/copy/status?account='+CP.acct);
  const panel=document.getElementById('cp_status_panel');
  if(!d.registered){panel.style.display='none';return;}
  panel.style.display='block';
  document.getElementById('cp_step_settings').style.display='none';
  document.getElementById('cp_step_sign').style.display='none';
  const s=d.settings;
  document.getElementById('cp_status_body').innerHTML=
   (d.paused?'<span class="cp-chip warn">PAUSED</span> '+(d.pausedReason||''):'<span class="cp-chip live">COPYING</span>')
   +'<br>multiplier '+s.sizeMultiplier+'\u00d7 · max lev '+s.maxLeverage+'\u00d7 · stop \u2212'+Math.round(s.maxDrawdownPct*100)+'%'
   +(s.excludedMarketIds.length?' · excluded markets: '+s.excludedMarketIds.join(', '):'');
  document.getElementById('cp_pause_btn').style.display=d.paused?'none':'inline-block';
  document.getElementById('cp_resume_btn').style.display=d.paused?'inline-block':'none';
 }catch(e){/* engine offline: keep last view */}
}

/* ===== Dashboard del seguidor: datos en vivo ===== */
let CPD_TIMER=null;
const CPD_DEMO_WALLET='0x69Be108a2aaA3e06Df75F854DF08d215ACf0Ca7A';
// wallet cuyos datos muestra el dashboard: la conectada, o la demo si no hay
function cpdTarget(){ return CP.acct || CPD_DEMO_WALLET; }
function cpdIsDemo(){ return !CP.acct; }
const CPD_MKT_NAMES={1:'BTC',2:'ETH',3:'BNB',4:'SOL',5:'HYPE',6:'XRP',7:'TAO',8:'ZEC',9:'ONDO',10:'NEAR',11:'VVV',12:'LIT',14:'DOGE'};
function cpdMkt(id){return CPD_MKT_NAMES[id]||('#'+id);}
function cpdPnlClass(v){return v>0?'cpd-pos':(v<0?'cpd-neg':'');}
function cpdSigned(v){return (v>0?'+':'')+U(v);}
function cpdSyncBanner(){ const b=document.getElementById('cpd_demo_banner'); if(b) b.style.display=cpdIsDemo()?'block':'none'; }

function cpdStartAutoRefresh(){
 if(CPD_TIMER) clearInterval(CPD_TIMER);
 let n=0;
 CPD_TIMER=setInterval(()=>{ cpdLoadPositions(); if(CP.acct) cpRefreshStatus(); if(++n%7===0) cpdLoadStats(); },8000);
}
async function cpdRefreshAll(){ cpdSyncBanner(); await Promise.all([cpdLoadPositions(), cpdLoadStats()]); }

async function cpdLoadPositions(){
 const box=document.getElementById('cpd_positions');
 try{
  const d=await (await fetch('/api/wallet?account='+encodeURIComponent(cpdTarget()))).json();
  const list=(d&&Array.isArray(d.positions))?d.positions:[];
  document.getElementById('cpd_openn').textContent=list.length;
  const pc=document.getElementById('cpd_pos_count'); if(pc) pc.textContent=list.length?list.length+' open':'flat';
  if(d&&d.balance!=null) document.getElementById('cpd_equity').textContent=U(d.balance);
  if(!list.length){ box.innerHTML='<div class="cpd-empty">'+(cpdIsDemo()?'This sample wallet has no open positions right now.':'No open positions right now. When the leader opens a trade, your mirrored position appears here.')+'</div>'; document.getElementById('cpd_openoi').textContent='—'; return; }
  let totOi=0, rows='';
  list.forEach(p=>{
   const isLong=(p.side||'').toString().toUpperCase().indexOf('LONG')>=0;
   const sz=Math.abs(parseFloat(p.size||0));
   const notional=parseFloat(p.notional||0)||sz*parseFloat(p.mark||p.entry||0);
   const pnl=parseFloat(p.upnl||0); const pnlpct=parseFloat(p.upnl_pct||0); totOi+=notional;
   rows+='<tr><td>'+(p.market||'?')+'</td>'
    +'<td class="'+(isLong?'cpd-side-long':'cpd-side-short')+'">'+(isLong?'LONG':'SHORT')+'</td>'
    +'<td>'+sz.toLocaleString(undefined,{maximumFractionDigits:4})+'</td>'
    +'<td>'+U(notional)+'</td>'
    +'<td>'+(p.entry?'$'+(+p.entry).toLocaleString(undefined,{maximumFractionDigits:2}):'—')+'</td>'
    +'<td>'+(p.mark?'$'+(+p.mark).toLocaleString(undefined,{maximumFractionDigits:2}):'—')+'</td>'
    +'<td>'+(p.leverage?(Math.round(+p.leverage))+'×':'—')+'</td>'
    +'<td class="'+cpdPnlClass(pnl)+'">'+cpdSigned(pnl)+(pnlpct?' <span style="font-size:11px;opacity:.8">('+(pnlpct>0?'+':'')+pnlpct.toFixed(1)+'%)</span>':'')+'</td></tr>';
  });
  document.getElementById('cpd_openoi').textContent=U(totOi)+' notional';
  box.innerHTML='<table class="cpd-table"><thead><tr><th>Market</th><th>Side</th><th>Size</th><th>Notional</th><th>Entry</th><th>Mark</th><th>Lev</th><th>uPnL</th></tr></thead><tbody>'+rows+'</tbody></table>';
 }catch(e){ box.innerHTML='<div class="cpd-empty">Couldn\\u2019t load positions right now.</div>'; }
}

async function cpdLoadStats(){
 try{
  const d=await (await fetch('/api/wallet-stats?account='+encodeURIComponent(cpdTarget()))).json();
  if(!d||!d.ok) return;
  const set=(id,v)=>{const el=document.getElementById(id); if(el) el.textContent=v;};
  const setc=(id,v,cls)=>{const el=document.getElementById(id); if(el){el.textContent=v; el.className='val '+(cls||'');}};
  if(!d.trades_analyzed){
   set('cpd_pnl','$0'); set('cpd_wr','—'); set('cpd_vol','$0'); set('cpd_pf','—');
   document.getElementById('cpd_pnlsub').textContent='no closed trades in 30d';
   return;
  }
  setc('cpd_pnl', cpdSigned(d.total_realized_pnl||0), cpdPnlClass(d.total_realized_pnl||0));
  document.getElementById('cpd_pnlsub').textContent=(d.closes_analyzed||0)+' closed trades';
  set('cpd_wr', (d.win_rate_pct||0).toFixed(0)+'%');
  document.getElementById('cpd_wrsub').textContent=(d.wins||0)+'W · '+(d.losses||0)+'L';
  set('cpd_vol', U(d.market_pnl?d.market_pnl.reduce((a,m)=>a+(m.volume||0),0):0));
  document.getElementById('cpd_voltrades').textContent=(d.trades_analyzed||0)+' fills · '+(d.trades_per_day||0)+'/day';
  set('cpd_pf', (d.profit_factor||0).toFixed(2));
  setc('cpd_best', cpdSigned(d.best_trade_pnl||0),'cpd-pos');
  setc('cpd_worst', cpdSigned(d.worst_trade_pnl||0),'cpd-neg');
  set('cpd_dd', U(d.max_drawdown||0));
  set('cpd_fees', U(d.total_fees_paid||0));
  set('cpd_liq', (d.n_liquidations||0));
  set('cpd_avg', U(d.avg_trade_size||0));
  // performance por mercado
  const mbox=document.getElementById('cpd_markets');
  const mk=(d.market_pnl||[]).slice(0,8);
  if(!mk.length){ mbox.innerHTML='<div class="cpd-empty">No market activity yet.</div>'; }
  else{
   const maxAbs=Math.max(...mk.map(m=>Math.abs(m.realized_pnl||0)),1);
   mbox.innerHTML=mk.map(m=>{
    const pnl=m.realized_pnl||0; const w=Math.round(Math.abs(pnl)/maxAbs*100);
    const wr=(m.wins+m.losses)?Math.round(m.wins/(m.wins+m.losses)*100):0;
    return '<div class="cpd-mkt-row"><div><b>'+(m.market||'?')+'</b> <span style="color:var(--muted);font-size:11px">· '+m.trades+' trades · '+wr+'% win · '+U(m.volume||0)+' vol</span>'
     +'<div class="cpd-bar"><i style="width:'+w+'%;background:'+(pnl>=0?'var(--green)':'var(--red)')+'"></i></div></div>'
     +'<div class="'+cpdPnlClass(pnl)+'" style="font-family:var(--mono);font-weight:600">'+cpdSigned(pnl)+'</div></div>';
   }).join('');
  }
 }catch(e){/* keep last */}
 cpdLoadTrades();
}

async function cpdLoadTrades(){
 const box=document.getElementById('cpd_trades');
 try{
  const d=await (await fetch('/api/wallet-trades?account='+encodeURIComponent(cpdTarget())+'&limit=15')).json();
  const ts=(d&&d.trades)||[];
  if(!ts.length){ box.innerHTML='<div class="cpd-empty">No trades yet on this account.</div>'; return; }
  const rows=ts.map(t=>{
   const side=(t.side||'').toString().toUpperCase(); const isBuy=side==='BUY'||side==='0';
   const sz=Math.abs(parseFloat(t.size||0)); const px=parseFloat(t.price||0);
   const pnl=parseFloat(t.realized_pnl||0);
   const when=t.ts?new Date((''+t.ts).length>12?+t.ts:+t.ts*1000):null;
   const ago=when?cpdAgo(when):'';
   return '<tr><td style="color:var(--muted)">'+ago+'</td>'
    +'<td>'+cpdMkt(t.market_id)+'</td>'
    +'<td class="'+(isBuy?'cpd-side-long':'cpd-side-short')+'">'+(isBuy?'BUY':'SELL')+(t.is_liq?' ⚡':'')+'</td>'
    +'<td>'+sz.toLocaleString(undefined,{maximumFractionDigits:4})+'</td>'
    +'<td>'+(px?'$'+px.toLocaleString(undefined,{maximumFractionDigits:2}):'—')+'</td>'
    +'<td class="'+cpdPnlClass(pnl)+'">'+(pnl?cpdSigned(pnl):'—')+'</td></tr>';
  }).join('');
  box.innerHTML='<table class="cpd-table"><thead><tr><th>When</th><th>Market</th><th>Side</th><th>Size</th><th>Price</th><th>Realized</th></tr></thead><tbody>'+rows+'</tbody></table>';
 }catch(e){ box.innerHTML='<div class="cpd-empty">Couldn\\u2019t load trades right now.</div>'; }
}
function cpdAgo(d){ const s=Math.floor((Date.now()-d.getTime())/1000); if(s<60)return s+'s'; if(s<3600)return Math.floor(s/60)+'m'; if(s<86400)return Math.floor(s/3600)+'h'; return Math.floor(s/86400)+'d'; }
async function cpSign(){
 if(!CP.info){toast('Connect the engine first','err');return;}
 if(!CP.acct){toast('Connect your wallet first','err');return;}
 const btn=document.getElementById('cp_sign_btn');btn.disabled=true;btn.textContent='Waiting for signature…';
 try{
  const p=await cpFetch('/api/copy/params?account='+CP.acct);
  const sig=await window.ethereum.request({method:'eth_signTypedData_v4',params:[CP.acct,JSON.stringify(p.typedData)]});
  const allMkts=CP.info.markets.map(m=>m.id);
  const picked=[...document.querySelectorAll('.cp_mkt:checked')].map(x=>+x.value);
  const excluded=allMkts.filter(id=>!picked.includes(id));
  const settings={
   label:cpShort(CP.acct),
   sizeMultiplier:parseFloat(document.getElementById('cp_mult').value)||1,
   maxLeverage:parseInt(document.getElementById('cp_lev').value)||5,
   maxDrawdownPct:(parseInt(document.getElementById('cp_dd').value)||25)/100,
   excludedMarketIds:excluded};
  const r=await cpFetch('/api/copy/register',{method:'POST',headers:{'Content-Type':'application/json'},
   body:JSON.stringify({account:CP.acct,signature:sig,message:p.typedData.message,settings})});
  toast(r.dryRun?'Registered with the engine (dry-run \u2014 not sent to RISEx yet)':'Copying started \u2713','ok');
  cpRefreshStatus();
 }catch(e){toast('Authorization failed: '+e.message,'err');}
 btn.disabled=false;btn.textContent='Sign authorization & start copying';
}
async function cpControl(action){
 if(!CP.acct) return;
 try{
  const ts=Math.floor(Date.now()/1000);
  const msg='RISExscan copy-trading\naction: '+action+'\naccount: '+CP.acct+'\nts: '+ts;
  const sig=await window.ethereum.request({method:'personal_sign',params:[msg,CP.acct]});
  await cpFetch('/api/copy/'+action,{method:'POST',headers:{'Content-Type':'application/json'},
   body:JSON.stringify({account:CP.acct,ts:ts,signature:sig})});
  toast(action==='pause'?'Copying paused':'Copying resumed','ok');
  cpRefreshStatus();
 }catch(e){toast(action+' failed: '+e.message,'err');}
}
const CMDK_ACTIONS=[
 {name:'Toggle theme',ic:'🎨',run:()=>toggleTheme()},
 {name:'Open simulator',ic:'🧮',run:()=>{location.hash='';navigateView('tools');}},
 {name:'Refresh now',ic:'↻',run:()=>{loadAll(true);loadSparks();}},
 {name:'Copy current URL',ic:'🔗',run:()=>navigator.clipboard.writeText(location.href)},
];
let _cmdkSel=0,_cmdkItems=[];
function openCmdK(){
 const b=document.getElementById('cmdk_back');b.classList.add('open');
 const inp=document.getElementById('cmdk_input');inp.value='';
 setTimeout(()=>inp.focus(),20);
 renderCmdK('');
}
function closeCmdK(){document.getElementById('cmdk_back').classList.remove('open');}
function navigateView(key){
 document.querySelectorAll('.navitem').forEach(x=>x.classList.remove('active'));
 document.querySelectorAll('.view').forEach(x=>x.classList.remove('active'));
 const ni=document.querySelector(`.navitem[data-v="${key}"]`);if(ni)ni.classList.add('active');
 const v=document.getElementById('v_'+key);if(v)v.classList.add('active');
 // trigger associated loader
 const map={ranking:'loadRanking',markets:'loadHistory',acctoi:'loadAcctOi',users:'loadUsers',
  volranking:'loadVolRanking',oiranking:'loadOiRanking',funding:'loadFunding',pnl:'loadPnl',
  funded:'loadFunded',liq:'loadLiq',feed:'loadFeed',longshort:'loadLongShort',
  heatmap:'loadHeatmap',marketshare:'loadMarketShare',watchlist:'loadWatchlist',tools:'initTools',copy:'initCopy'};
 const fn=window[map[key]];if(typeof fn==='function')fn();
}
function renderCmdK(q){
 q=(q||'').toLowerCase().trim();
 const list=document.getElementById('cmdk_list');_cmdkItems=[];
 let html='';
 // wallet address detection
 if(/^0x[0-9a-f]{0,40}$/.test(q)){
  if(q.length===42){
   html+='<div class="cmdk-sec">Wallet</div>';
   _cmdkItems.push({type:'wallet',addr:q,html:`<div class="cmdk-item"><span class="ic">👛</span><span class="lab">Open wallet</span><span class="sub">${q.slice(0,10)}…${q.slice(-6)}</span></div>`});
   html+=_cmdkItems[_cmdkItems.length-1].html.replace('cmdk-item','cmdk-item" data-i="0');
  }
 }
 // markets
 const ms=(DATA&&DATA.markets)||[];
 const matches=ms.filter(m=>(m.name||'').toLowerCase().includes(q)||String(m.market_id)===q).slice(0,8);
 if(matches.length){
  html+='<div class="cmdk-sec">Markets</div>';
  for(const m of matches){
   const i=_cmdkItems.length;
   _cmdkItems.push({type:'market',mid:m.market_id});
   const pcv=chgPct(m);html+=`<div class="cmdk-item" data-i="${i}"><span class="ic">📈</span><span class="lab">${m.name}</span><span class="sub">$${P(m.last_price)} · ${pcv>=0?'+':''}${pcv.toFixed(2)}%</span></div>`;
  }
 }
 // views
 const vmatch=CMDK_VIEWS.filter(v=>v.name.toLowerCase().includes(q)).slice(0,10);
 if(vmatch.length){
  html+='<div class="cmdk-sec">Navigate</div>';
  for(const v of vmatch){
   const i=_cmdkItems.length;
   _cmdkItems.push({type:'view',key:v.key});
   html+=`<div class="cmdk-item" data-i="${i}"><span class="ic">${v.ic}</span><span class="lab">${v.name}</span></div>`;
  }
 }
 // actions
 const amatch=CMDK_ACTIONS.filter(a=>a.name.toLowerCase().includes(q)).slice(0,6);
 if(amatch.length){
  html+='<div class="cmdk-sec">Actions</div>';
  for(const a of amatch){
   const i=_cmdkItems.length;
   _cmdkItems.push({type:'action',fn:a.run});
   html+=`<div class="cmdk-item" data-i="${i}"><span class="ic">${a.ic}</span><span class="lab">${a.name}</span></div>`;
  }
 }
 if(!_cmdkItems.length){html='<div class="cmdk-sec">No results</div><div style="padding:12px 18px;color:var(--muted);font-size:12.5px">Try a wallet (0x…), a market symbol, or a view name.</div>';}
 list.innerHTML=html;_cmdkSel=0;markCmdKSel();
}
function markCmdKSel(){
 document.querySelectorAll('.cmdk-item').forEach((el,i)=>el.classList.toggle('sel',i===_cmdkSel));
}
function runCmdKItem(it){
 closeCmdK();
 if(it.type==='wallet')location.hash='wallet='+it.addr;
 else if(it.type==='market')location.hash='market='+it.mid;
 else if(it.type==='view'){location.hash='';navigateView(it.key);}
 else if(it.type==='action'&&typeof it.fn==='function')it.fn();
}
document.addEventListener('keydown',e=>{
 const isMac=navigator.platform.toUpperCase().includes('MAC');
 const mod=isMac?e.metaKey:e.ctrlKey;
 if(mod&&e.key==='k'){e.preventDefault();openCmdK();return;}
 const back=document.getElementById('cmdk_back');
 if(back&&back.classList.contains('open')){
  if(e.key==='Escape'){closeCmdK();return;}
  if(e.key==='ArrowDown'){e.preventDefault();_cmdkSel=Math.min(_cmdkItems.length-1,_cmdkSel+1);markCmdKSel();const el=document.querySelectorAll('.cmdk-item')[_cmdkSel];if(el)el.scrollIntoView({block:'nearest'});return;}
  if(e.key==='ArrowUp'){e.preventDefault();_cmdkSel=Math.max(0,_cmdkSel-1);markCmdKSel();const el=document.querySelectorAll('.cmdk-item')[_cmdkSel];if(el)el.scrollIntoView({block:'nearest'});return;}
  if(e.key==='Enter'){e.preventDefault();if(_cmdkItems[_cmdkSel])runCmdKItem(_cmdkItems[_cmdkSel]);return;}
 }
});
document.addEventListener('input',e=>{if(e.target&&e.target.id==='cmdk_input')renderCmdK(e.target.value);});
document.addEventListener('click',e=>{const it=e.target.closest('.cmdk-item');if(it){const idx=+it.dataset.i;if(_cmdkItems[idx])runCmdKItem(_cmdkItems[idx]);}});

// ====== Right-click context menu ======
function showCtxMenu(x,y,items){
 const root=document.getElementById('ctx_root');root.innerHTML='';
 const m=document.createElement('div');m.className='ctxmenu';
 // adjust position to viewport
 m.style.left=Math.min(x,window.innerWidth-220)+'px';
 m.style.top=Math.min(y,window.innerHeight-items.length*36)+'px';
 for(const it of items){
  const e=document.createElement('div');
  if(it.sep){e.className='it sep';m.appendChild(e);continue;}
  e.className='it';e.innerHTML=`<span class="ic">${it.ic||''}</span> ${it.label}`;
  e.onclick=()=>{hideCtxMenu();if(it.run)it.run();};
  m.appendChild(e);
 }
 root.appendChild(m);
}
function hideCtxMenu(){document.getElementById('ctx_root').innerHTML='';}
document.addEventListener('click',e=>{if(!e.target.closest('.ctxmenu'))hideCtxMenu();});
document.addEventListener('contextmenu',e=>{
 // find row with address (anchor with #wallet=) or market link
 const a=e.target.closest('a[href^="#wallet="]');
 const ma=e.target.closest('a[href^="#market="]');
 if(a){
  e.preventDefault();
  const addr=a.getAttribute('href').replace('#wallet=','');
  showCtxMenu(e.clientX,e.clientY,[
   {ic:'👛',label:'Open wallet',run:()=>{location.hash='wallet='+addr;}},
   {ic:'🔗',label:'Open in new tab',run:()=>window.open('/#wallet='+addr,'_blank')},
   {ic:'📋',label:'Copy address',run:()=>navigator.clipboard.writeText(addr)},
   {ic:'⭐',label:'Add to watchlist',run:()=>{const w=JSON.parse(localStorage.getItem('rise_watchlist')||'[]');if(!w.find(x=>x.addr===addr)){w.push({addr,label:''});localStorage.setItem('rise_watchlist',JSON.stringify(w));toast('Added to watchlist','ok');}else toast('Already in watchlist','warn');}},
   {ic:'📤',label:'Share link',run:()=>{navigator.clipboard.writeText(location.origin+'/share/wallet/'+addr);toast('Share link copied','ok');}},
   {sep:1},
   {ic:'🔍',label:'View on explorer',run:()=>window.open('https://explorer.risechain.com/address/'+addr,'_blank')},
  ]);
 } else if(ma){
  e.preventDefault();
  const mid=ma.getAttribute('href').replace('#market=','');
  showCtxMenu(e.clientX,e.clientY,[
   {ic:'📈',label:'Open market',run:()=>{location.hash='market='+mid;}},
   {ic:'🔗',label:'Open in new tab',run:()=>window.open('/#market='+mid,'_blank')},
   {ic:'📤',label:'Share link',run:()=>{navigator.clipboard.writeText(location.origin+'/share/market/'+mid);toast('Share link copied','ok');}},
  ]);
 }
});

// ====== Keyboard shortcuts: 1-7 for wallet tabs, esc, etc ======
document.addEventListener('keydown',e=>{
 // ignore if typing in input
 if(/INPUT|TEXTAREA|SELECT/.test((e.target||{}).tagName||''))return;
 const view=document.getElementById('v_wallet');
 if(view&&view.classList.contains('active')){
  const idx=parseInt(e.key,10);
  if(idx>=1&&idx<=9){
   const tabs=view.querySelectorAll('.walltab');
   if(tabs[idx-1]){tabs[idx-1].click();}
  }
 }
});

// ====== Toast notifications ======
const TOAST_ICS={ok:'✓',warn:'⚠',error:'⚠',info:'ℹ'};
function toast(msg,kind,duration){
 const root=document.getElementById('toasts');if(!root)return;
 kind=kind||'info';
 const el=document.createElement('div');
 el.className='toast '+kind;
 el.innerHTML=`<span class="ic">${TOAST_ICS[kind]||TOAST_ICS.info}</span><span>${msg}</span><span class="x">×</span>`;
 el.querySelector('.x').onclick=()=>{el.classList.add('dismiss');setTimeout(()=>el.remove(),250);};
 root.appendChild(el);
 if(window._soundOn&&kind==='ok')beep(880,80);
 if(window._soundOn&&(kind==='warn'||kind==='error'))beep(220,160);
 setTimeout(()=>{if(el.parentNode){el.classList.add('dismiss');setTimeout(()=>el.remove(),250);}},duration||5500);
}

// ====== Sound effects (Web Audio, off by default) ======
window._soundOn=localStorage.getItem('rise_sound')==='1';
let _audioCtx=null;
function _ctx(){return _audioCtx||(_audioCtx=new (window.AudioContext||window.webkitAudioContext)());}
function beep(freq,dur){
 if(!window._soundOn)return;
 try{const ctx=_ctx();const o=ctx.createOscillator();const g=ctx.createGain();
  o.connect(g);g.connect(ctx.destination);
  o.frequency.value=freq;o.type='sine';
  g.gain.setValueAtTime(0,ctx.currentTime);g.gain.linearRampToValueAtTime(0.05,ctx.currentTime+0.01);g.gain.exponentialRampToValueAtTime(0.0001,ctx.currentTime+(dur||100)/1000);
  o.start();o.stop(ctx.currentTime+(dur||100)/1000+0.05);
 }catch(e){}
}
function toggleSound(){window._soundOn=!window._soundOn;localStorage.setItem('rise_sound',window._soundOn?'1':'0');toast(window._soundOn?'Sound effects on':'Sound effects off',window._soundOn?'ok':'info');}

// ====== Nav-item pings ======
function navPing(viewKey,kind){
 const n=document.querySelector(`.navitem[data-v="${viewKey}"]`);if(!n)return;
 let p=n.querySelector('.ping');if(!p){p=document.createElement('span');p.className='ping';n.appendChild(p);}
 if(kind)p.classList.add(kind);
}
function navPingClear(viewKey){
 const n=document.querySelector(`.navitem[data-v="${viewKey}"]`);if(!n)return;
 const p=n.querySelector('.ping');if(p)p.remove();
}
// when user clicks a navitem, clear its ping
document.addEventListener('click',e=>{const n=e.target.closest('.navitem');if(n)navPingClear(n.dataset.v);});

// ====== "What's interesting" feed ======
function renderWhatsInteresting(){
 const root=document.getElementById('wif_panel');if(!root||!DATA)return;
 const cards=[];const t=DATA.totals||{};const ms=DATA.markets||[];
 // 1) Highest volume market
 if(ms.length){
  const top=[...ms].sort((a,b)=>(b.volume_24h||0)-(a.volume_24h||0))[0];
  cards.push({lbl:'#1 by volume',val:top.name,sub:U(top.volume_24h)+' 24h',mid:top.market_id});
 }
 // 2) Biggest mover (% change)
 if(ms.length){
  const mover=[...ms].sort((a,b)=>Math.abs(chgPct(b))-Math.abs(chgPct(a)))[0];
  const mPct=chgPct(mover);const cls=mPct>=0?'pos':'neg';
  cards.push({lbl:'Biggest mover',val:mover.name,sub:`<span class="${cls}">${mPct>=0?'+':''}${mPct.toFixed(2)}%</span>`,mid:mover.market_id});
 }
 // 3) Most extreme funding
 if(ms.length){
  const extreme=[...ms].sort((a,b)=>Math.abs(b.funding_8h)-Math.abs(a.funding_8h))[0];
  const f=extreme.funding_8h*100*3*365;const cls=f>=0?'pos':'neg';
  cards.push({lbl:'Hottest funding',val:extreme.name,sub:`<span class="${cls}">${f>=0?'+':''}${f.toFixed(1)}% APR</span>`,mid:extreme.market_id});
 }
 // 4) Volume growth from history
 if(DATA.history&&DATA.history.points&&DATA.history.points.length>1){
  const p=DATA.history.points;const first=p[0],last=p[p.length-1];
  const dv=first.vol?((last.vol-first.vol)/first.vol*100):0;
  const cls=dv>=0?'pos':'neg';
  cards.push({lbl:'Volume trend',val:`${dv>=0?'+':''}${dv.toFixed(1)}% vs ${p.length}h ago`,sub:`Now ${U(last.vol)}`});
 }
 // render
 root.innerHTML=cards.map(c=>`<div class="wif" ${c.mid?`onclick="location.hash='market='+'${c.mid}'"`:''}>
  <div class="lbl">${c.lbl}</div><div class="val">${c.val}</div><div class="sub">${c.sub}</div></div>`).join('');
}

// ====== Treemap (squarified algorithm) ======
function _squarify(items,W,H){
 // simple slice-and-dice for our small case (works for <20 markets cleanly)
 items=items.slice().sort((a,b)=>b.v-a.v);
 const total=items.reduce((s,x)=>s+x.v,0)||1;
 const out=[];let x=0,y=0,rowH=0,rowW=0,rowItems=[];
 function flush(rowItems,x,y,W){
  const sumV=rowItems.reduce((s,r)=>s+r.v,0);
  const h=sumV/total*W*H/W; let cx=x;
  for(const r of rowItems){const w=r.v/sumV*W;out.push({x:cx,y:y,w:w,h:h,it:r});cx+=w;}
  return h;
 }
 // simple horizontal rows
 const target=Math.sqrt(W*H/items.length);
 let curRowItems=[];let curRowVal=0;
 for(const it of items){curRowItems.push(it);curRowVal+=it.v;
  const rowH=curRowVal/total*W*H/W;
  if(rowH>=target){const h=flush(curRowItems,x,y,W);y+=h;curRowItems=[];curRowVal=0;}
 }
 if(curRowItems.length)flush(curRowItems,x,y,W);
 // scale to fit total H
 const maxY=Math.max(...out.map(c=>c.y+c.h),H);
 if(maxY>0){const sc=H/maxY;out.forEach(c=>{c.y*=sc;c.h*=sc;});}
 return out;
}
function renderTreemap(containerId,items,opts){
 const el=document.getElementById(containerId);if(!el)return;
 const rect=el.getBoundingClientRect();const W=rect.width||600,H=rect.height||300;
 const cells=_squarify(items,W,H);
 el.innerHTML=cells.map(c=>{const it=c.it;
  const colorH=it.color||140;const sat=60;const lit=Math.min(60,30+it.v/Math.max(...items.map(x=>x.v))*30);
  const bg=`hsl(${colorH},${sat}%,${lit/2}%)`;
  return `<div class="tcell" style="left:${c.x}px;top:${c.y}px;width:${c.w}px;height:${c.h}px;background:${bg}" onclick="${it.onclick||''}">
   <div class="nm">${it.label}</div>
   <div class="vl">${it.sub||''}</div>
  </div>`;}).join('');
}
function renderMarketsTreemap(){
 const ms=(DATA&&DATA.markets)||[];if(!ms.length)return;
 const items=ms.map(m=>{
  const fund=m.funding_8h*100;
  // hue: 130 (green/teal) for low funding, 0 (red) for high positive, 200 (cyan) for negative
  const hue=fund>0?Math.max(0,160-fund*300):Math.min(260,160+Math.abs(fund)*300);
  return {label:m.name,v:m.volume_24h||0,sub:U(m.volume_24h),color:hue,onclick:`location.hash='market=${m.market_id}'`};
 });
 renderTreemap('treemap_vol',items);
}

// ====== Calendar heatmap (GitHub-style) ======
function renderCalendarHeatmap(containerId,dailyValues){
 // dailyValues: {YYYY-MM-DD: count}
 const el=document.getElementById(containerId);if(!el)return;
 const today=new Date();today.setUTCHours(0,0,0,0);
 // 53 weeks back
 const start=new Date(today);start.setUTCDate(today.getUTCDate()-(53*7-1));
 // align to Monday
 const day=start.getUTCDay();const diff=(day===0?6:day-1);start.setUTCDate(start.getUTCDate()-diff);
 const vals=Object.values(dailyValues||{}).filter(v=>v>0);
 const maxV=vals.length?Math.max(...vals):1;
 const lvl=v=>{if(!v)return 0;const r=v/maxV;if(r>=.75)return 4;if(r>=.5)return 3;if(r>=.25)return 2;return 1;};
 const cells=[];
 for(let w=0;w<53;w++){
  for(let d=0;d<7;d++){
   const dt=new Date(start);dt.setUTCDate(start.getUTCDate()+w*7+d);
   if(dt>today)continue;
   const key=dt.toISOString().slice(0,10);
   const v=dailyValues[key]||0;
   cells.push(`<div class="cell l${lvl(v)}" title="${key}: ${v} trade${v===1?'':'s'}" style="grid-column:${w+1};grid-row:${d+1}"></div>`);
  }
 }
 el.innerHTML=`<div class="calmap" style="grid-template-rows:repeat(7,1fr)">${cells.join('')}</div>
  <div class="calmap-legend">Less <span class="swc l0" style="background:#13181f"></span><span class="swc l1" style="background:rgba(0,255,212,.18)"></span><span class="swc l2" style="background:rgba(0,255,212,.36)"></span><span class="swc l3" style="background:rgba(0,255,212,.56)"></span><span class="swc l4" style="background:rgba(0,255,212,.85)"></span> More</div>`;
}

// ====== Identicon generator (blockies-style deterministic) ======
function _hashSeed(addr){
 // Simple FNV-1a-ish hash → seedable PRNG
 let h=2166136261>>>0;
 for(let i=0;i<addr.length;i++){h^=addr.charCodeAt(i);h=Math.imul(h,16777619)>>>0;}
 return h;
}
function identicon(addr,size){
 size=size||42;
 const n=5; // 5x5 grid (mirrored)
 let seed=_hashSeed((addr||'0x0').toLowerCase());
 const next=()=>{seed=Math.imul(seed^(seed>>>15),2246822507)>>>0;seed=Math.imul(seed^(seed>>>13),3266489909)>>>0;return seed/4294967296;};
 // pick accent color (teal hue range)
 const hue=Math.floor(next()*360);
 const fg=`hsl(${hue},75%,60%)`;
 const bg=`hsl(${hue},35%,12%)`;
 const cell=size/n;
 let svg=`<svg viewBox="0 0 ${size} ${size}" xmlns="http://www.w3.org/2000/svg"><rect width="${size}" height="${size}" fill="${bg}"/>`;
 // generate left half (cols 0,1,2), mirror to (4,3,2)
 for(let y=0;y<n;y++){
  for(let x=0;x<3;x++){
   if(next()<0.5){
    svg+=`<rect x="${x*cell}" y="${y*cell}" width="${cell}" height="${cell}" fill="${fg}"/>`;
    if(x<2)svg+=`<rect x="${(n-1-x)*cell}" y="${y*cell}" width="${cell}" height="${cell}" fill="${fg}"/>`;
   }
  }
 }
 svg+='</svg>';
 return svg;
}

// ====== Color theme system ======
const THEMES=['mint','magenta','solar','mono','light'];
function setColorTheme(t){
 if(t==='light')document.documentElement.setAttribute('data-theme','light');
 else if(t==='mint')document.documentElement.removeAttribute('data-theme');
 else document.documentElement.setAttribute('data-theme',t);
 localStorage.setItem('rise_color_theme',t);
 // Track theme variety
 try{const p=loadPS();const used=new Set(p.themes_tried||[]);used.add(t);p.themes_tried=Array.from(used);savePS(p);
  if(p.themes_tried.length>=5)unlockAchievement('theme_all');}catch(e){}
 document.querySelectorAll('.swatch').forEach(s=>s.classList.toggle('active',s.dataset.th===t));
 const btn=document.getElementById('theme_btn');if(btn)btn.textContent={mint:'🌿',magenta:'💜',solar:'🌞',mono:'⬜',light:'☀️'}[t]||'🎨';
 // refresh charts so colors update
 Object.values(charts||{}).forEach(c=>{try{c.update('none')}catch(e){}});
 document.getElementById('themepicker').classList.remove('open');
}
(function initColorTheme(){const t=localStorage.getItem('rise_color_theme')||'mint';setColorTheme(t);})();
// keyboard shortcut ⌘1-5 for themes
document.addEventListener('keydown',e=>{
 const isMac=navigator.platform.toUpperCase().includes('MAC');
 const mod=isMac?e.metaKey:e.ctrlKey;
 if(!mod)return;
 const n=parseInt(e.key,10);
 if(n>=1&&n<=5){e.preventDefault();setColorTheme(THEMES[n-1]);}
});

// Splash hide manually after load
window.addEventListener('load',()=>{const s=document.getElementById('splash');if(s)setTimeout(()=>s.remove(),2300);});

// ====== Explorer (real-time via Server-Sent Events) ======
let _expES=null,_expAgeTimer=null,_expTxBuf=[],_expBlockBuf=[],_expFlushTimer=null;
const MAX_BLOCK_ROWS=18,MAX_TX_ROWS=24;
function ageStr(ts){const s=Math.floor(Date.now()/1000)-ts;if(s<2)return 'just now';if(s<60)return s+'s ago';if(s<3600)return Math.floor(s/60)+'m '+(s%60)+'s ago';return Math.floor(s/3600)+'h ago';}
function shortHash(h,n){n=n||10;return h?h.slice(0,n)+'…'+h.slice(-4):'—';}
function blockRowHtml(b){
 return `<tr data-ts="${b.ts}">
  <td><a href="#block=${b.number}" style="color:var(--accent2);font-family:var(--mono);font-weight:700">#${b.number.toLocaleString('en-US')}</a></td>
  <td class="ex-age" style="font-family:var(--mono);color:var(--muted);font-size:11.5px">${ageStr(b.ts)}</td>
  <td><span style="font-family:var(--mono);font-weight:700">${b.tx_count}</span></td>
  <td style="font-family:var(--mono);font-size:11.5px">${(b.gas_used/1e6).toFixed(2)}M <span style="color:var(--muted);font-size:10px">(${b.gas_pct.toFixed(1)}%)</span></td>
  <td style="font-family:var(--mono);font-size:11px;color:var(--muted)">${shortHash(b.miner,6)}</td></tr>`;
}
function txRowHtml(t){
 return `<tr data-ts="${t.ts}">
  <td><a href="#tx=${t.hash}" style="color:var(--accent2);font-family:var(--mono);font-weight:600;font-size:11px">${shortHash(t.hash,8)}</a></td>
  <td class="ex-age" style="font-family:var(--mono);color:var(--muted);font-size:11px">${ageStr(t.ts)}</td>
  <td style="font-family:var(--mono);font-size:11px">${shortHash(t.from,6)} <span style="color:var(--muted)">→</span> ${t.to?shortHash(t.to,6):'<span style="color:var(--amber)">contract create</span>'}</td>
  <td style="font-family:var(--mono);font-size:11px">${t.value>0?t.value.toFixed(4)+' ETH':'<span style="color:var(--muted)">0</span>'}</td></tr>`;
}

// ====== Block / Tx detail views ======
function copyChip(val){return `<span class="copy" onclick="navigator.clipboard.writeText('${val}');this.textContent='✓';setTimeout(()=>this.textContent='⎘',1000)" title="Copy">⎘</span>`;}
function addrCellSm(a){return a?`<span class="micro-id">${identicon(a,18)}</span><a href="#wallet=${a}">${a}</a>${copyChip(a)}`:'<span style="color:var(--muted)">—</span>';}
async function loadBlockDetail(n){
 const out=document.getElementById('blockOut');
 out.innerHTML='<div class="empty">Loading block #'+n+'…</div>';
 try{const d=await (await fetch('/api/explorer/block/'+n)).json();
  if(!d.ok){out.innerHTML='<div class="empty">Block not found.</div>';return;}
  const b=d.block;const txs=d.txs||[];
  let h=`<div class="detail-head">
   <div class="dh-icon">📦</div>
   <div class="dh-info">
    <div class="dh-lbl">Block</div>
    <div class="dh-val">#${b.number.toLocaleString('en-US')}</div>
    <div class="dh-meta">${ageStr(b.ts)} · ${new Date(b.ts*1000).toUTCString()}</div>
   </div>
   <div class="dh-actions">
    <a class="chip" href="#block=${b.number-1}">← Prev</a>
    <a class="chip" href="#block=${b.number+1}">Next →</a>
    <a class="chip" href="https://explorer.risechain.com/block/${b.number}" target="_blank">View on official ↗</a>
    <span class="chip" onclick="goHome()">✕ Close</span>
   </div>
  </div>`;
  h+=`<div class="kv">
   <div class="k">Block hash</div><div class="v">${b.hash}${copyChip(b.hash)}</div>
   <div class="k">Transactions</div><div class="v"><b style="color:var(--accent);font-weight:700">${b.tx_count}</b></div>
   <div class="k">Timestamp</div><div class="v">${b.ts} <span style="color:var(--muted);font-family:-apple-system,Inter,sans-serif">(${new Date(b.ts*1000).toUTCString()})</span></div>
   <div class="k">Validator</div><div class="v">${addrCellSm(b.miner)}</div>
   <div class="k">Gas used</div><div class="v">${(b.gas_used/1e6).toFixed(3)}M <span style="color:var(--muted);font-family:-apple-system,Inter,sans-serif">/ ${(b.gas_limit/1e6).toFixed(0)}M limit · ${b.gas_pct.toFixed(2)}% used</span></div>
   <div class="k">Size</div><div class="v">${(b.size/1024).toFixed(2)} KB</div>
  </div>`;
  if(txs.length){
   h+=`<div class="panel" style="margin-top:14px;padding:0"><h2 style="padding:14px 18px 0;margin-bottom:8px">📜 Transactions in this block · ${txs.length}</h2>
    <div style="max-height:520px;overflow:auto"><table>
     <thead><tr><th>Hash</th><th>From → To</th><th>Value</th><th>Gas</th></tr></thead>
     <tbody>`;
   for(const t of txs.slice(0,200)){
    h+=`<tr class="mini-tx-row">
     <td><a href="#tx=${t.hash}" style="color:var(--accent2);font-family:var(--mono);font-weight:600">${shortHash(t.hash,8)}</a></td>
     <td style="font-family:var(--mono)">${shortHash(t.from,6)} <span style="color:var(--muted)">→</span> ${t.to?shortHash(t.to,6):'<span style="color:var(--amber)">contract create</span>'}</td>
     <td style="font-family:var(--mono)">${t.value>0?t.value.toFixed(6)+' ETH':'<span style="color:var(--muted)">0</span>'}</td>
     <td style="font-family:var(--mono);color:var(--muted)">${(t.gas/1e3).toFixed(0)}k</td>
    </tr>`;
   }
   if(txs.length>200)h+=`<tr><td colspan=4 style="text-align:center;color:var(--muted)">… + ${txs.length-200} more transactions (showing first 200)</td></tr>`;
   h+='</tbody></table></div></div>';
  }
  out.innerHTML=h;
 }catch(e){out.innerHTML='<div class="empty">Error loading block.</div>';}
}

async function loadTxDetail(hash){
 const out=document.getElementById('txOut');
 out.innerHTML='<div class="empty">Loading transaction…</div>';
 try{const d=await (await fetch('/api/explorer/tx/'+hash)).json();
  if(!d.ok){out.innerHTML='<div class="empty">Transaction not found.</div>';return;}
  const t=d.tx;
  const isContract=!t.to;
  const okStatus=t.status===1?'ok':(t.status===0?'fail':'');
  const statusHtml=t.status==null?'':`<span class="dh-status ${okStatus}">${t.status===1?'Success':'Failed'}</span>`;
  const inputSelector=t.input&&t.input.length>=10?t.input.slice(0,10):'';
  let h=`<div class="detail-head">
   <div class="dh-icon">📜</div>
   <div class="dh-info">
    <div class="dh-lbl">Transaction</div>
    <div class="dh-val" style="font-size:17px">${shortHash(t.hash,16)}</div>
    <div class="dh-meta">${t.ts?ageStr(t.ts)+' · ':''}${t.ts?new Date(t.ts*1000).toUTCString():''} ${statusHtml}</div>
   </div>
   <div class="dh-actions">
    <a class="chip" href="#block=${t.block}">📦 Block #${t.block.toLocaleString('en-US')}</a>
    <a class="chip" href="https://explorer.risechain.com/tx/${t.hash}" target="_blank">View on official ↗</a>
    <span class="chip" onclick="goHome()">✕ Close</span>
   </div>
  </div>`;
  h+=`<div class="kv">
   <div class="k">Hash</div><div class="v">${t.hash}${copyChip(t.hash)}</div>
   <div class="k">Status</div><div class="v">${t.status===1?'<span class="pos">✓ Success</span>':t.status===0?'<span class="neg">✕ Failed</span>':'<span style="color:var(--muted)">Pending / unknown</span>'}</div>
   <div class="k">Block</div><div class="v"><a href="#block=${t.block}">#${t.block.toLocaleString('en-US')}</a> · index ${t.tx_index}</div>
   <div class="k">Timestamp</div><div class="v">${t.ts?t.ts:'—'} <span style="color:var(--muted);font-family:-apple-system,Inter,sans-serif">${t.ts?'('+new Date(t.ts*1000).toUTCString()+')':''}</span></div>
   <div class="k">From</div><div class="v">${addrCellSm(t.from)}</div>
   <div class="k">To</div><div class="v">${t.to?addrCellSm(t.to)+' <span class="tag-contract" title="Account or contract">to</span>':'<span class="tag-contract">Contract creation</span>'+(t.contract_address?' '+addrCellSm(t.contract_address):'')}</div>
   <div class="k">Value</div><div class="v"><b style="color:${t.value>0?'var(--accent)':'var(--muted)'};font-weight:700">${t.value.toFixed(8)} ETH</b></div>
   <div class="k">Gas limit</div><div class="v">${t.gas.toLocaleString('en-US')}</div>
   ${t.gas_used!=null?`<div class="k">Gas used</div><div class="v">${t.gas_used.toLocaleString('en-US')} <span style="color:var(--muted);font-family:-apple-system,Inter,sans-serif">(${(t.gas_used/t.gas*100).toFixed(1)}% of limit)</span></div>`:''}
   <div class="k">Gas price</div><div class="v">${(t.gas_price/1e9).toFixed(6)} Gwei</div>
   <div class="k">Nonce</div><div class="v">${t.nonce.toLocaleString('en-US')}</div>
   <div class="k">Tx type</div><div class="v">${t.type} ${t.type===2?'<span style="color:var(--muted);font-family:-apple-system,Inter,sans-serif">(EIP-1559)</span>':''}</div>
   <div class="k">Chain ID</div><div class="v">${t.chain_id}</div>
  </div>`;
  // input data
  if(t.input&&t.input.length>2){
   h+=`<div class="panel" style="margin-top:14px;padding:18px 20px"><h2>📥 Input data · ${t.input_size} bytes</h2>
    ${inputSelector?`<div style="margin-bottom:10px;font-family:var(--mono);font-size:12px;color:var(--muted)">Function selector: <span class="selector" style="color:var(--accent);font-weight:700">${inputSelector}</span></div>`:''}
    <div class="inputdata">${t.input}</div>
   </div>`;
  }
  // logs
  if(t.logs&&t.logs.length){
   h+=`<div class="panel" style="margin-top:14px;padding:18px 20px"><h2>📡 Events emitted · ${t.logs_count}</h2>`;
   for(const lg of t.logs){
    h+=`<div class="logitem">
     <div class="log-h"><span class="log-addr">${lg.address}</span><span class="log-idx">log #${lg.log_index}</span></div>
     ${(lg.topics||[]).map((tp,i)=>`<div class="log-topic"><b>topic ${i}:</b> ${tp}</div>`).join('')}
     ${lg.data&&lg.data!=='0x'?`<div class="log-data">${lg.data}</div>`:''}
    </div>`;
   }
   if(t.logs_count>t.logs.length)h+=`<div class="note" style="text-align:center">… + ${t.logs_count-t.logs.length} more events (showing first ${t.logs.length})</div>`;
   h+='</div>';
  }
  out.innerHTML=h;
 }catch(e){out.innerHTML='<div class="empty">Error loading transaction.</div>';}
}
function _expSetStats(s){
 if(!s)return;
 const el=document.getElementById('ex_block');if(el)el.textContent='#'+s.last_block.toLocaleString('en-US');
 setVal('ex_tps',s.tps_now,n=>(+n).toFixed(2));
 setVal('ex_bt',s.avg_block_time,n=>(+n).toFixed(2)+'s');
 setVal('ex_buf',s.buffered_txs,n=>(+n).toLocaleString('en-US'));
 const m=document.getElementById('ex_buf_m');if(m)m.textContent='peak '+(s.tps_peak||0).toFixed(0)+' tx/block';
}
function _expPrepend(tbodyId,html,maxRows){
 const tb=document.getElementById(tbodyId);if(!tb)return;
 // Remove "empty" rows if any
 const empty=tb.querySelector('td.empty');if(empty)tb.innerHTML='';
 const div=document.createElement('tbody');div.innerHTML=html;
 const newRows=Array.from(div.children);
 // Insert at top with slide-in
 for(let i=newRows.length-1;i>=0;i--){
  const r=newRows[i];r.classList.add('lbrow');
  tb.insertBefore(r,tb.firstChild);
 }
 // Trim excess
 while(tb.children.length>maxRows)tb.removeChild(tb.lastChild);
}
function _updateAges(){
 document.querySelectorAll('#v_explorer .ex-age').forEach(td=>{
  const ts=+td.parentElement.dataset.ts;if(!ts)return;
  td.textContent=ageStr(ts);
 });
 // also update Latest Block subtitle age
 const m=document.getElementById('ex_block_m');if(m&&_expLastUpdate)m.textContent=agoStr(_expLastUpdate);
}
let _expLastUpdate=0;
function startExplorer(){
 if(_expES)return; // already streaming
 const m=document.getElementById('ex_block_m');if(m)m.textContent='connecting…';
 try{
  _expES=new EventSource('/api/explorer/stream');
  _expES.addEventListener('init',e=>{
   const d=JSON.parse(e.data);
   const tbB=document.getElementById('ex_blocks');
   const tbT=document.getElementById('ex_txs');
   if(tbB)tbB.innerHTML=(d.blocks||[]).map(blockRowHtml).join('')||'<tr><td class="empty" colspan=5>Waiting for next block…</td></tr>';
   if(tbT)tbT.innerHTML=(d.txs||[]).map(txRowHtml).join('')||'<tr><td class="empty" colspan=4>Waiting for next tx…</td></tr>';
   if(d.last_block){const el=document.getElementById('ex_block');if(el)el.textContent='#'+d.last_block.toLocaleString('en-US');}
   _expLastUpdate=Math.floor(Date.now()/1000);
  });
  _expES.addEventListener('block',e=>{
   const d=JSON.parse(e.data);
   if(d.block)_expBlockBuf.push(d.block);
   if(d.new_txs&&d.new_txs.length){
    // queue txs to render staggered
    for(const t of d.new_txs)_expTxBuf.push(t);
   }
   if(d.stats)_expSetStats(d.stats);
   _expLastUpdate=Math.floor(Date.now()/1000);
   _scheduleFlush();
  });
  _expES.onerror=()=>{};
 }catch(e){}
 // Local ticker for ages — smooth without re-requesting
 if(_expAgeTimer)clearInterval(_expAgeTimer);
 _expAgeTimer=setInterval(_updateAges,1000);
}
function _scheduleFlush(){if(_expFlushTimer)return;_expFlushTimer=setInterval(_flushNext,40);}
function _flushNext(){
 // Render at adaptive rate: aim to drain buffer over ~1 second
 // If buffer empty, kill timer.
 if(_expTxBuf.length===0&&_expBlockBuf.length===0){
  clearInterval(_expFlushTimer);_expFlushTimer=null;return;
 }
 // Render 1 block at a time, prepended above the staggered txs.
 if(_expBlockBuf.length>0){
  const b=_expBlockBuf.shift();
  _expPrepend('ex_blocks',blockRowHtml(b),MAX_BLOCK_ROWS);
 }
 // Drain txs: render 1-3 per tick depending on backlog
 const burst=_expTxBuf.length>50?3:(_expTxBuf.length>20?2:1);
 for(let i=0;i<burst&&_expTxBuf.length>0;i++){
  const t=_expTxBuf.shift();
  _expPrepend('ex_txs',txRowHtml(t),MAX_TX_ROWS);
 }
}
function stopExplorer(){if(_expES){_expES.close();_expES=null;}if(_expAgeTimer){clearInterval(_expAgeTimer);_expAgeTimer=null;}if(_expFlushTimer){clearInterval(_expFlushTimer);_expFlushTimer=null;}_expTxBuf=[];_expBlockBuf=[];}
// stop streaming when leaving the view
document.addEventListener('click',e=>{const t=e.target.closest('.mainnav a');if(t&&t.dataset.section!=='explorer'&&_expES)stopExplorer();});

// ====== Top nav + Sub nav refactor ======
const NAV_SECTIONS={
 overview:{label:'Overview',views:[{k:'overview',label:'Overview'}]},
 markets:{label:'Markets',views:[{k:'markets',label:'Markets'}]},
 traders:{label:'Traders',views:[
  {k:'pnl',label:'Top PnL'},
  {k:'volranking',label:'Volume'},
  {k:'acctoi',label:'Current OI'},
  {k:'oiranking',label:'Avg OI'},
  {k:'ranking',label:'Positions by market'},
  {k:'funded',label:'Funding payments'},
  {k:'liq',label:'Liquidations'},
  {k:'feed',label:'Live activity'},
 ]},
 explorer:{label:'Explorer',views:[{k:'explorer',label:'Explorer'}]},
 insights:{label:'Insights',views:[
  {k:'longshort',label:'Long / Short'},
  {k:'heatmap',label:'Liq. heatmap'},
  {k:'marketshare',label:'Market share'},
  {k:'funding',label:'Funding vs DEXes'},
 ]},
 tools:{label:'Tools',views:[
  {k:'tools',label:'Simulator & Calculator'},
  {k:'compare',label:'Compare wallets'},
  {k:'watchlist',label:'Watchlist'},
  {k:'alerts',label:'Alerts'},
 ]},
};
// Reverse lookup: view → section
const _viewToSection={};
for(const [s,sec] of Object.entries(NAV_SECTIONS))for(const v of sec.views)_viewToSection[v.k]=s;
function activateSection(sec){
 const conf=NAV_SECTIONS[sec];if(!conf)return;
 document.querySelectorAll('.mainnav a').forEach(a=>a.classList.toggle('active',a.dataset.section===sec));
 const sub=document.getElementById('subnav');
 if(conf.views.length<=1){sub.classList.remove('show');sub.innerHTML='';activateView(conf.views[0].k);return;}
 // populate subnav
 sub.classList.add('show');
 sub.innerHTML=conf.views.map((v,i)=>`<a data-v="${v.k}" class="${i===0?'active':''}">${v.label}</a>`).join('');
 // wire subnav clicks
 sub.querySelectorAll('a').forEach(a=>a.onclick=()=>{
  sub.querySelectorAll('a').forEach(x=>x.classList.remove('active'));
  a.classList.add('active');
  activateView(a.dataset.v);
 });
 // activate first view
 activateView(conf.views[0].k);
}
function activateView(vKey){
 // Trigger the legacy navitem click which already handles view switch + data loading
 const ni=document.querySelector(`.navitem[data-v="${vKey}"]`);
 if(ni)ni.click();
}
function syncTopNavWithCurrentView(){
 // when something else (cmdk, hash routing) activates a view, sync the top nav
 const active=document.querySelector('.view.active');
 if(!active)return;
 const viewKey=active.id.replace('v_','');
 if(viewKey==='wallet'||viewKey==='marketdetail')return;
 const sec=_viewToSection[viewKey];if(!sec)return;
 // update mainnav active
 const cur=document.querySelector('.mainnav a.active');
 if(cur&&cur.dataset.section===sec){
  // just sync subnav active
  document.querySelectorAll('#subnav a').forEach(x=>x.classList.toggle('active',x.dataset.v===viewKey));
  return;
 }
 // section changed
 document.querySelectorAll('.mainnav a').forEach(a=>a.classList.toggle('active',a.dataset.section===sec));
 const conf=NAV_SECTIONS[sec];const sub=document.getElementById('subnav');
 if(conf.views.length<=1){sub.classList.remove('show');sub.innerHTML='';}else{
  sub.classList.add('show');
  sub.innerHTML=conf.views.map(v=>`<a data-v="${v.k}" class="${v.k===viewKey?'active':''}">${v.label}</a>`).join('');
  sub.querySelectorAll('a').forEach(a=>a.onclick=()=>{
   sub.querySelectorAll('a').forEach(x=>x.classList.remove('active'));
   a.classList.add('active');activateView(a.dataset.v);
  });
 }
}
// Wire top nav clicks (legacy — hidden but harmless)
document.querySelectorAll('.mainnav a').forEach(a=>a.onclick=e=>{e.preventDefault();activateSection(a.dataset.section);});

// ====== Sidebar accordion ======
function toggleNgroup(sec){
 const g=document.querySelector(`.ngroup[data-sec="${sec}"]`);
 if(!g)return;
 const wasOpen=g.classList.contains('open');
 // accordion: close siblings, then toggle this one
 document.querySelectorAll('.ngroup.expandable').forEach(x=>x.classList.remove('open'));
 if(!wasOpen)g.classList.add('open');
}
// Expand the group containing the active view + highlight parent.
// Only fires on view CHANGE so manual accordion toggles persist.
let _lastSyncedVk=null;
function syncSidebarWithView(){
 const active=document.querySelector('.view.active');
 if(!active)return;
 const vk=active.id.replace('v_','');
 if(vk==='wallet'||vk==='marketdetail'||vk==='blockdetail'||vk==='txdetail')return;
 if(vk===_lastSyncedVk)return;
 _lastSyncedVk=vk;
 const sec=_viewToSection[vk];
 if(!sec)return;
 const g=document.querySelector(`.ngroup[data-sec="${sec}"]`);
 const isExpandable=g && g.classList.contains('expandable');
 // parent-active highlight on the group containing the active view
 document.querySelectorAll('.ngroup').forEach(x=>{
  x.classList.toggle('parent-active', x===g && isExpandable);
 });
 // If active view's parent is expandable: open it as the single accordion. Else leave groups as-is.
 if(isExpandable){
  document.querySelectorAll('.ngroup.expandable').forEach(x=>{
   x.classList.toggle('open', x===g);
  });
 }
}
setTimeout(syncSidebarWithView,80);
// Observe view changes to keep nav in sync
setInterval(()=>{syncTopNavWithCurrentView();syncSidebarWithView();},500);

// ====== Chart.js global tooltip + gradient fills ======
(function chartGlobal(){
 if(typeof Chart==='undefined'){setTimeout(chartGlobal,200);return;}
 Chart.defaults.font.family="-apple-system,Inter,sans-serif";
 Chart.defaults.font.size=11;
 Chart.defaults.color='#7a8694';
 Chart.defaults.borderColor='#1a2129';
 Chart.defaults.animation={duration:600,easing:'easeOutCubic'};
 Chart.defaults.plugins.tooltip={enabled:true,
  backgroundColor:'#0a0e13',
  titleColor:'#fff',titleFont:{family:"-apple-system,Inter,sans-serif",weight:700,size:12},
  bodyColor:'#c9d0d8',bodyFont:{family:"JetBrains Mono,monospace",size:11.5},
  borderColor:'rgba(0,255,212,.30)',borderWidth:1,
  padding:11,cornerRadius:8,
  caretSize:6,caretPadding:8,
  displayColors:true,boxWidth:8,boxHeight:8,boxPadding:5,
  multiKeyBackground:'transparent',
  callbacks:{}};
 Chart.defaults.plugins.legend.labels.color='#7a8694';
 Chart.defaults.plugins.legend.labels.font={size:11};
 Chart.defaults.plugins.legend.labels.usePointStyle=true;
 Chart.defaults.plugins.legend.labels.pointStyle='circle';
})();
function gradFill(ctx,h,color1,color2){
 const g=ctx.createLinearGradient(0,0,0,h);
 g.addColorStop(0,color1);g.addColorStop(1,color2);return g;
}

// ====== Particle constellation hero ======
function initParticles(){
 const c=document.getElementById('hero_particles');if(!c)return;
 const ctx=c.getContext('2d');const dpr=window.devicePixelRatio||1;
 function resize(){const r=c.getBoundingClientRect();c.width=r.width*dpr;c.height=r.height*dpr;ctx.scale(dpr,dpr);}
 resize();window.addEventListener('resize',()=>{ctx.setTransform(1,0,0,1,0,0);resize();},{passive:true});
 const particles=[];const N=42;
 for(let i=0;i<N;i++)particles.push({x:Math.random()*c.width/dpr,y:Math.random()*c.height/dpr,vx:(Math.random()-.5)*.18,vy:(Math.random()-.5)*.18,r:.6+Math.random()*1.2});
 function tick(){
  const W=c.width/dpr,H=c.height/dpr;
  ctx.clearRect(0,0,W,H);
  for(const p of particles){p.x+=p.vx;p.y+=p.vy;if(p.x<0||p.x>W)p.vx*=-1;if(p.y<0||p.y>H)p.vy*=-1;}
  // connections
  ctx.strokeStyle='rgba(0,255,212,.10)';ctx.lineWidth=.6;
  for(let i=0;i<N;i++){for(let j=i+1;j<N;j++){const dx=particles[i].x-particles[j].x,dy=particles[i].y-particles[j].y;const d=Math.hypot(dx,dy);
   if(d<90){ctx.globalAlpha=1-d/90;ctx.beginPath();ctx.moveTo(particles[i].x,particles[i].y);ctx.lineTo(particles[j].x,particles[j].y);ctx.stroke();}}}
  ctx.globalAlpha=1;
  // dots
  ctx.fillStyle='rgba(0,255,212,.65)';for(const p of particles){ctx.beginPath();ctx.arc(p.x,p.y,p.r,0,Math.PI*2);ctx.fill();}
  requestAnimationFrame(tick);
 }
 tick();
}
setTimeout(initParticles,400);

// ====== Activity news ticker ======
async function loadNewsticker(){
 try{
  // pull from daily story + live feed
  const [story,feed]=await Promise.all([
    fetch('/api/daily-story').then(r=>r.json()),
    fetch('/api/live-activity?limit=12').then(r=>r.json())
  ]);
  const items=[];
  if(story&&story.ok)for(const s of story.stories)items.push({icon:s.icon,html:s.html});
  if(feed&&feed.entries)for(const e of feed.entries.slice(0,10)){
   const dt=new Date(e.ts*1000).toISOString().slice(11,16);
   if(e.is_liq){
    items.push({icon:'💀',html:`<b>${dt}</b> · ${e.market} <span class="neg">−${U(Math.abs(e.realized_pnl))}</span> liquidation`});
   } else if(e.notional>=100000){
    const sCls=e.position_side==='Long'?'pos':'neg';
    items.push({icon:e.position_side==='Long'?'📈':'📉',html:`<b>${dt}</b> · ${e.market} <span class="${sCls}">${e.position_side}</span> ${U(e.notional)}`});
   }
  }
  if(!items.length)items.push({icon:'🌿',html:'Live perp analytics for RISE chain'});
  const html=items.map(i=>`<span class="nt-item"><span class="nt-i">${i.icon}</span>${i.html}</span>`).join('');
  // duplicate for seamless scroll
  document.getElementById('nt_track').innerHTML=html+html;
 }catch(e){}
}
setTimeout(loadNewsticker,1500);setInterval(loadNewsticker,60000);

// ====== Notification center 🔔 ======
const NF_KEY='rise_notifs';
function nfList(){return JSON.parse(localStorage.getItem(NF_KEY)||'[]');}
function nfSave(l){localStorage.setItem(NF_KEY,JSON.stringify(l.slice(0,30)));}
function nfAdd(icon,title,desc){
 const l=nfList();
 l.unshift({icon,title,desc,ts:Date.now(),read:false});
 nfSave(l);renderNotifs();
}
function nfMarkAllRead(){const l=nfList().map(n=>({...n,read:true}));nfSave(l);renderNotifs();}
function clearNotifs(){nfSave([]);renderNotifs();toggleNotif();}
function toggleNotif(){const p=document.getElementById('notif_panel');p.classList.toggle('open');
 if(p.classList.contains('open')){nfMarkAllRead();}}
document.addEventListener('click',e=>{const p=document.getElementById('notif_panel');const b=document.getElementById('notif_btn');
 if(p&&!p.contains(e.target)&&b&&!b.contains(e.target))p.classList.remove('open');});
function renderNotifs(){
 const list=document.getElementById('notif_list');if(!list)return;
 const l=nfList();
 const btn=document.getElementById('notif_btn');
 if(l.some(n=>!n.read))btn.classList.add('has-new');else btn.classList.remove('has-new');
 if(!l.length){list.innerHTML='<div class="notif-empty">No notifications yet · achievements & events will show here</div>';return;}
 list.innerHTML=l.map(n=>`<div class="notif-item ${n.read?'':'new'}">
  <div class="ni-ic">${n.icon}</div>
  <div class="ni-c"><div class="ni-t">${n.title}</div><div class="ni-d">${n.desc||''}</div><div class="ni-ts">${fmtTimeSince(n.ts)}</div></div></div>`).join('');
}
// Hook into achievements
const _origUnlockAch=unlockAchievement;
window.unlockAchievement=function(key,ttl,desc){
 const p=loadPS();const wasNew=!p.achievements.includes(key);
 _origUnlockAch(key,ttl,desc);
 if(wasNew){const a=ACHIEVEMENTS[key]||{};nfAdd(a.ic||'🏆','Achievement: '+(ttl||a.ttl||key),desc||a.desc||'');}
};
setTimeout(renderNotifs,500);

// ====== Custom selects (replace native) ======
function makeCustomSelects(){
 document.querySelectorAll('select:not([data-csel])').forEach(sel=>{
  if(sel.options.length===0)return;
  sel.dataset.csel='1';sel.style.display='none';
  const wrap=document.createElement('div');wrap.className='cselect';
  const btn=document.createElement('button');btn.type='button';btn.className='csel-btn';
  const list=document.createElement('div');list.className='csel-list';
  function refresh(){
   btn.textContent=sel.options[sel.selectedIndex]?.text||'';
   list.innerHTML='';
   for(let i=0;i<sel.options.length;i++){const o=sel.options[i];const it=document.createElement('div');
    it.className='csel-opt'+(i===sel.selectedIndex?' sel':'');it.textContent=o.text;
    it.onclick=()=>{sel.selectedIndex=i;sel.dispatchEvent(new Event('change'));refresh();wrap.classList.remove('open');};
    list.appendChild(it);}
  }
  btn.onclick=e=>{e.stopPropagation();document.querySelectorAll('.cselect.open').forEach(x=>{if(x!==wrap)x.classList.remove('open');});wrap.classList.toggle('open');};
  wrap.appendChild(btn);wrap.appendChild(list);
  sel.parentNode.insertBefore(wrap,sel.nextSibling);
  refresh();
  // sync if changed externally
  new MutationObserver(refresh).observe(sel,{childList:true,attributes:true,subtree:true});
 });
}
document.addEventListener('click',()=>document.querySelectorAll('.cselect.open').forEach(x=>x.classList.remove('open')));
setInterval(makeCustomSelects,2000);

// ====== Custom range slider fill ======
function updateRangeFill(){
 document.querySelectorAll('input[type="range"]').forEach(r=>{
  const min=+r.min||0,max=+r.max||100,val=+r.value||0;
  const pct=((val-min)/(max-min))*100;
  r.style.setProperty('--rngP',pct.toFixed(1)+'%');
 });
}
document.addEventListener('input',e=>{if(e.target.type==='range')updateRangeFill();});
setInterval(updateRangeFill,1000);

// ====== Drawing tools on candle chart ======
let DRAW_TOOL='none',DRAW_START=null,DRAW_PRE=null;
function drawingsKey(){return 'rise_draw_'+CH_MID+'_'+CH_IV;}
function getDrawings(){return JSON.parse(localStorage.getItem(drawingsKey())||'[]');}
function saveDrawings(d){localStorage.setItem(drawingsKey(),JSON.stringify(d));}
function wireDrawTools(){
 const tools=document.getElementById('draw_tools');if(!tools||tools.dataset.w)return;
 tools.dataset.w='1';
 tools.querySelectorAll('button').forEach(b=>b.onclick=()=>{
  const t=b.dataset.t;
  if(t==='clear'){saveDrawings([]);drawCandles();return;}
  tools.querySelectorAll('button').forEach(x=>x.classList.remove('active'));
  b.classList.add('active');DRAW_TOOL=t;DRAW_START=null;
  const c=document.getElementById('ch_canvas');if(c)c.style.cursor=t==='none'?'crosshair':'cell';
 });
 const c=document.getElementById('ch_canvas');if(!c||c.dataset.w)return;c.dataset.w='1';
 c.addEventListener('mousedown',e=>{
  if(DRAW_TOOL==='none'||!CH_DATA)return;
  const r=c.getBoundingClientRect();const xp=(e.clientX-r.left)/r.width;const yp=(e.clientY-r.top)/r.height;
  const price=screenToPrice(yp);const tIdx=Math.round(xp*(CH_DATA.length-1));
  if(DRAW_TOOL==='hline'){const d=getDrawings();d.push({t:'h',price});saveDrawings(d);drawCandles();}
  else if(!DRAW_START){DRAW_START={tIdx,price,xp,yp};}
  else{const d=getDrawings();d.push({t:DRAW_TOOL,a:DRAW_START,b:{tIdx,price,xp,yp}});saveDrawings(d);DRAW_START=null;drawCandles();}
 });
 c.addEventListener('mousemove',e=>{
  if(!CH_DATA)return;const r=c.getBoundingClientRect();const xp=(e.clientX-r.left)/r.width;const yp=(e.clientY-r.top)/r.height;
  drawCrosshair(xp,yp);
 });
 c.addEventListener('mouseleave',()=>{drawCandles();});
}
function screenToPrice(yp){
 if(!CH_DATA||!CH_DATA.length)return 0;let hi=-Infinity,lo=Infinity;for(const c of CH_DATA){if(c.h>hi)hi=c.h;if(c.l<lo)lo=c.l;}
 const range=hi-lo||1;hi+=range*.04;lo-=range*.04;
 // approximate: yp maps to price area (top padding 10, vol 70, padB 24, h=380)
 const padT=10,padB=24,volH=70,priceH=380-padT-padB-volH-6;
 const y=yp*380;return hi-((y-padT)/priceH)*(hi-lo);
}
function drawCrosshair(xp,yp){
 if(!CH_DATA||!CH_DATA.length)return;
 const canvas=document.getElementById('ch_canvas');if(!canvas)return;
 drawCandles();// redraw fresh, then overlay
 const ctx=canvas.getContext('2d');const r=canvas.getBoundingClientRect();
 const W=r.width,H=r.height;
 ctx.save();
 ctx.setLineDash([2,2]);ctx.strokeStyle='rgba(0,255,212,.4)';ctx.lineWidth=1;
 ctx.beginPath();ctx.moveTo(xp*W,0);ctx.lineTo(xp*W,H);ctx.stroke();
 ctx.beginPath();ctx.moveTo(0,yp*H);ctx.lineTo(W,yp*H);ctx.stroke();
 ctx.setLineDash([]);
 // candle at this x
 const idx=Math.round(xp*(CH_DATA.length-1));const c=CH_DATA[Math.max(0,Math.min(CH_DATA.length-1,idx))];
 if(c){const dt=new Date(c.t*1000);const dts=dt.toISOString().slice(5,16).replace('T',' ');
  const upCls=c.c>=c.o;
  const tipW=180,tipH=84;const tx=Math.min(W-tipW-8,xp*W+10);const ty=Math.max(8,yp*H-tipH-10);
  ctx.fillStyle='rgba(10,14,19,.96)';ctx.strokeStyle='rgba(0,255,212,.3)';ctx.lineWidth=1;
  ctx.beginPath();ctx.roundRect(tx,ty,tipW,tipH,7);ctx.fill();ctx.stroke();
  ctx.fillStyle='#6b7785';ctx.font='10px JetBrains Mono,monospace';ctx.fillText(dts+' UTC',tx+9,ty+15);
  ctx.fillStyle='#fff';ctx.font='10.5px JetBrains Mono,monospace';
  ctx.fillText('O '+c.o.toFixed(2),tx+9,ty+30);
  ctx.fillText('H '+c.h.toFixed(2),tx+9,ty+44);
  ctx.fillText('L '+c.l.toFixed(2),tx+9,ty+58);
  ctx.fillStyle=upCls?'#1aeeaa':'#ff3b6e';ctx.fillText('C '+c.c.toFixed(2),tx+9,ty+72);
  ctx.fillStyle='#6b7785';ctx.font='9.5px JetBrains Mono,monospace';
  ctx.fillText('V '+c.v.toFixed(3),tx+92,ty+72);}
 ctx.restore();
}
// Render drawings overlay
function drawDrawings(){
 const d=getDrawings();if(!d.length)return;
 const canvas=document.getElementById('ch_canvas');if(!canvas)return;
 const ctx=canvas.getContext('2d');const r=canvas.getBoundingClientRect();
 const W=r.width,H=r.height;
 const priceToY=p=>{let hi=-Infinity,lo=Infinity;for(const c of CH_DATA){if(c.h>hi)hi=c.h;if(c.l<lo)lo=c.l;}
  const range=hi-lo||1;hi+=range*.04;lo-=range*.04;
  const padT=10,padB=24,volH=70,priceH=H-padT-padB-volH-6;
  return padT+((hi-p)/(hi-lo))*priceH;};
 const idxToX=i=>10+(W-10-68)*i/(CH_DATA.length-1);
 ctx.save();ctx.lineWidth=1.5;
 for(const dr of d){
  ctx.strokeStyle='#ffb454';ctx.fillStyle='#ffb454';
  if(dr.t==='h'){const y=priceToY(dr.price);ctx.beginPath();ctx.moveTo(10,y);ctx.lineTo(W-68,y);ctx.stroke();
   ctx.fillStyle='#ffb454';ctx.font='10px JetBrains Mono';ctx.fillText(dr.price.toFixed(2),W-65,y-3);}
  else if(dr.t==='trend'){const ax=idxToX(dr.a.tIdx),ay=priceToY(dr.a.price);const bx=idxToX(dr.b.tIdx),by=priceToY(dr.b.price);
   ctx.beginPath();ctx.moveTo(ax,ay);ctx.lineTo(bx,by);ctx.stroke();}
  else if(dr.t==='fib'){const ay=priceToY(dr.a.price);const by=priceToY(dr.b.price);const fibs=[0,.236,.382,.5,.618,.786,1];
   for(const ff of fibs){const y=ay+(by-ay)*ff;const px=dr.a.price+(dr.b.price-dr.a.price)*ff;
    ctx.strokeStyle='rgba(255,180,84,.55)';ctx.setLineDash(ff===0||ff===1?[]:[3,2]);
    ctx.beginPath();ctx.moveTo(10,y);ctx.lineTo(W-68,y);ctx.stroke();ctx.setLineDash([]);
    ctx.fillStyle='#ffb454';ctx.font='9.5px JetBrains Mono';ctx.fillText((ff*100).toFixed(1)+'% · '+px.toFixed(2),12,y-3);}}
 }
 ctx.restore();
}
const _origDrawCandles=window.drawCandles;
window.drawCandles=function(){if(_origDrawCandles)_origDrawCandles();drawDrawings();};
setInterval(wireDrawTools,1500);

// ====== Trader Profile Cards ======
async function openTcard(addr){
 const m=document.getElementById('tcard_modal');m.classList.add('open');
 const out=document.getElementById('tcard_render');out.innerHTML='<div style="color:var(--muted)">Loading…</div>';
 try{const [w,prev]=await Promise.all([
   fetch('/api/wallet?account='+encodeURIComponent(addr)).then(r=>r.json()),
   fetch('/api/wallet-preview?account='+encodeURIComponent(addr)).then(r=>r.json())
  ]);
  const s=(w&&w.summary)||{};
  const tier=prev.volume_30d>=10e6?{lbl:'WHALE',ic:'🐋'}:prev.volume_30d>=1e6?{lbl:'PRO',ic:'💎'}:prev.volume_30d>=1e5?{lbl:'ACTIVE',ic:'⚡'}:{lbl:'TRADER',ic:'🎯'};
  const smartTag=prev.smart?'<div style="position:absolute;top:55px;right:18px;font-size:9.5px;font-weight:800;color:var(--accent);background:rgba(0,255,212,.10);border:1px solid var(--accent);padding:3px 7px;border-radius:4px;letter-spacing:1.3px;animation:slideHi 2.5s linear infinite">SMART</div>':'';
  const short=addr.slice(0,6)+'…'+addr.slice(-4);
  const pnl=prev.pnl_30d||0;const pnlCls=pnl>=0?'pos':'neg';
  const equity=(prev.equity_curve||[]).map(p=>p[1]);
  let svg='';
  if(equity.length>1){let lo=Infinity,hi=-Infinity;for(const v of equity){if(v<lo)lo=v;if(v>hi)hi=v;}
   if(lo===hi){lo-=1;hi+=1;}
   const n=equity.length,W=336,H=50;
   const xF=i=>(W*i/(n-1)).toFixed(1);const yF=v=>(H-((v-lo)/(hi-lo))*H).toFixed(1);
   let p='M '+xF(0)+' '+yF(equity[0]);for(let i=1;i<n;i++)p+=' L '+xF(i)+' '+yF(equity[i]);
   const stk=equity[n-1]>=equity[0]?'#1aeeaa':'#ff3b6e';
   svg=`<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" style="width:100%;height:100%"><path d="${p} L ${W} ${H} L 0 ${H} Z" fill="${stk.replace(')',',.15)').replace('rgb','rgba').replace('#1aeeaa','rgba(26,238,170,.15)').replace('#ff3b6e','rgba(255,59,110,.15)')}"/><path d="${p}" stroke="${stk}" stroke-width="1.6" fill="none" stroke-linecap="round" stroke-linejoin="round" vector-effect="non-scaling-stroke"/></svg>`;
  }
  out.innerHTML=`<div class="tcard" id="tcard_el">
   <div class="tc-header">
    <div class="tc-tier">${tier.lbl}</div>
    <div class="tc-rarity">${tier.ic}</div>
   </div>
   ${smartTag}
   <div class="tc-avatar">${identicon(addr,120)}</div>
   <div class="tc-name">${short}</div>
   <div class="tc-sub">Trader card · ${new Date().toISOString().slice(0,10)}</div>
   <div class="tc-curve">${svg}</div>
   <div class="tc-stats">
    <div class="tc-stat"><span class="tc-stat-lbl">PnL 30d</span><span class="tc-stat-val ${pnlCls}">${pnl>=0?'+':''}${U(pnl)}</span></div>
    <div class="tc-stat"><span class="tc-stat-lbl">Volume</span><span class="tc-stat-val">${U(prev.volume_30d||0)}</span></div>
    <div class="tc-stat"><span class="tc-stat-lbl">Win rate</span><span class="tc-stat-val">${prev.win_rate!=null?prev.win_rate.toFixed(1)+'%':'—'}</span></div>
    <div class="tc-stat"><span class="tc-stat-lbl">Trades</span><span class="tc-stat-val">${prev.trades||0}</span></div>
    <div class="tc-stat"><span class="tc-stat-lbl">Current OI</span><span class="tc-stat-val">${U(prev.current_oi||0)}</span></div>
    <div class="tc-stat"><span class="tc-stat-lbl">Liquidations</span><span class="tc-stat-val ${(prev.n_liquidations||0)?'neg':''}">${prev.n_liquidations||0}</span></div>
   </div>
   <div class="tc-footer"><span>RISEx · trader card</span><span class="tc-url">risexscan.io</span></div>
  </div>`;
 }catch(e){out.innerHTML='<div style="color:var(--red)">Failed to load card</div>';}
}
function closeTcard(){document.getElementById('tcard_modal').classList.remove('open');}
function downloadTcard(){
 const el=document.getElementById('tcard_el');if(!el)return;
 // Convert to canvas via SVG snapshot of element
 const html=`<svg xmlns="http://www.w3.org/2000/svg" width="380" height="520"><foreignObject width="100%" height="100%"><div xmlns="http://www.w3.org/1999/xhtml">${el.outerHTML}</div></foreignObject></svg>`;
 const blob=new Blob([html],{type:'image/svg+xml'});const url=URL.createObjectURL(blob);
 const img=new Image();img.crossOrigin='anonymous';
 img.onload=function(){const c=document.createElement('canvas');c.width=760;c.height=1040;
  const ctx=c.getContext('2d');ctx.drawImage(img,0,0,760,1040);
  c.toBlob(b=>{const u=URL.createObjectURL(b);const a=document.createElement('a');
   a.href=u;a.download='trader-card.png';a.click();setTimeout(()=>URL.revokeObjectURL(u),500);},'image/png');
  URL.revokeObjectURL(url);
 };
 img.onerror=()=>{toast('Browser blocked SVG-to-PNG. Use screenshot for now.','warn',5000);URL.revokeObjectURL(url);};
 img.src=url;
}

// ====== Personal stats + Streak + Anon ID + Welcome back ======
const PS_KEY='rise_personal';
function loadPS(){const d=JSON.parse(localStorage.getItem(PS_KEY)||'{}');return Object.assign({wallets_viewed:[],markets_viewed:[],cmdk_used:0,days:[],first_visit:Date.now(),anon_id:null,achievements:[]},d);}
function savePS(p){localStorage.setItem(PS_KEY,JSON.stringify(p));}
function psPing(){const p=loadPS();const today=new Date().toISOString().slice(0,10);
 if(!p.days.includes(today))p.days.push(today);if(p.days.length>365)p.days=p.days.slice(-365);
 if(!p.anon_id){p.anon_id=Math.floor(Math.random()*99999).toString().padStart(5,'0');}
 savePS(p);return p;}
function psTrack(kind,val){const p=loadPS();const k=kind+'_viewed';
 const arr=p[k]||[];if(!arr.includes(val)){arr.unshift(val);if(arr.length>50)arr.length=50;p[k]=arr;}
 savePS(p);return p;}
function streakDays(p){
 if(!p.days||!p.days.length)return 0;
 const sorted=p.days.slice().sort();const today=new Date();today.setUTCHours(0,0,0,0);
 let streak=0;for(let i=0;i<sorted.length;i++){const d=new Date(sorted[sorted.length-1-i]);
  const diff=Math.round((today-d)/86400000);
  if(diff===i)streak++;else break;}
 return streak;}
function fmtTimeSince(ms){const m=Math.floor((Date.now()-ms)/60000);
 if(m<60)return m+'m ago';if(m<1440)return Math.floor(m/60)+'h ago';return Math.floor(m/1440)+'d ago';}
function renderPersonalFooter(){
 const p=loadPS();const f=document.querySelector('.footer .row:first-child');if(!f)return;
 if(document.getElementById('ps_block'))return;
 const block=document.createElement('span');block.id='ps_block';block.style.cssText='display:inline-flex;gap:10px;align-items:center;margin-left:14px;flex-wrap:wrap;justify-content:center';
 const s=streakDays(p);
 const streakHtml=s>0?`<span class="streak"><span class="streak-fire">🔥</span> ${s} day${s>1?'s':''}</span>`:'';
 block.innerHTML=`${streakHtml}<span class="anon-id" title="Your anonymous ID — saved in your browser only"><span class="anon-av">${identicon('0x'+p.anon_id.padStart(40,'0'),14)}</span>Anon #<b>${p.anon_id}</b></span>
  <span style="font-size:10.5px;color:var(--muted)">Wallets: <b style="color:var(--accent2);font-family:var(--mono)">${(p.wallets_viewed||[]).length}</b> · Markets: <b style="color:var(--accent2);font-family:var(--mono)">${(p.markets_viewed||[]).length}</b> · ⌘K: <b style="color:var(--accent2);font-family:var(--mono)">${p.cmdk_used||0}</b></span>`;
 f.appendChild(block);
}
function welcomeBack(){
 const p=loadPS();const last=p.last_seen||0;
 if(!last){p.last_seen=Date.now();savePS(p);return;}
 const days=Math.floor((Date.now()-last)/86400000);
 if(days>=1){setTimeout(()=>toast(`Welcome back, Anon #${p.anon_id} 👋 · last visit ${fmtTimeSince(last)}`,'ok',6500),3500);}
 p.last_seen=Date.now();savePS(p);
}
psPing();welcomeBack();
// Streak achievements
setTimeout(()=>{const p=loadPS();const s=streakDays(p);
 if(s>=7)unlockAchievement('streak_7');if(s>=30)unlockAchievement('streak_30');},2000);
setInterval(renderPersonalFooter,1500);
// Track ⌘K usage
const _origOpenCmdK=openCmdK;
window.openCmdK=function(){const p=loadPS();p.cmdk_used=(p.cmdk_used||0)+1;savePS(p);
 if(p.cmdk_used===10)unlockAchievement('cmdk_master','⌘K Master','Used the command palette 10 times');
 return _origOpenCmdK();};

// ====== Achievements ======
const ACHIEVEMENTS={
 'first_wallet':{ic:'👛',ttl:'First wallet explored',desc:'You opened your first wallet'},
 'first_smart':{ic:'💎',ttl:'Smart Money spotter',desc:'Discovered a Smart Money wallet'},
 'cmdk_master':{ic:'⌨️',ttl:'⌘K Master',desc:'Used the command palette 10 times'},
 'wallet_10':{ic:'🔍',ttl:'Curious explorer',desc:'Explored 10 different wallets'},
 'wallet_50':{ic:'🧭',ttl:'Wallet hunter',desc:'Explored 50 different wallets'},
 'market_all':{ic:'📊',ttl:'Market analyst',desc:'Visited every active market'},
 'theme_all':{ic:'🎨',ttl:'Theme collector',desc:'Tried all 5 color themes'},
 'streak_7':{ic:'🔥',ttl:'Week streak',desc:'Visited 7 days in a row'},
 'streak_30':{ic:'💪',ttl:'Month streak',desc:'Visited 30 days in a row'},
 'random_whale':{ic:'🐋',ttl:'Whale watcher',desc:'Used the random whale button'},
 'compare':{ic:'⚖️',ttl:'The comparator',desc:'Compared 2+ wallets side by side'},
 'konami':{ic:'🌈',ttl:'Secret rainbow',desc:'Found the Konami code'},
 'tour_done':{ic:'🎓',ttl:'Tour graduate',desc:'Completed the welcome tour'},
};
function unlockAchievement(key,ttl,desc){
 const p=loadPS();if(p.achievements.includes(key))return;
 p.achievements.push(key);savePS(p);
 showAchievement(key,ttl||ACHIEVEMENTS[key]?.ttl,desc||ACHIEVEMENTS[key]?.desc);
 if(window._soundOn)beep(880,180);
 confetti({count:25,colors:['#00ffd4','#1aeeaa','#ffb454']});
}
function showAchievement(key,ttl,desc){
 const ic=ACHIEVEMENTS[key]?.ic||'🏆';
 const el=document.createElement('div');el.className='achievement';
 el.innerHTML=`<div class="ach-ic">${ic}</div><div><div class="ach-lbl">Achievement unlocked</div><div class="ach-ttl">${ttl}</div><div class="ach-desc">${desc||''}</div></div>`;
 document.body.appendChild(el);setTimeout(()=>el.remove(),5200);
}

// ====== Confetti & particle effects ======
function confetti(opts){opts=opts||{};const root=document.getElementById('confetti-root')||(()=>{const r=document.createElement('div');r.id='confetti-root';document.body.appendChild(r);return r;})();
 const colors=opts.colors||['#00ffd4','#1aeeaa','#ffb454','#5dc8ff','#ff8a96'];
 const count=opts.count||40;
 for(let i=0;i<count;i++){const piece=document.createElement('div');piece.className='confetti-piece';
  piece.style.left=(Math.random()*100)+'%';
  piece.style.background=colors[Math.floor(Math.random()*colors.length)];
  piece.style.animationDelay=(Math.random()*.4)+'s';
  piece.style.animationDuration=(2.5+Math.random()*1.5)+'s';
  piece.style.transform=`rotate(${Math.random()*360}deg)`;
  root.appendChild(piece);setTimeout(()=>piece.remove(),3800);}
}
function particleShoot(x,y,color){const root=document.getElementById('confetti-root')||(()=>{const r=document.createElement('div');r.id='confetti-root';document.body.appendChild(r);return r;})();
 for(let i=0;i<6;i++){const p=document.createElement('div');
  p.style.cssText=`position:absolute;width:6px;height:6px;border-radius:50%;background:${color||'#00ffd4'};box-shadow:0 0 8px ${color||'#00ffd4'};left:${x}px;top:${y}px;pointer-events:none`;
  const angle=Math.random()*Math.PI*2;const speed=80+Math.random()*120;
  const dx=Math.cos(angle)*speed,dy=Math.sin(angle)*speed-60;
  p.animate([{transform:'translate(0,0) scale(1)',opacity:1},{transform:`translate(${dx}px,${dy}px) scale(.2)`,opacity:0}],{duration:900,easing:'cubic-bezier(.2,.6,.4,1)'});
  root.appendChild(p);setTimeout(()=>p.remove(),900);}
}

// ====== Hover preview (universal) ======
let _hoverPrev=null,_hoverTimer=null,_hoverCache={};
async function showHoverPreview(target,kind,key){
 hideHoverPreview();
 const rect=target.getBoundingClientRect();
 const el=document.createElement('div');el.className='hover-prev';
 if(kind==='wallet'){
  const short=key.slice(0,6)+'…'+key.slice(-4);
  el.innerHTML=`<div class="hp-head"><div class="hp-avatar">${identicon(key,28)}</div><div class="hp-name">${short}</div></div><div class="hp-loading">Loading stats…</div>`;
 } else if(kind==='market'){
  el.innerHTML=`<div class="hp-head"><div class="hp-name">${key}</div></div><div class="hp-loading">Loading…</div>`;
 }
 document.body.appendChild(el);_hoverPrev=el;
 const top=Math.min(window.innerHeight-260,rect.bottom+8);
 const left=Math.min(window.innerWidth-330,Math.max(8,rect.left));
 el.style.top=top+'px';el.style.left=left+'px';
 requestAnimationFrame(()=>el.classList.add('show'));
 try{
  if(kind==='wallet'){
   const d=_hoverCache['w_'+key]||await (await fetch('/api/wallet-preview?account='+encodeURIComponent(key))).json();
   _hoverCache['w_'+key]=d;if(!_hoverPrev)return;
   const short=key.slice(0,6)+'…'+key.slice(-4);
   const wrCls=(d.pnl_30d||0)>=0?'pos':'neg';
   const wrPos=(d.win_rate||0)>=55?'pos':(d.win_rate||0)<45?'neg':'';
   let sparkSvg='';
   if(d.equity_curve&&d.equity_curve.length>1){
    const vals=d.equity_curve.map(p=>p[1]);
    let lo=Infinity,hi=-Infinity;for(const v of vals){if(v<lo)lo=v;if(v>hi)hi=v;}
    if(lo===hi){lo-=1;hi+=1;}
    const n=vals.length;const W=288,H=32;
    const xF=i=>(W*i/(n-1)).toFixed(1);
    const yF=v=>(H-((v-lo)/(hi-lo))*H).toFixed(1);
    let p='M '+xF(0)+' '+yF(vals[0]);
    for(let i=1;i<n;i++)p+=' L '+xF(i)+' '+yF(vals[i]);
    const stroke=vals[n-1]>=vals[0]?'#1aeeaa':'#ff3b6e';
    const fill=vals[n-1]>=vals[0]?'rgba(26,238,170,.15)':'rgba(255,59,110,.15)';
    sparkSvg=`<svg class="hp-spark" viewBox="0 0 ${W} ${H}" preserveAspectRatio="none"><path d="${p} L ${W} ${H} L 0 ${H} Z" fill="${fill}"/><path d="${p}" stroke="${stroke}" stroke-width="1.5" fill="none" stroke-linecap="round" stroke-linejoin="round" vector-effect="non-scaling-stroke"/></svg>`;
   }
   const smartBadge=d.smart?' <span class="smart-badge" style="font-size:8.5px;padding:1.5px 5px">SMART</span>':'';
   el.innerHTML=`<div class="hp-head"><div class="hp-avatar">${identicon(key,28)}</div><div class="hp-name">${short}${smartBadge}</div></div>
    <div class="hp-row"><span>Realized PnL · 30d</span><b class="${wrCls}">${(d.pnl_30d||0)>=0?'+':''}${U(d.pnl_30d||0)}</b></div>
    <div class="hp-row"><span>Volume · 30d</span><b>${U(d.volume_30d||0)}</b></div>
    <div class="hp-row"><span>Win rate</span><b class="${wrPos}">${d.win_rate!=null?d.win_rate.toFixed(1)+'%':'—'}</b></div>
    <div class="hp-row"><span>Trades · 30d</span><b>${d.trades||0}</b></div>
    ${d.current_oi?`<div class="hp-row"><span>Current OI</span><b>${U(d.current_oi)}</b></div>`:''}
    ${d.n_liquidations?`<div class="hp-row"><span>Liquidations</span><b class="neg">${d.n_liquidations}</b></div>`:''}
    ${sparkSvg}`;
  } else if(kind==='market'){
   const m=(DATA&&DATA.markets||[]).find(x=>String(x.market_id)===String(key)||x.name===key);
   if(!m||!_hoverPrev)return;
   const sparks=(window._SPARKS||{}).by_market||{};const closes=sparks[m.market_id];
   const mpct=chgPct(m);const chgCls=mpct>=0?'pos':'neg';
   const fc=(m.funding_8h||0)>=0?'pos':'neg';const apr=m.funding_apr;
   let sparkSvg='';
   if(closes&&closes.length>1){
    let lo=Infinity,hi=-Infinity;for(const v of closes){if(v<lo)lo=v;if(v>hi)hi=v;}
    if(lo===hi){lo-=1;hi+=1;}
    const n=closes.length;const W=288,H=32;
    const xF=i=>(W*i/(n-1)).toFixed(1);
    const yF=v=>(H-((v-lo)/(hi-lo))*H).toFixed(1);
    let p='M '+xF(0)+' '+yF(closes[0]);
    for(let i=1;i<n;i++)p+=' L '+xF(i)+' '+yF(closes[i]);
    const stroke=closes[n-1]>=closes[0]?'#1aeeaa':'#ff3b6e';
    sparkSvg=`<svg class="hp-spark" viewBox="0 0 ${W} ${H}" preserveAspectRatio="none"><path d="${p}" stroke="${stroke}" stroke-width="1.5" fill="none" stroke-linecap="round" stroke-linejoin="round" vector-effect="non-scaling-stroke"/></svg>`;
   }
   el.innerHTML=`<div class="hp-head"><div class="hp-name">${m.name}</div></div>
    <div class="hp-row"><span>Last price</span><b>${P(m.last_price)}</b></div>
    <div class="hp-row"><span>24h change</span><b class="${chgCls}">${mpct>=0?'+':''}${mpct.toFixed(2)}%</b></div>
    <div class="hp-row"><span>24h volume</span><b>${U(m.volume_24h)}</b></div>
    <div class="hp-row"><span>Open interest</span><b>${U(m.oi_usd)}</b></div>
    <div class="hp-row"><span>Funding APR</span><b class="${fc}">${apr>=0?'+':''}${apr.toFixed(1)}%</b></div>
    ${sparkSvg}`;
  }
 }catch(e){}
}
function hideHoverPreview(){if(_hoverPrev){_hoverPrev.remove();_hoverPrev=null;}clearTimeout(_hoverTimer);}
document.addEventListener('mouseover',e=>{
 const w=e.target.closest('a[href^="#wallet="]');
 const m=e.target.closest('a[href^="#market="]');
 if(!w&&!m){return;}
 clearTimeout(_hoverTimer);
 _hoverTimer=setTimeout(()=>{
  if(w){const addr=w.getAttribute('href').replace('#wallet=','');showHoverPreview(w,'wallet',addr);}
  else if(m){const mid=m.getAttribute('href').replace('#market=','');showHoverPreview(m,'market',mid);}
 },420);
});
document.addEventListener('mouseout',e=>{
 const w=e.target.closest('a[href^="#wallet="]');
 const m=e.target.closest('a[href^="#market="]');
 if(w||m)hideHoverPreview();
});
document.addEventListener('scroll',hideHoverPreview,{passive:true});

// ====== Today on RISEx daily story banner ======
let _dailyIdx=0,_dailyData=null;
async function loadDailyStory(){
 try{const d=await (await fetch('/api/daily-story')).json();
  if(!d.ok||!d.stories||!d.stories.length)return;
  _dailyData=d.stories;_dailyIdx=0;renderDailyStory();
 }catch(e){}
}
function renderDailyStory(){
 const el=document.getElementById('dailystory');if(!el||!_dailyData)return;
 const s=_dailyData[_dailyIdx%_dailyData.length];
 const txt=el.querySelector('.ds-text');const ic=el.querySelector('.ds-icon');
 if(txt&&ic){txt.innerHTML=s.html;ic.textContent=s.icon;}
}
function cycleDailyStory(){if(!_dailyData)return;_dailyIdx++;renderDailyStory();}
setInterval(()=>{if(_dailyData)cycleDailyStory();},9000);

// ====== Random whale ======
async function randomWhale(){
 try{const d=await (await fetch('/api/random-whale')).json();
  if(d.ok){unlockAchievement('random_whale');location.hash='wallet='+d.account;toast('🐋 Random whale — '+U(d.total_oi)+' OI','ok');}
 }catch(e){}
}

// ====== Suggested wallets at end of wallet view ======
async function loadSuggestedWallets(account){
 const wrap=document.getElementById('walletOut');if(!wrap)return;
 try{const d=await (await fetch('/api/suggested-wallets?account='+encodeURIComponent(account))).json();
  if(!d.ok||!d.suggestions||!d.suggestions.length)return;
  const sec=document.createElement('div');sec.className='suggested';
  sec.innerHTML=`<h3>Similar traders</h3><div class="suggested-grid">${d.suggestions.map(s=>{
   const cls=s.pnl_30d>=0?'pos':'neg';
   return `<a class="sug-card" href="#wallet=${s.account}">
    <div class="sc-avatar">${identicon(s.account,30)}</div>
    <div><div class="sc-name">${shortAddr(s.account)}</div>
     <div class="sc-meta">${U(s.volume)} vol · <span class="${cls}">${s.pnl_30d>=0?'+':''}${U(s.pnl_30d)}</span></div></div></a>`;
  }).join('')}</div>`;
  wrap.appendChild(sec);
 }catch(e){}
}
async function loadRelatedMarkets(mid){
 const out=document.getElementById('md_content');if(!out)return;
 try{const d=await (await fetch('/api/related-markets?market_id='+encodeURIComponent(mid))).json();
  if(!d.ok||!d.related||!d.related.length)return;
  const sec=document.createElement('div');sec.className='suggested';
  sec.innerHTML=`<h3>Related markets</h3><div class="suggested-grid">${d.related.map(r=>{
   const rpct=chgPct(r);const cls=rpct>=0?'pos':'neg';
   return `<a class="sug-card" href="#market=${r.market_id}">
    <div class="sc-avatar" style="background:linear-gradient(135deg,${rpct>=0?'var(--green)':'var(--red)'},transparent);display:flex;align-items:center;justify-content:center;color:#06090c;font-weight:800;font-family:-apple-system,Inter,sans-serif;font-size:11px">${(r.name||'').slice(0,3)}</div>
    <div><div class="sc-name">${r.name}</div>
     <div class="sc-meta">${P(r.last_price)} · <span class="${cls}">${rpct>=0?'+':''}${rpct.toFixed(2)}%</span></div></div></a>`;
  }).join('')}</div>`;
  out.appendChild(sec);
 }catch(e){}
}

// ====== Trending widget ======
async function loadTrending(){
 try{const d=await (await fetch('/api/trending?kind=wallet')).json();
  const root=document.getElementById('trending_panel');if(!root)return;
  if(!d.items||!d.items.length){root.style.display='none';return;}
  root.style.display='';
  const chips=d.items.slice(0,8).map(i=>`<a href="#wallet=${i.key}" class="tr-chip"><span class="tr-id">${identicon(i.key,14)}</span>${shortAddr(i.key)} <span class="tr-num">${i.views}×</span></a>`).join('');
  root.innerHTML=`<div class="tr-head">Trending wallets · last hour</div><div class="tr-list">${chips}</div>`;
 }catch(e){}
}
setInterval(loadTrending,45000);
// "Viewing now" on wallet/market detail
async function attachViewingNow(kind,key,targetSel){
 try{const d=await (await fetch('/api/trending?kind='+kind)).json();
  // get viewing-now via a dedicated mini-endpoint? we'll piggyback on trending: count active viewers
  const n=d.active_viewers||0;
  const el=document.querySelector(targetSel);
  if(el&&n>1){const v=document.createElement('span');v.className='viewing-now';v.textContent=n+' viewing now';el.appendChild(v);}
 }catch(e){}
}

// ====== 3D tilt ======
function attachTilt(){
 document.querySelectorAll('.card:not([data-tilt]), .hero-card:not([data-tilt]), .poscard:not([data-tilt]), .wif:not([data-tilt])').forEach(c=>{
  c.dataset.tilt='1';c.classList.add('tiltable');
  c.addEventListener('mousemove',e=>{
   const r=c.getBoundingClientRect();
   const x=(e.clientX-r.left)/r.width-.5;
   const y=(e.clientY-r.top)/r.height-.5;
   c.style.transform=`perspective(800px) rotateX(${-y*4}deg) rotateY(${x*4}deg) translateZ(0)`;
  });
  c.addEventListener('mouseleave',()=>{c.style.transform='';});
 });
}
setInterval(attachTilt,2000);

// ====== Welcome tour (first visit) ======
const TOUR=[
 {title:'Welcome to RISExscan',body:'The real-time analytics dashboard for RISE chain perps. Built on public data. No accounts, no tracking. Let me show you around.'},
 {title:'Press ⌘K anywhere',body:'Quickly jump to any wallet, market or view. Try it now — Cmd+K (or Ctrl+K on Windows). Paste an address, type a market name, or just navigate.'},
 {title:'Live data, everywhere',body:'Numbers tick in real time. Watch the dot in the topbar — it pulses on every refresh. Big trades, liquidations, funding payments — all live.'},
 {title:'5 color themes',body:'Click the 🎨 in the topbar. Mint (default), Magenta, Solar, Mono, Light. Use ⌘1-5. Each theme rotates the accent palette but keeps PnL green/red.'},
 {title:'Tools & calculators',body:'Position simulator, funding cost calculator, liquidation price grid. Click 🧰 Tools in the sidebar to access them. Right-click any wallet for quick actions.'},
];
let _tourStep=0;
function openTour(){_tourStep=0;document.getElementById('tour_back').classList.add('open');renderTour();}
function closeTour(){document.getElementById('tour_back').classList.remove('open');localStorage.setItem('rise_tour_seen','1');}
function renderTour(){
 const s=TOUR[_tourStep];if(!s)return closeTour();
 document.getElementById('tour_step').textContent=`Step ${_tourStep+1} of ${TOUR.length}`;
 document.getElementById('tour_title').textContent=s.title;
 document.getElementById('tour_body').textContent=s.body;
 document.getElementById('tour_prev').style.display=_tourStep===0?'none':'';
 document.getElementById('tour_next').textContent=_tourStep===TOUR.length-1?'Got it ✓':'Next →';
 document.getElementById('tour_dots').innerHTML=TOUR.map((_,i)=>`<div class="dot ${i===_tourStep?'active':''}"></div>`).join('');
}
function tourNext(){_tourStep++;if(_tourStep>=TOUR.length){closeTour();toast('Welcome aboard 🚀','ok');unlockAchievement('tour_done');return;}renderTour();}
function tourPrev(){if(_tourStep>0){_tourStep--;renderTour();}}
// Trigger tour on first visit (after splash)
window.addEventListener('load',()=>{
 if(!localStorage.getItem('rise_tour_seen'))setTimeout(openTour,3000);
});

// ====== What's new (auto-shown once when version bumps) ======
const APP_VERSION='5.3-polish';
const WHATSNEW={ttl:'New: ⌘K, themes, calendar & more',desc:'Command palette, 5 themes, calendar heatmap, position cards, treemap, count-up animations, toasts, identicons. Press ⌘K to explore.'};
function showWhatsNew(){
 const seen=localStorage.getItem('rise_seen_'+APP_VERSION);if(seen)return;
 const div=document.createElement('div');div.className='whatsnew';
 div.innerHTML=`<span class="ic">✨</span><div><div class="ttl">${WHATSNEW.ttl}</div><div class="desc">${WHATSNEW.desc}</div></div><span class="x">×</span>`;
 div.querySelector('.x').onclick=()=>{div.remove();localStorage.setItem('rise_seen_'+APP_VERSION,'1');};
 document.body.appendChild(div);
}
window.addEventListener('load',()=>setTimeout(showWhatsNew,4200));

// ====== Konami code easter egg ======
const KONAMI=['ArrowUp','ArrowUp','ArrowDown','ArrowDown','ArrowLeft','ArrowRight','ArrowLeft','ArrowRight','b','a'];
let _kbuf=[];
document.addEventListener('keydown',e=>{
 _kbuf.push(e.key);if(_kbuf.length>KONAMI.length)_kbuf.shift();
 if(_kbuf.join(',')===KONAMI.join(',')){
  toast('🌈 Rainbow mode activated','ok',4000);
  unlockAchievement('konami');confetti({count:80});
  document.documentElement.style.animation='hueRainbow 6s linear infinite';
  if(window._soundOn){[440,523,659,784,1047].forEach((f,i)=>setTimeout(()=>beep(f,120),i*120));}
  setTimeout(()=>{document.documentElement.style.animation='';},20000);
  _kbuf=[];
 }
});
const style=document.createElement('style');
style.textContent='@keyframes hueRainbow{0%{filter:hue-rotate(0deg)}100%{filter:hue-rotate(360deg)}}';
document.head.appendChild(style);

// ====== Density modes ======
function setDensity(d){
 if(d==='cozy')document.documentElement.removeAttribute('data-density');
 else document.documentElement.setAttribute('data-density',d);
 localStorage.setItem('rise_density',d);
 toast('Density: '+d,'ok',2000);
}
(function initDensity(){const d=localStorage.getItem('rise_density');if(d&&d!=='cozy')document.documentElement.setAttribute('data-density',d);})();

// ====== Pinned markets ======
function getPins(){return JSON.parse(localStorage.getItem('rise_pins_markets')||'[]');}
function setPins(p){localStorage.setItem('rise_pins_markets',JSON.stringify(p));}
function togglePin(mid){
 mid=String(mid);const p=getPins();const i=p.indexOf(mid);
 if(i>=0)p.splice(i,1);else p.unshift(mid);
 setPins(p);renderMarkets();toast(i>=0?'Unpinned':'Pinned ⭐','ok',1800);
}

// Footer uptime ticker
const _pageT0=Date.now();
setInterval(()=>{
 const el=document.getElementById('footer_uptime');if(!el)return;
 const s=Math.floor((Date.now()-_pageT0)/1000);
 const h=Math.floor(s/3600),m=Math.floor((s%3600)/60),ss=s%60;
 el.textContent='uptime '+(h?h+'h ':'')+(m?m+'m ':'')+ss+'s';
},1000);

// Backward-compat for legacy toggleTheme calls
function toggleTheme(){
 const cur=localStorage.getItem('rise_color_theme')||'mint';
 const i=THEMES.indexOf(cur);
 setColorTheme(THEMES[(i+1)%THEMES.length]);
}

// ====== Cursor-glow on hero ======
document.addEventListener('mousemove',e=>{
 const hero=document.querySelector('.hero');if(!hero)return;
 const r=hero.getBoundingClientRect();
 if(e.clientX<r.left||e.clientX>r.right||e.clientY<r.top||e.clientY>r.bottom)return;
 hero.style.setProperty('--mx',(e.clientX-r.left)+'px');
 hero.style.setProperty('--my',(e.clientY-r.top)+'px');
});

// ====== Live update helpers: setVal with flash + sparkline renderer ======
const _prevVals={};
function setVal(id,newNum,fmt){
 const el=document.getElementById(id);if(!el)return;
 const fn=fmt||(x=>x);
 const formatted=fn(newNum);
 if(el.classList.contains('skel'))el.classList.remove('skel');
 if(el.innerHTML==='—'||(el.children.length&&el.querySelector('.skel'))){el.innerHTML=formatted;_prevVals[id]=newNum;return;}
 const prev=_prevVals[id];
 if(prev!=null&&typeof newNum==='number'&&newNum!==prev){
  const up=newNum>prev;
  el.classList.remove('flash-up','flash-dn');void el.offsetWidth;
  el.classList.add(up?'flash-up':'flash-dn');
  setTimeout(()=>el.classList.remove('flash-up','flash-dn'),600);
 }
 el.innerHTML=formatted;
 _prevVals[id]=newNum;
}
function spark(svgId,values,opts){
 const svg=document.getElementById(svgId);if(!svg||!values||!values.length)return;
 const W=320,H=60,pad=2;
 let lo=Infinity,hi=-Infinity;for(const v of values){if(v<lo)lo=v;if(v>hi)hi=v;}
 if(lo===hi){lo-=1;hi+=1;}
 const o=opts||{};
 const col=o.color||'var(--accent)';
 const fill=o.fill||'rgba(0,255,212,.18)';
 const n=values.length;
 const x=i=>pad+(W-pad*2)*i/(n-1||1);
 const y=v=>pad+(H-pad*2)*(1-(v-lo)/(hi-lo));
 let d='M '+x(0)+' '+y(values[0]);
 for(let i=1;i<n;i++)d+=' L '+x(i)+' '+y(values[i]);
 const dFill=d+' L '+x(n-1)+' '+H+' L '+x(0)+' '+H+' Z';
 svg.innerHTML=`
  <defs><linearGradient id="${svgId}_g" x1="0" x2="0" y1="0" y2="1">
   <stop offset="0%" stop-color="${o.fillTop||'rgba(0,255,212,.35)'}"/>
   <stop offset="100%" stop-color="rgba(0,255,212,0)"/>
  </linearGradient></defs>
  <path d="${dFill}" fill="url(#${svgId}_g)"/>
  <path d="${d}" stroke="${col}" stroke-width="1.6" fill="none" stroke-linejoin="round" stroke-linecap="round" vector-effect="non-scaling-stroke"/>
  <circle cx="${x(n-1)}" cy="${y(values[n-1])}" r="3" fill="${col}"><animate attributeName="r" values="3;5;3" dur="2s" repeatCount="indefinite"/></circle>`;
}
async function fetchHero(){
 try{
  const h=await (await fetch('/api/history',{cache:'no-store'})).json();
  if(h&&h.points)renderHeroSparks(h.points);
 }catch(e){}
}
function renderHeroSparks(points){
 const pts=(points||[]).slice(-48);
 if(!pts.length)return;
 spark('hv_vol_spark',pts.map(p=>p.vol||0));
 spark('hv_oi_spark',pts.map(p=>p.oi||0));
 // % change from first to last
 const first=pts[0],last=pts[pts.length-1];
 if(first&&last){
  const dv=first.vol?((last.vol-first.vol)/first.vol*100):0;
  const di=first.oi?((last.oi-first.oi)/first.oi*100):0;
  const dvCls=dv>=0?'pos':'neg',diCls=di>=0?'pos':'neg';
  document.getElementById('hv_vol_m').innerHTML=
   `<span class="${dvCls}">${dv>=0?'+':''}${dv.toFixed(1)}%</span> from ${pts.length}h ago · <span class="livedot" style="margin-left:6px"></span> live`;
  document.getElementById('hv_oi_m').innerHTML=
   `<span class="${diCls}">${di>=0?'+':''}${di.toFixed(1)}%</span> from ${pts.length}h ago · <span class="livedot" style="margin-left:6px"></span> live`;
 }
}

// ====== Generic sortable tables + CSV export ======
function _parseCell(s){
 s=(s||'').replace(/[−]/g,'-').trim();
 if(s===''||s==='—')return -Infinity;
 const cleaned=s.replace(/[$,%\s]/g,'').replace(/B$/,'e9').replace(/M$/,'e6').replace(/K$/i,'e3').replace(/bps$/,'');
 const n=parseFloat(cleaned);
 return isFinite(n)?n:s.toLowerCase();
}
function makeSortable(tbl){
 if(!tbl||tbl.dataset.sorted==='1')return;
 const ths=tbl.querySelectorAll('thead th');if(!ths.length){return;}
 tbl.dataset.sorted='1';
 ths.forEach((th,i)=>{
  th.style.cursor='pointer';th.style.userSelect='none';
  th.addEventListener('click',()=>{
   const tbody=tbl.querySelector('tbody');if(!tbody)return;
   const dir=th.dataset.dir==='asc'?'desc':'asc';
   ths.forEach(t=>{t.dataset.dir='';
    const ind=t.querySelector('.sortInd');if(ind)ind.remove();});
   th.dataset.dir=dir;
   const ind=document.createElement('span');ind.className='sortInd';ind.style.cssText='margin-left:4px;color:var(--accent2);font-size:9px';
   ind.textContent=dir==='asc'?'▲':'▼';th.appendChild(ind);
   const rows=Array.from(tbody.querySelectorAll('tr')).filter(r=>!r.querySelector('.empty'));
   rows.sort((a,b)=>{
    const av=_parseCell(a.children[i]?.textContent);
    const bv=_parseCell(b.children[i]?.textContent);
    if(typeof av==='number'&&typeof bv==='number')return dir==='asc'?av-bv:bv-av;
    return dir==='asc'?String(av).localeCompare(String(bv)):String(bv).localeCompare(String(av));
   });
   rows.forEach(r=>tbody.appendChild(r));
  });
 });
}
function tableToCsv(tbl){
 return Array.from(tbl.querySelectorAll('tr')).map(tr=>
  Array.from(tr.querySelectorAll('th,td')).map(c=>{
   let t=(c.textContent||'').replace(/[−]/g,'-').replace(/\s+/g,' ').trim();
   if(t.includes(',')||t.includes('"')||t.includes('\n'))t='"'+t.replace(/"/g,'""')+'"';
   return t;
  }).join(',')).join('\n');
}
function downloadCsv(tbl,filename){
 const csv=tableToCsv(tbl);
 const blob=new Blob([csv],{type:'text/csv;charset=utf-8'});
 const url=URL.createObjectURL(blob);
 const a=document.createElement('a');a.href=url;a.download=filename;document.body.appendChild(a);a.click();
 setTimeout(()=>{URL.revokeObjectURL(url);a.remove();},500);
}
function addCsvBtn(panelEl,filename){
 if(!panelEl||panelEl.dataset.csvBtn==='1')return;
 const h2=panelEl.querySelector('h2');const tbl=panelEl.querySelector('table');
 if(!h2||!tbl)return;
 panelEl.dataset.csvBtn='1';
 const btn=document.createElement('span');
 btn.className='chip';
 btn.style.cssText='cursor:pointer;font-size:10.5px;margin-left:10px;vertical-align:middle';
 btn.innerHTML='↓ CSV';
 btn.title='Download visible rows as CSV';
 btn.onclick=(e)=>{e.stopPropagation();downloadCsv(tbl,filename+'.csv');};
 h2.appendChild(btn);
}
function enhanceAllTables(){
 document.querySelectorAll('.view.active table').forEach(makeSortable);
 // Wire CSV buttons on known panels (idempotent)
 const map=[
  ['#v_overview .panel:has(#tbody)','risex_markets'],
  ['#v_markets .panel','risex_markets'],
  ['#v_overview .panel:has(#bt_body)','risex_whale_trades'],
  ['#v_acctoi .panel','risex_account_oi'],
  ['#v_volranking .panel','risex_volume_ranking'],
  ['#v_oiranking .panel','risex_oi_ranking'],
  ['#v_pnl .panel','risex_pnl_ranking'],
  ['#v_funded .panel','risex_funding_payments'],
  ['#v_liq .panel','risex_liquidations'],
  ['#v_feed .panel','risex_live_activity'],
  ['#v_funding .panel','risex_funding_compare'],
  ['#v_longshort .panel','risex_longshort'],
  ['#v_marketshare .panel','risex_market_share'],
 ];
 for(const [sel,fn] of map){const el=document.querySelector(sel);if(el)addCsvBtn(el,fn);}
}
// run enhance after every nav click + on hash route + periodically
setInterval(enhanceAllTables,1500);

// ====== Auto-inject metric tooltips ======
const TIPS={
 '24h volume':'Sum of trade notional (price × size) across all markets in the last 24 hours.',
 'open interest':'Sum of notional of all open positions right now. Long OI = short OI in a perp DEX.',
 'tvl':'Total Value Locked: USDC deposited in the CollateralManager backing all positions and balances.',
 'oi / tvl ratio':'Open Interest divided by TVL. >2x means high protocol leverage; <1x is conservative.',
 '24h fees':'Sum of taker and maker fees paid in the last 24h, read directly from PerpsManager events onchain.',
 'realized volume':'Sum of trade notional for the wallet in the period, reconstructed from trade-history.',
 'win rate':'Percentage of trades closed with positive realized PnL. Ignores zero-PnL fills.',
 'profit factor':'Sum of winning PnL divided by absolute sum of losing PnL. >1.5 is considered strong, >2 is excellent.',
 'max drawdown':'Largest peak-to-trough decline in cumulative realized PnL during the period.',
 'avg trade size':'Mean notional per trade in the last 30 days.',
 'edge':'PnL ÷ Volume × 10000, in basis points. A trader\'s average per-dollar margin. Pros sustain 5–20 bps; degens swing wildly.',
 'funding apr':'Annualized funding rate: 8h rate × 3 × 365. Long pays when positive; short pays when negative.',
 'funding 8h':'Funding rate charged every 8 hours. Long pays short if positive, vice versa.',
 'mark / index':'Mark price (used for liq/PnL) vs Index price (oracle reference). Basis = (mark−index)/index.',
 'liq price':'Price at which the position would be liquidated. For Cross positions, depends on total account state.',
 'dist. to liq.':'Percentage from current mark price to liquidation price. Color: green safe, amber close, red critical.',
 'unrealized pnl':'PnL from open positions at current mark price (not yet realized).',
 'realized pnl':'PnL captured at trade close, summed across the period.',
 'balance (collateral)':'USDC available in the cross-margin account from the CollateralManager.',
 'equity':'Balance + Unrealized PnL. Account value if you closed everything at mark.',
 'cvd':'Cumulative Volume Delta: running sum of taker-buy notional minus taker-sell notional. Divergence with price hints at hidden pressure.',
 'pnl factor':'Sum of winning PnL ÷ absolute sum of losing PnL.',
 'twap oi':'Time-Weighted Average Open Interest: integral of OI over the window divided by duration.',
};
function enhanceTooltips(){
 document.querySelectorAll('.lbl, .panel h2, .sectitle, table thead th').forEach(el=>{
  if(el.dataset.tipped==='1')return;
  const txt=(el.textContent||'').toLowerCase().trim();
  for(const key in TIPS){
   if(txt.includes(key)){
    el.dataset.tipped='1';
    const s=document.createElement('span');
    s.className='info';s.setAttribute('data-tip',TIPS[key]);
    el.appendChild(s);
    break;
   }
  }
 });
}
setInterval(enhanceTooltips,2000);
// initial skeleton placeholders on first paint of overview cards
function injectSkeletons(){
 ['c_vol','c_oi','c_tvl','c_ratio','c_fee'].forEach(id=>{
  const el=document.getElementById(id);
  if(el && el.textContent==='—'){el.innerHTML='<span class="skel" style="min-width:120px"></span>';}
 });
}
injectSkeletons();

document.querySelectorAll('.navitem').forEach(t=>t.onclick=()=>{
 document.querySelectorAll('.navitem').forEach(x=>x.classList.remove('active'));
 document.querySelectorAll('.view').forEach(x=>x.classList.remove('active'));
 t.classList.add('active');document.getElementById('v_'+t.dataset.v).classList.add('active');
 // cerrar sidebar en movil tras seleccionar
 const sb=document.getElementById('sidebar');if(sb)sb.classList.remove('open');
 // limpiar hash si estamos saliendo de wallet/market detail
 if(t.dataset.v!=='wallet'&&t.dataset.v!=='marketdetail'&&(location.hash.includes('wallet=')||location.hash.includes('market=')))history.replaceState(null,'',location.pathname);
 if(t.dataset.v==='ranking')loadRanking();
 if(t.dataset.v==='markets')loadHistory();
 if(t.dataset.v==='acctoi')loadAcctOi();
 if(t.dataset.v==='users')loadUsers();
 if(t.dataset.v==='volranking')loadVolRanking();
 if(t.dataset.v==='oiranking')loadOiRanking();
 if(t.dataset.v==='funding')loadFunding();
 if(t.dataset.v==='pnl')loadPnl();
 if(t.dataset.v==='funded')loadFunded();
 if(t.dataset.v==='liq')loadLiq();
 if(t.dataset.v==='feed')loadFeed();
 if(t.dataset.v==='longshort')loadLongShort();
 if(t.dataset.v==='heatmap')loadHeatmap();
 if(t.dataset.v==='marketshare')loadMarketShare();
 if(t.dataset.v==='watchlist')loadWatchlist();
 if(t.dataset.v==='tools')initTools();
 if(t.dataset.v==='markets')setTimeout(renderMarketsTreemap,80);
 if(t.dataset.v==='compare')renderCompare();
 if(t.dataset.v==='explorer')startExplorer();
});

// ======== Light/Dark theme toggle ========
function applyTheme(t){setColorTheme(t==='light'?'light':'mint');}

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
  // Compute global skew
  const totLong=d.markets.reduce((s,r)=>s+r.long_oi,0);
  const totShort=d.markets.reduce((s,r)=>s+r.short_oi,0);
  const totalOI=totLong+totShort;
  const globLong=totalOI?(totLong/totalOI*100):50;
  const globSkew=globLong-50;
  const skewCls=globSkew>=0?'pos':'neg';
  // Hero with global skew
  const banner=heroBanner({icon:'⚖️',label:'PROTOCOL-WIDE SKEW',value:`${globLong.toFixed(1)}% LONG`,
   meta:`<span class="pos">${U(totLong)} long</span> vs <span class="neg">${U(totShort)} short</span> · ${d.markets.length} markets`,
   side:`<div>Net bias<br><b class="${skewCls}">${globSkew>=0?'+':''}${globSkew.toFixed(2)}%</b></div>`});
  // Top 3 most skewed markets
  const sorted=d.markets.slice().sort((a,b)=>Math.abs(b.skew)-Math.abs(a.skew));
  const top3=sorted.slice(0,3);
  let pod='';
  if(top3.length>=3){
   const order=[1,0,2];const cls={0:'p1',1:'p2',2:'p3'};const med={0:'🥇',1:'🥈',2:'🥉'};
   pod=`<div class="podium">${order.map(i=>{const it=top3[i];if(!it)return '<div></div>';
    const sk=it.skew>=0?'pos':'neg';const mid=(DATA&&DATA.markets||[]).find(x=>x.name===it.market)?.market_id||'';
    return `<a class="ppos ${cls[i]}" href="#market=${mid}">
     <div class="pmedal">${med[i]}</div>
     <div class="pavatar" style="background:linear-gradient(135deg,${it.skew>=0?'var(--green)':'var(--red)'},transparent);display:flex;align-items:center;justify-content:center;font-weight:800;color:#06090c;font-family:-apple-system,Inter,sans-serif;font-size:13px">${it.market.slice(0,3)}</div>
     <div class="paddr">${it.market}</div>
     <div class="plbl">MOST SKEWED</div>
     <div class="pval ${sk}">${it.skew>=0?'+':''}${it.skew.toFixed(1)}%</div>
     <div class="psub">${it.long_pct.toFixed(1)}% L · ${it.short_pct.toFixed(1)}% S</div>
    </a>`;}).join('')}</div>`;
  }
  const tbl0=document.getElementById('ls_body').closest('table');
  ensureHero('ls',tbl0).innerHTML=banner+pod;
  const tb=document.getElementById('ls_body');
  tb.innerHTML=d.markets.map(r=>{const sk=r.skew>=0?'pos':'neg';
   const lp=r.long_pct,sp=r.short_pct;
   const mid=(DATA&&DATA.markets||[]).find(x=>x.name===r.market)?.market_id||'';
   const lsbar=`<div class="lsbar" title="${lp.toFixed(1)}% long / ${sp.toFixed(1)}% short">
    <div class="ls-long" style="flex:${lp.toFixed(2)}">${lp>10?lp.toFixed(0)+'%':''}</div>
    <div class="ls-short" style="flex:${sp.toFixed(2)}">${sp>10?sp.toFixed(0)+'%':''}</div></div>`;
   return `<tr><td class="mkt"><a href="#market=${mid}">${r.market}</a></td>
    <td class="pos">${U(r.long_oi)}</td><td class="neg">${U(r.short_oi)}</td>
    <td>${lsbar}</td>
    <td class="${sk}"><span class="deltarr ${sk}">${r.skew>=0?'▲':'▼'} ${Math.abs(r.skew).toFixed(2)}%</span></td>
    <td>${r.n_long}</td><td>${r.n_short}</td></tr>`;}).join('')||'<tr><td class="empty" colspan=7>No data yet.</td></tr>';
  const tbl=document.getElementById('ls_body').closest('table');if(tbl){if(!tbl.id)tbl.id='ls_tbl';stagger(tbl.id);}
  // chart: long % over time for top 5 markets by OI
  const top=d.markets.slice(0,5).map(m=>m.market);
  const colors=['#97FCE4','#36d39c','#ffb454','#5dc8ff','#ff8a96'];
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
 setVal('c_vol',t.volume_24h,U);
 if(!_prevVals.hv_vol)countUp('hv_vol',t.volume_24h,U,750);else setVal('hv_vol',t.volume_24h,U);
 document.getElementById('hv_vol_m').innerHTML=(t.num_markets||0)+' active markets · live';
 setVal('c_oi',t.open_interest_usd,U);
 // Hero sparklines from auto-recorded history
 if(d.history&&d.history.points){renderHeroSparks(d.history.points);} else {fetchHero();}
 if(!_prevVals.hv_oi)countUp('hv_oi',t.open_interest_usd,U,800);else setVal('hv_oi',t.open_interest_usd,U);
 setVal('sb_vol',t.volume_24h,U);
 setVal('sb_oi',t.open_interest_usd,U);
 renderTicker();renderWhatsInteresting();
 const ratio=t.oi_tvl_ratio!=null?t.oi_tvl_ratio.toFixed(2)+'x':'—';
 document.getElementById('hv_oi_m').innerHTML='OI/TVL ratio '+ratio+' · live';
 document.getElementById('c_vol_m').textContent=(t.num_markets||0)+' markets';
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
 lineChart('tvlChart',(d.tvl.series||[]).map(p=>new Date(p.date).toLocaleDateString('es-ES',{month:'short',day:'numeric'})),(d.tvl.series||[]).map(p=>p.tvl),'rgb(80,221,194)');
 const mv=[...(d.markets||[])].sort((a,b)=>b.volume_24h-a.volume_24h);
 barChart('volChart',mv.map(m=>m.name),mv.map(m=>m.volume_24h));
 const maxFund=Math.max(0,...(d.markets||[]).map(m=>Math.abs(m.funding_8h*100)));
 if(alerts.fund&&maxFund>+alerts.fund)notify('High funding: '+maxFund.toFixed(3)+'%');
}
function toggleAlert(id,on,msg){const el=document.getElementById(id);if(!el)return;
 el.classList.toggle('alert',!!on);if(on)notify(msg);}

// Find max abs funding across all markets, used to scale bipolar bar lengths
function _maxAbsFunding(ms){let mx=0;for(const m of ms){const v=Math.abs((m.funding_8h||0)*100);if(v>mx)mx=v;}return mx||1e-6;}
function _rowSparkSvg(closes,vid){
 if(!closes||closes.length<2)return '<span class="note" style="font-size:10px">—</span>';
 const W=80,H=30,pad=1;
 let lo=Infinity,hi=-Infinity;for(const v of closes){if(v<lo)lo=v;if(v>hi)hi=v;}
 if(lo===hi){lo-=1;hi+=1;}
 const n=closes.length;
 const x=i=>pad+(W-pad*2)*i/(n-1||1);
 const y=v=>pad+(H-pad*2)*(1-(v-lo)/(hi-lo));
 let d='M '+x(0).toFixed(1)+' '+y(closes[0]).toFixed(1);
 for(let i=1;i<n;i++)d+=' L '+x(i).toFixed(1)+' '+y(closes[i]).toFixed(1);
 const up=closes[n-1]>=closes[0];
 const stroke=up?'var(--green)':'var(--red)';
 const fg=up?'rgba(26,238,170,.16)':'rgba(255,59,110,.16)';
 const fillD=d+' L '+x(n-1).toFixed(1)+' '+H+' L '+x(0).toFixed(1)+' '+H+' Z';
 return `<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none"><path d="${fillD}" fill="${fg}"/><path d="${d}" fill="none" stroke="${stroke}" stroke-width="1.5" stroke-linejoin="round" stroke-linecap="round" vector-effect="non-scaling-stroke"/></svg>`;
}
function renderMarkets(){
 const m=[...(DATA.markets||[])];m.sort((a,b)=>{let x=a[sortK],y=b[sortK];
  if(typeof x==='string')return sortDir*x.localeCompare(y);return sortDir*((x||0)-(y||0));});
 // pinned at top
 const pins=getPins();
 m.sort((a,b)=>{const ap=pins.indexOf(String(a.market_id)),bp=pins.indexOf(String(b.market_id));
  if(ap>=0&&bp>=0)return ap-bp;if(ap>=0)return -1;if(bp>=0)return 1;return 0;});
 const mx=Math.max(...m.map(r=>r.volume_24h||0),1);
 const maxF=_maxAbsFunding(m);
 const sparks=(window._SPARKS||{}).by_market||{};
 const tb=document.getElementById('tbody');tb.innerHTML='';
 for(const r of m){const pct=chgPct(r);const cc=pct>=0?'pos':'neg',fd=r.funding_8h*100,fc=fd>=0?'pos':'neg',
  ap=r.funding_apr,ac=ap>=0?'pos':'neg',bc=r.basis_pct>=0?'pos':'neg';
  const isPin=pins.includes(String(r.market_id));
  // bipolar bar width
  const fpct=Math.min(100,Math.abs(fd)/maxF*100);
  const fbar=`<span class="fbar" title="${fd.toFixed(4)}% / 8h"><i class="${fc}" style="width:${(fpct/2).toFixed(1)}%"></i></span>`;
  const closes=sparks[r.market_id];
  const sparkCell=_rowSparkSvg(closes);
  const tr=document.createElement('tr');tr.style.cursor='pointer';if(isPin)tr.classList.add('pinned');
  tr.onclick=(e)=>{if(e.target.classList.contains('pinstar'))return;location.hash='market='+r.market_id;};
  tr.innerHTML=`<td class="mkt"><span class="pinstar ${isPin?'pinned':''}" onclick="event.stopPropagation();togglePin('${r.market_id}')">★</span>${r.name} <span style="color:var(--muted);font-size:11px">↗</span></td>
   <td>${P(r.last_price)}</td>
   <td><span class="chgpill ${cc}">${pct>=0?'+':''}${pct.toFixed(2)}%</span></td>
   <td class="spark-cell">${sparkCell}</td>
   <td>${U(r.volume_24h)}<div class="bar"><i style="width:${(100*(r.volume_24h||0)/mx).toFixed(1)}%"></i></div></td>
   <td>${U(r.oi_usd)}</td>
   <td class="${fc}">${fd>=0?'+':''}${fd.toFixed(4)}%${fbar}</td>
   <td class="${ac}">${ap>=0?'+':''}${ap.toFixed(1)}%</td>
   <td class="${bc}">${r.basis_pct>=0?'+':''}${r.basis_pct.toFixed(3)}%</td>
   <td>${r.spread_bps!=null?r.spread_bps.toFixed(1)+' bps':'—'}</td>
   <td>${r.max_leverage?'x'+r.max_leverage:'—'}</td>`;tb.appendChild(tr);}
}
async function loadSparks(){
 try{const d=await (await fetch('/api/market-sparks')).json();
  if(d&&d.ok){window._SPARKS=d;if(DATA&&DATA.markets)renderMarkets();}
 }catch(e){}
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
 // Skeleton with identicon + addr immediately
 out.innerHTML=`<div style="display:flex;gap:14px;align-items:center;padding:24px">
  ${identicon(addr,56)}
  <div style="flex:1">
   <div style="font-family:var(--mono);font-size:14px;color:var(--accent);font-weight:600">${addr.slice(0,10)}…${addr.slice(-6)}</div>
   <div style="color:var(--muted);font-size:12px;margin-top:4px"><span class="skel" style="display:inline-block;width:160px;height:10px"></span></div>
  </div></div>
  <div class="empty" style="padding:16px">Loading wallet…</div>`;
 // Fire both in parallel but DON'T wait for stats to render the main page
 const walletP = fetch('/api/wallet?account='+encodeURIComponent(addr)).then(r=>r.json());
 const statsP  = fetch('/api/wallet-stats?account='+encodeURIComponent(addr)).then(r=>r.json()).catch(()=>null);
 try{
  const w=await walletP;
  if(!w.ok){out.innerHTML='<div class="empty">'+(w.errors||['Error']).join(' ')+'</div>';return;}
  w.stats=null; // render without stats first
  window._currWallet=w;
  out.innerHTML=renderWalletPage(w);
  // Enrich page with stats when they arrive
  statsP.then(st=>{
   if(st && st.ok && window._currWallet && window._currWallet.account===w.account){
    window._currWallet.stats=st;
    out.innerHTML=renderWalletPage(window._currWallet);
   }
  });
  // Track in personal stats
  const ps=psTrack('wallets',addr.toLowerCase());
  if(ps.wallets_viewed.length===1)unlockAchievement('first_wallet');
  if(ps.wallets_viewed.length===10)unlockAchievement('wallet_10');
  if(ps.wallets_viewed.length===50)unlockAchievement('wallet_50');
  if(w.summary&&w.summary.smart_money)unlockAchievement('first_smart');
  // Suggested similar wallets at the bottom
  setTimeout(()=>loadSuggestedWallets(addr),100);
  // render only initial visible tab charts; rest render on demand when user clicks the tab
 }catch(e){out.innerHTML='<div class="empty">Error fetching data.</div>';}
}

function renderWalletEquityCurve(curve){
 const c=document.getElementById('eqCurve');
 if(!c||!curve||curve.length<2)return;
 if(charts.eqCurve)charts.eqCurve.destroy();
 const labels=curve.map(p=>new Date(p[0]*1000).toLocaleDateString('en-US',{month:'short',day:'numeric'}));
 const vals=curve.map(p=>p[1]);
 const final=vals[vals.length-1];
 const color=final>=0?'rgb(54,211,156)':'rgb(255,84,102)';
 const bg=final>=0?'rgba(54,211,156,.12)':'rgba(255,84,102,.12)';
 charts.eqCurve=new Chart(c,{type:'line',
  data:{labels,datasets:[{data:vals,borderColor:color,backgroundColor:bg,
   borderWidth:2,fill:true,tension:.18,pointRadius:0,pointHoverRadius:4}]},
  options:{plugins:{legend:{display:false},tooltip:{callbacks:{label:ctx=>U(ctx.parsed.y)}}},
   scales:{x:{ticks:{color:'#6b7785',maxTicksLimit:8},grid:{display:false}},
    y:{ticks:{color:'#6b7785',callback:v=>U(v)},grid:{color:'#1a2129'}}}}});
}
function renderWalletStatsCharts(st){
 // Activity heatmap
 const grid=document.getElementById('actHeatmap');
 if(grid && st.activity_heatmap){
  const days=['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];
  let maxV=0; st.activity_heatmap.forEach(row=>row.forEach(v=>{if(v>maxV)maxV=v;}));
  if(maxV===0)maxV=1;
  let html='<div style="display:grid;grid-template-columns:38px repeat(24,1fr);gap:2px;font-size:10px;color:var(--muted)">';
  html+='<div></div>';for(let h=0;h<24;h++)html+=`<div style="text-align:center">${h%3===0?h:''}</div>`;
  for(let d=0;d<7;d++){
   html+=`<div style="padding-right:6px;text-align:right;line-height:18px">${days[d]}</div>`;
   for(let h=0;h<24;h++){
    const v=st.activity_heatmap[d][h];
    const intensity=v/maxV;
    const bg=v>0?`rgba(80,221,194,${0.12+intensity*0.78})`:'#0f1418';
    html+=`<div title="${days[d]} ${h}:00 UTC · ${v} trades" style="height:18px;border-radius:3px;background:${bg};border:1px solid var(--line2)"></div>`;
   }
  }
  html+='</div>';
  grid.innerHTML=html;
 }
 // Calendar heatmap (yearly)
 if(st.daily_activity){renderCalendarHeatmap('calmap_wrap',st.daily_activity);}
 // Per-market PnL bar chart
 const c=document.getElementById('mktPnl');
 if(c && st.market_pnl && st.market_pnl.length){
  const top=st.market_pnl.slice(0,10);
  if(charts.mktPnl)charts.mktPnl.destroy();
  charts.mktPnl=new Chart(c,{type:'bar',
   data:{labels:top.map(m=>m.market),datasets:[{
    data:top.map(m=>m.realized_pnl),
    backgroundColor:top.map(m=>m.realized_pnl>=0?'rgba(54,211,156,.7)':'rgba(255,93,108,.7)'),
    borderColor:top.map(m=>m.realized_pnl>=0?'#36d39c':'#ff5d6c'),
    borderWidth:1,borderRadius:5,maxBarThickness:32
   }]},
   options:{indexAxis:'y',plugins:{legend:{display:false}},scales:{
    x:{ticks:{color:'#8b8b94',callback:v=>U(v)},grid:{color:'#21242e'}},
    y:{ticks:{color:'#cfd5e3',font:{weight:600}},grid:{display:false}}}}});
 }
}

function renderWalletPage(d){
 const s=d.summary||{};const upc=(s.total_upnl||0)>=0?'pos':'neg';
 const bal=d.balance;const equity=(bal!=null)?bal+(s.total_upnl||0):null;
 const rpc=(s.realized_pnl_shown||0)>=0?'pos':'neg';
 // Cabecera con avatar + address + acciones
 let h=`<div class="whead">
  <div class="row1">
   <div class="identicon">${identicon(d.account,42)}</div>
   <div>
    <div class="addr" id="full_addr">${d.account}${s.smart_money?' <span class="smart-badge" style="font-size:11px;padding:3px 8px">SMART MONEY</span>':''}</div>
    <div class="note" style="margin-top:2px">${s.num_positions||0} positions · ${s.num_open_orders||0} orders · ${s.num_trades||0} trades loaded</div>
   </div>
   <div class="actions">
    <span class="chip" onclick="navigator.clipboard.writeText('${d.account}');this.textContent='Copied ✓';setTimeout(()=>this.textContent='Copy address',1500)">Copy address</span>
    <span class="chip" style="background:linear-gradient(135deg,rgba(0,255,212,.12),rgba(0,255,212,.04));border-color:rgba(0,255,212,.3);color:var(--accent)" onclick="openTcard('${d.account}')">🎴 Trader card</span>
    <span class="chip" onclick="const u=location.origin+'/share/wallet/${d.account}';navigator.clipboard.writeText(u);this.textContent='Share link copied ✓';setTimeout(()=>this.textContent='Share',1500)" title="Copy a link that previews nicely on X/Telegram">Share</span>
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

 // ===== Tab navigation =====
 const st=d.stats;
 const nPos=d.positions.length, nOrd=d.open_orders.length, nTr=d.trades.length;
 const hasStats=st && st.trades_analyzed;
 const hasEquity=s.equity_curve && s.equity_curve.length>1;
 const hasMkt=hasStats && st.market_pnl && st.market_pnl.length;
 h+=`<div class="walltabs">
  <div class="walltab active" data-wt="positions"><span class="ico">📊</span>Positions${nPos?` <span class="count">${nPos}</span>`:''}</div>
  <div class="walltab" data-wt="orders"><span class="ico">📝</span>Open orders${nOrd?` <span class="count">${nOrd}</span>`:''}</div>
  ${hasStats?'<div class="walltab" data-wt="stats"><span class="ico">🎯</span>Trader stats</div>':''}
  ${hasEquity?'<div class="walltab" data-wt="equity"><span class="ico">📈</span>Equity curve</div>':''}
  ${hasMkt?'<div class="walltab" data-wt="markets"><span class="ico">🗂️</span>Per-market</div>':''}
  ${hasStats?'<div class="walltab" data-wt="activity"><span class="ico">🔥</span>Activity</div>':''}
  <div class="walltab" data-wt="trades"><span class="ico">📜</span>Trades${nTr?` <span class="count">${nTr}</span>`:''}</div>
  <div class="walltab" data-wt="transfers"><span class="ico">💸</span>Transfers</div>
 </div>`;

 // ===== Tab: Positions (default) =====
 h+=`<div class="wallsec active" id="ws_positions">`;
 if(d.positions.length){
  h+=`<div style="display:flex;justify-content:flex-end;margin-bottom:10px">
   <span class="viewtoggle" id="pos_vt">
    <button data-vt="cards" class="active">▦ Cards</button>
    <button data-vt="table">☷ Table</button>
   </span></div>`;
  // Position cards (default)
  h+=`<div id="pos_view_cards" class="poscards">`;
  for(const p of d.positions){const pc=p.upnl>=0?'pos':'neg';
   const distPct=p.dist_to_liq_pct!=null?p.dist_to_liq_pct:50;
   const safeBar=Math.min(100,Math.max(2,distPct*2)); // 50% dist → full
   const dngr=distPct<15?'danger':'';
   h+=`<div class="poscard ${dngr}">
    <div class="pc-top">
     <div class="pc-mkt">${p.market}</div>
     <span class="pillside ${p.side}">${p.side}</span>
     <span class="pc-lev">x${p.leverage?(+p.leverage).toFixed(0):'—'}</span>
    </div>
    <div class="pc-pnl ${pc}">${p.upnl>=0?'+':''}${U(p.upnl)}</div>
    <div class="pc-meta">${p.upnl>=0?'+':''}${p.upnl_pct.toFixed(2)}% · notional ${U(p.notional)}</div>
    <div class="pc-grid">
     <div><span class="lab">Entry</span><span>${P(p.entry)}</span></div>
     <div><span class="lab">Mark</span><span>${P(p.mark)}</span></div>
     <div><span class="lab">Size</span><span>${(+p.size).toLocaleString('en-US',{maximumFractionDigits:4})}</span></div>
     <div><span class="lab">Margin</span><span>${p.margin_mode}</span></div>
    </div>
    <div class="pc-liqbar" style="--pct:${safeBar.toFixed(1)}%">
     <div class="lab"><span>Liq @ ${p.liq_price!=null?P(p.liq_price):'—'}</span><span>${p.dist_to_liq_pct!=null?(p.dist_to_liq_pct>=0?p.dist_to_liq_pct.toFixed(1)+'% away':'LIQUIDATED'):'—'}</span></div>
     <div class="track"></div>
    </div>
   </div>`;}
  h+=`</div>`;
  // Position table (alt)
  h+=`<div id="pos_view_table" style="display:none">
   <div class="panel" style="padding:0"><table><thead><tr>
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
  h+='</tbody></table></div></div>';
  h+='<div class="note" style="margin-top:10px">Liq. price for <b>Cross</b> positions accounts for the total account balance and unrealized PnL of all other cross positions. For <b>Isolated</b>, it uses the dedicated isolated USDC balance.</div>';
 } else {
  h+='<div class="empty">No open positions right now.</div>';
 }
 h+=`</div>`;

 // ===== Tab: Open orders =====
 h+=`<div class="wallsec" id="ws_orders">`;
 if(d.open_orders.length){
  h+=`<div class="panel" style="padding:0"><table><thead><tr><th>Market</th><th>Side</th><th>Price</th><th>Size</th><th>Notional</th></tr></thead><tbody>`;
  for(const o of d.open_orders) h+=`<tr><td class="mkt">${o.market}</td>
   <td><span class="pillside ${o.side}">${o.side}</span></td>
   <td>${P(o.price)}</td><td>${o.size}</td><td>${U(o.price*o.size)}</td></tr>`;
  h+='</tbody></table></div>';
 } else {
  h+='<div class="empty">No open orders.</div>';
 }
 h+=`</div>`;

 // ===== Tab: Trader stats =====
 if(hasStats){
  const pf=st.profit_factor; const pfc=pf>=1?'pos':'neg';
  h+=`<div class="wallsec" id="ws_stats">
   <div class="cards" style="grid-template-columns:repeat(5,1fr)">
    <div class="card"><div class="lbl">Win rate</div><div class="val">${st.win_rate_pct.toFixed(1)}%</div><div class="meta">${st.wins} wins / ${st.losses} losses</div></div>
    <div class="card"><div class="lbl">Profit factor</div><div class="val ${pfc}">${pf.toFixed(2)}</div><div class="meta">sum wins / sum losses</div></div>
    <div class="card"><div class="lbl">Max drawdown</div><div class="val neg">${U(st.max_drawdown)}</div><div class="meta">peak-to-trough</div></div>
    <div class="card"><div class="lbl">Avg trade size</div><div class="val">${U(st.avg_trade_size)}</div><div class="meta">largest ${U(st.largest_trade_notional)}</div></div>
    <div class="card"><div class="lbl">Activity</div><div class="val">${st.trades_per_day}/day</div><div class="meta">${st.trades_analyzed.toLocaleString('en-US')} trades · ${st.days_active}d active</div></div>
   </div>
   <div class="cards" style="grid-template-columns:repeat(3,1fr);margin-top:8px">
    <div class="card"><div class="lbl">Best trade</div><div class="val pos">+${U(st.best_trade_pnl)}</div></div>
    <div class="card"><div class="lbl">Worst trade</div><div class="val neg">${U(st.worst_trade_pnl)}</div></div>
    <div class="card"><div class="lbl">Liquidations</div><div class="val ${st.n_liquidations?'neg':''}">${st.n_liquidations}</div><div class="meta">forced closes in history</div></div>
   </div>
  </div>`;
 }

 // ===== Tab: Equity curve =====
 if(hasEquity){
  const c=s.equity_curve;const final=c[c.length-1][1];const fc=final>=0?'pos':'neg';
  const dd=s.max_dd_30d||0;
  const p1=s.pnl_1d||0,p7=s.pnl_7d||0,pc1=p1>=0?'pos':'neg',pc7=p7>=0?'pos':'neg';
  h+=`<div class="wallsec" id="ws_equity">
   <div class="cards" style="grid-template-columns:repeat(4,1fr);margin-bottom:10px">
    <div class="card"><div class="lbl">PnL · 24h</div><div class="val ${pc1}">${p1>=0?'+':''}${U(p1)}</div></div>
    <div class="card"><div class="lbl">PnL · 7d</div><div class="val ${pc7}">${p7>=0?'+':''}${U(p7)}</div></div>
    <div class="card"><div class="lbl">PnL · 30d</div><div class="val ${fc}">${final>=0?'+':''}${U(final)}</div></div>
    <div class="card"><div class="lbl">Max drawdown · 30d</div><div class="val neg">−${U(dd)}</div><div class="meta">peak-to-trough realized</div></div>
   </div>
   <div class="panel" style="padding:14px 18px"><canvas id="eqCurve" height="200"></canvas></div>
  </div>`;
 }

 // ===== Tab: Per-market PnL =====
 if(hasMkt){
  h+=`<div class="wallsec" id="ws_markets">
   <div class="grid2">
    <div class="panel" style="padding:14px 18px"><canvas id="mktPnl" height="220"></canvas></div>
    <div class="panel" style="padding:0;max-height:380px;overflow:auto"><table><thead><tr>
     <th>Market</th><th>Realized PnL</th><th>Trades</th><th>W / L</th><th>Volume</th><th>Fees</th></tr></thead><tbody>`;
   for(const m of st.market_pnl){const c=m.realized_pnl>=0?'pos':'neg';
    h+=`<tr><td class="mkt">${m.market}</td>
     <td class="${c}">${m.realized_pnl>=0?'+':''}${U(m.realized_pnl)}</td>
     <td>${m.trades}</td><td><span class="pos">${m.wins}</span>/<span class="neg">${m.losses}</span></td>
     <td>${U(m.volume)}</td><td style="color:var(--muted)">${U(m.fees)}</td></tr>`;}
   h+=`</tbody></table></div></div>
  </div>`;
 }

 // ===== Tab: Activity heatmap + calendar =====
 if(hasStats){
  h+=`<div class="wallsec" id="ws_activity">
   <div class="panel" style="padding:18px"><h2>Hour × Day heatmap · UTC</h2><div id="actHeatmap"></div>
    <div class="note" style="margin-top:8px">Brighter teal = more trades at that hour. Reveals trading timezones and bot vs manual patterns.</div></div>
   <div class="panel" style="padding:18px;margin-top:14px"><h2>📅 Activity calendar · last 53 weeks</h2><div id="calmap_wrap"></div>
    <div class="note" style="margin-top:8px">GitHub-style calendar. Each cell is a day; intensity = trades that day. Hover to see counts.</div></div>
  </div>`;
 }

 // ===== Tab: Trade history =====
 h+=`<div class="wallsec" id="ws_trades">`;
 if(d.trades.length){
  h+=`<div class="note" style="margin-bottom:10px">Each row is one <b>order</b> — fills consolidated by order_id (avg price weighted by size).</div>`;
  h+=`<div class="panel" style="padding:0;max-height:680px;overflow:auto"><table><thead><tr>
   <th>Time (UTC)</th><th>Market</th><th>Side</th><th>Role</th><th>Avg Price</th><th>Size</th>
   <th>Notional</th><th>Fees</th><th>Realized PnL</th><th>Fills</th></tr></thead><tbody>`;
  for(const t of d.trades){
   const pc=t.realized_pnl>=0?'pos':'neg';
   const dt=new Date(t.ts*1000);
   const dts=dt.toISOString().slice(0,16).replace('T',' ');
   let role='';
   if(t.role==='TAKER')role='<span class="role-T">TAKER</span>';
   else if(t.role==='MAKER')role='<span class="role-M">MAKER</span>';
   else role='<span class="role-M" style="opacity:.6">MIXED</span>';
   const liqTag=t.is_liq?' <span class="liq-near" style="font-size:10px">LIQ</span>':'';
   const fills=t.n_fills||1;
   const fillsTag=fills>1?`<span style="font-family:var(--mono);font-size:11px;color:var(--accent);background:rgba(0,255,212,.08);padding:1px 6px;border-radius:8px">${fills}</span>`:'<span style="color:var(--muted);font-size:11px">1</span>';
   h+=`<tr><td style="font-family:var(--mono);font-size:12px;color:var(--muted)">${dts}</td>
    <td class="mkt">${t.market}</td>
    <td><span class="pillside ${t.side}">${t.side}</span>${liqTag}</td>
    <td>${role}</td>
    <td>${P(t.price)}</td>
    <td>${(+t.size).toLocaleString('en-US',{maximumFractionDigits:4})}</td>
    <td>${U(t.notional)}</td>
    <td style="color:var(--muted)">${U(t.fee)}</td>
    <td class="${pc}">${t.realized_pnl!==0?(t.realized_pnl>=0?'+':'')+U(t.realized_pnl):'—'}</td>
    <td>${fillsTag}</td>
   </tr>`;}
  h+='</tbody></table></div>';
 } else {
  h+='<div class="empty">No trade history.</div>';
 }
 h+=`</div>`;

 // ===== Tab: Transfers (deposits & withdrawals) =====
 h+=`<div class="wallsec" id="ws_transfers">`;
 h+=`<div class="note" id="tr_status" style="margin-bottom:10px">Loading…</div>`;
 h+=`<div id="tr_body"></div>`;
 h+=`</div>`;

 return h;
}

// ===== Load wallet transfers lazily when tab is opened =====
async function loadWalletTransfers(addr){
 try{
  const d=await (await fetch('/api/wallet-transfers?account='+encodeURIComponent(addr)+'&limit=200')).json();
  const st=document.getElementById('tr_status'); const body=document.getElementById('tr_body');
  if(!st||!body)return;
  if(!d.ok||d.count===0){
   st.innerHTML=`<span style="color:var(--muted)">No deposits or withdrawals found onchain (indexer scans last ~24h initially; full history loads in background).</span>`;
   body.innerHTML=''; return;
  }
  st.innerHTML=`<b>${d.count}</b> transfers · Deposited: <span class="pos">+${U(d.total_deposits)}</span> · Withdrew: <span class="neg">−${U(d.total_withdrawals)}</span> · Net: ${(d.net>=0?'<span class="pos">+':'<span class="neg">')+U(Math.abs(d.net))+'</span>'}`;
  let html=`<div class="panel" style="padding:0;max-height:560px;overflow:auto"><table><thead><tr>
   <th>Time (UTC)</th><th>Type</th><th>Amount</th><th>Token</th><th>Counterparty</th><th>Tx</th></tr></thead><tbody>`;
  for(const t of d.transfers){
   const dt=new Date(t.ts*1000);
   const dts=dt.toISOString().slice(0,16).replace('T',' ');
   const kindTag=t.kind==='deposit'?'<span class="pos" style="font-weight:700">DEPOSIT ↓</span>':'<span class="neg" style="font-weight:700">WITHDRAW ↑</span>';
   const amtCls=t.kind==='deposit'?'pos':'neg';
   const sign=t.kind==='deposit'?'+':'−';
   const cpShort=t.counterparty?t.counterparty.slice(0,10)+'…':'—';
   html+=`<tr>
    <td style="font-family:var(--mono);font-size:12px;color:var(--muted)">${dts}</td>
    <td>${kindTag}</td>
    <td class="${amtCls}" style="font-weight:700">${sign}${U(t.amount)}</td>
    <td style="font-size:11px;color:var(--muted)">USDC</td>
    <td style="font-family:var(--mono);font-size:11px;color:var(--muted)">${cpShort}</td>
    <td><a href="#tx=${t.tx_hash}" style="font-family:var(--mono);font-size:11px">${t.tx_hash.slice(0,10)}…</a></td>
   </tr>`;
  }
  html+='</tbody></table></div>';
  body.innerHTML=html;
 }catch(e){
  const st=document.getElementById('tr_status');if(st)st.textContent='Error loading transfers.';
 }
}
// Wire positions cards/table toggle (delegated)
document.addEventListener('click',e=>{
 const b=e.target.closest('#pos_vt button');if(!b)return;
 document.querySelectorAll('#pos_vt button').forEach(x=>x.classList.remove('active'));
 b.classList.add('active');
 const vt=b.dataset.vt;
 document.getElementById('pos_view_cards').style.display=vt==='cards'?'':'none';
 document.getElementById('pos_view_table').style.display=vt==='table'?'':'none';
});
// Wire wallet tabs (delegated)
document.addEventListener('click',e=>{
 const t=e.target.closest('.walltab');if(!t)return;
 const view=document.getElementById('v_wallet');if(!view||!view.contains(t))return;
 const key=t.dataset.wt;if(!key)return;
 view.querySelectorAll('.walltab').forEach(x=>x.classList.remove('active'));
 t.classList.add('active');
 view.querySelectorAll('.wallsec').forEach(x=>x.classList.remove('active'));
 const sec=document.getElementById('ws_'+key);if(sec)sec.classList.add('active');
 // re-render charts on demand for tabs with canvases
 if(key==='equity'){const w=window._currWallet;if(w)setTimeout(()=>renderWalletEquityCurve(w.summary&&w.summary.equity_curve),20);}
 if(key==='markets'||key==='activity'){const w=window._currWallet;if(w&&w.stats)setTimeout(()=>renderWalletStatsCharts(w.stats),20);}
 if(key==='transfers'){const w=window._currWallet;if(w&&w.account)setTimeout(()=>loadWalletTransfers(w.account),20);}
});

// Routing por hash: #wallet=0x... o #market=ID
function handleHashRoute(){
 const h=location.hash||'';
 if(h==='#copy'){ navigateView('copy'); return; } // puerta directa (sin entrada en menú)
 const w=h.match(/wallet=(0x[0-9a-fA-F]{40})/);
 const mk=h.match(/market=(\d+)/);
 const bl=h.match(/block=(\d+)/);
 const tx=h.match(/tx=(0x[0-9a-fA-F]{64})/);
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
 } else if(bl){
  document.querySelectorAll('.navitem').forEach(x=>x.classList.remove('active'));
  document.querySelectorAll('.view').forEach(x=>x.classList.remove('active'));
  document.getElementById('v_blockdetail').classList.add('active');
  if(typeof stopExplorer==='function')stopExplorer();
  loadBlockDetail(+bl[1]);
 } else if(tx){
  document.querySelectorAll('.navitem').forEach(x=>x.classList.remove('active'));
  document.querySelectorAll('.view').forEach(x=>x.classList.remove('active'));
  document.getElementById('v_txdetail').classList.add('active');
  if(typeof stopExplorer==='function')stopExplorer();
  loadTxDetail(tx[1]);
 } else {
  // sin hash: si estamos en wallet/market/block/tx detail, volver a Visión general
  const inDetail=['v_wallet','v_marketdetail','v_blockdetail','v_txdetail'].some(id=>document.getElementById(id)&&document.getElementById(id).classList.contains('active'));
  const inWallet=document.getElementById('v_wallet').classList.contains('active');
  if(inWallet)loadWallet(null);
  if(inDetail){
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

async function loadCumulativeGrowth(){
 try{
  const d=await (await fetch('/api/cumulative')).json();
  const p=d.points||[];
  if(!p.length)return;
  const lab=p.map(x=>new Date(x.t*1000).toLocaleDateString('en-US',{month:'short',day:'numeric'}));
  // Use the shared lineChart helper — it reuses Chart instances instead of recreating them
  lineChart('hCumVol', lab, p.map(x=>x.cum_vol),  'rgb(51,214,166)');
  lineChart('hCumFees',lab, p.map(x=>x.cum_fees), 'rgb(255,180,84)');
  lineChart('hOiTL',   lab, p.map(x=>x.oi),       'rgb(93,200,255)');
  const lastVol=p[p.length-1].cum_vol||0;
  const lastFees=p[p.length-1].cum_fees||0;
  const lastOi=p[p.length-1].oi||0;
  const nv=document.getElementById('hCumVol_n');if(nv)nv.textContent=U(lastVol)+' over '+p.length+' day'+(p.length!==1?'s':'');
  const nf=document.getElementById('hCumFees_n');if(nf)nf.textContent=U(lastFees)+' since first sample';
  const no=document.getElementById('hOiTL_n');if(no)no.textContent='Current '+U(lastOi);
 }catch(e){}}

async function loadHistory(){
 try{
  // Reuse the prefetch promise (first call only); subsequent calls do a fresh fetch
  let d=window._earlyFetch&&window._earlyFetch.history?await window._earlyFetch.history:null;
  if(window._earlyFetch)window._earlyFetch.history=null;
  if(!d)d=await (await fetch('/api/history')).json();
  const p=d.points||[];
  // Cumulative growth charts (since launch): downsampled-to-daily series
  loadCumulativeGrowth();
  // Funding APR history per market (top 5 markets by current OI)
  const fhCanvas=document.getElementById('fhChart');
  if(fhCanvas){
   const points=p.filter(x=>x.mk);
   const allMkts=new Set();points.forEach(x=>Object.keys(x.mk||{}).forEach(m=>allMkts.add(m)));
   const ranked=[...allMkts].map(m=>{
    const lastOi=[...points].reverse().find(x=>x.mk&&x.mk[m])?.mk[m]?.oi||0;
    return {name:m,oi:lastOi};
   }).sort((a,b)=>b.oi-a.oi).slice(0,6);
   const colors=['#97FCE4','#36d39c','#ffb454','#5dc8ff','#ff8a96','#a0e8d6'];
   const datasets=ranked.map((m,i)=>{
    const data=points.map(x=>({x:x.t*1000,y:x.mk&&x.mk[m.name]?x.mk[m.name].f:null})).filter(p=>p.y!=null);
    return {label:m.name,data,borderColor:colors[i%colors.length],
     backgroundColor:'transparent',tension:.3,pointRadius:0,borderWidth:2,spanGaps:true};
   });
   if(charts.fhChart)charts.fhChart.destroy();
   charts.fhChart=new Chart(fhCanvas,{type:'line',data:{datasets},
    options:{plugins:{legend:{display:true,labels:{color:'#cfd5e3',font:{size:11}}}},
     scales:{x:{type:'time',ticks:{color:'#8b8b94',maxTicksLimit:8},grid:{display:false}},
      y:{ticks:{color:'#8b8b94',callback:v=>v.toFixed(1)+'%'},grid:{color:'#21242e'}}}}});
   document.getElementById('fh_status').innerHTML=
    points.length<5?`<span style="color:var(--amber)">Building history… ${points.length} points so far (need a few hours for a useful chart)</span>`
                   :`<span style="color:var(--green)">${points.length} data points · top ${ranked.length} markets by OI</span>`;
  }
 }catch(e){}}

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
function rkRow(r,i,maxN){const pc=r.upnl>=0?'pos':'neg';
 return `<tr><td>${rankPill(i)}</td><td>${addrCell(r.account,{tier:r.notional})}</td>
  <td>${(+r.size).toLocaleString('en-US',{maximumFractionDigits:4})}</td>
  <td>${barFillCell(U(r.notional),r.notional,maxN)}</td>
  <td>${P(r.entry)}</td>
  <td class="${pc}">${r.upnl>=0?'+':''}${U(r.upnl)}</td><td>x${(+r.leverage).toFixed(0)}</td></tr>`;}
function renderRanking(){if(!RANK)return;const m=document.getElementById('rk_market').value;
 const r=(RANK.ranking||{})[m];const L=document.getElementById('rk_long'),S=document.getElementById('rk_short');
 if(!r){L.innerHTML=S.innerHTML='<tr><td class="empty" colspan=7>No data.</td></tr>';document.getElementById('rk_oi').textContent='';return;}
 document.getElementById('rk_oi').innerHTML=`OI longs: <b class="pos">${U(r.oi_long)}</b> (${r.n_long||r.longs.length}) · shorts: <b class="neg">${U(r.oi_short)}</b> (${r.n_short||r.shorts.length})`;
 const maxL=Math.max(...(r.longs||[]).map(x=>x.notional||0),1);
 const maxS=Math.max(...(r.shorts||[]).map(x=>x.notional||0),1);
 L.innerHTML=(r.longs||[]).map((x,i)=>rkRow(x,i,maxL)).join('')||'<tr><td class="empty" colspan=7>—</td></tr>';
 S.innerHTML=(r.shorts||[]).map((x,i)=>rkRow(x,i,maxS)).join('')||'<tr><td class="empty" colspan=7>—</td></tr>';
 const lt=L.closest('table');if(lt){if(!lt.id)lt.id='rk_ltbl';stagger(lt.id);}
 const st=S.closest('table');if(st){if(!st.id)st.id='rk_stbl';stagger(st.id);}
}

// ====== Leaderboard helpers ======
function ensureHero(prefix,beforeEl){
 let h=document.getElementById(prefix+'_hero');
 if(!h){h=document.createElement('div');h.id=prefix+'_hero';beforeEl.parentNode.insertBefore(h,beforeEl);}
 return h;
}
function rankPill(i){const cls=i===0?'r1':i===1?'r2':i===2?'r3':'';
 const medal=i===0?'🥇 ':i===1?'🥈 ':i===2?'🥉 ':'#';
 return `<span class="rankpill ${cls}">${medal}${i+1}</span>`;}
function tierBadge(volume){
 if(volume>=10_000_000)return '<span class="tier whale">🐋 Whale</span>';
 if(volume>=1_000_000)return '<span class="tier pro">💎 Pro</span>';
 if(volume>=100_000)return '<span class="tier active">⚡ Active</span>';
 return '';}
function addrCell(addr,opts){opts=opts||{};
 const t=opts.tier?tierBadge(opts.tier):'';
 const sm=opts.smart?'<span class="smart-badge" style="margin-left:4px;font-size:8.5px;padding:1.5px 5px">SMART</span>':'';
 return `<div class="addrcell"><span class="idsm">${identicon(addr,22)}</span><a href="#wallet=${addr}">${shortAddr(addr)}</a>${sm}${t}</div>`;}
function barFillCell(label,v,max,cls){
 const pct=max>0?Math.min(100,Math.abs(v)/max*100):0;
 const c=cls||(v>=0?'pos':'neg');
 return `<span class="barfill ${c}" style="--w:${pct.toFixed(1)}%"><span>${label}</span></span>`;}
function podiumHtml(items,getVal,getSub,fmt){
 if(!items||items.length<3)return '';
 const order=[1,0,2]; // silver, gold, bronze for visual centerpiece
 const cls={0:'p1',1:'p2',2:'p3'};
 const med={0:'🥇',1:'🥈',2:'🥉'};
 const lbl={0:'CHAMPION',1:'2ND PLACE',2:'3RD PLACE'};
 return `<div class="podium">${order.map(i=>{const it=items[i];if(!it)return '<div></div>';
  return `<a class="ppos ${cls[i]}" href="#wallet=${it.account}">
   <div class="pmedal">${med[i]}</div>
   <div class="pavatar">${identicon(it.account,38)}</div>
   <div class="paddr">${shortAddr(it.account)}</div>
   <div class="plbl">${lbl[i]}</div>
   <div class="pval ${it._pos?'pos':it._neg?'neg':''}">${fmt(getVal(it))}</div>
   <div class="psub">${getSub?getSub(it):''}</div>
  </a>`;}).join('')}</div>`;}
function heroBanner(opts){
 return `<div class="lbhero">
  <div class="lh-icon">${opts.icon||'🏆'}</div>
  <div class="lh-info">
   <div class="lh-lbl">${opts.label}</div>
   <div class="lh-val">${opts.value}</div>
   <div class="lh-meta">${opts.meta||''}</div>
  </div>
  <div class="lh-side">${opts.side||''}</div>
 </div>`;}
function applyHeat(tableId,colIdx){
 const tb=document.querySelector('#'+tableId);if(!tb)return;
 const rows=Array.from(tb.querySelectorAll('tbody tr'));
 const n=rows.length;if(n<10)return;
 const top10=Math.ceil(n*0.1);
 const bot10=Math.ceil(n*0.1);
 rows.slice(0,top10).forEach(r=>{const c=r.children[colIdx];if(c)c.classList.add('heat-top');});
 rows.slice(n-bot10).forEach(r=>{const c=r.children[colIdx];if(c)c.classList.add('heat-bot');});
}
function stagger(tableId){
 const rows=document.querySelectorAll('#'+tableId+' tbody tr');
 rows.forEach((r,i)=>{r.classList.add('lbrow');r.style.animationDelay=Math.min(i*15,300)+'ms';});
}
// Filter bar HTML (returns string)
function lbFilterBar(prefix,opts){
 opts=opts||{};
 return `<div class="lbfilter">
  <input type="search" id="${prefix}_search" placeholder="Search address…" />
  ${opts.smartFilter?`<label><input type="checkbox" id="${prefix}_smart" style="accent-color:var(--accent2)"> Smart Money only</label>`:''}
  ${opts.minVal?`<label>Min ${opts.minLabel||'value'}:
    <input type="range" id="${prefix}_min" min="0" max="6" step="1" value="0" style="accent-color:var(--accent);width:120px">
    <span class="pickv" id="${prefix}_minlab">$0</span></label>`:''}
  <span style="margin-left:auto;font-size:11px;color:var(--muted2)" id="${prefix}_shown"></span>
 </div>`;}
function _minMap(idx){return [0,1000,10000,100000,1000000,10000000,100000000][idx]||0;}
function wireFilterBar(prefix,onChange){
 const s=document.getElementById(prefix+'_search');if(s)s.oninput=onChange;
 const sm=document.getElementById(prefix+'_smart');if(sm)sm.onchange=onChange;
 const mn=document.getElementById(prefix+'_min');if(mn){mn.oninput=()=>{
  document.getElementById(prefix+'_minlab').textContent=U(_minMap(+mn.value));
  onChange();};}
}
function filterRows(rows,prefix,addrKey,valKey,smartKey){
 const s=(document.getElementById(prefix+'_search')||{}).value||'';
 const sm=(document.getElementById(prefix+'_smart')||{}).checked;
 const mn=_minMap(+((document.getElementById(prefix+'_min')||{}).value||0));
 return rows.filter(r=>{
  if(s&&!String(r[addrKey]||'').toLowerCase().includes(s.toLowerCase()))return false;
  if(sm&&smartKey&&!r[smartKey])return false;
  if(mn&&valKey&&Math.abs(r[valKey]||0)<mn)return false;
  return true;
 });
}

let AO_DATA=null;
async function loadAcctOi(){
 try{const d=await (await fetch('/api/account-oi-ranking')).json();AO_DATA=d;
  let st='';
  if(d.phase!=='live')st=`<b style="color:var(--amber)">Indexer loading…</b>`;
  else st=`<b style="color:var(--green)">Current snapshot</b> · ${d.active_accounts} accounts with positions · ${d.positions_count} positions · ${agoStr(d.last_update)} ago`;
  document.getElementById('ao_status').innerHTML=st;
  renderAcctOi();
 }catch(e){document.getElementById('ao_status').textContent='Error.';}}
function renderAcctOi(){
 const d=AO_DATA;if(!d)return;
 const tot=d.total_oi||0;
 // Hero banner + podium
 const banner=heroBanner({icon:'💼',label:'TOTAL OPEN INTEREST',value:U(tot),meta:`${d.active_accounts||0} accounts with positions · ${d.positions_count||0} positions open right now`,
  side:`<div>Last update<br><b>${agoStr(d.last_update||0)}</b> ago</div>`});
 const top3=(d.ranking||[]).slice(0,3).map(r=>({...r,_pos:true}));
 const pod=podiumHtml(top3,r=>r.total_oi,r=>`${r.positions} positions · ${(r.total_oi/tot*100).toFixed(1)}% of total`,v=>U(v));
 const filt=lbFilterBar('ao',{minVal:true,minLabel:'OI'});
 const tbl0=document.getElementById('ao_body').closest('table');
 ensureHero('ao',tbl0).innerHTML=banner+pod+filt;
 const tot_old=document.getElementById('ao_totals');if(tot_old)tot_old.style.display='none';
 wireFilterBar('ao',renderAcctOi);
 // Table rows
 const all=(d.ranking||[]);
 const rows=filterRows(all,'ao','account','total_oi');
 document.getElementById('ao_shown').textContent=`Showing ${rows.length} of ${all.length}`;
 const maxV=Math.max(...rows.map(r=>r.total_oi||0),1);
 const html=rows.map((r,i)=>{const pc=r.upnl>=0?'pos':'neg';
  const ml=r.markets.slice(0,4).join(', ')+(r.markets.length>4?` +${r.markets.length-4}`:'');
  return `<tr><td>${rankPill(i)}</td>
   <td>${addrCell(r.account,{tier:r.total_oi})}</td>
   <td>${barFillCell(U(r.total_oi),r.total_oi,maxV)}</td>
   <td>${tot?((r.total_oi/tot*100).toFixed(2)+'%'):'—'}</td>
   <td class="pos">${U(r.long_oi)}</td><td class="neg">${U(r.short_oi)}</td>
   <td class="${pc}">${r.upnl>=0?'+':''}${U(r.upnl)}</td>
   <td>${r.positions}</td><td style="font-size:11.5px;color:var(--muted)">${ml}</td></tr>`;}).join('');
 document.getElementById('ao_body').innerHTML=html||'<tr><td class="empty" colspan=9>No data matches.</td></tr>';
 // need the table's id for heat/stagger:
 const tbl=document.getElementById('ao_body').closest('table');if(tbl){if(!tbl.id)tbl.id='ao_tbl';applyHeat(tbl.id,6);stagger(tbl.id);}
}

let VR_PERIOD='1d';
document.querySelectorAll('#vr_seg button').forEach(b=>b.onclick=()=>{
 document.querySelectorAll('#vr_seg button').forEach(x=>x.classList.remove('active'));
 b.classList.add('active');VR_PERIOD=b.dataset.p;loadVolRanking();});
let VR_DATA=null;
async function loadVolRanking(){
 try{const d=await (await fetch('/api/volume-ranking?period='+VR_PERIOD)).json();VR_DATA=d;
  let st='';
  if(d.phase==='waiting')st=`<b style="color:var(--amber)">Waiting for census…</b>`;
  else if(d.phase==='scanning')st=`<b style="color:var(--amber)">Calculating…</b> ${d.scanned}/${d.total_accounts}`;
  else st=`<b style="color:var(--green)">Ready</b> · ${d.count_with_volume} accounts with volume in ${d.period} · ${agoStr(d.last_update)} ago`;
  document.getElementById('vr_status').innerHTML=st;
  renderVolRanking();
 }catch(e){document.getElementById('vr_status').textContent='Error.';}}
function renderVolRanking(){
 const d=VR_DATA;if(!d)return;
 const tot=d.total_volume||0;
 const banner=heroBanner({icon:'💹',label:`TOTAL REALIZED VOLUME · ${d.period}`,value:U(tot),
  meta:`${d.count_with_volume||0} traders with activity`,
  side:`<div>Last update<br><b>${agoStr(d.last_update||0)}</b> ago</div>`});
 const top3=(d.ranking||[]).slice(0,3);
 const pod=podiumHtml(top3,r=>r.volume,r=>`${r.trades} trades · ${(r.volume/tot*100).toFixed(1)}% share`,v=>U(v));
 const filt=lbFilterBar('vr',{minVal:true,minLabel:'volume'});
 const tbl0=document.getElementById('vr_body').closest('table');
 ensureHero('vr',tbl0).innerHTML=banner+pod+filt;
 const tot_old=document.getElementById('vr_totals');if(tot_old)tot_old.style.display='none';
 wireFilterBar('vr',renderVolRanking);
 const all=d.ranking||[];
 const rows=filterRows(all,'vr','account','volume');
 document.getElementById('vr_shown').textContent=`Showing ${rows.length} of ${all.length}`;
 const maxV=Math.max(...rows.map(r=>r.volume||0),1);
 const html=rows.map((r,i)=>`<tr><td>${rankPill(i)}</td>
  <td>${addrCell(r.account,{tier:r.volume})}</td>
  <td>${barFillCell(U(r.volume),r.volume,maxV)}</td>
  <td>${tot?((r.volume/tot*100).toFixed(2)+'%'):'—'}</td>
  <td>${r.trades}</td></tr>`).join('');
 document.getElementById('vr_body').innerHTML=html||'<tr><td class="empty" colspan=5>No data matches.</td></tr>';
 const tbl=document.getElementById('vr_body').closest('table');if(tbl){if(!tbl.id)tbl.id='vr_tbl';stagger(tbl.id);applyHeat(tbl.id,2);}
}

let OIR_PERIOD='1d';
document.querySelectorAll('#oir_seg button').forEach(b=>b.onclick=()=>{
 document.querySelectorAll('#oir_seg button').forEach(x=>x.classList.remove('active'));
 b.classList.add('active');OIR_PERIOD=b.dataset.p;loadOiRanking();});
let OIR_DATA=null;
async function loadOiRanking(){
 try{const d=await (await fetch('/api/oi-ranking?period='+OIR_PERIOD)).json();OIR_DATA=d;
  let st='';
  if(d.phase==='waiting')st=`<b style="color:var(--amber)">Waiting for census…</b>`;
  else if(d.phase==='scanning')st=`<b style="color:var(--amber)">Reconstructing positions…</b> ${d.scanned}/${d.total_accounts}`;
  else st=`<b style="color:var(--green)">Ready</b> · ${d.count_with_oi} accounts with avg OI in ${d.period} · ${agoStr(d.last_update)} ago`;
  document.getElementById('oir_status').innerHTML=st;
  renderOiRanking();
 }catch(e){document.getElementById('oir_status').textContent='Error.';}}
function renderOiRanking(){
 const d=OIR_DATA;if(!d)return;
 const tot=d.total_avg_oi||0;
 let metaExtra='';if(d.since_ts){const dt=new Date(d.since_ts*1000);metaExtra=` · window since <b>${dt.toUTCString().slice(5,22)} UTC</b>`;}
 const banner=heroBanner({icon:'⏱️',label:`TWAP OI · ${d.period}`,value:U(tot),
  meta:`${d.count_with_oi||0} traders with avg OI${metaExtra}`,
  side:`<div>Last update<br><b>${agoStr(d.last_update||0)}</b> ago</div>`});
 const top3=(d.ranking||[]).slice(0,3);
 const pod=podiumHtml(top3,r=>r.avg_oi,r=>`${r.trades} trades · ${(r.avg_oi/tot*100).toFixed(1)}% share`,v=>U(v));
 const filt=lbFilterBar('oir',{minVal:true,minLabel:'avg OI'});
 const tbl0=document.getElementById('oir_body').closest('table');
 ensureHero('oir',tbl0).innerHTML=banner+pod+filt;
 const tot_old=document.getElementById('oir_totals');if(tot_old)tot_old.style.display='none';
 wireFilterBar('oir',renderOiRanking);
 const all=d.ranking||[];
 const rows=filterRows(all,'oir','account','avg_oi');
 document.getElementById('oir_shown').textContent=`Showing ${rows.length} of ${all.length}`;
 const maxV=Math.max(...rows.map(r=>r.avg_oi||0),1);
 const html=rows.map((r,i)=>`<tr><td>${rankPill(i)}</td>
  <td>${addrCell(r.account,{tier:r.avg_oi})}</td>
  <td>${barFillCell(U(r.avg_oi),r.avg_oi,maxV)}</td>
  <td>${tot?((r.avg_oi/tot*100).toFixed(2)+'%'):'—'}</td>
  <td>${r.trades}</td></tr>`).join('');
 document.getElementById('oir_body').innerHTML=html||'<tr><td class="empty" colspan=5>No data matches.</td></tr>';
 const tbl=document.getElementById('oir_body').closest('table');if(tbl){if(!tbl.id)tbl.id='oir_tbl';stagger(tbl.id);applyHeat(tbl.id,2);}
}

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
function fmtDelta(x){if(x==null||isNaN(x))return'<span style="color:var(--muted)">—</span>';
 const cls=x>0?'pos':x<0?'neg':'zero';const ar=x>0?'▲':x<0?'▼':'·';
 return `<span class="deltarr ${cls}"><span class="ar">${ar}</span>${x>=0?'+':''}${x.toFixed(FC_MODE==='apr'?2:5)}%</span>`;}
function renderFunding(){if(!FC_DATA)return;
 let rows=FC_DATA.rows.slice();
 if(FC_FILTER==='risex')rows=rows.filter(r=>r.in_risex);
 rows.sort((a,b)=>{const aa=Math.abs(a.diff_pacifica_apr||a.diff_hyperliquid_apr||0);
  const bb=Math.abs(b.diff_pacifica_apr||b.diff_hyperliquid_apr||0);return bb-aa;});
 // Hero with arb opportunities (Pacifica + Hyperliquid only)
 const biggestArb=rows.find(r=>Math.abs(r.diff_pacifica_apr||0)>0||Math.abs(r.diff_hyperliquid_apr||0)>0);
 const banner=heroBanner({icon:'💱',label:'CROSS-DEX FUNDING ARB',
  value:biggestArb?(()=>{const ds=[biggestArb.diff_pacifica_apr,biggestArb.diff_hyperliquid_apr].filter(x=>x!=null);const mx=ds.reduce((a,b)=>Math.abs(b)>Math.abs(a)?b:a,0);return (mx>=0?'+':'')+mx.toFixed(1)+'%';})():'—',
  meta:`${rows.length} markets compared · RISEx vs Pacifica, Hyperliquid`,
  side:`<div>Best arb<br><b>${biggestArb?biggestArb.symbol:'—'}</b></div>`});
 // Top-3 arb opportunities podium
 let pod='';
 if(rows.length>=3){
  const top3=rows.slice(0,3);const order=[1,0,2];const cls={0:'p1',1:'p2',2:'p3'};const med={0:'💎',1:'🥈',2:'🥉'};
  pod=`<div class="podium">${order.map(i=>{const it=top3[i];if(!it)return '<div></div>';
   const ds=[it.diff_pacifica_apr,it.diff_hyperliquid_apr].filter(x=>x!=null);
   const mx=ds.reduce((a,b)=>Math.abs(b)>Math.abs(a)?b:a,0);
   const sk=mx>=0?'pos':'neg';
   const mid=(DATA&&DATA.markets||[]).find(x=>(x.name||'').toUpperCase().startsWith(it.symbol))?.market_id||'';
   return `<a class="ppos ${cls[i]}" ${mid?`href="#market=${mid}"`:''}>
    <div class="pmedal">${med[i]}</div>
    <div class="pavatar" style="background:linear-gradient(135deg,${mx>=0?'var(--green)':'var(--red)'},transparent);display:flex;align-items:center;justify-content:center;font-weight:800;color:#06090c;font-family:-apple-system,Inter,sans-serif;font-size:12px">${it.symbol.slice(0,3)}</div>
    <div class="paddr">${it.symbol}</div>
    <div class="plbl">BEST ARB · RISEx vs</div>
    <div class="pval ${sk}">${mx>=0?'+':''}${mx.toFixed(2)}%</div>
    <div class="psub">vs ${Math.abs(it.diff_pacifica_apr||0)>Math.abs(it.diff_hyperliquid_apr||0)?'Pacifica':'Hyperliquid'}</div>
   </a>`;}).join('')}</div>`;
 }
 const tbl0=document.getElementById('fc_body').closest('table');
 ensureHero('fc',tbl0).innerHTML=banner+pod;
 const k=FC_MODE==='apr'?'_apr':'_h';
 const tb=document.getElementById('fc_body');
 tb.innerHTML=rows.map(r=>{
  const rx=r['risex'+k]!=null?r['risex'+k]*(FC_MODE==='apr'?1:100):null;
  const pc=r['pacifica'+k]!=null?r['pacifica'+k]*(FC_MODE==='apr'?1:100):null;
  const hl=r['hyperliquid'+k]!=null?r['hyperliquid'+k]*(FC_MODE==='apr'?1:100):null;
  const dP=r.diff_pacifica_apr!=null?(FC_MODE==='apr'?r.diff_pacifica_apr:r.diff_pacifica_apr/24/365):null;
  const dH=r.diff_hyperliquid_apr!=null?(FC_MODE==='apr'?r.diff_hyperliquid_apr:r.diff_hyperliquid_apr/24/365):null;
  return `<tr><td class="mkt"><b>${r.symbol}</b>${r.in_risex?'':' <span class="note" style="font-size:10px">(not on RISEx)</span>'}</td>
   <td>${fmtPct(rx)}</td><td>${fmtPct(pc)}</td><td>${fmtPct(hl)}</td>
   <td>${fmtDelta(dP)}</td><td>${fmtDelta(dH)}</td></tr>`;}).join('')||'<tr><td class="empty" colspan=6>No data.</td></tr>';
 const tbl=document.getElementById('fc_body').closest('table');if(tbl){if(!tbl.id)tbl.id='fc_tbl';stagger(tbl.id);}}

// Top PnL — period + smart-money filter
let PN_PERIOD='30d', PN_ONLY_SMART=false;
document.querySelectorAll('#pn_period button').forEach(b=>b.onclick=()=>{
 document.querySelectorAll('#pn_period button').forEach(x=>x.classList.remove('active'));
 b.classList.add('active');PN_PERIOD=b.dataset.p;loadPnl();});
document.getElementById('pn_only_smart').onchange=function(){PN_ONLY_SMART=this.checked;loadPnl();};
function smartBadge(s){return s?' <span class="smart-badge" title="Smart Money — consistently profitable">SMART</span>':'';}
function edgeCell(bps){const c=bps>0?'pos':(bps<0?'neg':'');const sign=bps>0?'+':'';return `<span class="${c}">${sign}${bps.toFixed(1)}bps</span>`;}
let PN_DATA=null;
async function loadPnl(){
 try{const d=await (await fetch('/api/pnl-ranking?period='+PN_PERIOD)).json();PN_DATA=d;
  const label={'1d':'last 24h','7d':'last 7d','30d':'last 30d','all':'30d realized + current unrealized'}[d.period||'30d'];
  document.getElementById('pn_status').innerHTML=d.phase==='live'
    ?`<b style="color:var(--green)">Ranking ready</b> · ${label} · ${d.count} accounts · ${agoStr(d.last_update)} ago`
    :`<b style="color:var(--amber)">Calculating…</b> · ${label}`;
  document.getElementById('pn_totals').innerHTML=`<span class="smart-badge" style="font-size:10px">SMART</span> ${d.n_smart||0} wallets`;
  renderPnl();
 }catch(e){document.getElementById('pn_status').textContent='Error.';}}
function renderPnl(){
 const d=PN_DATA;if(!d)return;
 const label={'1d':'last 24h','7d':'last 7d','30d':'last 30d','all':'30d + current unrealized'}[d.period||'30d'];
 // Big hero with #1 PnL
 const top=(d.winners||[])[0];
 const banner=heroBanner({icon:'🏆',label:`HALL OF FAME · ${label}`,value:top?(top.total>=0?'+':'')+U(top.total):'—',
  meta:top?`Leading: ${shortAddr(top.account)} · ${top.trades} trades · edge ${(top.edge_bps||0).toFixed(1)}bps`:'',
  side:`<div><span class="smart-badge" style="font-size:10px">SMART</span> ${d.n_smart||0} wallets<br>${d.count||0} total</div>`});
 // Winners podium
 const top3W=(d.winners||[]).slice(0,3).map(r=>({...r,_pos:true}));
 const podW=podiumHtml(top3W,r=>r.total,r=>`Edge ${(r.edge_bps||0).toFixed(1)}bps · ${U(r.volume)} vol`,v=>(v>=0?'+':'')+U(v));
 const grid=document.getElementById('pn_winners').closest('.grid2');
 ensureHero('pn',grid).innerHTML=banner+podW;
 const filt=PN_ONLY_SMART?(rows=>rows.filter(r=>r.smart)):(rows=>rows);
 const ws=filt(d.winners||[]);const ls=filt(d.losers||[]);
 const maxAbs=Math.max(...ws.concat(ls).map(r=>Math.abs(r.total||0)),1);
 const W=document.getElementById('pn_winners'),L=document.getElementById('pn_losers');
 W.innerHTML=ws.map((r,i)=>{return `<tr><td>${rankPill(i)}</td>
  <td>${addrCell(r.account,{smart:r.smart,tier:r.volume})}</td>
  <td>${barFillCell((r.total>=0?'+':'')+U(r.total),r.total,maxAbs,'pos')}</td>
  <td>${U(r.volume)}</td>
  <td>${edgeCell(r.edge_bps||0)}</td>
  <td>${r.trades}</td></tr>`;}).join('')||'<tr><td class="empty" colspan=6>—</td></tr>';
 L.innerHTML=ls.map((r,i)=>{return `<tr><td>${rankPill(i)}</td>
  <td>${addrCell(r.account,{smart:r.smart,tier:r.volume})}</td>
  <td>${barFillCell((r.total>=0?'+':'')+U(r.total),r.total,maxAbs,'neg')}</td>
  <td>${U(r.volume)}</td>
  <td>${edgeCell(r.edge_bps||0)}</td>
  <td>${r.n_liquidations||0}</td></tr>`;}).join('')||'<tr><td class="empty" colspan=6>—</td></tr>';
 const wt=W.closest('table');if(wt){if(!wt.id)wt.id='pn_wtbl';stagger(wt.id);}
 const lt=L.closest('table');if(lt){if(!lt.id)lt.id='pn_ltbl';stagger(lt.id);}
}

// Funding payments leaderboard
async function loadFunded(){
 try{const d=await (await fetch('/api/funding-ranking')).json();
  document.getElementById('fp_status').innerHTML=`<b style="color:var(--green)">${d.tracked_accounts} accounts tracked</b> · ${agoStr(d.last_update)} ago`;
  document.getElementById('fp_totals').innerHTML=
   `Total currently owed: <b class="neg">−${U(d.total_paid)}</b> · Total to be received: <b class="pos">+${U(d.total_received)}</b>`;
  const P=document.getElementById('fp_payers'),R=document.getElementById('fp_receivers');
  P.innerHTML=(d.payers||[]).map((r,i)=>`<tr><td>${i+1}</td>
   <td class="mkt"><a href="#wallet=${r.account}">${shortAddr(r.account)}</a></td>
   <td class="neg">${U(r.funding)}</td>
   <td>${U(r.unsettled)}</td>
   <td>${U(r.portfolio)}</td></tr>`).join('')||'<tr><td class="empty" colspan=5>Indexing… come back in a few minutes.</td></tr>';
  R.innerHTML=(d.receivers||[]).map((r,i)=>`<tr><td>${i+1}</td>
   <td class="mkt"><a href="#wallet=${r.account}">${shortAddr(r.account)}</a></td>
   <td class="pos">+${U(r.funding)}</td>
   <td>${U(r.unsettled)}</td>
   <td>${U(r.portfolio)}</td></tr>`).join('')||'<tr><td class="empty" colspan=5>Indexing…</td></tr>';
 }catch(e){document.getElementById('fp_status').textContent='Error.';}}

// Liquidaciones
let LQ_DATA=null;
async function loadLiq(){
 try{const d=await (await fetch('/api/liquidations')).json();LQ_DATA=d;
  document.getElementById('lq_status').innerHTML=`<b style="color:var(--green)">${d.count} liquidations in 24h</b> · ${agoStr(d.last_update)} ago`;
  renderLiq();
 }catch(e){document.getElementById('lq_status').textContent='Error.';}}
function renderLiq(){
 const d=LQ_DATA;if(!d)return;
 // Banner with totals — notional liquidated is the standard industry headline metric
 const banner=heroBanner({icon:'💀',label:'NOTIONAL LIQUIDATED · LAST 24H',value:U(d.total_notional||0),
  meta:`${d.count||0} liquidations · −${U(d.total_liq_loss||0)} realized losses to traders`,
  side:`<div>Last update<br><b>${agoStr(d.last_update||0)}</b> ago</div>`});
 // Pain podium: top 3 biggest losses
 const ents=(d.entries||[]).slice();
 ents.sort((a,b)=>Math.abs(b.realized_pnl||0)-Math.abs(a.realized_pnl||0));
 const top3=ents.slice(0,3).map(r=>({...r,account:r.account,_neg:true}));
 let pod='';
 if(top3.length>=3){
  const cls={0:'p1 ppain1',1:'p2 ppain2',2:'p3 ppain3'};
  const med={0:'💀',1:'😵',2:'🥀'};
  const lbl={0:'WORST WIPE',1:'2ND',2:'3RD'};
  const order=[1,0,2];
  pod=`<div class="podium">${order.map(i=>{const it=top3[i];if(!it)return '<div></div>';
   return `<a class="ppos ${cls[i]}" href="#wallet=${it.account}">
    <div class="pmedal">${med[i]}</div>
    <div class="pavatar">${identicon(it.account,38)}</div>
    <div class="paddr">${shortAddr(it.account)}</div>
    <div class="plbl">${lbl[i]}</div>
    <div class="pval neg">−${U(Math.abs(it.realized_pnl))}</div>
    <div class="psub">${it.market} · ${U(it.notional)} notional</div>
   </a>`;}).join('')}</div>`;
 }
 const filt=lbFilterBar('lq',{minVal:true,minLabel:'loss'});
 const tbl0=document.getElementById('lq_body').closest('table');
 ensureHero('lq',tbl0).innerHTML=banner+pod+filt;
 const tot_old=document.getElementById('lq_totals');if(tot_old)tot_old.style.display='none';
 wireFilterBar('lq',renderLiq);
 // Rows with identicons + bar fill
 const all=(d.entries||[]).map(e=>({...e,loss:Math.abs(e.realized_pnl||0)}));
 const rows=filterRows(all,'lq','account','loss');
 document.getElementById('lq_shown').textContent=`Showing ${rows.length} of ${all.length}`;
 const maxLoss=Math.max(...rows.map(r=>r.loss||0),1);
 const tb=document.getElementById('lq_body');
 tb.innerHTML=rows.map((e,i)=>{const dt=new Date(e.ts*1000);
  const dts=dt.toISOString().slice(11,16)+' '+dt.toISOString().slice(5,10);
  return `<tr><td style="font-family:var(--mono);font-size:11.5px;color:var(--muted)">${dts}</td>
   <td>${addrCell(e.account,{tier:e.notional})}</td>
   <td><b>${e.market}</b></td>
   <td><span class="pillside ${e.position_side}">${e.position_side}</span></td>
   <td>${(+e.size).toLocaleString('en-US',{maximumFractionDigits:4})}</td>
   <td>${P(e.price)}</td>
   <td>${U(e.notional)}</td>
   <td>${barFillCell('−'+U(e.loss),e.loss,maxLoss,'neg')}</td></tr>`;}).join('')||'<tr><td class="empty" colspan=8>No liquidations detected.</td></tr>';
 const tbl=document.getElementById('lq_body').closest('table');if(tbl){if(!tbl.id)tbl.id='lq_tbl';stagger(tbl.id);}
}

// Live activity feed
let FD_FILTER='all';
document.querySelectorAll('#fd_filter button').forEach(b=>b.onclick=()=>{
 document.querySelectorAll('#fd_filter button').forEach(x=>x.classList.remove('active'));
 b.classList.add('active');FD_FILTER=b.dataset.f;loadFeed();});
let FD_DATA=null;
async function loadFeed(){
 try{const url='/api/live-activity'+(FD_FILTER==='liq'?'?only_liq=true':'');
  const d=await (await fetch(url)).json();FD_DATA=d;
  renderFeed();
 }catch(e){document.getElementById('fd_status').textContent='Error.';}}
function renderFeed(){
 const d=FD_DATA;if(!d)return;
 let rows=d.entries||[];
 if(FD_FILTER==='big')rows=rows.filter(e=>!e.is_liq);
 // Banner
 const nLiq=rows.filter(e=>e.is_liq).length;const nBig=rows.length-nLiq;
 const totalNotional=rows.reduce((s,e)=>s+(e.notional||0),0);
 const banner=heroBanner({icon:'⚡',label:'LIVE ACTIVITY · LAST 24H',value:U(totalNotional),
  meta:`${rows.length} events · ${nBig} whale trades · ${nLiq} liquidations`,
  side:`<div>Last update<br><b>${agoStr(d.last_update||0)}</b> ago</div>`});
 const filt=lbFilterBar('fd',{minVal:true,minLabel:'notional'});
 const tbl0=document.getElementById('fd_body').closest('table');
 ensureHero('fd',tbl0).innerHTML=banner+filt;
 const tot_old=document.getElementById('fd_totals');if(tot_old)tot_old.style.display='none';
 wireFilterBar('fd',renderFeed);
 // Filter
 const filtered=filterRows(rows,'fd','account','notional');
 document.getElementById('fd_shown').textContent=`Showing ${filtered.length} of ${rows.length}`;
 const maxN=Math.max(...filtered.map(r=>r.notional||0),1);
 const tb=document.getElementById('fd_body');
 tb.innerHTML=filtered.map(e=>{const dt=new Date(e.ts*1000);
  const dts=dt.toISOString().slice(11,19);
  const tcls=e.is_liq?'liq-near':(e.role==='TAKER'?'role-T':'role-M');
  const tag=e.is_liq?'<span class="liq-near" style="font-size:11px;font-weight:700">LIQ</span>':`<span class="${tcls}">${e.role||'—'}</span>`;
  const pnl=e.realized_pnl;const pc=pnl>=0?'pos':'neg';
  return `<tr><td style="font-family:var(--mono);font-size:11.5px;color:var(--muted)">${dts}</td>
   <td>${tag}</td>
   <td>${addrCell(e.account,{tier:e.notional})}</td>
   <td><b>${e.market}</b></td>
   <td><span class="pillside ${e.position_side}">${e.position_side}</span></td>
   <td>${e.role==='TAKER'?'<span class="role-T">T</span>':'<span class="role-M">M</span>'}</td>
   <td>${(+e.size).toLocaleString('en-US',{maximumFractionDigits:4})}</td>
   <td>${P(e.price)}</td>
   <td>${barFillCell(U(e.notional),e.notional,maxN)}</td>
   <td class="${pc}">${pnl===0?'—':(pnl>=0?'+':'')+U(pnl)}</td></tr>`;}).join('')||'<tr><td class="empty" colspan=10>No data yet. The indexer fills this feed while scanning accounts.</td></tr>';
 const tbl=document.getElementById('fd_body').closest('table');if(tbl){if(!tbl.id)tbl.id='fd_tbl';stagger(tbl.id);}
}

// Página por mercado
async function loadMarketDetail(mid){
 const out=document.getElementById('md_content');
 out.innerHTML='<div class="empty">Loading…</div>';
 try{const d=await (await fetch('/api/market-detail?market_id='+encodeURIComponent(mid))).json();
  const i=d.info||{};const ipct=chgPct(i);const ch=ipct>=0?'pos':'neg';const fc=i.funding_8h>=0?'pos':'neg';
  const symRaw=(i.name||'').toUpperCase().split('/')[0].split('-')[0];
  let h=`<div class="whead" style="position:relative">
   <div class="market-watermark">${symRaw}</div>
   <div class="row1" style="position:relative;z-index:1">
   <div class="avatar">${(i.name||'?').slice(0,1)}</div>
   <div><div class="addr" style="font-size:18px;font-weight:700">${i.name||mid}</div>
    <div class="note">market #${i.market_id||mid} · max leverage x${i.max_leverage||'—'}</div></div>
   <div class="actions">
    <span class="chip" onclick="const u=location.origin+'/share/market/${mid}';navigator.clipboard.writeText(u);this.textContent='Share link copied ✓';setTimeout(()=>this.textContent='Share',1500)" title="Copy a link that previews nicely on X/Telegram">Share</span>
    <span class="chip" onclick="goHome()">← Back</span>
   </div>
  </div>
  </div>`;
  h+=`<div class="cards">
   <div class="card"><div class="lbl">Price</div><div class="val">${P(i.last_price||0)}</div><div class="meta ${ch}">${ipct>=0?'+':''}${ipct.toFixed(2)}% 24h</div></div>
   <div class="card"><div class="lbl">24h Volume</div><div class="val">${U(i.volume_24h||0)}</div></div>
   <div class="card"><div class="lbl">Open Interest</div><div class="val">${U(i.oi_usd||0)}</div><div class="meta">${d.n_long||0} longs · ${d.n_short||0} shorts</div></div>
   <div class="card"><div class="lbl">Funding 8h</div><div class="val ${fc}">${i.funding_8h>=0?'+':''}${(i.funding_8h*100).toFixed(4)}%</div><div class="meta">APR ${i.funding_apr>=0?'+':''}${(i.funding_apr||0).toFixed(2)}%</div></div>
   <div class="card"><div class="lbl">Mark / Index</div><div class="val">${P(i.mark_price||0)}</div><div class="meta">basis ${(i.basis_pct||0).toFixed(3)}%</div></div>
  </div>`;
  // Price chart (live OHLCV from RISEx trading-view-data)
  h+=`<div class="panel" style="padding:18px 18px 14px">
   <div style="display:flex;align-items:center;gap:14px;margin-bottom:12px;flex-wrap:wrap">
    <h2 style="margin:0">📈 Price chart · ${i.name||'#'+mid}</h2>
    <span class="seg" id="ch_iv">
     <button data-i="1m">1m</button>
     <button data-i="5m" class="active">5m</button>
     <button data-i="15m">15m</button>
     <button data-i="1h">1h</button>
     <button data-i="4h">4h</button>
     <button data-i="1d">1d</button>
    </span>
    <span class="draw-tools" id="draw_tools" title="Drawing tools">
     <button data-t="none" class="active" title="No tool (cursor)"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M5 5l7 14 2-6 6-2z" stroke-linejoin="round"/></svg></button>
     <button data-t="hline" title="Horizontal line"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><line x1="3" y1="12" x2="21" y2="12" stroke-linecap="round"/></svg></button>
     <button data-t="trend" title="Trend line"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><line x1="4" y1="20" x2="20" y2="4" stroke-linecap="round"/></svg></button>
     <button data-t="fib" title="Fibonacci retracement"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="12" x2="21" y2="12" stroke-dasharray="3 2"/><line x1="3" y1="18" x2="21" y2="18"/></svg></button>
     <button data-t="clear" title="Clear all drawings"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M3 6h18M8 6v-2a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2M5 6l1 14a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2l1-14" stroke-linecap="round"/></svg></button>
    </span>
    <span class="note" id="ch_status" style="margin-left:auto"></span>
   </div>
   <canvas id="ch_canvas" height="380" style="width:100%;display:block"></canvas>
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
  psTrack('markets',mid);
  setTimeout(()=>loadRelatedMarkets(mid),100);
  // wire candle chart
  if(window._chTimer){clearInterval(window._chTimer);window._chTimer=null;}
  CH_MID=mid; CH_IV='5m';
  document.querySelectorAll('#ch_iv button').forEach(b=>b.onclick=()=>{
   document.querySelectorAll('#ch_iv button').forEach(x=>x.classList.remove('active'));
   b.classList.add('active');CH_IV=b.dataset.i;loadCandles();});
  loadCandles();
  window._chTimer=setInterval(loadCandles,15000);
  window.addEventListener('hashchange',()=>{if(window._chTimer){clearInterval(window._chTimer);window._chTimer=null;}},{once:true});
 }catch(e){out.innerHTML='<div class="empty">Error loading.</div>';}}

// ====== Wallet comparator ======
function cmpList(){return JSON.parse(localStorage.getItem('rise_cmp')||'[]');}
function cmpSave(l){localStorage.setItem('rise_cmp',JSON.stringify(l));}
function cmpAdd(){
 const v=(document.getElementById('cmp_add').value||'').trim();
 if(!/^0x[0-9a-fA-F]{40}$/.test(v)){toast('Invalid address','warn');return;}
 const l=cmpList();
 if(l.includes(v.toLowerCase())){toast('Already added','warn');return;}
 if(l.length>=4){toast('Max 4 wallets','warn');return;}
 l.push(v.toLowerCase());cmpSave(l);
 document.getElementById('cmp_add').value='';
 renderCompare();
 if(l.length>=2)unlockAchievement('compare');
}
function cmpRemove(a){const l=cmpList().filter(x=>x!==a);cmpSave(l);renderCompare();}
function cmpClear(){if(confirm('Clear all wallets from comparison?')){cmpSave([]);renderCompare();}}
async function renderCompare(){
 const out=document.getElementById('cmp_out');if(!out)return;
 const l=cmpList();
 if(!l.length){out.innerHTML='<div class="emptystate"><svg viewBox="0 0 80 80" xmlns="http://www.w3.org/2000/svg"><rect x="8" y="14" width="28" height="52" rx="4" fill="none" stroke="var(--accent)" stroke-width="2"/><rect x="44" y="14" width="28" height="52" rx="4" fill="none" stroke="var(--accent)" stroke-width="2"/><path d="M36 40h8" stroke="var(--accent)" stroke-width="2"/></svg><div class="ttl">No wallets to compare yet</div><div class="sub">Paste an address above to start. Up to 4 wallets side by side.</div></div>';return;}
 out.innerHTML='<div class="empty">Loading '+l.length+' wallet'+(l.length>1?'s':'')+'…</div>';
 const data=await Promise.all(l.map(async a=>{
  try{const [w,st]=await Promise.all([
    fetch('/api/wallet?account='+encodeURIComponent(a)).then(r=>r.json()),
    fetch('/api/wallet-stats?account='+encodeURIComponent(a)).then(r=>r.json())]);
   return {addr:a,w:w.ok?w:null,st:(st&&st.ok)?st:null};
  }catch(e){return {addr:a,w:null,st:null};}
 }));
 const cols=data.length;
 const gridStyle=`display:grid;grid-template-columns:160px repeat(${cols},minmax(180px,1fr));border:1px solid var(--line);border-radius:11px;overflow:hidden;background:var(--panel)`;
 const cellL='padding:13px 16px;font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:1px;font-weight:700;background:rgba(0,255,212,.02);border-right:1px solid var(--line);border-bottom:1px solid var(--line2);display:flex;align-items:center;font-family:-apple-system,Inter,sans-serif';
 const cellV='padding:13px 16px;font-family:var(--mono);font-size:13.5px;color:#fff;border-bottom:1px solid var(--line2);border-right:1px solid var(--line2);display:flex;align-items:center';
 const head='padding:14px 16px;border-bottom:1px solid var(--line);border-right:1px solid var(--line2);background:linear-gradient(180deg,rgba(0,255,212,.04),transparent);display:flex;flex-direction:column;gap:8px';
 let html=`<div style="${gridStyle}">`;
 html+=`<div style="${cellL};background:transparent;border-right:1px solid var(--line)"></div>`;
 for(const d of data){
  const short=d.addr.slice(0,6)+'…'+d.addr.slice(-4);
  const smart=d.w&&d.w.summary&&d.w.summary.smart_money;
  html+=`<div style="${head}">
   <div style="display:flex;align-items:center;gap:8px">
    <span style="width:32px;height:32px;border-radius:7px;overflow:hidden;border:1px solid rgba(255,255,255,.08);flex:0 0 32px">${identicon(d.addr,32)}</span>
    <div style="flex:1;min-width:0">
     <div style="font-family:var(--mono);font-size:12px;font-weight:700;white-space:nowrap;overflow:hidden;text-overflow:ellipsis"><a href="#wallet=${d.addr}" style="color:#fff;border:none">${short}</a></div>
     ${smart?'<div><span class="smart-badge" style="font-size:8.5px;padding:1.5px 5px">SMART</span></div>':''}
    </div>
    <span style="cursor:pointer;color:var(--muted);font-size:18px;padding:0 4px;line-height:1" onclick="cmpRemove('${d.addr}')" title="Remove">×</span>
   </div>
  </div>`;
 }
 const rows=[
  {lbl:'Balance',get:d=>U(d.w?.balance)},
  {lbl:'Equity',get:d=>{const b=d.w?.balance;const u=d.w?.summary?.total_upnl||0;return b!=null?U(b+u):'—';}},
  {lbl:'Unrealized PnL',get:d=>{const v=d.w?.summary?.total_upnl;return v==null?'—':`<span class="${v>=0?'pos':'neg'}">${v>=0?'+':''}${U(v)}</span>`;}},
  {lbl:'Open positions',get:d=>d.w?.summary?.num_positions ?? '—'},
  {lbl:'Total notional',get:d=>U(d.w?.summary?.total_notional)},
  {lbl:'Realized PnL · 30d',get:d=>{const v=d.w?.summary?.realized_pnl_30d;return v==null?'—':`<span class="${v>=0?'pos':'neg'}">${v>=0?'+':''}${U(v)}</span>`;}},
  {lbl:'Volume · 30d',get:d=>U(d.w?.summary?.volume?.['30d'])},
  {lbl:'Trades · 30d',get:d=>d.w?.summary?.volume?.trades_30d ?? '—'},
  {lbl:'Win rate',get:d=>d.st?.win_rate_pct!=null?d.st.win_rate_pct.toFixed(1)+'%':'—'},
  {lbl:'Profit factor',get:d=>{const v=d.st?.profit_factor;return v==null?'—':`<span class="${v>=1?'pos':'neg'}">${v.toFixed(2)}</span>`;}},
  {lbl:'Max drawdown',get:d=>{const v=d.st?.max_drawdown;return v==null?'—':`<span class="neg">−${U(v)}</span>`;}},
  {lbl:'Avg trade size',get:d=>U(d.st?.avg_trade_size)},
  {lbl:'Largest trade',get:d=>U(d.st?.largest_trade_notional)},
  {lbl:'Best trade',get:d=>{const v=d.st?.best_trade_pnl;return v==null?'—':`<span class="pos">+${U(v)}</span>`;}},
  {lbl:'Worst trade',get:d=>{const v=d.st?.worst_trade_pnl;return v==null?'—':`<span class="neg">${U(v)}</span>`;}},
  {lbl:'Liquidations',get:d=>{const v=d.st?.n_liquidations||0;return v>0?`<span class="neg">${v}</span>`:'<span style="color:var(--muted)">0</span>';}},
  {lbl:'Days active',get:d=>d.st?.days_active ?? '—'},
  {lbl:'Trades / day',get:d=>d.st?.trades_per_day ?? '—'},
 ];
 for(const r of rows){
  html+=`<div style="${cellL}">${r.lbl}</div>`;
  for(const d of data) html+=`<div style="${cellV}">${r.get(d)??'—'}</div>`;
 }
 html+='</div>';
 out.innerHTML=html;
}

// =========== Position simulator + Funding cost calculator ===========
let SIM_SIDE='long', SIM_MM='isolated', FC_SIDE='long';
function _mktByName(name){return (DATA&&DATA.markets||[]).find(m=>m.name===name);}
function _mktById(id){return (DATA&&DATA.markets||[]).find(m=>String(m.market_id)===String(id));}
function initTools(){
 const ms=DATA&&DATA.markets;if(!ms||!ms.length){setTimeout(initTools,500);return;}
 const opts=ms.map(m=>`<option value="${m.market_id}">${m.name}</option>`).join('');
 const s1=document.getElementById('sim_mkt');if(s1&&!s1.dataset.ready){s1.innerHTML=opts;s1.dataset.ready=1;s1.onchange=onSimMarketChange;}
 const s2=document.getElementById('fc_mkt');if(s2&&!s2.dataset.ready){s2.innerHTML=opts;s2.dataset.ready=1;s2.onchange=updateFunding;}
 // wire segs once
 document.querySelectorAll('#sim_side button').forEach(b=>b.onclick=()=>{document.querySelectorAll('#sim_side button').forEach(x=>x.classList.remove('active'));b.classList.add('active');SIM_SIDE=b.dataset.d;updateSim();});
 document.querySelectorAll('#sim_mm button').forEach(b=>b.onclick=()=>{document.querySelectorAll('#sim_mm button').forEach(x=>x.classList.remove('active'));b.classList.add('active');SIM_MM=b.dataset.m;updateSim();});
 document.querySelectorAll('#fc_side button').forEach(b=>b.onclick=()=>{document.querySelectorAll('#fc_side button').forEach(x=>x.classList.remove('active'));b.classList.add('active');FC_SIDE=b.dataset.d;updateFunding();});
 ['sim_entry','sim_size','sim_lev','sim_coll','sim_mark'].forEach(id=>document.getElementById(id).oninput=updateSim);
 ['fc_not','fc_days'].forEach(id=>document.getElementById(id).oninput=updateFunding);
 // first run with first market
 onSimMarketChange();updateFunding();
}
function onSimMarketChange(){
 const m=_mktById(document.getElementById('sim_mkt').value);if(!m)return;
 const px=+m.mark_price||+m.last_price||0;
 if(!document.getElementById('sim_entry').value)document.getElementById('sim_entry').value=px;
 if(!document.getElementById('sim_size').value)document.getElementById('sim_size').value=(1000/px).toFixed(6);
 document.getElementById('sim_mark').value=px;
 updateSim();
}
function useCurrentMark(){const m=_mktById(document.getElementById('sim_mkt').value);if(m)document.getElementById('sim_mark').value=(+m.mark_price||+m.last_price||0);updateSim();}
function autofillSize(){
 const m=_mktById(document.getElementById('sim_mkt').value);if(!m)return;
 const entry=+document.getElementById('sim_entry').value||+m.mark_price;
 const notUSD=prompt('Notional in USD?','10000');if(!notUSD)return;
 document.getElementById('sim_size').value=(+notUSD/entry).toFixed(6);updateSim();
}
function _mmrFromMarket(m){
 // m.mmf_bps in basis points; if missing default 50bps (0.5%)
 const bps=+m?.mmf_bps;return bps>0?bps/10000:0.005;
}
function updateSim(){
 const m=_mktById(document.getElementById('sim_mkt').value);if(!m)return;
 const entry=+document.getElementById('sim_entry').value;
 const size=+document.getElementById('sim_size').value;
 const lev=Math.max(1,+document.getElementById('sim_lev').value||1);
 const coll=+document.getElementById('sim_coll').value||0;
 const mark=+document.getElementById('sim_mark').value||entry;
 if(!entry||!size){return;}
 const notional=entry*size;
 const im=notional/lev;
 const mmr=_mmrFromMarket(m);
 const dir=SIM_SIDE==='long'?1:-1;
 // liq price
 let liq;
 if(SIM_MM==='isolated'){
  // isolated: margin = im; liq when loss = im - mmr*notional_at_liq
  // approx: liq = entry × (1 - dir/lev + dir*mmr)
  liq=entry*(1 - dir*(1/lev) + dir*mmr);
 } else {
  // cross: total available = im + extra collateral; liq when notional * (mmr) >= equity
  const totEq=im+coll;
  // loss-at-liq = size * |liq-entry| (in same direction as adverse move)
  // equation: totEq - size*|liq-entry| = mmr*size*liq
  // long: totEq - size*(entry-liq) = mmr*size*liq → liq = (size*entry - totEq) / (size*(1-mmr)) ... rearrange:
  // For long: totEq - size*entry + size*liq = mmr*size*liq → liq = (size*entry - totEq) / (size*(1-mmr))
  // For short: totEq - size*(liq-entry) = mmr*size*liq → totEq + size*entry = size*liq*(1+mmr) → liq=(totEq+size*entry)/(size*(1+mmr))
  if(SIM_SIDE==='long'){liq=(size*entry-totEq)/(size*(1-mmr));}
  else{liq=(totEq+size*entry)/(size*(1+mmr));}
  if(liq<0)liq=0;
 }
 const pnl=size*(mark-entry)*dir;
 const roi=(pnl/im)*100;
 const distLiq=(liq-entry)/entry*100;
 document.getElementById('sim_not').textContent=U(notional);
 document.getElementById('sim_im').textContent=U(im);
 document.getElementById('sim_liq').innerHTML=`<span class="${SIM_SIDE==='long'?'neg':'pos'}">${P(liq)}</span>`;
 document.getElementById('sim_liq_meta').textContent=`${distLiq>=0?'+':''}${distLiq.toFixed(2)}% from entry · MMR ${(mmr*100).toFixed(2)}%`;
 const pc=pnl>=0?'pos':'neg';
 document.getElementById('sim_pnl').innerHTML=`<span class="${pc}">${pnl>=0?'+':''}${U(pnl)}</span>`;
 document.getElementById('sim_pnl_meta').textContent=`ROI ${roi>=0?'+':''}${roi.toFixed(2)}% · mark ${P(mark)}`;
 // ladder ±20%, step 1%
 const tb=document.getElementById('sim_ladder');let html='';
 const steps=[];for(let p=20;p>=-20;p--)steps.push(p);
 for(const pct of steps){
  const price=entry*(1+pct/100);
  const pl=size*(price-entry)*dir;
  const r=(pl/im)*100;
  let status='';
  if((SIM_SIDE==='long'&&price<=liq)||(SIM_SIDE==='short'&&price>=liq)){status='<span class="liq-near">LIQUIDATED</span>';}
  else if(pl>=im*2){status='<span class="pos">+2R</span>';}
  else if(pl<=-im*0.5){status='<span class="neg">−50% margin</span>';}
  const cls=pl>=0?'pos':'neg';const cur=Math.abs(price-mark)/mark<0.005?' style="background:rgba(151,252,228,.06)"':'';
  html+=`<tr${cur}><td>${P(price)}</td><td class="${pct>=0?'pos':'neg'}">${pct>=0?'+':''}${pct}%</td><td class="${cls}">${pl>=0?'+':''}${U(pl)}</td><td class="${cls}">${r>=0?'+':''}${r.toFixed(1)}%</td><td>${status}</td></tr>`;
 }
 tb.innerHTML=html;
}
function updateFunding(){
 const m=_mktById(document.getElementById('fc_mkt').value);if(!m){return;}
 const notional=+document.getElementById('fc_not').value||0;
 const days=+document.getElementById('fc_days').value||0;
 const fund8h=+m.funding_8h||0;
 const apr=fund8h*3*365*100; // %
 const cycles=days*3; // 3 per day
 // direction: long pays when funding>0; short pays when funding<0. positive = paid, negative = received.
 const dirMul=FC_SIDE==='long'?1:-1;
 const per8 = notional*fund8h*dirMul;
 const per8_per_1k = (1000*fund8h*dirMul);
 const totalPaid = per8 * cycles;
 const breakeven = (totalPaid/notional)*100; // % price move needed
 document.getElementById('fc_apr').innerHTML=`<span class="${apr>=0?'pos':'neg'}">${apr>=0?'+':''}${apr.toFixed(2)}%</span>`;
 document.getElementById('fc_apr_meta').textContent=`8h rate ${(fund8h*100).toFixed(4)}%`;
 document.getElementById('fc_per8').innerHTML=`<span class="${per8_per_1k>=0?'neg':'pos'}">${per8_per_1k>=0?'−':'+'}${U(Math.abs(per8_per_1k))}</span>`;
 document.getElementById('fc_total').innerHTML=`<span class="${totalPaid>=0?'neg':'pos'}">${totalPaid>=0?'−':'+'}${U(Math.abs(totalPaid))}</span>`;
 document.getElementById('fc_total_meta').textContent=`${cycles.toFixed(1)} funding cycles · ${FC_SIDE==='long'?(fund8h>=0?'paying':'receiving'):(fund8h>=0?'receiving':'paying')}`;
 const beMove=Math.abs(breakeven);
 document.getElementById('fc_be').innerHTML=`<span>${beMove.toFixed(3)}%</span>`;
}

// =========== Real-time candle chart (custom canvas renderer) ===========
let CH_MID=null, CH_IV='5m', CH_DATA=null, CH_TICKS=null;
let CH_SHOW_CVD=false, CH_SHOW_MARK=false;
async function loadCandles(){
 if(!CH_MID)return;
 try{
  const dc=await fetch(`/api/candles?market_id=${CH_MID}&interval=${CH_IV}&limit=240`).then(r=>r.json());
  if(!dc.ok||!dc.candles||!dc.candles.length){
   const s=document.getElementById('ch_status');if(s)s.textContent='No data';return;
  }
  CH_DATA=dc.candles;CH_TICKS=null;
  drawCandles();
  const last=dc.candles[dc.candles.length-1];
  const first=dc.candles[0];
  const chg=(last.c-first.o)/first.o*100;
  const chgCls=chg>=0?'pos':'neg';
  document.getElementById('ch_status').innerHTML=
   `<span style="color:var(--accent)">${dc.count} candles</span> · last ${P(last.c)} <span class="${chgCls}">${chg>=0?'+':''}${chg.toFixed(2)}%</span> · refresh 15s`;
 }catch(e){const s=document.getElementById('ch_status');if(s)s.textContent='Error.';}
}
function drawCandles(){
 const canvas=document.getElementById('ch_canvas');if(!canvas||!CH_DATA||!CH_DATA.length)return;
 const dpr=window.devicePixelRatio||1;
 const cssW=canvas.clientWidth||800;const cssH=380;
 canvas.width=Math.round(cssW*dpr);canvas.height=Math.round(cssH*dpr);
 canvas.style.height=cssH+'px';
 const ctx=canvas.getContext('2d');ctx.scale(dpr,dpr);
 ctx.clearRect(0,0,cssW,cssH);
 const padL=10,padR=68,padT=10,padB=24;
 const volH=70;
 const priceH=cssH-padT-padB-volH-6;
 const W=cssW-padL-padR, n=CH_DATA.length;
 // price range
 let hi=-Infinity,lo=Infinity,maxV=0;
 for(const c of CH_DATA){if(c.h>hi)hi=c.h;if(c.l<lo)lo=c.l;if(c.v>maxV)maxV=c.v;}
 const range=hi-lo||hi*0.01||1;hi+=range*0.04;lo-=range*0.04;
 const cw=W/n;
 const bodyW=Math.max(1,Math.min(cw*0.7,14));
 // background grid
 ctx.strokeStyle='#1a2129';ctx.lineWidth=1;ctx.font='10px ui-monospace,Menlo,monospace';ctx.fillStyle='#6b7785';
 for(let i=0;i<=4;i++){
  const y=padT+(priceH/4)*i;
  ctx.beginPath();ctx.moveTo(padL,y);ctx.lineTo(padL+W,y);ctx.stroke();
  const px=hi-((hi-lo)/4)*i;
  ctx.textAlign='left';ctx.fillText(px.toLocaleString('en-US',{maximumFractionDigits:px<10?5:(px<1000?3:2)}),padL+W+4,y+3);
 }
 // candles
 const px2y=p=>padT+(hi-p)/(hi-lo)*priceH;
 for(let i=0;i<n;i++){
  const c=CH_DATA[i];
  const x=padL+i*cw+cw/2;
  const up=c.c>=c.o;
  const col=up?'#36d39c':'#ff5466';
  ctx.strokeStyle=col;ctx.fillStyle=col;
  ctx.lineWidth=1;
  // wick
  ctx.beginPath();ctx.moveTo(x,px2y(c.h));ctx.lineTo(x,px2y(c.l));ctx.stroke();
  // body
  const yo=px2y(c.o),yc=px2y(c.c);
  const ty=Math.min(yo,yc),bh=Math.max(1,Math.abs(yc-yo));
  ctx.fillRect(x-bodyW/2,ty,bodyW,bh);
 }
 // volume bars
 const volTop=padT+priceH+6;
 ctx.fillStyle='#6b7785';ctx.font='9px -apple-system,Inter,sans-serif';
 ctx.fillText('VOLUME',padL,volTop-2);
 for(let i=0;i<n;i++){
  const c=CH_DATA[i];const x=padL+i*cw+cw/2;
  const vh=maxV>0?(c.v/maxV)*volH:0;
  const up=c.c>=c.o;
  ctx.fillStyle=up?'rgba(54,211,156,.45)':'rgba(255,84,102,.45)';
  ctx.fillRect(x-bodyW/2,volTop+volH-vh,bodyW,vh);
 }
 // x-axis time labels (4-6)
 ctx.fillStyle='#6b7785';ctx.font='10px ui-monospace,Menlo,monospace';
 const nLabels=Math.min(6,n);const step=Math.max(1,Math.floor(n/nLabels));
 const intvSec={'1m':60,'5m':300,'15m':900,'1h':3600,'4h':14400,'1d':86400}[CH_IV]||60;
 ctx.textAlign='center';
 for(let i=0;i<n;i+=step){
  const c=CH_DATA[i];const x=padL+i*cw+cw/2;
  const dt=new Date(c.t*1000);
  const lab=intvSec>=86400?dt.toISOString().slice(5,10)
           :intvSec>=3600?dt.toISOString().slice(8,10)+' '+dt.toISOString().slice(11,16)
           :dt.toISOString().slice(11,16);
  ctx.fillText(lab,x,cssH-6);
 }
 // last price line
 const last=CH_DATA[CH_DATA.length-1];
 const ly=px2y(last.c);
 ctx.setLineDash([3,3]);ctx.strokeStyle='#97FCE4';ctx.lineWidth=1;
 ctx.beginPath();ctx.moveTo(padL,ly);ctx.lineTo(padL+W,ly);ctx.stroke();
 ctx.setLineDash([]);
 // last price tag
 ctx.fillStyle='#97FCE4';ctx.fillRect(padL+W+1,ly-9,padR-4,16);
 ctx.fillStyle='#070a0d';ctx.font='bold 10px ui-monospace,Menlo,monospace';ctx.textAlign='left';
 ctx.fillText(last.c.toLocaleString('en-US',{maximumFractionDigits:last.c<10?5:(last.c<1000?3:2)}),padL+W+4,ly+3);
}
window.addEventListener('resize',()=>{if(CH_DATA)drawCandles();});

async function loadUsers(){
 try{
  let d=window._earlyFetch&&window._earlyFetch.users?await window._earlyFetch.users:null;
  if(window._earlyFetch)window._earlyFetch.users=null;
  if(!d)d=await (await fetch('/api/users')).json();
  document.getElementById('us_status').innerHTML=`<span style="color:var(--green)">Data from the official explorer</span>`;
  // RISEx perp-specific metrics
  document.getElementById('us_total').textContent=(d.total_traders||0).toLocaleString('en-US');
  document.getElementById('us_active_1d').textContent=(d.active_1d||0).toLocaleString('en-US');
  document.getElementById('us_active_7d').textContent=(d.active_7d||0).toLocaleString('en-US');
  document.getElementById('us_active_30d').textContent=(d.active_30d||0).toLocaleString('en-US');
  document.getElementById('us_active').textContent=(d.active_with_position||0).toLocaleString('en-US');
  // Backward-compat for any other dependent code
  const usAddr=document.getElementById('us_addr');if(usAddr)usAddr.textContent='—';
  const usToday=document.getElementById('us_today');if(usToday)usToday.textContent='+'+(d.new_today||0);
  const us7d=document.getElementById('us_7d');if(us7d)us7d.textContent='+'+(d.new_7d||0);
  const s=d.series||[];const lab=s.map(x=>x.date.slice(5));
  intChart('usNew','bar',lab,s.map(x=>x.new),'#33d6a6');
  intChart('usCum','line',lab,s.map(x=>x.cum),'#50ddc2');
  intChart('usAct','bar',lab,s.map(x=>x.active),'#ffb454');
 }catch(e){document.getElementById('us_status').textContent='Error.';}}

document.getElementById('al_vol').value=alerts.vol||'';
document.getElementById('al_oi').value=alerts.oi||'';
document.getElementById('al_fund').value=alerts.fund||'';
function saveAlerts(){alerts.vol=document.getElementById('al_vol').value;alerts.oi=document.getElementById('al_oi').value;
 alerts.fund=document.getElementById('al_fund').value;localStorage.setItem('rise_alerts',JSON.stringify(alerts));
 document.getElementById('al_status').textContent='Saved ✓';if(DATA)renderOverview(DATA);}
function askNotif(){if('Notification'in window)Notification.requestPermission().then(p=>document.getElementById('al_status').textContent=p==='granted'?'Active ✓':'Permission denied');}

// Telegram alerts (token + chat_id stored in localStorage)
const tg=JSON.parse(localStorage.getItem('rise_tg')||'{}');
const tgI=document.getElementById('tg_token');if(tgI)tgI.value=tg.token||'';
const tgC=document.getElementById('tg_chat');if(tgC)tgC.value=tg.chat||'';
function saveTg(){
 tg.token=document.getElementById('tg_token').value.trim();
 tg.chat=document.getElementById('tg_chat').value.trim();
 localStorage.setItem('rise_tg',JSON.stringify(tg));
 document.getElementById('tg_status').textContent='Saved ✓';
}
async function sendTg(text){
 if(!tg.token||!tg.chat)return false;
 try{const r=await fetch(`https://api.telegram.org/bot${tg.token}/sendMessage`,{
  method:'POST',headers:{'Content-Type':'application/json'},
  body:JSON.stringify({chat_id:tg.chat,text:text,parse_mode:'HTML',disable_web_page_preview:true})});
  const d=await r.json();return d.ok===true;
 }catch(e){return false;}
}
async function testTg(){
 const st=document.getElementById('tg_status');st.textContent='Sending…';
 const ok=await sendTg('✅ <b>RISExscan</b>\nTelegram alerts are working!');
 st.textContent=ok?'Test message sent ✓':'Failed — check token & chat ID';
}

let lastNotif={};
function notify(msg){
 const now=Date.now();if(lastNotif[msg]&&now-lastNotif[msg]<300000)return;lastNotif[msg]=now;
 // Browser notification
 if('Notification'in window&&Notification.permission==='granted'){
  try{new Notification('RISEx',{body:msg});}catch(e){}}
 // Telegram (if configured)
 if(tg.token&&tg.chat){sendTg('🚨 <b>RISEx alert</b>\n'+msg);}
}

// Whale Trades feed (overview)
let BT_MIN=100000, BT_SORT='size';
document.querySelectorAll('#bt_min button').forEach(b=>b.onclick=()=>{
 document.querySelectorAll('#bt_min button').forEach(x=>x.classList.remove('active'));
 b.classList.add('active');BT_MIN=+b.dataset.m;loadBigTrades();});
document.querySelectorAll('#bt_sort button').forEach(b=>b.onclick=()=>{
 document.querySelectorAll('#bt_sort button').forEach(x=>x.classList.remove('active'));
 b.classList.add('active');BT_SORT=b.dataset.s;loadBigTrades();});
async function loadBigTrades(){
 try{const d=await (await fetch(`/api/big-trades?min_usd=${BT_MIN}&sort=${BT_SORT}`)).json();
  const tb=document.getElementById('bt_body');if(!tb)return;
  const longN=d.by_side?.Long||0,shortN=d.by_side?.Short||0;
  document.getElementById('bt_totals').innerHTML=
   `<b>${d.count}</b> whales · total <b>${U(d.total_notional)}</b> · `+
   `<span class="pos">L ${U(longN)}</span> / <span class="neg">S ${U(shortN)}</span>`;
  tb.innerHTML=(d.entries||[]).map(e=>{const dt=new Date(e.ts*1000);
   const dts=dt.toISOString().slice(11,19);
   return `<tr><td style="font-family:ui-monospace,monospace;font-size:12px;color:var(--muted)">${dts}</td>
    <td class="mkt"><a href="#wallet=${e.account}">${shortAddr(e.account)}</a></td>
    <td>${e.market}</td>
    <td><span class="pillside ${e.position_side}">${e.position_side}</span></td>
    <td>${(+e.size).toLocaleString('en-US',{maximumFractionDigits:4})}</td>
    <td>${P(e.price)}</td>
    <td><b>${U(e.notional)}</b></td>
    <td>${e.role==='TAKER'?'<span class="role-T">T</span>':'<span class="role-M">M</span>'}</td></tr>`;
  }).join('')||'<tr><td class="empty" colspan=8>No whale trades captured yet at this threshold.</td></tr>';
 }catch(e){}
}
async function loadAll(){const dot=document.getElementById('dot');
 try{
  let d=window._earlyFetch&&window._earlyFetch.data?await window._earlyFetch.data:null;
  if(window._earlyFetch)window._earlyFetch.data=null;
  if(!d)d=await (await fetch('/api/data',{cache:'no-store'})).json();
  DATA=d;
  renderOverview(d);renderMarkets();
  // Stagger non-critical loads to keep the hero rendering snappy
  setTimeout(loadHistory, 50);
  setTimeout(loadUsers, 250);
  const dt=new Date((d.generated_at||0)*1000);
  document.getElementById('updated').textContent='updated '+dt.toLocaleTimeString('en-US');
  dot.style.background='#36d39c';
 }catch(e){dot.style.background='#ff5d6c';document.getElementById('updated').textContent='no connection';}}
loadAll();setInterval(loadAll,45000);
loadSparks();setInterval(loadSparks,300000);
loadDailyStory();setInterval(loadDailyStory,90000);
loadTrending();
setTimeout(attachTilt,800);
setInterval(()=>{
 if(document.getElementById('v_ranking').classList.contains('active'))loadRanking();
 if(document.getElementById('v_acctoi').classList.contains('active'))loadAcctOi();
 if(document.getElementById('v_users').classList.contains('active'))loadUsers();
 if(document.getElementById('v_volranking').classList.contains('active'))loadVolRanking();
 if(document.getElementById('v_oiranking').classList.contains('active'))loadOiRanking();
 if(document.getElementById('v_funding').classList.contains('active'))loadFunding();
 if(document.getElementById('v_pnl').classList.contains('active'))loadPnl();
 if(document.getElementById('v_funded').classList.contains('active'))loadFunded();
 if(document.getElementById('v_liq').classList.contains('active'))loadLiq();
 if(document.getElementById('v_feed').classList.contains('active'))loadFeed();
 if(document.getElementById('v_longshort').classList.contains('active'))loadLongShort();
 if(document.getElementById('v_marketshare').classList.contains('active'))loadMarketShare();
},15000);
</script></body></html>"""


if __name__ == "__main__":
    main()
