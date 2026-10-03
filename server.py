#!/usr/bin/env python3
"""
wxwhales dashboard — a local web app over wxwhales.py.

  python3 server.py            # serves http://127.0.0.1:8765 and opens your browser
  python3 server.py --no-open  # headless

A background thread keeps the SQLite DB current (fast sync every `interval` seconds,
full event discovery every 10 minutes, leaderboard hourly) and fires macOS
notifications for alert-worthy trades when enabled. Binds to localhost only.
Stdlib only.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import threading
import time
import traceback
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import wxwhales as wx

HERE = os.path.dirname(os.path.abspath(__file__))
STATIC = os.path.join(HERE, "static")
SETTINGS_PATH = os.path.join(HERE, "data", "settings.json")

DEFAULT_SETTINGS = {
    "interval": 30,          # seconds between fast syncs
    "min_trade": 1000,       # alert on any trade ≥ this
    "min_usd": 25,           # alert on watched wallets' trades ≥ this (also storage floor)
    "top": 20,               # auto-watch top-N wallets (DB PnL + official leaderboard)
    "watch_days": 30,
    "include_sweeps": False,  # alert on fills at ≥0.98 / ≤0.02
    "notify": False,          # macOS notifications
}

STATE = {
    "started": time.time(), "last_sync": None, "last_discover": 0, "last_leaderboard": 0,
    "last_error": None, "syncing": False, "new_last_sync": 0, "events_open": 0,
}
LOCK = threading.Lock()


def load_settings() -> dict:
    s = dict(DEFAULT_SETTINGS)
    try:
        with open(SETTINGS_PATH) as f:
            s.update({k: v for k, v in json.load(f).items() if k in DEFAULT_SETTINGS})
    except FileNotFoundError:
        pass
    except Exception as e:  # noqa: BLE001
        print(f"settings: {e}", file=sys.stderr)
    return s


def save_settings(s: dict):
    os.makedirs(os.path.dirname(SETTINGS_PATH), exist_ok=True)
    with open(SETTINGS_PATH, "w") as f:
        json.dump(s, f, indent=2)


SETTINGS = load_settings()


def db():
    return wx.connect(wx.DEFAULT_DB)


def clean(x):
    """JSON can't carry ±inf/NaN (open-ended buckets); send null."""
    if isinstance(x, float) and (math.isinf(x) or math.isnan(x)):
        return None
    if isinstance(x, dict):
        return {k: clean(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [clean(v) for v in x]
    return x


def is_sweep(price: float) -> bool:
    return price >= 0.98 or price <= 0.02


# --------------------------------------------------------------------------- watchlist

def read_manual_watchlist() -> list[str]:
    out = []
    if os.path.exists(wx.WATCHLIST):
        for line in open(wx.WATCHLIST):
            w = line.split("#", 1)[0].strip().lower()
            if w.startswith("0x"):
                out.append(w)
    return out


def write_manual_watchlist(wallets: list[str]):
    with open(wx.WATCHLIST, "w") as f:
        f.write("# One wallet per line (0x...). Comments after #. Managed by the wxwhales app too.\n")
        for w in sorted(set(wallets)):
            f.write(w + "\n")


_WATCH_CACHE = {"at": 0, "wl": {}}


def watchlist(conn, force=False) -> dict[str, str]:
    if force or time.time() - _WATCH_CACHE["at"] > 300:
        _WATCH_CACHE["wl"] = wx.load_watchlist(conn, SETTINGS["top"], SETTINGS["watch_days"], "pnl")
        _WATCH_CACHE["at"] = time.time()
    return _WATCH_CACHE["wl"]


# --------------------------------------------------------------------------- background sync

def sync_loop(stop: threading.Event):
    conn = db()
    first = True
    while not stop.is_set():
        try:
            with LOCK:
                STATE["syncing"] = True
            now = time.time()
            if now - STATE["last_leaderboard"] > 3600:
                wx.fetch_leaderboard(conn)
                STATE["last_leaderboard"] = now
                watchlist(conn, force=True)
            rediscover = now - STATE["last_discover"] > 600
            # first pass looks back a week so a closed laptop catches up on resolutions
            new = wx.sync(conn, days=(8 if first else 1) if rediscover else 0, min_usd=SETTINGS["min_usd"],
                          quiet=True, discover=rediscover)
            if rediscover:
                STATE["last_discover"] = now
            STATE["events_open"] = conn.execute("SELECT COUNT(*) FROM events WHERE closed=0").fetchone()[0]
            STATE["new_last_sync"] = len(new)
            STATE["last_sync"] = time.time()
            STATE["last_error"] = None
            if not first and SETTINGS["notify"]:
                wl = watchlist(conn)
                for t in sorted(new, key=lambda t: int(t["timestamp"])):
                    usd = float(t["size"]) * float(t["price"])
                    if not SETTINGS["include_sweeps"] and is_sweep(float(t["price"])):
                        continue
                    tag = wl.get((t.get("proxyWallet") or "").lower())
                    if usd >= SETTINGS["min_trade"] or tag:
                        wx.notify("wxwhales", wx.fmt_alert(t, f"[{tag}]" if tag else "[size]"))
            first = False
        except Exception as e:  # noqa: BLE001 — keep the loop alive
            STATE["last_error"] = f"{type(e).__name__}: {e}"
            traceback.print_exc()
        finally:
            with LOCK:
                STATE["syncing"] = False
        stop.wait(SETTINGS["interval"])


# --------------------------------------------------------------------------- api

def api_status(conn, q):
    n_trades = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    newest = conn.execute("SELECT MAX(ts) FROM trades").fetchone()[0]
    return {**STATE, "trades": n_trades, "newest_trade": newest, "now": time.time(),
            "settings": SETTINGS, "watching": len(watchlist(conn))}


def api_feed(conn, q):
    hours = float(q.get("hours", 6))
    min_trade = float(q.get("min_trade", SETTINGS["min_trade"]))
    mode = q.get("mode", "alerts")          # alerts | watched | all
    sweeps = q.get("sweeps", "0") == "1"
    city = q.get("city") or None
    us_only = city == "__us"
    limit = min(int(q.get("limit", 300)), 2000)
    since = int(time.time() - hours * 3600)
    wl = watchlist(conn)
    rows = conn.execute("""
        SELECT t.*, m.label, e.city, e.date, e.kind, e.station FROM trades t
        LEFT JOIN markets m ON m.condition_id=t.condition_id
        LEFT JOIN events e ON e.slug=t.event_slug
        WHERE t.ts >= ? ORDER BY t.ts DESC LIMIT 20000""", (since,)).fetchall()
    out = []
    for r in rows:
        r = dict(r)
        if us_only:
            if r["city"] not in wx.US_CITIES:
                continue
        elif city and r["city"] != city:
            continue
        if not sweeps and is_sweep(r["price"]):
            continue
        tag = wl.get(r["wallet"])
        if mode == "watched" and not tag:
            continue
        if mode == "alerts" and not (r["usd"] >= min_trade or tag):
            continue
        if mode == "all" and r["usd"] < min_trade:
            continue
        r["tag"] = tag
        r["yes_usd"] = wx.yes_dollars(r["side"], r["outcome_index"], r["usd"])
        us = wx.US_CITIES.get(r["city"] or "")
        r["us_code"] = us[0] if us and r["kind"] == "high" else None
        out.append(r)
        if len(out) >= limit:
            break
    return {"trades": out, "watching": len(wl)}


def api_whales(conn, q):
    rows = wx.rank_wallets(conn, int(q.get("days", 7)), q.get("by", "pnl"),
                           int(q.get("top", 50)), int(q.get("min_trades", 5)))
    wl = watchlist(conn)
    manual = set(read_manual_watchlist())
    for r in rows:
        r["tag"] = wl.get(r["wallet"])
        r["manual"] = r["wallet"] in manual
    return {"wallets": rows}


def api_leaderboard(conn, q):
    period = q.get("period", "MONTH")
    if q.get("refresh") == "1":
        wx.fetch_leaderboard(conn, (period,), 50)
    rows = [dict(r) for r in conn.execute(
        "SELECT * FROM leaderboard WHERE period=? ORDER BY rank", (period,))]
    if not rows:
        wx.fetch_leaderboard(conn, (period,), 50)
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM leaderboard WHERE period=? ORDER BY rank", (period,))]
    manual = set(read_manual_watchlist())
    stats = {r["wallet"]: r for r in wx.wallet_stats(conn, 30)}
    for r in rows:
        r["manual"] = r["wallet"] in manual
        r["local"] = stats.get(r["wallet"])
    return {"period": period, "rows": rows}


def api_wallet(conn, q):
    w = (q.get("address") or "").lower()
    if not w.startswith("0x"):
        raise ValueError("address required")
    if q.get("live") == "1" or not conn.execute("SELECT 1 FROM trades WHERE wallet=? LIMIT 1", (w,)).fetchone():
        tr = wx.get(f"{wx.DATA}/trades", {"user": w, "limit": 1000})
        tr = [t for t in tr if wx.parse_event_slug(t.get("eventSlug", ""))]
        wx.insert_trades(conn, tr)
        conn.commit()
    trades = [dict(r) for r in conn.execute("""
        SELECT t.*, m.label, m.winner, e.city, e.date FROM trades t
        LEFT JOIN markets m ON m.condition_id=t.condition_id
        LEFT JOIN events e ON e.slug=t.event_slug
        WHERE t.wallet=? ORDER BY t.ts DESC LIMIT 300""", (w,))]
    for t in trades:
        if t["winner"] is not None:
            pay = t["winner"] if t["outcome_index"] == 0 else 1 - t["winner"]
            t["won"] = (t["side"] == "BUY") == (pay == 1)
            t["pnl"] = t["size"] * (pay - t["price"]) if t["side"] == "BUY" else t["size"] * (t["price"] - pay)
        else:
            t["won"] = t["pnl"] = None
    st = next((r for r in wx.wallet_stats(conn, 3650) if r["wallet"] == w), None)
    if st:
        st["roi"] = st["pnl"] / st["resolved_vol"] if st["resolved_vol"] else None
        st["hit"] = st["wins"] / st["resolved_n"] if st["resolved_n"] else None
    lb = [dict(r) for r in conn.execute("SELECT * FROM leaderboard WHERE wallet=?", (w,))]
    # per-city breakdown
    cities: dict[str, dict] = {}
    for t in trades:
        c = cities.setdefault(t["city"] or "?", {"city": t["city"] or "?", "n": 0, "vol": 0.0, "pnl": 0.0})
        c["n"] += 1
        c["vol"] += t["usd"]
        c["pnl"] += t["pnl"] or 0
    return {"wallet": w, "stats": st, "leaderboard": lb, "trades": trades,
            "cities": sorted(cities.values(), key=lambda c: -c["vol"]),
            "tag": watchlist(conn).get(w), "manual": w in set(read_manual_watchlist())}


def api_events(conn, q):
    today = time.strftime("%Y-%m-%d", time.gmtime(time.time() - 86400))
    rows = conn.execute("""SELECT city, date, station, slug, closed,
        (SELECT COUNT(*) FROM trades t WHERE t.event_slug=e.slug) n,
        (SELECT COALESCE(SUM(usd),0) FROM trades t WHERE t.event_slug=e.slug) vol
        FROM events e WHERE kind='high' AND date >= ? ORDER BY date, city""", (today,)).fetchall()
    out = []
    for r in rows:
        r = dict(r)
        us = wx.US_CITIES.get(r["city"])
        r["us_code"] = us[0] if us else None
        r["us_station"] = us[1] if us else None
        out.append(r)
    return {"events": out}


def api_signal(conn, q):
    city = q.get("city", "nyc")
    city = wx.US_TO_INTL.get(city, city)
    date = q["date"]
    if city not in wx.US_CITIES:
        raise ValueError(f"no Polymarket US market for {city}")
    code, us_station = wx.US_CITIES[city]
    min_usd = float(q.get("min_usd", 500))
    top = int(q.get("top", SETTINGS["top"]))
    wl = set(wx.load_watchlist(conn, top, SETTINGS["watch_days"], "pnl")) if top else set()
    ev, per_bucket, per_deg, used = wx.whale_signal(conn, city, date, wl, min_usd)
    if not ev:
        raise ValueError(f"no international event for {city} {date} yet")
    try:
        ladder = wx.map_to_us(per_deg, wx.us_ladder(code, date))
        us_err = None
    except Exception as e:  # noqa: BLE001
        ladder, us_err = [], f"Polymarket US ladder unavailable: {e}"
    intl = []
    for lbl, v in per_bucket.items():
        b = wx.parse_intl_bucket(lbl)
        intl.append({"label": lbl, "lo": b[0] if b else None, "yes_usd": v})
    intl.sort(key=lambda x: (x["lo"] is None, x["lo"] if x["lo"] is not None else 0))
    # make tails sort to the ends
    intl.sort(key=lambda x: -1e9 if x["lo"] == -math.inf else x["lo"] if x["lo"] is not None else 1e9)
    us_word = us_station.split("(")[0].split()[:2]
    same_station = bool(ev["station"]) and any(wd in ev["station"] for wd in us_word)
    wallets: dict[str, dict] = {}
    for r in used:
        x = wallets.setdefault(r["wallet"], {"wallet": r["wallet"], "name": r["name"], "usd": 0.0, "n": 0})
        x["usd"] += r["usd"]
        x["n"] += 1
    return {"city": city, "date": date, "us_code": code, "intl_station": ev["station"],
            "us_station": us_station, "same_station": same_station, "intl": intl, "ladder": ladder,
            "us_error": us_err, "n_trades": len(used),
            "wallets": sorted(wallets.values(), key=lambda w: -w["usd"])[:15],
            "net_long": sum(max(0, b.get("whale_usd", 0)) for b in ladder)}


def api_watch(conn, body):
    w = (body.get("wallet") or "").lower()
    if not w.startswith("0x"):
        raise ValueError("wallet required")
    cur = set(read_manual_watchlist())
    if body.get("on", True):
        cur.add(w)
    else:
        cur.discard(w)
    write_manual_watchlist(list(cur))
    watchlist(conn, force=True)
    return {"manual": sorted(cur)}


def api_settings(conn, body):
    for k, v in body.items():
        if k in DEFAULT_SETTINGS:
            SETTINGS[k] = type(DEFAULT_SETTINGS[k])(v)
    SETTINGS["interval"] = max(10, SETTINGS["interval"])
    save_settings(SETTINGS)
    watchlist(conn, force=True)
    return {"settings": SETTINGS}


GET_ROUTES = {"/api/status": api_status, "/api/feed": api_feed, "/api/whales": api_whales,
              "/api/leaderboard": api_leaderboard, "/api/wallet": api_wallet,
              "/api/events": api_events, "/api/signal": api_signal}
def api_shutdown(conn, body):
    threading.Timer(0.3, lambda: (SERVER["srv"].shutdown() if SERVER.get("srv") else None)).start()
    return {"ok": True}


SERVER: dict = {}
POST_ROUTES = {"/api/watch": api_watch, "/api/settings": api_settings, "/api/shutdown": api_shutdown}

MIME = {".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8",
        ".css": "text/css; charset=utf-8", ".svg": "image/svg+xml", ".png": "image/png"}


class Handler(BaseHTTPRequestHandler):
    server_version = "wxwhales/1.0"

    def log_message(self, fmt, *args):  # quiet
        pass

    def _json(self, code, obj):
        body = json.dumps(clean(obj), default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _guard(self) -> bool:
        # localhost only, and refuse cross-site POSTs (DNS-rebinding / CSRF hygiene)
        host = (self.headers.get("Host") or "").split(":")[0]
        if host not in ("127.0.0.1", "localhost"):
            self._json(403, {"error": "forbidden host"})
            return False
        return True

    def do_GET(self):
        if not self._guard():
            return
        u = urllib.parse.urlparse(self.path)
        q = {k: v[-1] for k, v in urllib.parse.parse_qs(u.query).items()}
        if u.path in GET_ROUTES:
            conn = db()
            try:
                self._json(200, GET_ROUTES[u.path](conn, q))
            except (ValueError, KeyError) as e:
                self._json(400, {"error": str(e)})
            except Exception as e:  # noqa: BLE001
                traceback.print_exc()
                self._json(500, {"error": f"{type(e).__name__}: {e}"})
            finally:
                conn.close()
            return
        path = "index.html" if u.path in ("/", "") else u.path.lstrip("/")
        fp = os.path.normpath(os.path.join(STATIC, path))
        if not fp.startswith(STATIC) or not os.path.isfile(fp):
            self.send_error(404)
            return
        with open(fp, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type", MIME.get(os.path.splitext(fp)[1], "application/octet-stream"))
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if not self._guard():
            return
        origin = self.headers.get("Origin")
        if origin and urllib.parse.urlparse(origin).hostname not in ("127.0.0.1", "localhost"):
            self._json(403, {"error": "cross-origin"})
            return
        u = urllib.parse.urlparse(self.path)
        if u.path not in POST_ROUTES:
            self._json(404, {"error": "not found"})
            return
        n = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(n) or b"{}")
        except json.JSONDecodeError:
            self._json(400, {"error": "bad json"})
            return
        conn = db()
        try:
            self._json(200, POST_ROUTES[u.path](conn, body))
        except (ValueError, KeyError) as e:
            self._json(400, {"error": str(e)})
        finally:
            conn.close()


def already_running(port) -> bool:
    import urllib.request
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/status", timeout=2) as r:
            return r.status == 200
    except Exception:  # noqa: BLE001
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--no-open", action="store_true")
    ap.add_argument("--no-sync", action="store_true", help="serve the DB without the background sync")
    a = ap.parse_args()
    url = f"http://127.0.0.1:{a.port}/"
    if already_running(a.port):
        print(f"already running — opening {url}")
        if not a.no_open:
            webbrowser.open(url)
        return
    db().close()  # create schema
    stop = threading.Event()
    if not a.no_sync:
        threading.Thread(target=sync_loop, args=(stop,), daemon=True).start()
    srv = ThreadingHTTPServer(("127.0.0.1", a.port), Handler)
    SERVER["srv"] = srv
    print(f"wxwhales dashboard on {url}  (Ctrl-C to stop)")
    if not a.no_open:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        srv.server_close()


if __name__ == "__main__":
    main()
