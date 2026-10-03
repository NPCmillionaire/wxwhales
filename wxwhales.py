#!/usr/bin/env python3
"""
wxwhales — follow the biggest wallets in Polymarket temperature markets.

Polymarket US (polymarket.us) is a KYC'd exchange and publishes no trader identities,
so the wallets come from international Polymarket (Polygon), whose public Data API tags
every trade with a proxy wallet. Same cities, same dates. Where a city also trades on
Polymarket US, whale positioning is mapped onto the US bucket ladder as a signal.

Watch-only. Nothing here places orders.

Stdlib only. Python 3.9+.

  python3 wxwhales.py sync                 # pull events + trades into SQLite
  python3 wxwhales.py whales               # rank wallets (biggest trades, volume, PnL)
  python3 wxwhales.py leaderboard          # Polymarket's own WEATHER leaderboard
  python3 wxwhales.py wallet 0xabc...      # one wallet's recent temperature trades
  python3 wxwhales.py signal nyc 2026-09-25  # whale $ mapped onto the US ladder
  python3 wxwhales.py follow               # live loop, alerts on whale trades
  python3 wxwhales.py selftest             # offline checks against saved fixtures
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import re
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

GAMMA = "https://gamma-api.polymarket.com"
DATA = "https://data-api.polymarket.com"
US_GATEWAY = "https://gateway.polymarket.us/v1"

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB = os.path.join(HERE, "data", "wxwhales.sqlite")
WATCHLIST = os.path.join(HERE, "watchlist.txt")

# intl city slug -> Polymarket US event code. US settles on the NWS CLI at these stations.
US_CITIES = {
    "nyc": ("nyc", "Central Park (KNYC)"),
    "miami": ("mia", "Miami International Airport (KMIA)"),
    "chicago": ("mdw", "Chicago Midway Airport (KMDW)"),
    "los-angeles": ("lax", "Los Angeles International Airport (KLAX)"),
    "san-francisco": ("sfo", "San Francisco International Airport (KSFO)"),
}
US_TO_INTL = {v[0]: k for k, v in US_CITIES.items()}

MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august",
     "september", "october", "november", "december"], 1)}

SLUG_RE = re.compile(r"^(highest|lowest)-temperature-in-(.+)-on-([a-z]+)-(\d{1,2})-(\d{4})$")


# --------------------------------------------------------------------------- http

def get(url: str, params: dict | None = None, retries: int = 3):
    if params:
        url += ("&" if "?" in url else "?") + urllib.parse.urlencode(
            {k: v for k, v in params.items() if v is not None})
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "wxwhales/1.0",
                                                       "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=25) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code in (400, 404):
                raise
            last = e
        except Exception as e:  # noqa: BLE001 — network flakiness, retry
            last = e
        time.sleep(1.5 * (i + 1))
    raise RuntimeError(f"GET failed after {retries} tries: {url}: {last}")


# --------------------------------------------------------------------------- parsing

def parse_event_slug(slug: str):
    """highest-temperature-in-nyc-on-september-25-2026 -> ('high','nyc','2026-09-25')"""
    m = SLUG_RE.match(slug)
    if not m:
        return None
    kind, city, mon, day, yr = m.groups()
    if mon not in MONTHS:
        return None
    return ("high" if kind == "highest" else "low", city,
            f"{int(yr):04d}-{MONTHS[mon]:02d}-{int(day):02d}")


def parse_intl_bucket(label: str):
    """'58-59°F' -> (58,59,'F'); '57°F or below' -> (-inf,57,'F');
    '76°F or higher' -> (76,inf,'F'); '33°C' -> (33,33,'C')."""
    s = label.replace("º", "°").strip()
    unit = "C" if "°C" in s else "F"
    n = [int(x) for x in re.findall(r"(?<!\d)-?\d+", s.replace("°", " "))]
    low = s.lower()
    if ("below" in low or "lower" in low or "less" in low) and n:
        return (-math.inf, n[0], unit)
    if ("higher" in low or "above" in low or "more" in low) and n:
        return (n[0], math.inf, unit)
    if len(n) >= 2:
        return (n[0], n[1], unit)
    if len(n) == 1:
        return (n[0], n[0], unit)
    return None


def parse_us_bucket(title_short: str):
    """'67 to 68' -> (67,68); '66 or below' -> (-inf,66); '75 or above' -> (75,inf)."""
    s = title_short.lower()
    n = [int(x) for x in re.findall(r"-?\d+", s)]
    if not n:
        return None
    if "below" in s or "less" in s:
        return (-math.inf, n[0])
    if "above" in s or "more" in s or "higher" in s:
        return (n[0], math.inf)
    if len(n) >= 2:
        return (n[0], n[1])
    return (n[0], n[0])


def intl_station(description: str) -> str | None:
    m = re.search(r"recorded by \w+ at (?:the )?(.+?) Station", description or "")
    return m.group(1).strip() if m else None


def degrees_of(lo, hi, ray_width=2):
    """Integer degrees a bucket covers. Open-ended buckets get `ray_width` degrees
    at their edge so whale money on a tail bucket still lands somewhere sensible."""
    if lo == -math.inf:
        return list(range(int(hi) - ray_width + 1, int(hi) + 1))
    if hi == math.inf:
        return list(range(int(lo), int(lo) + ray_width))
    return list(range(int(lo), int(hi) + 1))


def in_bucket(deg, lo, hi):
    return lo <= deg <= hi


def yes_dollars(side: str, outcome_index: int, usd: float) -> float:
    """Signed Yes-equivalent exposure: + means the trade backs the bucket hitting."""
    backs_yes = (side == "BUY") == (outcome_index == 0)
    return usd if backs_yes else -usd


# --------------------------------------------------------------------------- db

SCHEMA = """
CREATE TABLE IF NOT EXISTS events(
  id INTEGER PRIMARY KEY, slug TEXT UNIQUE, kind TEXT, city TEXT, date TEXT,
  station TEXT, unit TEXT, closed INTEGER, end_date TEXT, last_trade_ts INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS markets(
  condition_id TEXT PRIMARY KEY, event_id INTEGER, slug TEXT, label TEXT,
  lo REAL, hi REAL, unit TEXT, winner INTEGER);
CREATE TABLE IF NOT EXISTS trades(
  tx TEXT, asset TEXT, wallet TEXT, side TEXT, size REAL, price REAL, usd REAL,
  ts INTEGER, condition_id TEXT, event_slug TEXT, outcome TEXT, outcome_index INTEGER,
  name TEXT, alerted INTEGER DEFAULT 0,
  PRIMARY KEY(tx, asset, wallet, side, size, price));
CREATE INDEX IF NOT EXISTS trades_wallet ON trades(wallet);
CREATE INDEX IF NOT EXISTS trades_ts ON trades(ts);
CREATE INDEX IF NOT EXISTS trades_event ON trades(event_slug);
CREATE TABLE IF NOT EXISTS leaderboard(
  wallet TEXT, period TEXT, rank INTEGER, name TEXT, vol REAL, pnl REAL, fetched TEXT,
  PRIMARY KEY(wallet, period));
"""


def connect(path: str) -> sqlite3.Connection:
    if path != ":memory:":
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    db = sqlite3.connect(path, timeout=30)
    db.row_factory = sqlite3.Row
    if path != ":memory:":
        db.execute("PRAGMA journal_mode=WAL")  # the app reads while its sync thread writes
    db.executescript(SCHEMA)
    return db


def upsert_event(db, ev: dict) -> bool:
    p = parse_event_slug(ev.get("slug", ""))
    if not p:
        return False
    kind, city, date = p
    mkts = ev.get("markets") or []
    station = next((intl_station(m.get("description", "")) for m in mkts
                    if intl_station(m.get("description", ""))), None)
    unit = None
    for m in mkts:
        b = parse_intl_bucket(m.get("groupItemTitle") or "")
        winner = None
        if m.get("closed") and m.get("umaResolutionStatus") == "resolved":
            try:
                winner = 1 if float(json.loads(m["outcomePrices"])[0]) > 0.5 else 0
            except Exception:  # noqa: BLE001
                winner = None
        if b:
            unit = b[2]
        db.execute("""INSERT INTO markets VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(condition_id) DO UPDATE SET winner=COALESCE(excluded.winner, winner),
              label=excluded.label, lo=excluded.lo, hi=excluded.hi, unit=excluded.unit""",
                   (m.get("conditionId"), int(ev["id"]), m.get("slug"), m.get("groupItemTitle"),
                    b[0] if b else None, b[1] if b else None, b[2] if b else None, winner))
    db.execute("""INSERT INTO events(id,slug,kind,city,date,station,unit,closed,end_date)
        VALUES(?,?,?,?,?,?,?,?,?)
        ON CONFLICT(id) DO UPDATE SET closed=excluded.closed, station=COALESCE(excluded.station,station)""",
               (int(ev["id"]), ev["slug"], kind, city, date, station, unit,
                1 if ev.get("closed") else 0, ev.get("endDate")))
    return True


def insert_trades(db, trades: list[dict]) -> list[dict]:
    new = []
    for t in trades:
        usd = float(t["size"]) * float(t["price"])
        cur = db.execute("""INSERT OR IGNORE INTO trades
            (tx,asset,wallet,side,size,price,usd,ts,condition_id,event_slug,outcome,outcome_index,name)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                         (t.get("transactionHash"), t.get("asset"), (t.get("proxyWallet") or "").lower(),
                          t.get("side"), float(t["size"]), float(t["price"]), usd, int(t["timestamp"]),
                          t.get("conditionId"), t.get("eventSlug"), t.get("outcome"),
                          int(t.get("outcomeIndex", 0)), t.get("name") or t.get("pseudonym")))
        if cur.rowcount:
            new.append(t)
    return new


# --------------------------------------------------------------------------- sync

def discover_events(closed: bool, since_days: int, kinds=("high",)) -> list[dict]:
    out, offset = [], 0
    params = {"tag_slug": "weather", "closed": str(closed).lower(), "limit": 100}
    if closed:
        params["end_date_min"] = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=since_days)).strftime(
            "%Y-%m-%dT00:00:00Z")
        params["order"] = "endDate"
        params["ascending"] = "false"
    while offset < 5000:
        page = get(f"{GAMMA}/events", {**params, "offset": offset})
        if not page:
            break
        for e in page:
            p = parse_event_slug(e.get("slug", ""))
            if p and p[0] in kinds:
                out.append(e)
        if len(page) < 100:
            break
        offset += 100
    return out


def pull_trades(event_ids: list[int], since_ts: int, min_usd: float, taker_only: bool) -> list[dict]:
    """Trades for up to ~40 events in one request stream, newest first, stopping at since_ts."""
    out, offset = [], 0
    while offset <= 9000:
        page = get(f"{DATA}/trades", {
            "eventId": ",".join(str(i) for i in event_ids), "limit": 1000, "offset": offset,
            "takerOnly": str(taker_only).lower(),
            "start": since_ts or None,
            # the Data API sits behind a 5-minute CDN cache; a moving `end` makes each poll fresh
            "end": int(time.time()) + 60,
            "filterType": "CASH" if min_usd > 0 else None,
            "filterAmount": min_usd if min_usd > 0 else None})
        if not page:
            break
        fresh = [t for t in page if int(t["timestamp"]) >= since_ts]
        out += fresh
        if len(fresh) < len(page) or len(page) < 1000:
            break
        offset += 1000
    return out


def sync(db, days: int = 3, min_usd: float = 25, include_low: bool = False,
         taker_only: bool = True, quiet: bool = False, chunk: int = 40,
         discover: bool = True) -> list[dict]:
    kinds = ("high", "low") if include_low else ("high",)
    if discover:
        evs = discover_events(False, 0, kinds)
        if days > 0:
            evs += discover_events(True, days, kinds)
        for e in evs:
            upsert_event(db, e)
        # events we still think are open but gamma no longer lists as open have closed
        open_ids = [int(e["id"]) for e in evs if not e.get("closed")]
        if open_ids:
            db.execute(f"UPDATE events SET closed=1 WHERE closed=0 AND id NOT IN ({','.join('?'*len(open_ids))})",
                       open_ids)
        db.commit()
    else:  # fast path for the follow loop: reuse the open events already in the DB
        evs = [{"id": r["id"]} for r in db.execute(
            f"SELECT id FROM events WHERE closed=0 AND kind IN ({','.join('?'*len(kinds))})", kinds)]
    state = {r["id"]: r for r in db.execute(
        f"SELECT id, slug, last_trade_ts, closed FROM events WHERE id IN ({','.join('?'*len(evs))})",
        [int(e["id"]) for e in evs])} if evs else {}
    # closed events already pulled once after close need nothing; new events backfill one
    # at a time (they can hold thousands of trades); known open events go in batches.
    todo = [r for r in state.values() if not (r["closed"] and r["last_trade_ts"])]
    fresh_evs = [r for r in todo if not r["last_trade_ts"]]
    known = sorted((r for r in todo if r["last_trade_ts"]), key=lambda r: r["last_trade_ts"])
    groups = [[r] for r in fresh_evs] + [known[i:i + chunk] for i in range(0, len(known), chunk)]
    new_all = []
    for g in groups:
        since = min(r["last_trade_ts"] or 0 for r in g)
        since = max(0, since - 600) if since else 0  # 10-min overlap; the PK dedupes
        tr = pull_trades([r["id"] for r in g], since, min_usd, taker_only)
        new_all += insert_trades(db, tr)
        latest: dict[str, int] = {}
        for t in tr:
            latest[t["eventSlug"]] = max(latest.get(t["eventSlug"], 0), int(t["timestamp"]))
        for r in g:
            ts = latest.get(r["slug"])
            if not ts and not r["last_trade_ts"]:
                ts = int(time.time()) - 3600  # nothing yet: mark seen so it joins the batches
            if ts and ts > (r["last_trade_ts"] or 0):
                db.execute("UPDATE events SET last_trade_ts=? WHERE id=?", (ts, r["id"]))
        db.commit()
    if not quiet:
        print(f"synced {len(evs)} events ({len(groups)} requests), {len(new_all)} new trades ≥ ${min_usd:g}",
              file=sys.stderr)
    return new_all


def fetch_leaderboard(db, periods=("MONTH", "ALL"), limit=50):
    now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    rows = []
    for p in periods:
        try:
            lb = get(f"{DATA}/v1/leaderboard", {"category": "WEATHER", "timePeriod": p,
                                                "orderBy": "PNL", "limit": limit})
        except Exception as e:  # noqa: BLE001
            print(f"leaderboard {p}: {e}", file=sys.stderr)
            continue
        if lb:
            db.execute("DELETE FROM leaderboard WHERE period=?", (p,))  # drop wallets that fell off
        for r in lb:
            w = (r.get("proxyWallet") or "").lower()
            db.execute("INSERT OR REPLACE INTO leaderboard VALUES(?,?,?,?,?,?,?)",
                       (w, p, int(r.get("rank", 0)), r.get("userName"), r.get("vol"), r.get("pnl"), now))
            rows.append((p, r))
    db.commit()
    return rows


# --------------------------------------------------------------------------- analysis

def wallet_stats(db, days: int, min_trades: int = 1):
    since = int(time.time()) - days * 86400
    q = """
    SELECT t.wallet, MAX(t.name) name, COUNT(*) n, SUM(t.usd) vol, MAX(t.usd) biggest,
      SUM(CASE WHEN m.winner IS NOT NULL THEN
           (CASE WHEN t.side='BUY'
                 THEN t.size*((CASE WHEN t.outcome_index=0 THEN m.winner ELSE 1-m.winner END) - t.price)
                 ELSE t.size*(t.price - (CASE WHEN t.outcome_index=0 THEN m.winner ELSE 1-m.winner END)) END)
          END) pnl,
      SUM(CASE WHEN m.winner IS NOT NULL THEN t.usd END) resolved_vol,
      SUM(CASE WHEN m.winner IS NOT NULL AND
           ((t.side='BUY') = ((CASE WHEN t.outcome_index=0 THEN m.winner ELSE 1-m.winner END)=1))
          THEN 1 ELSE 0 END) wins,
      SUM(CASE WHEN m.winner IS NOT NULL THEN 1 ELSE 0 END) resolved_n,
      COUNT(DISTINCT t.event_slug) events,
      MAX(t.ts) last_ts
    FROM trades t LEFT JOIN markets m ON m.condition_id=t.condition_id
    WHERE t.ts >= ?
    GROUP BY t.wallet HAVING n >= ?"""
    return [dict(r) for r in db.execute(q, (since, min_trades))]


def rank_wallets(db, days=7, by="biggest", top=25, min_trades=1):
    rows = wallet_stats(db, days, min_trades)
    lb = {r["wallet"]: dict(r) for r in db.execute("SELECT * FROM leaderboard WHERE period='MONTH'")}
    for r in rows:
        r["lb_rank"] = lb.get(r["wallet"], {}).get("rank")
        r["lb_pnl"] = lb.get(r["wallet"], {}).get("pnl")
        r["roi"] = (r["pnl"] / r["resolved_vol"]) if r["resolved_vol"] else None
        r["hit"] = (r["wins"] / r["resolved_n"]) if r["resolved_n"] else None
    key = {"biggest": "biggest", "volume": "vol", "pnl": "pnl", "roi": "roi"}[by]
    rows.sort(key=lambda r: (r[key] is not None, r[key] or 0), reverse=True)
    return rows[:top]


def load_watchlist(db, top_n: int, days: int, by: str) -> dict[str, str]:
    """Manual watchlist.txt + top N by rank + top N on the official monthly leaderboard."""
    wl: dict[str, str] = {}
    if os.path.exists(WATCHLIST):
        for line in open(WATCHLIST):
            line = line.split("#", 1)[0].strip()
            if line.startswith("0x"):
                wl[line.lower()] = "manual"
    for r in rank_wallets(db, days, by, top_n):
        wl.setdefault(r["wallet"], f"top{by}")
    for r in db.execute("SELECT wallet FROM leaderboard WHERE period='MONTH' AND rank<=?", (top_n,)):
        wl.setdefault(r["wallet"], "leaderboard")
    return wl


def us_ladder(code: str, date: str):
    ev = get(f"{US_GATEWAY}/events/slug/temp-{code}high-{date}")
    ev = ev.get("event", ev)
    out = []
    for m in sorted(ev.get("markets", []), key=lambda m: m.get("sortOrder", 0)):
        b = parse_us_bucket(m.get("titleShort", ""))
        if not b:
            continue
        side = next((s for s in m.get("marketSides", []) if s.get("long")), {})
        bid = float(side["price"]) if side.get("price") is not None else None
        ask = (m.get("bestAskQuote") or {}).get("value")
        ask = float(ask) if ask is not None else None
        out.append({"label": m.get("titleShort"), "lo": b[0], "hi": b[1], "bid": bid, "ask": ask,
                    "slug": m.get("slug")})
    return out


def whale_signal(db, city: str, date: str, wallets: set[str] | None, min_usd: float):
    """Net Yes-$ per intl bucket from qualifying trades, spread across degrees."""
    ev = db.execute("SELECT * FROM events WHERE city=? AND date=? AND kind='high'", (city, date)).fetchone()
    if not ev:
        return None, {}, {}, []
    rows = db.execute("""SELECT t.*, m.lo, m.hi, m.label FROM trades t
        JOIN markets m ON m.condition_id=t.condition_id WHERE t.event_slug=?""", (ev["slug"],)).fetchall()
    per_bucket: dict[str, float] = {}
    per_deg: dict[int, float] = {}
    used = []
    for r in rows:
        if r["lo"] is None:
            continue
        if not ((wallets and r["wallet"] in wallets) or r["usd"] >= min_usd):
            continue
        x = yes_dollars(r["side"], r["outcome_index"], r["usd"])
        per_bucket[r["label"]] = per_bucket.get(r["label"], 0) + x
        degs = degrees_of(r["lo"], r["hi"])
        for d in degs:
            per_deg[d] = per_deg.get(d, 0) + x / len(degs)
        used.append(r)
    return ev, per_bucket, per_deg, used


def map_to_us(per_deg: dict[int, float], ladder: list[dict]):
    for b in ladder:
        b["whale_usd"] = sum(v for d, v in per_deg.items() if in_bucket(d, b["lo"], b["hi"]))
    pos = sum(max(0, b["whale_usd"]) for b in ladder)
    for b in ladder:
        b["whale_share"] = max(0, b["whale_usd"]) / pos if pos else None
        b["mid"] = ((b["bid"] + b["ask"]) / 2) if b["bid"] is not None and b["ask"] is not None else b["bid"]
    return ladder


# --------------------------------------------------------------------------- output

def money(x):
    if x is None:
        return "—"
    s = "-" if x < 0 else ""
    x = abs(x)
    return f"{s}${x/1e6:.2f}M" if x >= 1e6 else f"{s}${x/1e3:.1f}k" if x >= 1e3 else f"{s}${x:.0f}"


def pct(x):
    return "—" if x is None else f"{x*100:.0f}%"


def short(w):
    return w[:6] + "…" + w[-4:]


def ago(ts):
    s = int(time.time()) - int(ts)
    return f"{s//86400}d" if s >= 86400 else f"{s//3600}h" if s >= 3600 else f"{s//60}m"


def table(rows, cols):
    widths = [max(len(c[0]), *(len(str(c[1](r))) for r in rows)) if rows else len(c[0]) for c in cols]
    print("  ".join(c[0].ljust(w) for c, w in zip(cols, widths)))
    for r in rows:
        print("  ".join(str(c[1](r)).ljust(w) for c, w in zip(cols, widths)))


def notify(title, msg):
    if sys.platform == "darwin":
        try:
            subprocess.run(["osascript", "-e",
                            f'display notification {json.dumps(msg)} with title {json.dumps(title)}'],
                           timeout=5, check=False)
        except Exception:  # noqa: BLE001
            pass


# --------------------------------------------------------------------------- commands

def cmd_sync(a, db):
    sync(db, a.days, a.min_usd, a.include_low, not a.all_sides)
    if not a.no_leaderboard:
        fetch_leaderboard(db)


def cmd_whales(a, db):
    rows = rank_wallets(db, a.days, a.by, a.top, a.min_trades)
    print(f"Top wallets — temperature markets, last {a.days}d, by {a.by}"
          f"  (PnL/ROI/hit only count resolved buckets; held-to-settle assumption)\n")
    table(rows, [
        ("wallet", lambda r: short(r["wallet"])), ("name", lambda r: (r["name"] or "")[:16]),
        ("trades", lambda r: r["n"]), ("events", lambda r: r["events"]),
        ("volume", lambda r: money(r["vol"])), ("biggest", lambda r: money(r["biggest"])),
        ("pnl", lambda r: money(r["pnl"])), ("roi", lambda r: pct(r["roi"])),
        ("hit", lambda r: pct(r["hit"])), ("lb#", lambda r: r["lb_rank"] or ""),
        ("last", lambda r: ago(r["last_ts"]))])
    if a.full:
        print()
        for r in rows:
            print(r["wallet"])


def cmd_leaderboard(a, db):
    rows = fetch_leaderboard(db, (a.period,), a.top)
    print(f"Polymarket WEATHER leaderboard — {a.period}, by PnL\n")
    table([r for _, r in rows], [
        ("#", lambda r: r.get("rank")), ("wallet", lambda r: r.get("proxyWallet")),
        ("name", lambda r: (r.get("userName") or "")[:18]),
        ("volume", lambda r: money(r.get("vol"))), ("pnl", lambda r: money(r.get("pnl")))])


def cmd_wallet(a, db):
    w = a.wallet.lower()
    rows = db.execute("""SELECT t.*, m.label, m.winner FROM trades t
        LEFT JOIN markets m ON m.condition_id=t.condition_id
        WHERE t.wallet=? ORDER BY t.ts DESC LIMIT ?""", (w, a.limit)).fetchall()
    if not rows or a.live:
        tr = get(f"{DATA}/trades", {"user": w, "limit": 500})
        tr = [t for t in tr if parse_event_slug(t.get("eventSlug", ""))]
        insert_trades(db, tr)
        db.commit()
        rows = db.execute("""SELECT t.*, m.label, m.winner FROM trades t
            LEFT JOIN markets m ON m.condition_id=t.condition_id
            WHERE t.wallet=? ORDER BY t.ts DESC LIMIT ?""", (w, a.limit)).fetchall()
    print(f"{w}  — {len(rows)} most recent temperature trades\n")
    table(rows, [
        ("when", lambda r: ago(r["ts"])), ("event", lambda r: r["event_slug"].replace("highest-temperature-in-", "")[:34]),
        ("bucket", lambda r: r["label"] or ""), ("side", lambda r: f'{r["side"]} {r["outcome"]}'),
        ("px", lambda r: f'{r["price"]:.3f}'), ("usd", lambda r: money(r["usd"])),
        ("won", lambda r: "" if r["winner"] is None else
         ("✓" if (r["side"] == "BUY") == ((r["winner"] if r["outcome_index"] == 0 else 1 - r["winner"]) == 1) else "✗"))])


def cmd_signal(a, db):
    code = a.city.lower()
    intl = US_TO_INTL.get(code, code)
    if intl not in US_CITIES:
        sys.exit(f"city must be one of {', '.join(US_TO_INTL)} (or {', '.join(US_CITIES)})")
    code, us_station = US_CITIES[intl]
    if not a.no_sync:
        sync(db, 0, a.min_usd_store, quiet=True)
    wl = set(load_watchlist(db, a.top, a.days, "pnl")) if a.top else set()
    ev, per_bucket, per_deg, used = whale_signal(db, intl, a.date, wl, a.min_usd)
    if not ev:
        sys.exit(f"no intl event in DB for {intl} {a.date} — run sync, or check the date")
    ladder = map_to_us(per_deg, us_ladder(code, a.date))
    print(f"{intl.upper()} {a.date} — {len(used)} qualifying trades "
          f"(≥ ${a.min_usd:g} or top-{a.top} wallet), {len({r['wallet'] for r in used})} wallets\n")
    print(f"intl settles: {ev['station'] or '?'} (NOAA hourly obs max)")
    print(f"US settles:   {us_station} (NWS CLI daily max)")
    if ev["station"] and not any(k in ev["station"] for k in us_station.split("(")[0].split()[:2]):
        print("⚠  DIFFERENT STATIONS — whale view is on another thermometer; treat as a prior, not a price.")
    print("\nintl buckets, net Yes-$ (+ backs the bucket, − fades it):")
    for lbl, v in sorted(per_bucket.items(), key=lambda kv: parse_intl_bucket(kv[0])[0]):
        print(f"  {lbl:>16}  {money(v):>9}")
    print("\nmapped onto the Polymarket US ladder:")
    table(ladder, [
        ("US bucket", lambda b: b["label"]), ("bid", lambda b: f'{b["bid"]:.2f}' if b["bid"] is not None else "—"),
        ("ask", lambda b: f'{b["ask"]:.2f}' if b["ask"] is not None else "—"),
        ("whale $", lambda b: money(b["whale_usd"])), ("whale share", lambda b: pct(b["whale_share"])),
        ("share−mid", lambda b: "—" if b["whale_share"] is None or b["mid"] is None
         else f'{(b["whale_share"]-b["mid"])*100:+.0f}pt')])
    pos = sum(max(0, b["whale_usd"]) for b in ladder)
    if pos < 1000:
        print(f"\n⚠  only {money(pos)} of net-long whale money — too thin to read share−mid as a signal.")
    print("\nwhale share = positive net whale $ normalised across the ladder. It is a crowd-of-whales\n"
          "view, not a probability — compare it with the forecast before acting on it.")


def fmt_alert(t, tag):
    usd = float(t["size"]) * float(t["price"])
    ev = t.get("eventSlug", "").replace("highest-temperature-in-", "").replace("lowest-temperature-in-", "LOW ")
    b = t.get("slug", "").rsplit("-", 1)[-1]
    return (f'{dt.datetime.fromtimestamp(int(t["timestamp"])).strftime("%H:%M:%S")}  '
            f'{money(usd):>8}  {t["side"]:<4} {t.get("outcome",""):<3} @{float(t["price"]):.3f}  '
            f'{ev} [{b}]  {(t.get("name") or "")[:14]} {short((t.get("proxyWallet") or "").lower())}  {tag}')


def cmd_follow(a, db):
    print(f"following temperature markets every {a.interval}s — alert ≥ ${a.min_trade:g} "
          f"or any trade ≥ ${a.min_usd:g} by a watched wallet. Ctrl-C to stop.", file=sys.stderr)
    fetch_leaderboard(db)
    sync(db, 0, a.min_usd, quiet=True)  # baseline, no alerts for history
    wl = load_watchlist(db, a.top, a.days, "pnl")
    print(f"watching {len(wl)} wallets", file=sys.stderr)
    last_refresh = last_discover = time.time()
    while True:
        try:
            time.sleep(a.interval)
            rediscover = time.time() - last_discover > 600
            new = sync(db, 0, a.min_usd, quiet=True, discover=rediscover)
            if rediscover:
                last_discover = time.time()
            new.sort(key=lambda t: int(t["timestamp"]))
            for t in new:
                usd = float(t["size"]) * float(t["price"])
                w = (t.get("proxyWallet") or "").lower()
                tag = wl.get(w)
                px = float(t["price"])
                if not a.include_sweeps and (px >= 0.98 or px <= 0.02):
                    continue  # settlement sweeps / dust: large but carry no view
                if usd >= a.min_trade or tag:
                    line = fmt_alert(t, f"[{tag}]" if tag else "[size]")
                    p = parse_event_slug(t.get("eventSlug", ""))
                    if p and p[1] in US_CITIES and p[0] == "high":
                        line += f"  → US temp-{US_CITIES[p[1]][0]}high-{p[2]}"
                    print(line, flush=True)
                    if a.notify:
                        notify("wxwhales", line)
            if time.time() - last_refresh > 3600:
                fetch_leaderboard(db)
                wl = load_watchlist(db, a.top, a.days, "pnl")
                last_refresh = time.time()
        except KeyboardInterrupt:
            break
        except Exception as e:  # noqa: BLE001 — keep the loop alive
            print(f"[warn] {e}", file=sys.stderr)


# --------------------------------------------------------------------------- selftest

def cmd_selftest(a, db):
    fx = os.path.join(HERE, "tests", "fixtures")
    ok = True

    def check(name, cond):
        nonlocal ok
        print(("PASS " if cond else "FAIL ") + name)
        ok &= bool(cond)

    check("slug parse", parse_event_slug("highest-temperature-in-nyc-on-september-25-2026") == ("high", "nyc", "2026-09-25"))
    check("slug parse low/multiword", parse_event_slug("lowest-temperature-in-los-angeles-on-october-3-2026") == ("low", "los-angeles", "2026-10-03"))
    check("slug reject", parse_event_slug("precipitation-in-nyc-in-september") is None)
    check("intl range", parse_intl_bucket("58-59°F") == (58, 59, "F"))
    check("intl below", parse_intl_bucket("57°F or below") == (-math.inf, 57, "F"))
    check("intl higher", parse_intl_bucket("76°F or higher") == (76, math.inf, "F"))
    check("intl celsius", parse_intl_bucket("33°C") == (33, 33, "C"))
    check("us range", parse_us_bucket("67 to 68") == (67, 68))
    check("us below", parse_us_bucket("66 or below") == (-math.inf, 66))
    check("us above", parse_us_bucket("75 or above") == (75, math.inf))
    check("yes$ buy yes", yes_dollars("BUY", 0, 100) == 100)
    check("yes$ buy no", yes_dollars("BUY", 1, 100) == -100)
    check("yes$ sell yes", yes_dollars("SELL", 0, 100) == -100)
    check("yes$ sell no", yes_dollars("SELL", 1, 100) == 100)
    check("tail degrees", degrees_of(-math.inf, 57) == [56, 57] and degrees_of(76, math.inf) == [76, 77])

    mem = connect(":memory:")
    ev = json.load(open(os.path.join(fx, "gamma_event_nyc.json")))[0]
    check("upsert open event", upsert_event(mem, ev))
    e = mem.execute("SELECT * FROM events").fetchone()
    check("station parsed", e["station"] and "LaGuardia" in e["station"])
    check("11 buckets", mem.execute("SELECT COUNT(*) FROM markets").fetchone()[0] == len(ev["markets"]))
    trades = json.load(open(os.path.join(fx, "trades_nyc.json")))
    n1 = len(insert_trades(mem, trades))
    n2 = len(insert_trades(mem, trades))
    check("trades insert + dedupe", n1 > 0 and n2 == 0)

    cl = json.load(open(os.path.join(fx, "gamma_event_closed.json")))[0]
    upsert_event(mem, cl)
    win = mem.execute("SELECT label FROM markets WHERE event_id=? AND winner=1", (int(cl["id"]),)).fetchall()
    check("closed event has exactly one winner", len(win) == 1)
    # synthetic trades on the closed event: BUY Yes winner @0.40 x100 -> +60 ; BUY Yes loser @0.20 x50 -> -10
    wm = mem.execute("SELECT condition_id FROM markets WHERE event_id=? AND winner=1", (int(cl["id"]),)).fetchone()[0]
    lm = mem.execute("SELECT condition_id FROM markets WHERE event_id=? AND winner=0", (int(cl["id"]),)).fetchone()[0]
    now = int(time.time())
    insert_trades(mem, [
        {"transactionHash": "0x1", "asset": "a", "proxyWallet": "0xW", "side": "BUY", "size": 100, "price": 0.4,
         "timestamp": now, "conditionId": wm, "eventSlug": cl["slug"], "outcome": "Yes", "outcomeIndex": 0},
        {"transactionHash": "0x2", "asset": "b", "proxyWallet": "0xW", "side": "BUY", "size": 50, "price": 0.2,
         "timestamp": now, "conditionId": lm, "eventSlug": cl["slug"], "outcome": "Yes", "outcomeIndex": 0},
        {"transactionHash": "0x3", "asset": "c", "proxyWallet": "0xW", "side": "BUY", "size": 10, "price": 0.9,
         "timestamp": now, "conditionId": lm, "eventSlug": cl["slug"], "outcome": "No", "outcomeIndex": 1}])
    w = [r for r in wallet_stats(mem, 1) if r["wallet"] == "0xw"][0]
    check("pnl math (+60 −10 +1 = 51)", abs(w["pnl"] - 51) < 1e-9)
    check("hit rate 2/3", w["wins"] == 2 and w["resolved_n"] == 3)

    us = json.load(open(os.path.join(fx, "us_event_nyc.json")))
    ue = us.get("event", us)
    lad = []
    for m in sorted(ue["markets"], key=lambda m: m.get("sortOrder", 0)):
        b = parse_us_bucket(m["titleShort"])
        lad.append({"label": m["titleShort"], "lo": b[0], "hi": b[1], "bid": 0.1, "ask": 0.2})
    # $100 on intl 70-71 -> 50 on 70, 50 on 71; US '69 to 70' gets 50, '71 to 72' gets 50
    lad = map_to_us({70: 50.0, 71: 50.0}, lad)
    got = {b["label"]: b["whale_usd"] for b in lad}
    check("degree mapping onto US ladder", got.get("69 to 70") == 50 and got.get("71 to 72") == 50)
    check("share normalises", abs(sum(b["whale_share"] or 0 for b in lad) - 1) < 1e-9)
    print("\nALL PASS" if ok else "\nFAILURES")
    sys.exit(0 if ok else 1)


# --------------------------------------------------------------------------- main

def main(argv=None):
    ap = argparse.ArgumentParser(prog="wxwhales", description=__doc__.split("\n\n")[0])
    ap.add_argument("--db", default=DEFAULT_DB)
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("sync", help="pull events + trades")
    s.add_argument("--days", type=int, default=3, help="also pull events closed in the last N days")
    s.add_argument("--min-usd", type=float, default=25, help="store trades ≥ this notional")
    s.add_argument("--include-low", action="store_true", help="also lowest-temperature markets")
    s.add_argument("--all-sides", action="store_true", help="include maker fills (takerOnly=false)")
    s.add_argument("--no-leaderboard", action="store_true")
    s.set_defaults(fn=cmd_sync)

    s = sub.add_parser("whales", help="rank wallets")
    s.add_argument("--days", type=int, default=7)
    s.add_argument("--by", choices=["biggest", "volume", "pnl", "roi"], default="biggest")
    s.add_argument("--top", type=int, default=25)
    s.add_argument("--min-trades", type=int, default=1)
    s.add_argument("--full", action="store_true", help="also print full wallet addresses")
    s.set_defaults(fn=cmd_whales)

    s = sub.add_parser("leaderboard", help="Polymarket's WEATHER leaderboard")
    s.add_argument("--period", choices=["DAY", "WEEK", "MONTH", "ALL"], default="MONTH")
    s.add_argument("--top", type=int, default=25)
    s.set_defaults(fn=cmd_leaderboard)

    s = sub.add_parser("wallet", help="one wallet's temperature trades")
    s.add_argument("wallet")
    s.add_argument("--limit", type=int, default=40)
    s.add_argument("--live", action="store_true", help="refresh from the API first")
    s.set_defaults(fn=cmd_wallet)

    s = sub.add_parser("signal", help="whale $ mapped onto a Polymarket US ladder")
    s.add_argument("city", help="nyc | mia | mdw | lax | sfo (or intl slug names)")
    s.add_argument("date", help="YYYY-MM-DD")
    s.add_argument("--min-usd", type=float, default=500, help="count any trade ≥ this as whale money")
    s.add_argument("--top", type=int, default=20, help="also count every trade by the top-N wallets")
    s.add_argument("--days", type=int, default=30, help="lookback for picking top wallets")
    s.add_argument("--min-usd-store", type=float, default=25)
    s.add_argument("--no-sync", action="store_true")
    s.set_defaults(fn=cmd_signal)

    s = sub.add_parser("follow", help="live alerts")
    s.add_argument("--interval", type=int, default=60)
    s.add_argument("--min-trade", type=float, default=1000, help="alert on any trade ≥ this")
    s.add_argument("--min-usd", type=float, default=25, help="alert on watched wallets' trades ≥ this")
    s.add_argument("--top", type=int, default=20, help="auto-watch top-N wallets")
    s.add_argument("--days", type=int, default=30)
    s.add_argument("--notify", action="store_true", help="macOS notification per alert")
    s.add_argument("--include-sweeps", action="store_true", help="also alert on fills at ≥0.98 / ≤0.02")
    s.set_defaults(fn=cmd_follow)

    s = sub.add_parser("selftest")
    s.set_defaults(fn=cmd_selftest)

    a = ap.parse_args(argv)
    db = connect(a.db) if a.cmd != "selftest" else None
    a.fn(a, db)


if __name__ == "__main__":
    main()
