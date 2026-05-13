"""audimo-importers — music platform import addon.

Pulls track lists from public sources (Spotify public playlists via
anonymous web-player token; CSV exports from Exportify et al. coming
later), queues them in SQLite, and serves a tab UI that drives core's
window.audimo.acquireTrack() to download N at a time.

Architecture:
  - sidecar (this file): owns the queue + import logic
  - iframe page (/ui/page): renders the queue, runs the worker loop
    that calls window.audimo.acquireTrack via the postMessage RPC
    bridge and PATCHes status back to /api/queue/{id}.

No source-resolution logic lives here — that's core's job (via
acquireTrack). The addon never inspects the chosen source.
"""
from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import re
import sqlite3
import time
import urllib.parse
from pathlib import Path

import httpx
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

PORT = int(os.environ.get("AUDIMO_ADDON_PORT", 9010))
DATA_DIR = Path(os.environ.get("AUDIMO_ADDON_DATA", Path.home() / ".audimo-importers"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "queue.db"
MANIFEST_PATH = Path(__file__).resolve().parent / "manifest.json"
UI_PATH = Path(__file__).resolve().parent / "ui" / "page.html"

app = FastAPI(title="audimo-importers")

# Permissive CORS for now — the iframe is same-origin, but the
# manifest fetch from the core app comes from a different origin
# (the desktop WebView or 127.0.0.1:8000). Mirror what other addons
# in this org do; tighten later if we add credentials.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)


# ── DB ──────────────────────────────────────────────────────────────

def _db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    return con


def _init_db() -> None:
    with _db() as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS queue (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              batch_id TEXT NOT NULL,
              title TEXT NOT NULL,
              artist TEXT NOT NULL DEFAULT '',
              album TEXT NOT NULL DEFAULT '',
              isrc TEXT NOT NULL DEFAULT '',
              duration_ms INTEGER NOT NULL DEFAULT 0,
              status TEXT NOT NULL DEFAULT 'pending',
              error TEXT NOT NULL DEFAULT '',
              policy_json TEXT NOT NULL DEFAULT '{}',
              source TEXT NOT NULL DEFAULT '',
              addon_id TEXT NOT NULL DEFAULT '',
              pct INTEGER NOT NULL DEFAULT 0,
              created_at INTEGER NOT NULL,
              updated_at INTEGER NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS batches (
              id TEXT PRIMARY KEY,
              source_label TEXT NOT NULL,
              policy_json TEXT NOT NULL DEFAULT '{}',
              total INTEGER NOT NULL DEFAULT 0,
              created_at INTEGER NOT NULL
            )
            """
        )


_init_db()


def _now() -> int:
    return int(time.time())


def _row_dict(r: sqlite3.Row) -> dict:
    d = dict(r)
    try:
        d["policy"] = json.loads(d.pop("policy_json") or "{}")
    except Exception:
        d["policy"] = {}
    return d


# ── Spotify public-playlist import (embed-page scrape) ─────────────
#
# Spotify killed the anonymous web-player token endpoint in 2024 (it
# now returns 403/400 with an explicit "not permitted under Developer
# Terms" message). The embed page at /embed/playlist/{id} still
# renders publicly without auth and ships the first ~50 tracks
# inline as JSON inside a __NEXT_DATA__ script tag. That's the path
# we use.
#
# Trade-offs vs. the old API approach:
#   • Capped at the first ~50 tracks per playlist. Longer playlists
#     are silently truncated by Spotify's embed; we surface this in
#     the import result so the UI can warn.
#   • No album name and no ISRC — embed only carries title+artist+
#     duration. Source resolution by title+artist works fine for
#     the streamers/indexers; it's the cross-platform matchers that
#     suffer (none plumbed yet, so fine for v0.1).
#   • Public playlists only. Private/Liked require real OAuth.

_SPOTIFY_PLAYLIST_ID_RE = re.compile(
    r"(?:^|/)playlist/([A-Za-z0-9]+)|spotify:playlist:([A-Za-z0-9]+)"
)
_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.DOTALL,
)
_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0 Safari/537.36"
)


def _extract_spotify_playlist_id(url_or_id: str) -> str | None:
    s = (url_or_id or "").strip()
    if not s:
        return None
    if re.fullmatch(r"[A-Za-z0-9]{20,}", s):
        return s
    m = _SPOTIFY_PLAYLIST_ID_RE.search(s)
    if m:
        return m.group(1) or m.group(2)
    return None


def _walk_for_track_list(obj):
    """Find the first list keyed under 'trackList' anywhere in the tree.

    Defensive against Spotify reshuffling __NEXT_DATA__ paths over time.
    The canonical path today is
    props.pageProps.state.data.trackList — but we walk to survive
    future renames.
    """
    if isinstance(obj, dict):
        if isinstance(obj.get("trackList"), list):
            return obj["trackList"]
        for v in obj.values():
            r = _walk_for_track_list(v)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for v in obj:
            r = _walk_for_track_list(v)
            if r is not None:
                return r
    return None


def _walk_for_entity_name(obj):
    """Pull the playlist's display name from the entity record. Same
    defensive walk; canonical path today is
    props.pageProps.state.data.entity.name."""
    if isinstance(obj, dict):
        entity = obj.get("entity")
        if isinstance(entity, dict) and isinstance(entity.get("name"), str):
            return entity["name"]
        for v in obj.values():
            r = _walk_for_entity_name(v)
            if r:
                return r
    elif isinstance(obj, list):
        for v in obj:
            r = _walk_for_entity_name(v)
            if r:
                return r
    return None


async def _fetch_spotify_playlist(playlist_url: str) -> tuple[str, list[dict], bool]:
    """Return (playlist_label, tracks, truncated).

    `truncated` is True when Spotify's embed capped the list at its
    page size (currently 50). The caller can warn the user that
    only the first N tracks were imported.
    """
    pid = _extract_spotify_playlist_id(playlist_url)
    if not pid:
        raise HTTPException(400, "Could not parse Spotify playlist URL")

    embed_url = f"https://open.spotify.com/embed/playlist/{pid}"
    headers = {
        "User-Agent": _BROWSER_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
    async with httpx.AsyncClient(follow_redirects=True) as client:
        r = await client.get(embed_url, headers=headers, timeout=15)
    if r.status_code == 404:
        raise HTTPException(404, "Playlist not found")
    if r.status_code != 200:
        raise HTTPException(502, f"Spotify embed returned {r.status_code}")

    m = _NEXT_DATA_RE.search(r.text)
    if not m:
        raise HTTPException(502, "Spotify embed did not contain track data")
    try:
        data = json.loads(m.group(1))
    except Exception:
        raise HTTPException(502, "Could not parse Spotify embed payload")

    track_list = _walk_for_track_list(data) or []
    label = _walk_for_entity_name(data) or "Spotify playlist"

    tracks: list[dict] = []
    for t in track_list:
        if not isinstance(t, dict):
            continue
        name = (t.get("title") or "").strip()
        if not name:
            continue
        # `subtitle` is artist(s). For multi-artist tracks the embed
        # joins them with ", " already; pass through verbatim.
        artist = (t.get("subtitle") or "").strip()
        tracks.append({
            "title": name,
            "artist": artist,
            "album": "",  # not in embed payload
            "isrc": "",   # not in embed payload
            "duration_ms": int(t.get("duration") or 0),
        })

    # The embed page caps at 50 tracks today. If we got exactly the
    # cap, the source playlist might be longer — flag for the caller.
    truncated = len(tracks) >= 50
    return label, tracks, truncated


# ── Routes: manifest, UI, queue, import ────────────────────────────

@app.get("/manifest.json")
async def manifest():
    return JSONResponse(json.loads(MANIFEST_PATH.read_text()))


@app.get("/ui/page", response_class=HTMLResponse)
async def ui_page():
    if UI_PATH.exists():
        return HTMLResponse(UI_PATH.read_text())
    return HTMLResponse("<!doctype html><h1>UI missing</h1>", status_code=500)


# ── CSV import (Exportify et al.) ───────────────────────────────────
#
# Bulletproof unlimited-tracks path: user runs exportify.net (or
# similar), exports any playlist (including private + Liked Songs) to
# CSV, drops the file in. We auto-detect common schemas.
#
# Exportify column names (canonical):
#   Track Name, Album Name, Artist Name(s), Release Date,
#   Duration (ms), ISRC, ...
#
# Soundiiz, TuneMyMusic, and hand-rolled CSVs use slightly different
# headers — we normalize via a synonym map. Unknown columns are
# ignored; missing-but-needed columns raise.

_CSV_HEADER_SYNONYMS = {
    "title": ["track name", "title", "name", "song", "song name", "track"],
    "artist": ["artist name(s)", "artist", "artists", "artist name", "artist names"],
    "album": ["album name", "album"],
    "isrc": ["isrc"],
    "duration_ms": ["duration (ms)", "duration_ms", "duration ms"],
    "duration_s": ["duration (s)", "duration", "length", "duration (seconds)"],
}


def _csv_normalize_header(h: str) -> str:
    return (h or "").strip().lower()


def _csv_pick_column(header: list[str], canonical: str) -> int | None:
    wants = _CSV_HEADER_SYNONYMS.get(canonical, [])
    norm = [_csv_normalize_header(h) for h in header]
    for w in wants:
        if w in norm:
            return norm.index(w)
    return None


def _parse_csv_tracks(text: str) -> list[dict]:
    # Strip UTF-8 BOM if present, then sniff dialect. Default to comma
    # if the sniffer can't decide — Exportify and friends all use it.
    text = (text or "").lstrip("﻿")
    if not text.strip():
        raise HTTPException(400, "CSV is empty")
    try:
        dialect = csv.Sniffer().sniff(text[:4096], delimiters=",;\t")
    except csv.Error:
        dialect = csv.excel
    reader = csv.reader(io.StringIO(text), dialect)
    rows = list(reader)
    if not rows:
        raise HTTPException(400, "CSV has no rows")
    header = rows[0]
    body = rows[1:]
    if not body:
        raise HTTPException(400, "CSV has a header but no data rows")

    idx_title = _csv_pick_column(header, "title")
    idx_artist = _csv_pick_column(header, "artist")
    if idx_title is None or idx_artist is None:
        raise HTTPException(
            400,
            "Could not find Track Name + Artist columns. Expected headers "
            "like 'Track Name' and 'Artist Name(s)' (Exportify) or "
            "'Title' and 'Artist'.",
        )
    idx_album = _csv_pick_column(header, "album")
    idx_isrc = _csv_pick_column(header, "isrc")
    idx_dur_ms = _csv_pick_column(header, "duration_ms")
    idx_dur_s = _csv_pick_column(header, "duration_s")

    def cell(row, i):
        return (row[i].strip() if i is not None and i < len(row) else "")

    tracks: list[dict] = []
    for row in body:
        title = cell(row, idx_title)
        artist = cell(row, idx_artist)
        if not title or not artist:
            continue
        if idx_dur_ms is not None:
            try:
                dur_ms = int(float(cell(row, idx_dur_ms) or 0))
            except ValueError:
                dur_ms = 0
        elif idx_dur_s is not None:
            try:
                dur_ms = int(float(cell(row, idx_dur_s) or 0) * 1000)
            except ValueError:
                dur_ms = 0
        else:
            dur_ms = 0
        tracks.append({
            "title": title,
            "artist": artist,
            "album": cell(row, idx_album),
            "isrc": cell(row, idx_isrc),
            "duration_ms": dur_ms,
        })
    if not tracks:
        raise HTTPException(400, "CSV had rows but none with title+artist")
    return tracks


def _commit_batch(label: str, tracks: list[dict], policy: dict, source_id: str) -> dict:
    batch_id = f"b_{_now()}_{source_id}"
    now = _now()
    policy_json = json.dumps(policy or {})
    with _db() as con:
        con.execute(
            "INSERT OR REPLACE INTO batches (id, source_label, policy_json, total, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (batch_id, label, policy_json, len(tracks), now),
        )
        con.executemany(
            "INSERT INTO queue (batch_id, title, artist, album, isrc, duration_ms, "
            "policy_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (batch_id, t["title"], t["artist"], t["album"], t["isrc"],
                 t["duration_ms"], policy_json, now, now)
                for t in tracks
            ],
        )
    return {"batch_id": batch_id, "label": label, "count": len(tracks)}


class ImportCSVBody(BaseModel):
    csv: str
    label: str | None = None
    policy: dict = Field(default_factory=dict)


@app.post("/import/csv")
async def import_csv_paste(body: ImportCSVBody):
    tracks = _parse_csv_tracks(body.csv)
    label = (body.label or "").strip() or "CSV import"
    return _commit_batch(label, tracks, body.policy, "csv")


@app.post("/import/csv_file")
async def import_csv_file(
    file: UploadFile = File(...),
    label: str = Form(""),
    policy: str = Form("{}"),
):
    raw = await file.read()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("latin-1")
    tracks = _parse_csv_tracks(text)
    try:
        pol = json.loads(policy or "{}") or {}
    except json.JSONDecodeError:
        pol = {}
    # Use the filename (sans extension) as the batch label when the
    # user didn't supply one. Exportify saves as
    # "playlist-name__owner.csv" — fine to keep as-is.
    if not label:
        label = (file.filename or "CSV import").rsplit(".", 1)[0]
    return _commit_batch(label, tracks, pol, "csvfile")


class ImportURLBody(BaseModel):
    url: str
    policy: dict = Field(default_factory=dict)


@app.post("/import/url")
async def import_url(body: ImportURLBody):
    s = (body.url or "").lower()
    if "spotify" not in s and not _extract_spotify_playlist_id(body.url):
        raise HTTPException(400, "Only Spotify playlists are supported in v0.1")
    label, tracks, truncated = await _fetch_spotify_playlist(body.url)
    if not tracks:
        raise HTTPException(404, "Playlist is empty or unreadable")
    pid = _extract_spotify_playlist_id(body.url) or "spotify"
    result = _commit_batch(label, tracks, body.policy, pid)
    result["truncated"] = truncated
    return result


@app.get("/api/queue")
async def get_queue(status: str | None = None, limit: int = 500):
    q = "SELECT * FROM queue"
    args: tuple = ()
    if status:
        q += " WHERE status = ?"
        args = (status,)
    q += " ORDER BY id ASC LIMIT ?"
    args = args + (max(1, min(2000, int(limit))),)
    with _db() as con:
        rows = con.execute(q, args).fetchall()
        counts_rows = con.execute(
            "SELECT status, COUNT(*) AS c FROM queue GROUP BY status"
        ).fetchall()
    counts = {r["status"]: r["c"] for r in counts_rows}
    return {
        "items": [_row_dict(r) for r in rows],
        "counts": {
            "pending": counts.get("pending", 0),
            "downloading": counts.get("downloading", 0),
            "done": counts.get("done", 0),
            "failed": counts.get("failed", 0),
            "total": sum(counts.values()),
        },
    }


@app.get("/api/queue/next")
async def claim_next():
    """Atomically claim the next pending row and mark it downloading.

    The iframe worker calls this in a loop with concurrency=N. SQLite
    serializes the UPDATE so two parallel workers don't double-claim.
    """
    now = _now()
    with _db() as con:
        cur = con.execute(
            "SELECT id FROM queue WHERE status = 'pending' "
            "ORDER BY id ASC LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            return {"item": None}
        cur = con.execute(
            "UPDATE queue SET status='downloading', updated_at=? "
            "WHERE id=? AND status='pending'",
            (now, row["id"]),
        )
        if cur.rowcount == 0:
            # Lost the race — try again.
            return {"item": None}
        item = con.execute("SELECT * FROM queue WHERE id=?", (row["id"],)).fetchone()
    return {"item": _row_dict(item)}


class PatchBody(BaseModel):
    status: str | None = None
    error: str | None = None
    source: str | None = None
    addon_id: str | None = None
    pct: int | None = None


@app.patch("/api/queue/{row_id}")
async def patch_row(row_id: int, body: PatchBody):
    sets, args = [], []
    for f in ("status", "error", "source", "addon_id"):
        v = getattr(body, f)
        if v is not None:
            sets.append(f"{f} = ?")
            args.append(v)
    if body.pct is not None:
        sets.append("pct = ?")
        args.append(max(0, min(100, int(body.pct))))
    if not sets:
        return {"ok": True}
    sets.append("updated_at = ?")
    args.append(_now())
    args.append(row_id)
    with _db() as con:
        con.execute(f"UPDATE queue SET {', '.join(sets)} WHERE id = ?", tuple(args))
    return {"ok": True}


@app.post("/api/queue/{row_id}/retry")
async def retry_row(row_id: int):
    with _db() as con:
        con.execute(
            "UPDATE queue SET status='pending', error='', pct=0, updated_at=? WHERE id=?",
            (_now(), row_id),
        )
    return {"ok": True}


@app.delete("/api/queue/{row_id}")
async def delete_row(row_id: int):
    with _db() as con:
        con.execute("DELETE FROM queue WHERE id = ?", (row_id,))
    return {"ok": True}


@app.post("/api/queue/clear_done")
async def clear_done():
    with _db() as con:
        con.execute("DELETE FROM queue WHERE status = 'done'")
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=PORT, log_level="info")
