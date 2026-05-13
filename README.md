# audimo-importers

Audimo addon: import playlists from other music platforms and download
them through your installed source addons (streamers / soulseek /
indexers / debrid) N at a time.

## v0.1

- **Spotify public playlists** via anonymous web-player token (no app
  registration needed; public playlists only — Liked Songs / private
  playlists need real OAuth, deferred).
- **Per-batch policy:** at import time, pick an addon to prefer; the
  worker falls back to any available source if the preferred addon
  doesn't have the track.
- **Worker loop** with configurable concurrency runs in the iframe
  and drives core's `window.audimo.acquireTrack` via the addon RPC
  bridge.
- **Queue UI:** Pending / Downloading (with progress bar) / Done /
  Failed (with retry). SQLite-backed, persists across restarts.

## Coming later

- CSV / JSON import (Exportify, Soundiiz, TuneMyMusic).
- Apple Music public playlists.
- YouTube Music playlists.
- BYO Spotify client ID for Liked Songs + private playlists.
- Per-row policy override.

## Dev

```bash
./run_native.sh
# addon serves on http://localhost:9010
# install in Audimo: Addons → Add URL → http://localhost:9010
```

## Requirements

- Audimo native app ≥ 0.4.0 (needs the `ui.tab.page_url` iframe support
  + `window.audimo.acquireTrack` RPC bridge).
- At least one installed addon that advertises `resolve.sources` —
  e.g. audimo-streamers (YouTube/SoundCloud/Bandcamp), audimo-soulseek,
  or audimo-indexers + a debrid backend.
