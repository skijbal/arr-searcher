from __future__ import annotations

import json
import os
import sys
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None

def log(prefix: str, msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{prefix}] {ts} {msg}", flush=True)

LIDARR_URL = os.environ.get("LIDARR_URL", "http://lidarr:8686").rstrip("/")
LIDARR_API_KEY = os.environ.get("LIDARR_API_KEY", "").strip()
TAG_SEARCH = os.environ.get("LIDARR_TAG_SEARCH", "search")
TAG_DONE = os.environ.get("LIDARR_TAG_DONE", "done")

COOLDOWN_DAYS = int(os.environ.get("LIDARR_COOLDOWN_DAYS", "7"))
MAX_ARTISTS_PER_RUN = int(os.environ.get("LIDARR_SEARCH_MAX_ARTISTS_PER_RUN", "10"))
STATE_PATH = Path(os.environ.get("LIDARR_SEARCH_STATE_PATH", "/data/state/lidarr_search_state.json"))
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "30"))

def headers() -> Dict[str, str]:
    return {"X-Api-Key": LIDARR_API_KEY}

def api_get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    r = requests.get(f"{LIDARR_URL}{path}", headers=headers(), params=params, timeout=HTTP_TIMEOUT)
    if r.status_code == 401:
        raise RuntimeError("401 Unauthorized (check LIDARR_API_KEY)")
    r.raise_for_status()
    return r.json()

def api_post(path: str, payload: Dict[str, Any]) -> Any:
    r = requests.post(f"{LIDARR_URL}{path}", headers=headers(), json=payload, timeout=HTTP_TIMEOUT)
    if r.status_code == 401:
        raise RuntimeError("401 Unauthorized (check LIDARR_API_KEY)")
    r.raise_for_status()
    return r.json() if r.text.strip() else None

def api_put(path: str, payload: Any) -> Any:
    r = requests.put(f"{LIDARR_URL}{path}", headers=headers(), json=payload, timeout=HTTP_TIMEOUT)
    if r.status_code == 401:
        raise RuntimeError("401 Unauthorized (check LIDARR_API_KEY)")
    r.raise_for_status()
    return r.json() if r.text.strip() else None

def tag_id_by_label(tags: List[Dict[str, Any]], label: str) -> Optional[int]:
    want = label.strip().lower()
    for t in tags:
        if str(t.get("label", "")).strip().lower() == want:
            try:
                return int(t["id"])
            except Exception:
                return None
    return None

def ensure_tag(label: str) -> int:
    tags = api_get("/api/v1/tag")
    if not isinstance(tags, list):
        raise RuntimeError("Unexpected /api/v1/tag response")
    tid = tag_id_by_label(tags, label)
    if tid is not None:
        return tid
    created = api_post("/api/v1/tag", {"label": label})
    if isinstance(created, dict) and "id" in created:
        return int(created["id"])
    tags = api_get("/api/v1/tag")
    tid = tag_id_by_label(tags, label)
    if tid is None:
        raise RuntimeError(f"Unable to create/find tag {label!r}")
    return tid

def has_tag(obj: Dict[str, Any], tid: int) -> bool:
    tags = obj.get("tags") or []
    try:
        return int(tid) in [int(x) for x in tags]
    except Exception:
        return False

def get_artist(artist_id: int) -> Dict[str, Any]:
    d = api_get(f"/api/v1/artist/{artist_id}")
    if not isinstance(d, dict):
        raise RuntimeError("Unexpected artist payload")
    return d

def update_artist(artist: Dict[str, Any]) -> None:
    api_put("/api/v1/artist", artist)

def wanted_missing_count_for_artist(artist_id: int) -> int:
    # best-effort server-side filter
    for params in ({"artistId": artist_id, "page": 1, "pageSize": 1}, {"artistId": artist_id}):
        try:
            data = api_get("/api/v1/wanted/missing", params=params)
            if isinstance(data, dict):
                if isinstance(data.get("totalRecords"), int):
                    return int(data["totalRecords"])
                recs = data.get("records")
                if isinstance(recs, list):
                    return len(recs)
            if isinstance(data, list) and "artistId" in params:
                return len(data)
        except Exception:
            pass

    # fallback: small page and filter
    try:
        data = api_get("/api/v1/wanted/missing", params={"page": 1, "pageSize": 200})
        if isinstance(data, dict) and isinstance(data.get("records"), list):
            return sum(1 for r in data["records"] if isinstance(r, dict) and int(r.get("artistId", -1)) == int(artist_id))
    except Exception:
        pass

    return 1  # conservative

def queue_missing_search(artist_id: int, name: str) -> bool:
    for payload in ({"name": "MissingAlbumSearch", "artistId": artist_id}, {"name": "ArtistSearch", "artistId": artist_id}):
        try:
            api_post("/api/v1/command", payload)
            log("lidarr_search", f"{payload['name']} queued: {name} id={artist_id}")
            return True
        except Exception as e:
            log("lidarr_search", f"ERROR {payload['name']} for {name} id={artist_id}: {e}")
    return False

def should_cooldown(last_iso: Optional[str], days: int, now: datetime) -> bool:
    if not last_iso:
        return False
    dt = parse_dt(last_iso)
    if not dt:
        return False
    return now < (dt + timedelta(days=days))

def load_state() -> Dict[str, Any]:
    try:
        if STATE_PATH.exists():
            d = json.loads(STATE_PATH.read_text("utf-8"))
            if isinstance(d, dict):
                d.setdefault("artists", {})
                return d
    except Exception:
        pass
    return {"artists": {}}

def save_state(state: Dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(STATE_PATH.suffix + ".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True), "utf-8")
    tmp.replace(STATE_PATH)

def main() -> None:
    if not LIDARR_URL or not LIDARR_API_KEY:
        raise SystemExit("LIDARR_URL and LIDARR_API_KEY are required")

    now = utc_now()
    search_tid = ensure_tag(TAG_SEARCH)
    done_tid = ensure_tag(TAG_DONE)

    state = load_state()
    state.setdefault("artists", {})

    artists = api_get("/api/v1/artist")
    if not isinstance(artists, list):
        raise RuntimeError("Unexpected /api/v1/artist response")

    search_tagged = [a for a in artists if isinstance(a, dict) and has_tag(a, search_tid)]
    log("lidarr_search", f"Tagged '{TAG_SEARCH}': {len(search_tagged)} artists")

    # Required: SEARCH->DONE if not missing
    search_to_done = 0
    eligible: List[Dict[str, Any]] = []

    for a in search_tagged:
        aid = int(a.get("id", 0) or 0)
        name = str(a.get("artistName") or a.get("name") or f"id={aid}")
        if aid <= 0:
            continue

        missing_count = wanted_missing_count_for_artist(aid)
        if missing_count <= 0:
            artist = get_artist(aid)
            tags = artist.get("tags") or []
            if not isinstance(tags, list):
                tags = []
            new_tags = []
            for t in tags:
                try:
                    ti = int(t)
                except Exception:
                    continue
                if ti == search_tid:
                    continue
                new_tags.append(ti)
            if done_tid not in new_tags:
                new_tags.append(done_tid)
            artist["tags"] = sorted(list({int(x) for x in new_tags}))
            try:
                update_artist(artist)
                search_to_done += 1
                log("lidarr_search", f"SEARCH->DONE (no missing): {name} id={aid}")
            except Exception as e:
                log("lidarr_search", f"ERROR update_artist {name} id={aid}: {e}")
            continue

        eligible.append(a)

    random.shuffle(eligible)
    limit = MAX_ARTISTS_PER_RUN if MAX_ARTISTS_PER_RUN > 0 else 10**9

    searched = 0
    cooldown_skipped = 0

    for a in eligible:
        if searched >= limit:
            break
        aid = int(a.get("id", 0) or 0)
        name = str(a.get("artistName") or a.get("name") or f"id={aid}")
        last_iso = state["artists"].get(str(aid), {}).get("last_searched_utc")
        if should_cooldown(last_iso, COOLDOWN_DAYS, now):
            cooldown_skipped += 1
            continue

        if queue_missing_search(aid, name):
            state["artists"].setdefault(str(aid), {})["last_searched_utc"] = now.isoformat()
            searched += 1

    save_state(state)
    log("lidarr_search", f"Done. search_to_done={search_to_done} searched={searched} cooldown_skipped={cooldown_skipped} state={STATE_PATH}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
