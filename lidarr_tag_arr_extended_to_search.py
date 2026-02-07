#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import random
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

LIDARR_URL = os.environ.get("LIDARR_URL", "").rstrip("/")
LIDARR_API_KEY = os.environ.get("LIDARR_API_KEY", "")
TAG_FROM = os.environ.get("LIDARR_TAG_FROM", "arr-extended").strip()
TAG_TO = os.environ.get("LIDARR_TAG_TO", "search").strip()
MAX_PER_RUN = int(os.environ.get("LIDARR_TAGGER_MAX_ARTISTS_PER_RUN", "500"))  # 0 = unlimited
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "30"))

def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[lidarr_tagger] {ts} {msg}", flush=True)

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

def has_tag(obj: Dict[str, Any], tag_id: int) -> bool:
    tags = obj.get("tags") or []
    try:
        return int(tag_id) in [int(x) for x in tags]
    except Exception:
        return False

def get_artist(artist_id: int) -> Dict[str, Any]:
    d = api_get(f"/api/v1/artist/{artist_id}")
    if not isinstance(d, dict):
        raise RuntimeError("Unexpected artist payload")
    return d

def update_artist(artist: Dict[str, Any]) -> None:
    api_put("/api/v1/artist", artist)

def main() -> None:
    if not LIDARR_URL or not LIDARR_API_KEY:
        raise SystemExit("LIDARR_URL and LIDARR_API_KEY are required")

    from_id = ensure_tag(TAG_FROM)
    to_id = ensure_tag(TAG_TO)

    artists = api_get("/api/v1/artist")
    if not isinstance(artists, list):
        raise RuntimeError("Unexpected /api/v1/artist response")

    candidates = [a for a in artists if isinstance(a, dict) and has_tag(a, from_id)]
    random.shuffle(candidates)
    if MAX_PER_RUN > 0:
        candidates = candidates[:MAX_PER_RUN]

    log(f"Found {len(candidates)} artist(s) tagged '{TAG_FROM}' (convert -> '{TAG_TO}')")

    converted = 0
    for a in candidates:
        aid = int(a.get("id", 0) or 0)
        name = str(a.get("artistName") or a.get("name") or f"id={aid}")
        if aid <= 0:
            continue

        artist = get_artist(aid)
        tags = artist.get("tags") or []
        if not isinstance(tags, list):
            tags = []

        new_tags: List[int] = []
        for t in tags:
            try:
                ti = int(t)
            except Exception:
                continue
            if ti == from_id:
                continue
            new_tags.append(ti)
        if to_id not in new_tags:
            new_tags.append(to_id)

        artist["tags"] = sorted(list({int(x) for x in new_tags}))
        try:
            update_artist(artist)
            converted += 1
            log(f"Updated: {name} (id={aid}) '{TAG_FROM}' -> '{TAG_TO}'")
        except Exception as e:
            log(f"ERROR updating {name} (id={aid}): {e}")

    log(f"Done. converted={converted}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
