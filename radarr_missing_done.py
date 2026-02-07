#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import random
import sys
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


def load_json(path: Path, default: Dict[str, Any]) -> Dict[str, Any]:
    try:
        if path.exists():
            data = json.loads(path.read_text("utf-8"))
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return dict(default)


def atomic_write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True), "utf-8")
    tmp.replace(path)


RADARR_URL = os.environ.get("RADARR_URL", "http://radarr:7878").rstrip("/")
RADARR_API_KEY = os.environ.get("RADARR_API_KEY", "").strip()
TAG_SEARCH = os.environ.get("RADARR_TAG_SEARCH", "search")
TAG_DONE = os.environ.get("RADARR_TAG_DONE", "done")

DONE_RECHECK_HOURS = int(os.environ.get("RADARR_DONE_RECHECK_HOURS", "24"))
DONE_RECHECK_MAX = int(os.environ.get("RADARR_DONE_RECHECK_MAX_MOVIES_PER_RUN", "20"))
DONE_SEARCH_MAX = int(os.environ.get("RADARR_DONE_SEARCH_MAX_MOVIES_PER_RUN", "10"))

STATE_PATH = Path(os.environ.get("RADARR_MISSING_DONE_STATE_PATH", "/data/state/radarr_missing_done_state.json"))
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "30"))

class RadarrClient:
    def __init__(self, url: str, api_key: str):
        self.url = url
        self.s = requests.Session()
        self.s.headers.update({"X-Api-Key": api_key})

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        r = self.s.get(f"{self.url}{path}", params=params, timeout=HTTP_TIMEOUT)
        if r.status_code == 401:
            raise RuntimeError("401 Unauthorized (check RADARR_API_KEY)")
        r.raise_for_status()
        return r.json()

    def post(self, path: str, payload: Any) -> Any:
        r = self.s.post(f"{self.url}{path}", json=payload, timeout=HTTP_TIMEOUT)
        if r.status_code == 401:
            raise RuntimeError("401 Unauthorized (check RADARR_API_KEY)")
        r.raise_for_status()
        return r.json() if r.text.strip() else None

    def put(self, path: str, payload: Any) -> Any:
        r = self.s.put(f"{self.url}{path}", json=payload, timeout=HTTP_TIMEOUT)
        if r.status_code == 401:
            raise RuntimeError("401 Unauthorized (check RADARR_API_KEY)")
        r.raise_for_status()
        return r.json() if r.text.strip() else None

    def all_tags(self) -> List[Dict[str, Any]]:
        return self.get("/api/v3/tag")

    def create_tag(self, label: str) -> Dict[str, Any]:
        return self.post("/api/v3/tag", {"label": label})

    def all_movies(self) -> List[Dict[str, Any]]:
        return self.get("/api/v3/movie")

    def update_movie(self, movie_obj: Dict[str, Any]) -> None:
        self.put("/api/v3/movie", movie_obj)

    def movies_search(self, movie_ids: List[int]) -> None:
        self.post("/api/v3/command", {"name": "MoviesSearch", "movieIds": movie_ids})

def tag_id_by_label(tags: List[Dict[str, Any]], label: str) -> Optional[int]:
    want = label.strip().lower()
    for t in tags:
        if str(t.get("label", "")).strip().lower() == want:
            try:
                return int(t["id"])
            except Exception:
                return None
    return None

def ensure_tag(client: RadarrClient, label: str) -> int:
    tags = client.all_tags()
    tid = tag_id_by_label(tags, label)
    if tid is not None:
        return tid
    created = client.create_tag(label)
    if isinstance(created, dict) and "id" in created:
        return int(created["id"])
    tags = client.all_tags()
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

def set_done(movie: Dict[str, Any], search_tid: int, done_tid: int) -> bool:
    tags = [int(x) for x in (movie.get("tags") or []) if str(x).isdigit()]
    new_tags = [t for t in tags if t != search_tid]
    if done_tid not in new_tags:
        new_tags.append(done_tid)
    changed = sorted(new_tags) != sorted(tags)
    movie["tags"] = new_tags
    return changed

def should_wait(last_iso: Optional[str], hours: int, now: datetime) -> bool:
    if not last_iso:
        return False
    dt = parse_dt(last_iso)
    if not dt:
        return False
    return now < (dt + timedelta(hours=hours))

def main() -> None:
    if not RADARR_API_KEY:
        raise SystemExit("RADARR_API_KEY is required")

    client = RadarrClient(RADARR_URL, RADARR_API_KEY)
    now = utc_now()

    search_tid = ensure_tag(client, TAG_SEARCH)
    done_tid = ensure_tag(client, TAG_DONE)

    state = load_json(STATE_PATH, {"movies": {}})
    state.setdefault("movies", {})

    movies = client.all_movies()
    search_tagged = [m for m in movies if has_tag(m, search_tid)]
    done_tagged = [m for m in movies if has_tag(m, done_tid)]

    log("radarr_missing_done", f"Tagged '{TAG_SEARCH}': {len(search_tagged)} movies")
    log("radarr_missing_done", f"Tagged '{TAG_DONE}': {len(done_tagged)} movies")

    # Required: SEARCH->DONE when not missing
    search_to_done = 0
    for m in search_tagged:
        if bool(m.get("hasFile", False)):
            if set_done(m, search_tid, done_tid):
                try:
                    client.update_movie(m)
                    search_to_done += 1
                    log("radarr_missing_done", f"SEARCH->DONE (has file): movieId={m.get('id')}")
                except Exception as e:
                    log("radarr_missing_done", f"ERROR update_movie movieId={m.get('id')}: {e}")

    missing_done = [m for m in done_tagged if not bool(m.get("hasFile", False))]
    random.shuffle(missing_done)

    recheck_limit = DONE_RECHECK_MAX if DONE_RECHECK_MAX > 0 else 10**9
    search_limit = DONE_SEARCH_MAX if DONE_SEARCH_MAX > 0 else 10**9

    done_wait_skipped = 0
    done_considered = 0
    to_search: List[int] = []

    for m in missing_done:
        if done_considered >= recheck_limit:
            break
        mid = int(m.get("id"))
        last_recheck = state["movies"].get(str(mid), {}).get("last_done_recheck_utc")
        if should_wait(last_recheck, DONE_RECHECK_HOURS, now):
            done_wait_skipped += 1
            continue
        done_considered += 1
        state["movies"].setdefault(str(mid), {})["last_done_recheck_utc"] = now.isoformat()
        to_search.append(mid)
        if len(to_search) >= search_limit:
            break

    searched = 0
    if to_search:
        try:
            client.movies_search(to_search)
            searched = len(to_search)
            for mid in to_search:
                state["movies"].setdefault(str(mid), {})["last_done_searched_utc"] = now.isoformat()
            log("radarr_missing_done", f"MoviesSearch queued (DONE missing): {searched} movie(s)")
        except Exception as e:
            log("radarr_missing_done", f"ERROR MoviesSearch DONE: {e}")

    atomic_write_json(STATE_PATH, state)
    log("radarr_missing_done", f"Done. search_to_done={search_to_done} done_missing_searched={searched} done_wait_skipped={done_wait_skipped} state={STATE_PATH}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
