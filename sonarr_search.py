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


SONARR_URL = os.environ.get("SONARR_URL", "http://sonarr:8989").rstrip("/")
SONARR_API_KEY = os.environ.get("SONARR_API_KEY", "").strip()
TAG_SEARCH = os.environ.get("SONARR_TAG_SEARCH", "search")
TAG_DONE = os.environ.get("SONARR_TAG_DONE", "done")

COOLDOWN_DAYS = int(os.environ.get("SONARR_COOLDOWN_DAYS", "7"))
MAX_SERIES_PER_RUN = int(os.environ.get("SONARR_SEARCH_MAX_SERIES_PER_RUN", "20"))
STATE_PATH = Path(os.environ.get("SONARR_SEARCH_STATE_PATH", "/data/state/sonarr_search_state.json"))
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "30"))

@dataclass
class SonarrClient:
    base_url: str
    api_key: str

    def _headers(self) -> Dict[str, str]:
        return {"X-Api-Key": self.api_key}

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        r = requests.get(f"{self.base_url}/api/v3{path}", headers=self._headers(), params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, payload: Dict[str, Any]) -> Any:
        r = requests.post(f"{self.base_url}/api/v3{path}", headers=self._headers(), json=payload, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json() if r.content else None

    def _put(self, path: str, payload: Dict[str, Any]) -> Any:
        r = requests.put(f"{self.base_url}/api/v3{path}", headers=self._headers(), json=payload, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json() if r.content else None

    def get_or_create_tag_id(self, label: str) -> int:
        tags = self._get("/tag")
        for t in tags:
            if str(t.get("label", "")).lower() == label.lower():
                return int(t["id"])
        created = self._post("/tag", {"label": label})
        return int(created["id"])

    def list_series(self) -> List[Dict[str, Any]]:
        return self._get("/series")

    def list_episodes(self, series_id: int) -> List[Dict[str, Any]]:
        return self._get("/episode", params={"seriesId": series_id})

    def update_series_tags(self, series_obj: Dict[str, Any], new_tags: List[int]) -> None:
        payload = dict(series_obj)
        payload["tags"] = new_tags
        self._put(f"/series/{series_obj['id']}", payload)

    def command_series_search(self, series_id: int) -> None:
        self._post("/command", {"name": "SeriesSearch", "seriesId": series_id})

def missing_aired_episode_ids(episodes: List[Dict[str, Any]], as_of: datetime) -> List[int]:
    missing: List[int] = []
    for ep in episodes:
        if not ep.get("monitored", True):
            continue
        if ep.get("hasFile", False):
            continue
        air = parse_dt(ep.get("airDateUtc")) or parse_dt(ep.get("airDate"))
        if air is None or air > as_of:
            continue
        missing.append(int(ep["id"]))
    return missing

def should_cooldown(last_iso: Optional[str], days: int, now: datetime) -> bool:
    if not last_iso:
        return False
    dt = parse_dt(last_iso)
    if not dt:
        return False
    return now < (dt + timedelta(days=days))

def main() -> None:
    if not SONARR_API_KEY:
        raise SystemExit("SONARR_API_KEY is required")

    client = SonarrClient(SONARR_URL, SONARR_API_KEY)
    now = utc_now()

    tag_search_id = client.get_or_create_tag_id(TAG_SEARCH)
    tag_done_id = client.get_or_create_tag_id(TAG_DONE)

    state = load_json(STATE_PATH, {"series": {}})
    state.setdefault("series", {})

    series_all = client.list_series()
    search_tagged = [s for s in series_all if tag_search_id in (s.get("tags") or [])]
    log("sonarr_search", f"Tagged '{TAG_SEARCH}': {len(search_tagged)} series")

    eligible: List[Dict[str, Any]] = []
    search_to_done = 0

    # Required: if not missing, flip SEARCH->DONE
    for s in search_tagged:
        sid = int(s["id"])
        try:
            eps = client.list_episodes(sid)
        except Exception as e:
            log("sonarr_search", f"ERROR list_episodes seriesId={sid}: {e}")
            continue

        miss = missing_aired_episode_ids(eps, now)
        if not miss:
            tags = set(s.get("tags") or [])
            tags.discard(tag_search_id)
            tags.add(tag_done_id)
            try:
                client.update_series_tags(s, sorted(tags))
                search_to_done += 1
                log("sonarr_search", f"SEARCH->DONE (no missing aired): seriesId={sid}")
            except Exception as e:
                log("sonarr_search", f"ERROR update_series_tags seriesId={sid}: {e}")
            continue

        eligible.append(s)

    random.shuffle(eligible)
    limit = MAX_SERIES_PER_RUN if MAX_SERIES_PER_RUN > 0 else 10**9

    searched = 0
    cooldown_skipped = 0

    for s in eligible:
        if searched >= limit:
            break
        sid = int(s["id"])
        last_iso = state["series"].get(str(sid), {}).get("last_searched_utc")

        if should_cooldown(last_iso, COOLDOWN_DAYS, now):
            cooldown_skipped += 1
            continue

        try:
            client.command_series_search(sid)
            state["series"].setdefault(str(sid), {})["last_searched_utc"] = now.isoformat()
            searched += 1
            log("sonarr_search", f"SeriesSearch queued: seriesId={sid}")
        except Exception as e:
            log("sonarr_search", f"ERROR SeriesSearch seriesId={sid}: {e}")

    atomic_write_json(STATE_PATH, state)
    log("sonarr_search", f"Done. search_to_done={search_to_done} searched={searched} cooldown_skipped={cooldown_skipped} state={STATE_PATH}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
