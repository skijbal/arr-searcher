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

DONE_RECHECK_HOURS = int(os.environ.get("SONARR_DONE_RECHECK_HOURS", "24"))
DONE_RECHECK_MAX_SERIES = int(os.environ.get("SONARR_DONE_RECHECK_MAX_SERIES_PER_RUN", "20"))
DONE_SEARCH_MAX_SERIES = int(os.environ.get("SONARR_DONE_SEARCH_MAX_SERIES_PER_RUN", "5"))
DONE_SEARCH_MAX_EPS = int(os.environ.get("SONARR_DONE_SEARCH_MAX_EPISODES_PER_RUN", "50"))

STATE_PATH = Path(os.environ.get("SONARR_MISSING_DONE_STATE_PATH", "/data/state/sonarr_missing_done_state.json"))
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

    def command_episode_search(self, episode_ids: List[int]) -> None:
        self._post("/command", {"name": "EpisodeSearch", "episodeIds": episode_ids})

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

def should_wait(last_iso: Optional[str], hours: int, now: datetime) -> bool:
    if not last_iso:
        return False
    dt = parse_dt(last_iso)
    if not dt:
        return False
    return now < (dt + timedelta(hours=hours))

def chunked(xs: List[int], n: int) -> List[List[int]]:
    return [xs[i:i+n] for i in range(0, len(xs), n)]

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
    done_tagged = [s for s in series_all if tag_done_id in (s.get("tags") or [])]

    log("sonarr_missing_done", f"Tagged '{TAG_SEARCH}': {len(search_tagged)} series")
    log("sonarr_missing_done", f"Tagged '{TAG_DONE}': {len(done_tagged)} series")

    # Required: if search-tagged series has no missing aired episodes, flip SEARCH->DONE
    search_to_done = 0
    for s in search_tagged:
        sid = int(s["id"])
        try:
            eps = client.list_episodes(sid)
        except Exception as e:
            log("sonarr_missing_done", f"ERROR list_episodes seriesId={sid}: {e}")
            continue

        miss = missing_aired_episode_ids(eps, now)
        if miss:
            continue

        tags = set(s.get("tags") or [])
        tags.discard(tag_search_id)
        tags.add(tag_done_id)
        try:
            client.update_series_tags(s, sorted(tags))
            search_to_done += 1
            log("sonarr_missing_done", f"SEARCH->DONE (no missing aired): seriesId={sid}")
        except Exception as e:
            log("sonarr_missing_done", f"ERROR update_series_tags seriesId={sid}: {e}")

    # DONE: recheck limited; only EpisodeSearch if there are missing episodes
    random.shuffle(done_tagged)

    recheck_limit = DONE_RECHECK_MAX_SERIES if DONE_RECHECK_MAX_SERIES > 0 else 10**9
    max_series = DONE_SEARCH_MAX_SERIES if DONE_SEARCH_MAX_SERIES > 0 else 10**9
    max_eps = DONE_SEARCH_MAX_EPS if DONE_SEARCH_MAX_EPS > 0 else 10**12

    done_wait_skipped = 0
    rechecked = 0
    done_searched_series = 0
    done_searched_eps = 0

    for s in done_tagged:
        if rechecked >= recheck_limit:
            break

        sid = int(s["id"])
        last_recheck = state["series"].get(str(sid), {}).get("last_done_recheck_utc")
        if should_wait(last_recheck, DONE_RECHECK_HOURS, now):
            done_wait_skipped += 1
            continue

        rechecked += 1
        state["series"].setdefault(str(sid), {})["last_done_recheck_utc"] = now.isoformat()

        try:
            eps = client.list_episodes(sid)
        except Exception as e:
            log("sonarr_missing_done", f"ERROR list_episodes DONE seriesId={sid}: {e}")
            continue

        miss = missing_aired_episode_ids(eps, now)
        if not miss:
            continue  # ONLY SEARCH MISSING

        if done_searched_series >= max_series or done_searched_eps >= max_eps:
            continue

        remaining = max_eps - done_searched_eps
        to_search = miss[:max(0, remaining)]
        if not to_search:
            continue

        ok_any = False
        for c in chunked(to_search, 100):
            try:
                client.command_episode_search(c)
                ok_any = True
                log("sonarr_missing_done", f"EpisodeSearch (DONE tag): seriesId={sid} episodes={len(c)}")
            except Exception as e:
                log("sonarr_missing_done", f"ERROR EpisodeSearch DONE seriesId={sid}: {e}")

        if ok_any:
            done_searched_series += 1
            done_searched_eps += len(to_search)
            state["series"].setdefault(str(sid), {})["last_done_searched_utc"] = now.isoformat()

    atomic_write_json(STATE_PATH, state)
    log("sonarr_missing_done", f"Done. search_to_done={search_to_done} done_searched_series={done_searched_series} done_searched_eps={done_searched_eps} done_wait_skipped={done_wait_skipped} state={STATE_PATH}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
