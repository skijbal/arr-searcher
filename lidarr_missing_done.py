#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests


# =========================
# Config (env)
# =========================
LIDARR_URL = os.environ.get("LIDARR_URL", "").rstrip("/")
LIDARR_API_KEY = os.environ.get("LIDARR_API_KEY", "").strip()

TAG_SEARCH = os.environ.get("LIDARR_TAG_SEARCH", "search").strip()
TAG_DONE = os.environ.get("LIDARR_TAG_DONE", "done").strip()

# DONE-tag recheck cadence
DONE_RECHECK_HOURS = int(os.environ.get("LIDARR_DONE_RECHECK_HOURS", "24"))

# Caps
SEARCH_TO_DONE_MAX_ARTISTS_PER_RUN = int(os.environ.get("LIDARR_SEARCH_TO_DONE_MAX_ARTISTS_PER_RUN", "200"))  # 0=unlimited
DONE_RECHECK_MAX_ARTISTS_PER_RUN = int(os.environ.get("LIDARR_DONE_RECHECK_MAX_ARTISTS_PER_RUN", "20"))        # 0=unlimited
DONE_SEARCH_MAX_ARTISTS_PER_RUN = int(os.environ.get("LIDARR_DONE_SEARCH_MAX_ARTISTS_PER_RUN", "20"))          # 0=unlimited

# Wanted/missing paging (important for large libraries)
WANTED_PAGE_SIZE = int(os.environ.get("LIDARR_WANTED_PAGE_SIZE", "200"))  # keep small to avoid timeouts
WANTED_MAX_PAGES = int(os.environ.get("LIDARR_WANTED_MAX_PAGES", "200"))  # safety cap

# State
STATE_PATH = Path(os.environ.get("LIDARR_MISSING_DONE_STATE_PATH", "/data/state/lidarr_missing_done_state.json"))

# Requests
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "30"))
WANTED_RETRIES = int(os.environ.get("LIDARR_WANTED_RETRIES", "2"))
WANTED_RETRY_SLEEP_SECONDS = int(os.environ.get("LIDARR_WANTED_RETRY_SLEEP_SECONDS", "3"))


# =========================
# Logging
# =========================
def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[lidarr_missing_done] {ts} {msg}", flush=True)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def load_state(path: Path) -> Dict[str, Any]:
    try:
        if path.exists():
            data = json.loads(path.read_text("utf-8"))
            if isinstance(data, dict):
                data.setdefault("artists", {})
                return data
    except Exception as e:
        log(f"WARN: failed reading state: {e}")
    return {"artists": {}}


def atomic_write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True), "utf-8")
    tmp.replace(path)


class LidarrClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.api_key = api_key
        self.s = requests.Session()
        self.s.headers.update({"X-Api-Key": api_key})

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        r = self.s.get(self._url(path), params=params, timeout=HTTP_TIMEOUT)
        if r.status_code == 401:
            raise RuntimeError("401 Unauthorized (check LIDARR_API_KEY)")
        r.raise_for_status()
        return r.json()

    def post(self, path: str, payload: Any) -> Any:
        r = self.s.post(self._url(path), json=payload, timeout=HTTP_TIMEOUT)
        if r.status_code == 401:
            raise RuntimeError("401 Unauthorized (check LIDARR_API_KEY)")
        r.raise_for_status()
        return r.json() if r.text.strip() else None

    def put(self, path: str, payload: Any) -> Any:
        r = self.s.put(self._url(path), json=payload, timeout=HTTP_TIMEOUT)
        if r.status_code == 401:
            raise RuntimeError("401 Unauthorized (check LIDARR_API_KEY)")
        r.raise_for_status()
        return r.json() if r.text.strip() else None

    # ---- tags ----
    def all_tags(self) -> List[Dict[str, Any]]:
        return self.get("/api/v1/tag")

    def create_tag(self, label: str) -> Dict[str, Any]:
        return self.post("/api/v1/tag", {"label": label})

    # ---- artists ----
    def all_artists(self) -> List[Dict[str, Any]]:
        return self.get("/api/v1/artist")

    def artist_by_id(self, artist_id: int) -> Dict[str, Any]:
        return self.get(f"/api/v1/artist/{artist_id}")

    def update_artist(self, artist_obj: Dict[str, Any]) -> None:
        self.put("/api/v1/artist", artist_obj)

    # ---- missing ----
    def wanted_missing_page(self, page: int, page_size: int) -> Any:
        return self.get("/api/v1/wanted/missing", params={"page": page, "pageSize": page_size})

    # ---- commands ----
    def missing_album_search(self, artist_id: int) -> None:
        self.post("/api/v1/command", {"name": "MissingAlbumSearch", "artistId": artist_id})


def tag_id_by_label(tags: List[Dict[str, Any]], label: str) -> Optional[int]:
    want = label.strip().lower()
    for t in tags:
        if str(t.get("label", "")).strip().lower() == want:
            try:
                return int(t["id"])
            except Exception:
                return None
    return None


def ensure_tag(client: LidarrClient, label: str) -> int:
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


def has_tag(obj: Dict[str, Any], tag_id: int) -> bool:
    tags = obj.get("tags") or []
    try:
        return int(tag_id) in [int(x) for x in tags]
    except Exception:
        return False


def replace_tag_list(tag_list: List[Any], remove_id: int, add_id: int) -> List[int]:
    out: List[int] = []
    for t in tag_list:
        try:
            ti = int(t)
        except Exception:
            continue
        if ti == int(remove_id):
            continue
        out.append(ti)
    if int(add_id) not in out:
        out.append(int(add_id))
    return sorted(list({int(x) for x in out}))


def should_recheck(last_iso: Optional[str], hours: int, as_of: datetime) -> Tuple[bool, Optional[int]]:
    if not last_iso:
        return False, None
    dt = parse_dt(last_iso)
    if not dt:
        return False, None
    next_ok = dt + timedelta(hours=hours)
    if as_of < next_ok:
        return True, int((next_ok - as_of).total_seconds())
    return False, None


def missing_artist_ids(client: LidarrClient) -> Optional[Set[int]]:
    """Fetch /wanted/missing once (paged) and return set of artistId that have any missing.
    Returns None if we couldn't fetch it (to avoid incorrect tag flips).
    """
    ids: Set[int] = set()
    page = 1
    total_records: Optional[int] = None

    while page <= WANTED_MAX_PAGES:
        # Retry a couple of times on timeouts / transient errors
        last_err: Optional[Exception] = None
        for attempt in range(1, WANTED_RETRIES + 2):  # e.g. retries=2 => attempts=1..3
            try:
                data = client.wanted_missing_page(page=page, page_size=WANTED_PAGE_SIZE)
                last_err = None
                break
            except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout) as e:
                last_err = e
                log(f"Timeout fetching wanted/missing page={page} attempt={attempt}/{WANTED_RETRIES+1} (timeout={HTTP_TIMEOUT}s)")
            except requests.exceptions.RequestException as e:
                last_err = e
                log(f"Request error fetching wanted/missing page={page} attempt={attempt}/{WANTED_RETRIES+1}: {e}")

            if attempt <= WANTED_RETRIES:
                import time
                time.sleep(WANTED_RETRY_SLEEP_SECONDS)

        if last_err is not None:
            log(f"ERROR: Unable to fetch /wanted/missing (page={page}). Will skip this run to avoid bad tagging.")
            return None

        # Parse
        if isinstance(data, dict) and isinstance(data.get("records"), list):
            recs = data["records"]
            if total_records is None and isinstance(data.get("totalRecords"), int):
                total_records = int(data["totalRecords"])

            for r in recs:
                if isinstance(r, dict) and "artistId" in r:
                    try:
                        ids.add(int(r["artistId"]))
                    except Exception:
                        pass

            if page == 1:
                log(f"/wanted/missing totalRecords={total_records if total_records is not None else 'unknown'} pageSize={int(data.get('pageSize', WANTED_PAGE_SIZE))}")

            if len(recs) == 0:
                break

            if total_records is not None:
                page_size = int(data.get("pageSize", WANTED_PAGE_SIZE))
                if page * page_size >= total_records:
                    break

        elif isinstance(data, list):
            # Some Lidarr versions return a raw list
            if len(data) == 0:
                break
            for r in data:
                if isinstance(r, dict) and "artistId" in r:
                    try:
                        ids.add(int(r["artistId"]))
                    except Exception:
                        pass
            break
        else:
            break

        page += 1

    return ids


def main() -> None:
    if not LIDARR_URL:
        raise SystemExit("LIDARR_URL is required")
    if not LIDARR_API_KEY:
        raise SystemExit("LIDARR_API_KEY is required")

    client = LidarrClient(LIDARR_URL, LIDARR_API_KEY)
    as_of = utc_now()

    state = load_state(STATE_PATH)

    tag_search_id = ensure_tag(client, TAG_SEARCH)
    tag_done_id = ensure_tag(client, TAG_DONE)

    artists = client.all_artists()
    search_tagged = [a for a in artists if isinstance(a, dict) and has_tag(a, tag_search_id)]
    done_tagged = [a for a in artists if isinstance(a, dict) and has_tag(a, tag_done_id)]

    log(f"Tagged '{TAG_SEARCH}': {len(search_tagged)} artists")
    log(f"Tagged '{TAG_DONE}': {len(done_tagged)} artists")

    log("Fetching /wanted/missing to determine which artists are missing...")
    miss_ids = missing_artist_ids(client)
    if miss_ids is None:
        log("Skipping this run due to wanted/missing fetch failure (will retry next cycle).")
        return

    log(f"Artists with missing items: {len(miss_ids)}")

    # 1) SEARCH -> DONE for search-tagged artists that are NOT missing
    max_flip = SEARCH_TO_DONE_MAX_ARTISTS_PER_RUN if SEARCH_TO_DONE_MAX_ARTISTS_PER_RUN > 0 else 10**9
    flipped = 0
    random.shuffle(search_tagged)

    for a in search_tagged:
        if flipped >= max_flip:
            break
        aid = int(a.get("id", 0) or 0)
        name = str(a.get("artistName") or a.get("name") or f"id={aid}")
        if aid <= 0:
            continue
        if aid in miss_ids:
            continue

        try:
            full = client.artist_by_id(aid)
            cur_tags = full.get("tags") or []
            if not isinstance(cur_tags, list):
                cur_tags = []
            new_tags = replace_tag_list(cur_tags, tag_search_id, tag_done_id)

            cur_ints: List[int] = []
            for t in cur_tags:
                try:
                    cur_ints.append(int(t))
                except Exception:
                    pass
            if sorted(set(cur_ints)) == sorted(set(new_tags)):
                continue

            full["tags"] = new_tags
            client.update_artist(full)
            flipped += 1
            log(f"SEARCH->DONE (not missing): {name} id={aid}")
        except Exception as e:
            log(f"ERROR updating tags for {name} id={aid}: {e}")

    # 2) DONE-tag: only search those missing, and only recheck every DONE_RECHECK_HOURS
    eligible_done_missing: List[Tuple[int, str]] = []
    done_wait_skipped = 0

    for a in done_tagged:
        aid = int(a.get("id", 0) or 0)
        name = str(a.get("artistName") or a.get("name") or f"id={aid}")
        if aid <= 0:
            continue
        if aid not in miss_ids:
            continue

        last_iso = state.setdefault("artists", {}).get(str(aid), {}).get("last_done_recheck_utc")
        on_wait, _remaining = should_recheck(last_iso, DONE_RECHECK_HOURS, as_of)
        if on_wait:
            done_wait_skipped += 1
            continue

        eligible_done_missing.append((aid, name))

    random.shuffle(eligible_done_missing)

    max_recheck = DONE_RECHECK_MAX_ARTISTS_PER_RUN if DONE_RECHECK_MAX_ARTISTS_PER_RUN > 0 else 10**9
    max_search = DONE_SEARCH_MAX_ARTISTS_PER_RUN if DONE_SEARCH_MAX_ARTISTS_PER_RUN > 0 else 10**9

    considered = 0
    searched = 0

    for aid, name in eligible_done_missing:
        if considered >= max_recheck:
            break

        state.setdefault("artists", {}).setdefault(str(aid), {})["last_done_recheck_utc"] = as_of.isoformat()
        considered += 1

        if searched >= max_search:
            continue

        try:
            client.missing_album_search(aid)
            searched += 1
            state.setdefault("artists", {}).setdefault(str(aid), {})["last_done_searched_utc"] = as_of.isoformat()
            log(f"MissingAlbumSearch (DONE tag): {name} id={aid}")
        except Exception as e:
            log(f"ERROR MissingAlbumSearch (DONE) for {name} id={aid}: {e}")

    atomic_write_json(STATE_PATH, state)
    log(
        f"Done. search_to_done={flipped} "
        f"done_missing_searched={searched} done_rechecked={considered} "
        f"done_wait_skipped={done_wait_skipped} state={STATE_PATH}"
    )


if __name__ == "__main__":
    main()
