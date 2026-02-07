# Arr Tag Searcher (split containers) - complete v2

Requested behavior implemented:
- If an item is tagged `search` but is **NOT missing**, it is retagged to `done`.
  - Sonarr: not missing = no **missing aired** monitored episodes.
  - Radarr: not missing = `hasFile=true`.
  - Lidarr: not missing = `wanted/missing` count for that artist is 0.

Run:
  docker compose up -d --build
