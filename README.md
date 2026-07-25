# Turf Zones

`turf-exporter` follows the [Turf](https://turfgame.com/) API for one region
(Hovedstaden), exposes the local leaderboard as Prometheus metrics, and logs
every takeover.

**→ https://turf-exporter.fly.dev/** — leaderboard, `/zones` map, `/status`.

`go run . -help` for the flags. [TURF-API.md](TURF-API.md) is what we learned
probing the API, most of which is not documented anywhere else.
