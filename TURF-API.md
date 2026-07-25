# Turf API — notes

What we learned probing the live [Turf](https://turfgame.com/) API. Findings are
**empirical** (verified against the running API) unless marked *documented*.
The official wiki pages ([Turf API](https://wiki.turfgame.com/en/wiki/Turf_API))
are mostly deprecation stubs, so most of this is not written down anywhere else.

## Basics

- **Base URL:** `https://api.turfgame.com/v5` — v5 is the preferred version (as of Jan 2026).
  Older: `/v2`, `/v3`, `/v4` (deprecated); `/unstable` for experimental.
- **Auth:** none. Plain JSON in/out.
- **Rate limit:** **1 request per second**, global. Exceeding it returns
  `{"errorMessage":"Only one request per second allowed","errorCode":195887108}`
  (seen as HTTP 429 / inline error). Heavy or malicious use → ban.
- **CORS:** enabled and permissive — reflects the request `Origin` (and returns
  `Access-Control-Allow-Origin: *` for `null`/preflight). Methods `GET, POST,
  OPTIONS`; headers `Content-Type, X-Requested-With, Accept`. So a browser page
  (including `file://` and `*.github.io`) can call it directly — no proxy needed.
- **Timestamps:** ISO-8601 with `+0000` offset, e.g. `2026-07-21T16:03:19+0000`.

## Endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `/zones` | POST | Zones inside a bounding box |
| `/zones/<name\|id>` | GET | One zone, richer detail |
| `/users` | POST | Player stats (batch) |
| `/regions` | GET | All regions/areas per country |
| `/rounds` | GET | Monthly round schedule |
| `/feeds` | GET | Combined live event stream |
| `/feeds/<type>` | GET | Single event stream (`takeover`/`zone`/`medal`/`chat`) |

`GET` on `/zones` or `/users` returns **405** — they are POST-only.

### POST /zones — bounding box

Body is an **array** of box objects:

```json
[{"northEast":{"latitude":55.75,"longitude":12.65},
  "southWest":{"latitude":55.60,"longitude":12.45}}]
```

- Returns every zone in the rectangle (central Copenhagen ≈ 950 zones).
- ⚠️ **Multiple boxes in one array is unreliable** — only the first box is
  honoured. To cover a large area, tile it into separate requests (spaced ≥1s)
  and merge/dedupe by zone `id`.

**Zone object:**

```json
{
  "name": "Klocktaket", "id": 22208,
  "latitude": 59.303708, "longitude": 18.001624,
  "currentOwner": { "name": "Klan40", "id": 402197 },
  "region": { "name": "Stockholm", "id": 141,
              "area": { "name": "Stockholms kommun", "id": 1828 },
              "country": "se" },
  "type": { "name": "Holy", "id": 9 },        // optional; only special zones
  "pointsPerHour": 4, "takeoverPoints": 140,
  "totalTakeovers": 4992,
  "dateCreated": "2013-10-17T18:49:31+0000",
  "dateLastTaken": "2026-07-20T10:18:19+0000"
}
```

Everything for an ownership map is here: owner, coords, points, contest level
(`totalTakeovers`), and recency (`dateLastTaken`).

### POST /users — player stats

Body is an **array** of `{"name": ...}` and/or `{"id": ...}` objects (batch many
in one call). Plain string arrays (`["Siper"]`) return **500** — must be objects.

```json
[{"name":"Siper"},{"name":"drugge"},{"id":127332}]
```

**User object:**

```json
{
  "name": "drugge", "id": 27475, "country": "se",
  "region": { "name": "Stockholm", "id": 141 },
  "points": 224531,           // this round (monthly)
  "pointsPerHour": 271,       // current PPH from zones held right now
  "totalPoints": 10200966,    // all-time
  "place": 80,                // global leaderboard position
  "rank": 55,                 // rank level / title (NOT position)
  "taken": 47378,             // total takeovers ever
  "uniqueZonesTaken": ...,    // distinct zones ever taken
  "blocktime": 1320,          // seconds
  "medals": [16, 27, ...],    // medal ids
  "zones": [21586, 7834, ...] // ids of zones currently owned
}
```

- **No leaderboard / all-users / GET endpoint** — you must already know each
  player's `name` or `id`. Discover names by harvesting `currentOwner.name` /
  `previousOwner.name` from `/zones` and `/feeds/takeover`, then batch into `/users`.
- `zones` (owned) + `POST /zones` lets you plot where a player currently holds ground.

### GET /regions

Array of `{ "country": "nl", "name": "Utrecht", "areas": [{"name","id"}, …] }`.

### GET /rounds

Upcoming monthly rounds: `[{ "name": "July", "start": "2026-07-05T10:00:00+0000" }, …]`.

### GET /feeds — live event stream

Live events, newest-relevant. `/feeds` returns **all types combined**; each event
carries a `type`. Individual streams: `/feeds/takeover`, `/feeds/zone`,
`/feeds/medal`, `/feeds/chat`. **Any other subtype (`assist`, `user`, bogus, …)
returns `[]`** — those four are the whole live surface.

**Incremental polling:** `?afterDate=<ISO-8601>` (URL-encode it; `+` → `%2B`),
e.g. `?afterDate=2026-07-21T16%3A44%3A09%2B0000`. Returns events after that time.
It **never errors on a too-old value** — it simply returns whatever is still
retained.

**Event envelopes** (all have `time` + `type`):

- `takeover` — `{ zone:{…, previousOwner, currentOwner}, currentOwner, latitude, longitude, time, type }`
  (carries the full zone, the taker, the previous owner, and points — the richest event).
- `zone` — `{ zone:{…}, time, type }`
- `medal` — `{ medal, user, time, type }`
- `chat` — `{ sender, message, region, time, type }`

**Retention — OBSERVED, not documented (do not rely on exact numbers):**

| Feed | Observed window | Bound by |
|---|---|---|
| `takeover` | ~30 min / ~1600 events | the binding constraint (could be time or count) |
| `zone` | hours | very low volume |
| `medal` | ~5 h | ~99-event cap |
| `chat` | ~11 h | ~99-event cap |

The `afterDate` retention/lookback window is **not officially documented**
(searched EN + SV wikis: 0 hits). The only documented feed limit is the legacy
`GET /v3/rss` — *"10 latest takeovers"*, `count` param, max 100.

**Practical polling:** poll `/feeds` (combined) with `afterDate = now − lookback`
where `lookback > interval`, and dedupe (events have no unique id — hash the whole
object, or key on `type`+`time`+zone/user). Keep the interval short (≤ ~5–10 min)
so you never depend on the undocumented takeover retention. One combined request
per cycle stays far under the 1 req/sec limit.

## Gotchas

- POST-only endpoints (`/zones`, `/users`) 405 on GET.
- `/users` body must be objects, not strings (else 500).
- Multi-box `/zones` request → only the first box is used.
- 1 req/sec is global; space all calls, retry 429 with backoff.
- Feed events have no stable id — dedupe by content hash or composite key.
- Retention windows are observed, not guaranteed; treat them as soft.
