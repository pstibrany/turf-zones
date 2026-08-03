# Turf API — notes

What we learned probing the live [Turf](https://turfgame.com/) API. Findings are
**empirical** (verified against the running API) unless marked *documented*.

> **Read <https://api.turfgame.com/v5> first.** The base URL serves a full
> reference page. An earlier version of this file was written from the wiki
> ([Turf API](https://wiki.turfgame.com/en/wiki/Turf_API)), which is mostly
> deprecation stubs, and concluded there was no leaderboard endpoint. There is —
> `/users/top` — and building an approximation of it cost far more than reading
> the docs would have. Several other endpoints were missed the same way.

## Basics

- **Base URL:** `https://api.turfgame.com/v5` — v5 is the preferred version (as of Jan 2026).
  Older: `/v2`, `/v3`, `/v4` (deprecated); `/unstable` for experimental.
- **Auth:** none. Plain JSON in/out.
- **Rate limit:** **1 request per second per resource** (*documented* wording —
  endpoints appear to have separate budgets, though this code plays it safe with
  one shared limiter). Exceeding it returns
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
| `/zones` | POST | Zones inside a bounding box, **or by name/id** (see below) |
| `/zones/all` | GET | **Every zone in the game**, one request, ≤1 per 30 min |
| `/users` | POST | Player stats (batch, by name / id / email) |
| **`/users/top`** | **GET, POST** | **Leaderboard — global, or per region/country** |
| `/users/location` | GET | Players currently visible on the game map |
| `/regions` | GET, POST | All regions/areas per country; POST adds regionlords |
| `/rounds` | GET | Monthly round schedule |
| `/statistics` | GET | Game-wide counters (users online, zones taken today, …) |
| `/feeds` | GET | Combined live event stream |
| `/feeds/<type>` | GET | One or more streams; combine with `+`, e.g. `/feeds/chat+medal` |
| `/events`, `/events/{id}`, `/events/{id}/feed` | GET | Finished public events |

`GET` on `/zones` or `/users` returns **405** — they are POST-only.

### POST /users/top — the leaderboard

The endpoint that removes any need to discover players.

```json
{"region": "hovedstaden", "from": 1, "to": 50}
{"country": "dk", "from": 1, "to": 50}
{"from": 1, "to": 50}
```

Returns full user objects in rank order, with `place` **localized to the
requested scope** — 1, 2, 3… within that region or country rather than the
global position. `GET /users/top` gives the global top 50.

Verified against the game's own tabs: the values it displays are exactly the
API's `points`, so a regional list is **not** "points earned in that region" —
it is players *registered* to that region, ranked by their global round score.
The region tab ignores country (Hovedstaden includes a German and a Swede); the
country tab filters on country and excludes both.

- ⚠️ **`region` must be the name**, case-insensitive. `{"region": 172}` → **500**.
- ⚠️ **50 per request.** `to: 200` still returns 50.
- ⚠️ **Windows past the first 50 renumber.** `{"from": 51, "to": 100}` returned 24
  users whose `place` read 25–48, not 51–74. Treat anything beyond the first
  page as unreliable for ranking.
- A region can be shorter than 50: Hovedstaden had 38 players.

### POST /zones — bounding box, or by name/id

Body is an **array** of box objects:

```json
[{"northEast":{"latitude":55.75,"longitude":12.65},
  "southWest":{"latitude":55.60,"longitude":12.45}}]
```

**Fetch a single zone** with the same endpoint, passing name and/or id objects
(returns a one-element array):

```bash
curl -s -X POST https://api.turfgame.com/v5/zones \
  -H 'Content-Type: application/json' -d '[{"name":"Skeltoftevej"}]'
# or -d '[{"id":34437}]'
```

⚠️ There is **no** `GET /zones/<name|id>` — that path 404s (verified for both a
name and a numeric id). Name/id lookup is POST-only, like the box query, and
returns the **same zone object** (no extra "detail", and in particular no
geometry: a zone is only a center `latitude`/`longitude`, never a shape or
radius, even though the game app may draw some zones as polygons).

- Returns every zone in the rectangle (central Copenhagen ≈ 950 zones).
- ⚠️ **Multiple boxes in one array is unreliable** — only the first box is
  honoured. To cover a large area, tile it into separate requests (spaced ≥1s)
  and merge/dedupe by zone `id`.
- ⚠️ **The box has a size limit** — too large returns
  `{"errorMessage":"The area is too big","errorCode":195887106}` (HTTP 400).

The rule is **documented**, and it is a plain product of degrees:

```
(northEast.latitude - southWest.latitude) * (northEast.longitude - southWest.longitude) > 0.05
```

Measured values agree exactly — the cutoff is at 0.05 deg², nothing to do with
ground area or latitude:

| box (Δlat × Δlon) | deg² | result |
|---|---|---|
| 0.20 × 0.20 | 0.040 | OK (257 zones) |
| 0.15 × 0.30 | 0.045 | OK (422 zones) |
| 0.20 × 0.25 | 0.050 | rejected |
| 0.10 × 0.50 | 0.050 | rejected |
| 0.30 × 0.40 | 0.120 | rejected |

(An earlier note here claimed the limit tracked km² rather than degrees, inferred
from the two 0.050 boxes failing. They fail because they are both exactly at the
documented threshold, not because of their area.)

A safe tile is ~0.15 × 0.25. All of Denmark in one box is rejected and would need
~700 tiles — but see `/zones/all`, which returns every zone in the game in a
single request, at most once per 30 minutes. Catching `195887106` and quartering
the box is still the robust way to handle an oversized request.

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

- `/users` needs a name, id or email you already have — but **`/users/top` is the
  way to get a ranked list**, and `/zones/all` the way to enumerate holders.
  Harvesting names from `/feeds/takeover` is only worth it for tracking activity,
  not for building a leaderboard.
- `place` here is the **global** position, unlike in `/users/top` where it is
  localized to the requested region or country. Mixing the two silently changes
  what the number means.
- `zones` (owned) + `POST /zones` lets you plot where a player currently holds ground.
- **Batches are generous:** 500 players in one request returned all 500
  (~320 KB) with no error. So a whole regional roster refreshes in one or two
  calls — the 1 req/sec limit is not the constraint on player stats, discovery is.
- Unknown names are **omitted from the response**, not reported as an error, and
  asking for the same player twice (once by `name`, once by `id`) returns them
  twice. Dedupe by `id`.

### GET /regions

Array of `{ "country": "nl", "name": "Utrecht", "areas": [{"name","id"}, …] }`.

Denmark's five regions: **172** Hovedstaden, **173** Sjælland, **174**
Syddanmark, **175** Midtjylland, **176** Nordjylland. Note Bornholms
Regionskommune (2116) belongs to Hovedstaden despite being 200 km away, so a
single bounding box will not cover that region.

One call is enough to validate a configured region id *and* get its display
name, which is worth doing at startup — a wrong id otherwise just yields an
empty result set with no error.

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

- `takeover` — `{ zone:{…, previousOwner, currentOwner}, currentOwner, assists, latitude, longitude, time, type }`
  (carries the full zone, the taker, the previous owner, and points — the richest event).
  `assists` is *documented* as `[{"id":412,"name":"…"}, …]`; absent in every event sampled here.
  `currentOwner`/`latitude`/`longitude` are duplicated at top level and inside `zone`.
  `previousOwner` is **absent** for a zone that was neutral (new, or start of round).
- `zone` — `{ zone:{…}, time, type }` — a newly created zone. `dateLastTaken` is
  the sentinel `0002-12-02T00:00:00+0000` when never taken, which will parse as
  year 2 rather than fail, so guard against it if you compute ages.
- `medal` — `{ medal, user, time, type }`
- `chat` — `{ sender, message, region, time, type }`; `sender` is a full user
  object rather than the usual `{name,id}` pair.

**Volume and geography** (70-minute capture, 21 Jul 2026):

| | |
|---|---|
| takeover events | 3413 (≈ 49/min globally) |
| by country | se 2887 · gb 317 · fi 99 · de 71 · **dk 12** · no 8 · us 4 · nl 1 |
| `zone` events | 10 |
| `medal` events | 0 — none in 70 minutes; low volume, not absence |

The feed is **global with no server-side filter**, so a single country must be
selected client-side on `zone.region`. Denmark was 0.35% of traffic: roughly 10
events an hour, of which Hovedstaden was about half. Discovering a local player
base from the feed alone would take days — enumerate zones instead.

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

### Sample events

Verbatim from `/feeds/takeover` and `/feeds/zone`, 21 Jul 2026 (line-wrapped
here; they arrive as single-line JSON).

A takeover — note the duplicated owner and coordinates, and that
`zone.previousOwner` is what tells you who lost the zone:

```json
{"zone":{"previousOwner":{"name":"Burgundy","id":242299},
  "dateCreated":"2014-12-24T12:00:00+0000","dateLastTaken":"2026-07-21T16:58:17+0000",
  "latitude":59.512086,"longitude":15.980183,
  "currentOwner":{"name":"BjörnBel","id":205132},
  "name":"Nibblezon","id":42851,"totalTakeovers":9101,
  "region":{"area":{"name":"Köpings kommun","id":1813},"country":"se",
            "name":"Västmanland","id":143},
  "pointsPerHour":4,"takeoverPoints":140},
 "latitude":59.512086,"longitude":15.980183,
 "currentOwner":{"name":"BjörnBel","id":205132},
 "time":"2026-07-21T16:58:17+0000","type":"takeover"}
```

A new zone — no `currentOwner` at all, and the never-taken sentinel date:

```json
{"zone":{"dateCreated":"2026-07-21T17:00:00+0000",
  "dateLastTaken":"0002-12-02T00:00:00+0000",
  "latitude":55.434991,"longitude":13.849304,
  "name":"Boulebanor","id":810438,"totalTakeovers":0,
  "region":{"area":{"name":"Ystads kommun","id":1953},"country":"se",
            "name":"Skåne","id":135},
  "pointsPerHour":2,"takeoverPoints":170},
 "time":"2026-07-21T17:00:00+0000","type":"zone"}
```

## Gotchas

- **Check <https://api.turfgame.com/v5> before inferring anything.** The wiki is
  stubs; the base URL is the real reference. Assuming otherwise is how this file
  came to deny that `/users/top` exists.
- `/users/top` takes a region by **name**; an id returns 500. Its `place` is
  localized; `/users`' `place` is global.
- POST-only endpoints (`/zones`, `/users`) 405 on GET.
- `/users` body must be objects, not strings (else 500); dedupe results by `id`,
  since one player asked for twice comes back twice.
- Multi-box `/zones` request → only the first box is used.
- `/zones` boxes above ~320 km² are rejected (`195887106`); tile, or split on error.
- 1 req/sec is global; space all calls, retry 429 with backoff.
- **Space requests by noticeably more than 1s.** A client spaces requests when
  they *leave*; the API counts them when they *arrive*, and variable latency
  compresses the gap. 1.1s spacing still drew a 429 on the second request of a
  cold start from Stockholm. 5s costs nothing when steady state is ~1 req/min.
- The feed is global and unfiltered — filter on `zone.region` yourself.
- Feed events have no stable id — dedupe by content hash or composite key
  (`time` + zone id + taker id works for takeovers).
- Retention windows are observed, not guaranteed; treat them as soft.
- `dateLastTaken` can be `0002-12-02T00:00:00+0000` for a never-taken zone. It
  parses fine as year 2, so it corrupts age arithmetic rather than erroring.
