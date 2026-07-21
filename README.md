# Turf Zones — ownership map

A single-page map that overlays live [Turf](https://turfgame.com/) zone data on
OpenStreetMap, colored by current owner. No backend, no API key — the page calls
the public Turf API (`https://api.turfgame.com/v5/zones`) directly from the
browser and the API allows cross-origin requests.

## Use it

Open `index.html` in a browser, or host it (see below). Then:

- **Load zones in this view** — splits the current map bounds into a grid and
  fetches each tile in turn, merging and de-duping by zone id. Requests are
  spaced ≥1s apart (the API allows 1 request/second) and 429s are retried with
  backoff, so large areas load without hitting the rate limit. **Stop** cancels.
- **📍 Near me** — jumps to your location and loads nearby zones.
- **Tick owners** in the sidebar to show only those players/clans (multi-select,
  including `(neutral)`); untick all to show everything. **All / None** and the
  filter box help wrangle long lists.
- Click a zone for owner, points, total takeovers, and when it was last taken.

## Host on GitHub Pages

1. Create a repo and push this folder:
   ```bash
   git remote add origin git@github.com:<you>/turf-zones.git
   git push -u origin main
   ```
2. Repo **Settings → Pages → Build and deployment**: source = *Deploy from a
   branch*, branch = `main`, folder = `/ (root)`.
3. Wait ~1 min; the site appears at `https://<you>.github.io/turf-zones/`.

Because the page is fully static and the Turf API sends
`Access-Control-Allow-Origin: *`, it works from any `https://` origin with no
proxy.

## Data

Live from the Turf API v5. Each zone includes `currentOwner`, `latitude`,
`longitude`, `pointsPerHour`, `takeoverPoints`, `totalTakeovers`,
`dateLastTaken`, and `region`. Data © Turf; map tiles © OpenStreetMap / CARTO.
