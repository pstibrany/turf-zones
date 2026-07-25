package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"net"
	"net/http"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
)

// feedCursorKey is where the newest processed event time is persisted, so a
// restart resumes instead of re-reading (or worse, skipping) events.
const feedCursorKey = "feed.takeover.cursor"

// Pages are compiled into the binary so it runs standalone, with no asset
// directory to deploy alongside it. index.html is the zone map, which doubles as
// the GitHub Pages entry point for this repository — hence the name.
//
//go:embed templates/leaderboard.html templates/status.html
var templateFS embed.FS

//go:embed index.html
var zoneMapPage []byte

// pageTemplates is parsed once at startup: every page is rendered from memory,
// so serving one never touches the database or the Turf API.
var pageTemplates = template.Must(template.ParseFS(templateFS, "templates/*.html"))

// ---------------------------------------------------------------------------
// Monitored area
// ---------------------------------------------------------------------------

// rosterScope decides who belongs on the leaderboard.
//
// scopeHome reproduces the game's own regional list: everyone registered to the
// region, ranked by round points. scopeArea instead ranks whoever is holding or
// taking ground here, which is a different and occasionally more interesting
// question — but it is not what the game shows, because a visitor arrives
// carrying every point they have earned elsewhere.
type rosterScope string

const (
	scopeHome rosterScope = "home"
	scopeArea rosterScope = "area"
)

func (r rosterScope) valid() bool { return r == scopeHome || r == scopeArea }

// scope selects how much of the global feed a stage cares about.
type scope string

const (
	scopeNone      scope = "none"
	scopeMonitored scope = "area"
	scopeAll       scope = "all"
)

func (s scope) valid() bool {
	return s == scopeNone || s == scopeMonitored || s == scopeAll
}

func (s scope) includes(monitored bool) bool {
	switch s {
	case scopeAll:
		return true
	case scopeMonitored:
		return monitored
	default:
		return false
	}
}

// areaSelector is a membership test over zone regions. An empty level matches
// everything at that level, so {countries: dk} is all of Denmark and
// {countries: dk, regions: 172} is the Copenhagen region.
//
// The API has no "everything in Denmark" query, so the monitored geography is
// expressed twice: this test filters the global feed, and a bounding box (see
// scanLoop) enumerates zones for player discovery.
type areaSelector struct {
	countries map[string]bool
	regions   map[int64]bool
	areas     map[int64]bool
	summary   string
	// regionName is the display name when exactly one region is monitored, so
	// pages can say "Hovedstaden" rather than "172".
	regionName string
}

func (s areaSelector) matches(r *Region) bool {
	if r == nil {
		return false
	}
	if len(s.countries) > 0 && !s.countries[strings.ToLower(r.Country)] {
		return false
	}
	if len(s.regions) > 0 && !s.regions[r.ID] {
		return false
	}
	if len(s.areas) > 0 && !s.areas[r.areaID()] {
		return false
	}
	return true
}

// matchesUser tests a player's own registration rather than where their zones
// are, which is how the game builds its leaderboards.
//
// The two tabs filter differently, and this mirrors that: the region tab lists
// everyone whose home region is that region, whatever their country — Hovedstaden
// includes a German and a Swede — while the country tab filters on country and
// drops both of them. So when regions are configured they decide alone, and
// country only applies when no region was given.
func (s areaSelector) matchesUser(u *User) bool {
	if len(s.regions) > 0 {
		return u.Region != nil && s.regions[u.Region.ID]
	}
	if len(s.countries) > 0 {
		return s.countries[strings.ToLower(u.Country)]
	}
	return true
}

// resolveSelector turns configuration into a selector. Regions and areas may be
// given as numeric ids or as names ("Hovedstaden", "Københavns Kommune"); names
// cost one GET /regions at startup, which also catches typos before they
// silently produce an empty roster.
func resolveSelector(ctx context.Context, c *turfClient, countries, regions, areas []string) (areaSelector, error) {
	sel := areaSelector{
		countries: map[string]bool{},
		regions:   map[int64]bool{},
		areas:     map[int64]bool{},
	}
	for _, country := range countries {
		if country = strings.ToLower(strings.TrimSpace(country)); country != "" {
			sel.countries[country] = true
		}
	}

	// Fetch the catalog whenever regions or areas are configured, even when they
	// were given as numeric ids: it validates them, and it supplies the names the
	// pages display. One request at startup.
	var catalog []CountryRegions
	if len(regions) > 0 || len(areas) > 0 {
		var err error
		if catalog, err = c.regions(ctx); err != nil {
			return areaSelector{}, fmt.Errorf("resolving region names: %w", err)
		}
	}

	names := map[int64]string{}
	for _, spec := range regions {
		id, name, err := lookupRegion(catalog, sel.countries, spec)
		if err != nil {
			return areaSelector{}, err
		}
		sel.regions[id] = true
		names[id] = name
	}
	for _, spec := range areas {
		id, name, err := lookupArea(catalog, sel.countries, spec)
		if err != nil {
			return areaSelector{}, err
		}
		sel.areas[id] = true
		names[id] = name
	}

	var parts []string
	if len(sel.countries) > 0 {
		parts = append(parts, "countries="+strings.Join(sortedKeys(sel.countries), "+"))
	}
	if len(sel.regions) > 0 {
		parts = append(parts, "regions="+strings.Join(idLabels(sel.regions, names), "+"))
	}
	if len(sel.areas) > 0 {
		parts = append(parts, "areas="+strings.Join(idLabels(sel.areas, names), "+"))
	}
	sel.summary = "everywhere"
	if len(parts) > 0 {
		sel.summary = strings.Join(parts, " ")
	}
	if len(sel.regions) == 1 {
		for id := range sel.regions {
			sel.regionName = names[id]
		}
	}
	return sel, nil
}

func lookupRegion(catalog []CountryRegions, countries map[string]bool, spec string) (int64, string, error) {
	wantID, isID := parseID(spec)
	for _, cr := range catalog {
		if !countryAllowed(countries, cr.Country) {
			continue
		}
		if (isID && cr.ID == wantID) || (!isID && strings.EqualFold(cr.Name, spec)) {
			return cr.ID, cr.Name, nil
		}
	}
	return 0, "", fmt.Errorf("unknown region %q (not found in %s)", spec, countryList(countries))
}

func lookupArea(catalog []CountryRegions, countries map[string]bool, spec string) (int64, string, error) {
	wantID, isID := parseID(spec)
	for _, cr := range catalog {
		if !countryAllowed(countries, cr.Country) {
			continue
		}
		for _, a := range cr.Areas {
			if (isID && a.ID == wantID) || (!isID && strings.EqualFold(a.Name, spec)) {
				return a.ID, a.Name, nil
			}
		}
	}
	return 0, "", fmt.Errorf("unknown area %q (not found in %s)", spec, countryList(countries))
}

func countryAllowed(countries map[string]bool, country string) bool {
	return len(countries) == 0 || countries[strings.ToLower(country)]
}

func parseID(spec string) (int64, bool) {
	id, err := strconv.ParseInt(spec, 10, 64)
	return id, err == nil
}

func countryList(countries map[string]bool) string {
	if len(countries) == 0 {
		return "any country"
	}
	return strings.Join(sortedKeys(countries), ", ")
}

func idLabels(ids map[int64]bool, names map[int64]string) []string {
	out := make([]string, 0, len(ids))
	for id := range ids {
		if name := names[id]; name != "" {
			out = append(out, fmt.Sprintf("%s(%d)", name, id))
		} else {
			out = append(out, strconv.FormatInt(id, 10))
		}
	}
	slices.Sort(out)
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// ---------------------------------------------------------------------------
// Exporter
// ---------------------------------------------------------------------------

// exporter wires the pieces together and runs the background loops.
type exporter struct {
	cfg      Config
	log      *slog.Logger
	events   *slog.Logger
	client   *turfClient
	store    *store
	sel      areaSelector
	metrics  *opsMetrics
	players  *playerCollector
	registry *prometheus.Registry

	// areaZones maps player id to zones held inside the monitored area as of the
	// last discovery scan. Written by scanLoop, read by the stats refresher.
	areaZones atomic.Pointer[map[int64]int]

	rank        rankBy
	pinnedNames []string
	pinnedIDs   []int64

	// firstScan closes once the discovery scan has finished its first attempt.
	// The stats refresher waits for it so that a cold start ranks the players the
	// scan found, instead of an empty roster it would then sit on for a full
	// refresh interval.
	firstScan     chan struct{}
	firstScanOnce sync.Once

	// feedCursorUnix is the newest event time processed, as a Unix timestamp (0
	// for none). Atomic because the feed loop writes it and the status page reads
	// it. Feed timestamps have whole-second resolution, so nothing is lost.
	feedCursorUnix atomic.Int64

	// dbStats is sampled after each stats refresh so the status page can report
	// database size without running a query per request.
	dbStats atomic.Pointer[dbStats]

	startedAt time.Time
}

// dbStats is a point-in-time measurement of how much the database holds.
type dbStats struct {
	Bytes       int64
	Takeovers   int64
	HistoryRows int64
	Players     int64
	MeasuredAt  time.Time
}

func (e *exporter) cursor() time.Time {
	if v := e.feedCursorUnix.Load(); v != 0 {
		return time.Unix(v, 0)
	}
	return time.Time{}
}

func (e *exporter) setCursor(t time.Time) { e.feedCursorUnix.Store(t.Unix()) }

// newExporter builds the exporter. It performs the startup work that can fail:
// resolving region names against the API, and opening the database.
func newExporter(ctx context.Context, cfg Config, log, events *slog.Logger) (*exporter, error) {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	metrics := newOpsMetrics(registry)

	client := newTurfClient(cfg.APIBaseURL, cfg.APIMinInterval, cfg.APIMaxRetries, log, metrics)

	sel, err := resolveSelector(ctx, client, cfg.Countries, cfg.Regions, cfg.Areas)
	if err != nil {
		return nil, err
	}

	st, err := openStore(ctx, cfg.DBPath, log)
	if err != nil {
		return nil, err
	}

	names, ids, err := splitPlayerSpecs(cfg.Players)
	if err != nil {
		_ = st.Close()
		return nil, err
	}

	e := &exporter{
		cfg:         cfg,
		log:         log,
		events:      events,
		client:      client,
		store:       st,
		sel:         sel,
		metrics:     metrics,
		players:     newPlayerCollector(),
		registry:    registry,
		rank:        rankBy(cfg.RankBy),
		pinnedNames: names,
		pinnedIDs:   ids,
		firstScan:   make(chan struct{}),
		startedAt:   time.Now(),
	}
	registry.MustRegister(e.players)

	e.log.Info("monitoring area", "selector", sel.summary,
		"scan_boxes", cfg.Boxes.String(), "scan_tiles", len(e.scanTiles()))
	// Never log the token itself, only whether there is one — otherwise the
	// secret ends up in the log aggregator it was meant to be protected from.
	e.log.Info("api endpoints configured", "authenticated", cfg.APIToken != "")
	return e, nil
}

func (e *exporter) Close() error { return e.store.Close() }

// run starts every loop and the HTTP server, and returns when ctx is cancelled
// or the server fails.
func (e *exporter) run(ctx context.Context) error {
	e.warmStart(ctx)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return e.serve(ctx, "public", e.cfg.ListenAddress, e.publicMux()) })
	if e.cfg.InternalAddress != "" {
		g.Go(func() error { return e.serve(ctx, "internal", e.cfg.InternalAddress, e.internalMux()) })
	}
	g.Go(func() error { return e.feedLoop(ctx) })
	g.Go(func() error { return e.statsLoop(ctx) })
	g.Go(func() error { return e.scanLoop(ctx) })
	g.Go(func() error { return e.pruneLoop(ctx) })
	return g.Wait()
}

// warmStart publishes the stats stored by the previous run, so /metrics is
// useful immediately instead of empty until the first refresh completes.
func (e *exporter) warmStart(ctx context.Context) {
	now := time.Now()
	roster, err := e.store.roster(ctx, now.Add(-e.cfg.RosterTTL))
	if err != nil {
		e.log.Warn("could not read roster", "err", err)
		return
	}
	if len(roster) == 0 {
		return
	}
	onRoster := make(map[int64]bool, len(roster))
	pinned := make(map[int64]bool, len(e.pinnedIDs))
	for _, id := range e.pinnedIDs {
		pinned[id] = true
	}
	for _, p := range roster {
		onRoster[p.ID] = true
		if p.Pinned {
			pinned[p.ID] = true
		}
	}

	stored, err := e.store.loadStats(ctx)
	if err != nil {
		e.log.Warn("could not load stored player stats", "err", err)
		return
	}
	// Rank only current roster members: stats outlive the roster by design, and
	// ranking a departed player would show a stale name at the top of the list.
	samples := make([]playerSample, 0, len(stored))
	for _, s := range stored {
		if onRoster[s.User.ID] {
			samples = append(samples, s)
		}
	}
	if len(samples) == 0 {
		return
	}
	if counts, err := e.store.takeoverCounts(ctx, now.Add(-e.cfg.TakeoverRetention)); err == nil {
		for i := range samples {
			samples[i].ObservedTakeovers = counts[samples[i].User.ID]
		}
	}
	ranked := e.rankSamples(samples, pinned)
	e.players.set(ranked)
	e.metrics.exposedPlayers.Set(float64(len(ranked)))
	e.log.Info("published stored player stats while waiting for first refresh",
		"roster", len(roster), "stored", len(samples), "exposed", len(ranked))
}

// ---------------------------------------------------------------------------
// Takeover feed
// ---------------------------------------------------------------------------

// takeoverKey is the identity of a takeover, matching the takeovers primary key.
type takeoverKey struct{ time, zoneID, takerID int64 }

func keyOf(t takeover) takeoverKey {
	return takeoverKey{time: t.Time.Unix(), zoneID: t.ZoneID, takerID: t.TakerID}
}

// feedLoop polls the takeover feed, logs each takeover, stores it, and feeds the
// players it sees into the roster.
func (e *exporter) feedLoop(ctx context.Context) error {
	if cursor, ok, err := e.store.getMetaTime(ctx, feedCursorKey); err != nil {
		e.log.Warn("could not read feed cursor, starting from now", "err", err)
	} else if ok {
		e.setCursor(cursor)
		e.log.Info("resuming takeover feed", "cursor", cursor.Format(time.RFC3339))
	}
	return e.everyTick(ctx, e.cfg.FeedInterval, "takeover feed poll", func() error {
		err := e.pollFeed(ctx)
		e.metrics.feedPolls.WithLabelValues(outcomeLabel(err)).Inc()
		return err
	})
}

func (e *exporter) pollFeed(ctx context.Context) error {
	after := e.feedAfter(time.Now())
	events, err := e.client.feed(ctx, eventTakeover, after)
	if err != nil {
		return err
	}

	var (
		newest    time.Time
		toStore   []takeover
		monitored []takeover
	)
	inArea := map[takeoverKey]bool{}
	for i := range events {
		ev := &events[i]
		e.metrics.feedEvents.WithLabelValues(ev.Type).Inc()
		if ev.Time.After(newest) {
			newest = ev.Time.Time
		}

		t, ok := takeoverFromEvent(ev)
		if !ok {
			continue
		}
		monitoredEvent := e.sel.matches(ev.eventRegion())
		e.metrics.feedTakeovers.WithLabelValues(t.Country, boolLabel(monitoredEvent)).Inc()
		inArea[keyOf(t)] = monitoredEvent

		if e.cfg.FeedStore.includes(monitoredEvent) {
			toStore = append(toStore, t)
		}
		if monitoredEvent {
			monitored = append(monitored, t)
		}
	}

	// Insert first: the rows that were actually new are exactly the events not
	// yet logged, which is how overlapping polls avoid duplicate log lines.
	fresh, err := e.store.insertTakeovers(ctx, toStore, e.cfg.FeedStoreRaw)
	if err != nil {
		return err
	}
	e.metrics.feedStored.Add(float64(len(fresh)))
	for _, t := range fresh {
		if e.cfg.FeedLog.includes(inArea[keyOf(t)]) {
			e.logTakeover(ctx, t)
		}
	}

	if err := e.recordTakeoverPlayers(ctx, monitored); err != nil {
		return err
	}

	if newest.After(e.cursor()) {
		e.setCursor(newest)
		if err := e.store.setMetaTime(ctx, feedCursorKey, newest); err != nil {
			e.log.Warn("could not persist feed cursor", "err", err)
		}
	}
	if cursor := e.cursor(); !cursor.IsZero() {
		e.metrics.feedCursor.Set(float64(cursor.Unix()))
	}
	if !newest.IsZero() {
		e.metrics.feedLag.Set(time.Since(newest).Seconds())
	}

	e.log.Debug("polled takeover feed", "after", after.Format(time.RFC3339),
		"events", len(events), "monitored", len(monitored), "stored", len(fresh))
	return nil
}

// feedAfter returns the afterDate to request: a little behind the cursor, but
// never further back than the feed's ~30 minute retention makes useful.
func (e *exporter) feedAfter(now time.Time) time.Time {
	floor := now.Add(-e.cfg.FeedMaxLookback)
	cursor := e.cursor()
	if cursor.IsZero() {
		return floor
	}
	if candidate := cursor.Add(-e.cfg.FeedOverlap); candidate.After(floor) {
		return candidate
	}
	return floor
}

// recordTakeoverPlayers adds both sides of a takeover to the roster: the taker
// is obviously active in the area, and so is whoever they took the zone from.
func (e *exporter) recordTakeoverPlayers(ctx context.Context, takeovers []takeover) error {
	if len(takeovers) == 0 {
		return nil
	}
	seen := make(map[int64]bool, len(takeovers)*2)
	owners := make([]Owner, 0, len(takeovers)*2)
	add := func(id int64, name string) {
		if id == 0 || name == "" || seen[id] {
			return
		}
		seen[id] = true
		owners = append(owners, Owner{ID: id, Name: name})
	}
	for _, t := range takeovers {
		add(t.TakerID, t.TakerName)
		add(t.PreviousOwnerID, t.PreviousOwnerName)
	}
	return e.store.seePlayers(ctx, owners, time.Now(), false)
}

func (e *exporter) logTakeover(ctx context.Context, t takeover) {
	attrs := []slog.Attr{
		slog.Time("takeover_time", t.Time),
		slog.String("zone", t.ZoneName),
		slog.Int64("zone_id", t.ZoneID),
		slog.String("taker", t.TakerName),
		slog.Int64("taker_id", t.TakerID),
		slog.String("previous_owner", t.PreviousOwnerName),
		slog.Int64("previous_owner_id", t.PreviousOwnerID),
		slog.String("country", t.Country),
		slog.String("region", t.RegionName),
		slog.String("area", t.AreaName),
		slog.Int64("takeover_points", t.TakeoverPoints),
		slog.Int64("points_per_hour", t.PointsPerHour),
		slog.Int64("zone_total_takeovers", t.ZoneTotalTakeover),
		slog.Float64("latitude", t.Latitude),
		slog.Float64("longitude", t.Longitude),
	}
	if e.cfg.FeedStoreRaw && t.Raw != "" {
		attrs = append(attrs, slog.String("raw", t.Raw))
	}
	e.events.LogAttrs(ctx, slog.LevelInfo, "takeover", attrs...)
}

// ---------------------------------------------------------------------------
// Zone discovery scan
// ---------------------------------------------------------------------------

func (e *exporter) scanTiles() []bbox {
	var tiles []bbox
	for _, box := range e.cfg.Boxes {
		tiles = append(tiles, box.tiles(e.cfg.ScanTileLat, e.cfg.ScanTileLon)...)
	}
	return tiles
}

// scanLoop enumerates zones in the configured bounding boxes to discover players.
// It is what makes the roster useful from the first minute: the takeover feed is
// global, and in a 30-minute sample only 12 of 3413 takeovers were Danish, so
// waiting for the feed alone to reveal the local player base would take days. A
// zone scan names every current zone holder in the area in one pass.
func (e *exporter) scanLoop(ctx context.Context) error {
	if !e.cfg.ScanEnabled {
		e.log.Info("zone discovery scan disabled")
		e.scanFinished()
		return nil
	}
	e.metrics.scanTiles.Set(float64(len(e.scanTiles())))
	return e.everyTick(ctx, e.cfg.ScanInterval, "zone discovery scan", func() error {
		err := e.scanZones(ctx)
		e.scanFinished() // release the stats refresher, successfully or not
		e.metrics.scans.WithLabelValues(outcomeLabel(err)).Inc()
		if err == nil {
			e.metrics.scanTime.Set(float64(time.Now().Unix()))
		}
		return err
	})
}

func (e *exporter) scanZones(ctx context.Context) error {
	tiles := e.scanTiles()
	start := time.Now()
	e.log.Info("starting zone discovery scan", "tiles", len(tiles),
		"estimated_duration", (time.Duration(len(tiles)) * e.cfg.APIMinInterval).Round(time.Second).String())

	// Adjacent tiles share their edges, so dedupe by zone id.
	seenZone := map[int64]bool{}
	owners := map[int64]Owner{}
	zonesPerOwner := map[int64]int{}
	var inArea, outside int

	for i, tile := range tiles {
		zones, err := e.client.zones(ctx, tile, e.cfg.ScanMaxSplit)
		if err != nil {
			return err
		}
		for _, z := range zones {
			if seenZone[z.ID] {
				continue
			}
			seenZone[z.ID] = true
			if !e.sel.matches(z.Region) {
				outside++
				continue
			}
			inArea++
			if z.CurrentOwner != nil && z.CurrentOwner.ID != 0 {
				owners[z.CurrentOwner.ID] = *z.CurrentOwner
				zonesPerOwner[z.CurrentOwner.ID]++
			}
		}
		e.log.Debug("scanned tile", "tile", i+1, "of", len(tiles),
			"box", tile.String(), "zones", len(zones))
	}

	list := make([]Owner, 0, len(owners))
	for _, o := range owners {
		list = append(list, o)
	}
	if err := e.store.seePlayers(ctx, list, time.Now(), false); err != nil {
		return err
	}

	e.areaZones.Store(&zonesPerOwner)
	e.metrics.scanZones.WithLabelValues("true").Set(float64(inArea))
	e.metrics.scanZones.WithLabelValues("false").Set(float64(outside))

	e.log.Info("zone discovery scan complete",
		"duration", time.Since(start).Round(time.Second).String(),
		"zones_in_area", inArea, "zones_outside_area", outside, "zone_owners", len(owners))
	return nil
}

// ---------------------------------------------------------------------------
// Player stats and the local top list
// ---------------------------------------------------------------------------

// rankBy names the stat the local top list is ordered by.
type rankBy string

var rankFuncs = map[rankBy]func(playerSample) int64{
	"points":        func(s playerSample) int64 { return s.User.Points },
	"totalPoints":   func(s playerSample) int64 { return s.User.TotalPoints },
	"pointsPerHour": func(s playerSample) int64 { return s.User.PointsPerHour },
	"taken":         func(s playerSample) int64 { return s.User.Taken },
	"zones":         func(s playerSample) int64 { return int64(len(s.User.Zones)) },
	"areaZones":     func(s playerSample) int64 { return int64(s.AreaZones) },
}

func (r rankBy) valid() bool {
	_, ok := rankFuncs[r]
	return ok
}

// rankByValues lists the accepted ranking keys, for flag help.
func rankByValues() string {
	out := make([]string, 0, len(rankFuncs))
	for k := range rankFuncs {
		out = append(out, string(k))
	}
	sort.Strings(out)
	return strings.Join(out, ", ")
}

// statsLoop refreshes player stats and republishes the top list.
//
// The API has no leaderboard endpoint, so "top list" here means: of the players
// discovered in the monitored area, these are the highest ranked. One POST /users
// carries up to 400 players, so the whole refresh costs one or two requests.
func (e *exporter) statsLoop(ctx context.Context) error {
	select {
	case <-e.firstScan:
	case <-ctx.Done():
		return nil
	}
	return e.everyTick(ctx, e.cfg.StatsInterval, "player stats refresh", func() error {
		err := e.refreshStats(ctx)
		e.metrics.refreshes.WithLabelValues(outcomeLabel(err)).Inc()
		if err == nil {
			e.metrics.refreshTime.Set(float64(time.Now().Unix()))
		}
		return err
	})
}

func (e *exporter) refreshStats(ctx context.Context) error {
	now := time.Now()
	roster, err := e.store.roster(ctx, now.Add(-e.cfg.RosterTTL))
	if err != nil {
		return err
	}
	e.metrics.rosterPlayers.Set(float64(len(roster)))

	refs, pinned := e.userRefs(roster)
	if len(refs) == 0 {
		e.log.Info("roster is empty, nothing to refresh yet")
		return nil
	}

	users, err := e.client.users(ctx, refs)
	if err != nil {
		return err
	}
	if err := e.store.saveStats(ctx, users, now); err != nil {
		return err
	}
	// Pinned players given by name only get their id here, once the API has
	// resolved the name for us. This also adds them to pinned, so they are
	// exposed on this cycle rather than the next one.
	if err := e.recordPinned(ctx, users, pinned, now); err != nil {
		return err
	}

	counts, err := e.store.takeoverCounts(ctx, now.Add(-e.cfg.TakeoverRetention))
	if err != nil {
		return err
	}
	if total, err := e.store.countTakeovers(ctx); err == nil {
		e.metrics.storedTakeovers.Set(float64(total))
	}

	zonesInArea := e.areaZones.Load()
	samples := make([]playerSample, 0, len(users))
	// One player can be reached by more than one ref — an id and a name in
	// -players, or a rename that leaves the roster holding the old name — and the
	// API answers each ref separately. Two samples for one id would mean two
	// series with identical labels, which the registry drops while quietly
	// counting an error on every scrape.
	seen := make(map[int64]bool, len(users))
	var visitors int
	for _, u := range users {
		if u.ID == 0 {
			continue // an unresolved name: the API omits it rather than erring
		}
		if seen[u.ID] {
			continue
		}
		seen[u.ID] = true
		// Discovery finds whoever holds or takes a zone here, which includes
		// visitors from abroad carrying their whole national score. Ranking those
		// against locals buries the local leaderboard: nine of the top ten were
		// Swedes passing through. Pinned players are exempt — they were asked for
		// by name.
		if e.cfg.RosterScope == scopeHome && !pinned[u.ID] && !e.sel.matchesUser(&u) {
			visitors++
			continue
		}
		s := playerSample{
			User:              u,
			AreaZones:         -1,
			ObservedTakeovers: counts[u.ID],
			UpdatedAt:         now,
		}
		if zonesInArea != nil {
			s.AreaZones = (*zonesInArea)[u.ID]
		}
		samples = append(samples, s)
	}

	// Snapshot every player we fetched, not just the ones that rank: someone
	// outside today's top 50 is exactly who you want history for when they climb.
	e.recordHistory(ctx, samples, now)

	ranked := e.rankSamples(samples, pinned)
	e.players.set(ranked)
	e.metrics.exposedPlayers.Set(float64(len(ranked)))
	e.measureDB(ctx)

	e.metrics.filteredPlayers.Set(float64(visitors))
	e.log.Info("refreshed player stats", "roster", len(roster),
		"fetched", len(users), "not_local", visitors,
		"exposed", len(ranked), "rank_by", string(e.rank))
	return nil
}

// measureDB samples database size and row counts for the status page. Errors are
// logged and dropped: a stale size on a status page is not worth failing a
// refresh over.
func (e *exporter) measureDB(ctx context.Context) {
	s := &dbStats{MeasuredAt: time.Now()}

	// WAL and shared-memory files are part of what the volume is holding, so
	// reporting only the main file would understate it.
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if info, err := os.Stat(e.cfg.DBPath + suffix); err == nil {
			s.Bytes += info.Size()
		}
	}
	if n, err := e.store.countPlayers(ctx); err == nil {
		s.Players = n
	}
	if n, err := e.store.countTakeovers(ctx); err == nil {
		s.Takeovers = n
	}
	if n, err := e.store.countHistory(ctx); err == nil {
		s.HistoryRows = n
	}
	e.dbStats.Store(s)
	e.metrics.dbBytes.Set(float64(s.Bytes))
}

// recordHistory writes a snapshot for the current time bucket. Failing to store
// history should not fail the refresh — the metrics are the primary product.
func (e *exporter) recordHistory(ctx context.Context, samples []playerSample, now time.Time) {
	if e.cfg.HistoryInterval <= 0 {
		return
	}
	bucket := now.Truncate(e.cfg.HistoryInterval).UTC()
	written, err := e.store.insertHistory(ctx, samples, bucket)
	if err != nil {
		e.log.Error("could not store player history", "err", err)
		e.metrics.historyWrites.WithLabelValues("error").Inc()
		return
	}
	e.metrics.historyWrites.WithLabelValues("success").Inc()
	if written > 0 {
		e.metrics.historyRows.Add(float64(written))
		e.log.Info("stored player history snapshot",
			"bucket", bucket.Format(time.RFC3339), "rows", written)
	}
	if total, err := e.store.countHistory(ctx); err == nil {
		e.metrics.historyStored.Set(float64(total))
	}
}

// userRefs builds the POST /users body: every roster player by id, plus pinned
// players configured by name that the roster does not know yet.
func (e *exporter) userRefs(roster []player) ([]UserRef, map[int64]bool) {
	refs := make([]UserRef, 0, len(roster)+len(e.pinnedNames)+len(e.pinnedIDs))
	pinned := make(map[int64]bool, len(e.pinnedIDs))
	haveID := make(map[int64]bool, len(roster))
	haveName := make(map[string]bool, len(roster))

	for _, id := range e.pinnedIDs {
		pinned[id] = true
	}
	for _, p := range roster {
		refs = append(refs, UserRef{ID: p.ID})
		haveID[p.ID] = true
		haveName[strings.ToLower(p.Name)] = true
		if p.Pinned {
			pinned[p.ID] = true
		}
	}
	for _, id := range e.pinnedIDs {
		if !haveID[id] {
			refs = append(refs, UserRef{ID: id})
			haveID[id] = true
		}
	}
	for _, name := range e.pinnedNames {
		if !haveName[strings.ToLower(name)] {
			refs = append(refs, UserRef{Name: name})
		}
	}
	return refs, pinned
}

// recordPinned makes sure pinned players survive roster pruning, and that ones
// given by name are stored with their id. It adds any name-configured player it
// resolves to pinned, so the caller can rank them in the same cycle.
func (e *exporter) recordPinned(ctx context.Context, users []User, pinned map[int64]bool, now time.Time) error {
	wanted := make(map[string]bool, len(e.pinnedNames))
	for _, n := range e.pinnedNames {
		wanted[strings.ToLower(n)] = true
	}
	var owners []Owner
	for _, u := range users {
		if u.ID == 0 {
			continue
		}
		if pinned[u.ID] || wanted[strings.ToLower(u.Name)] {
			pinned[u.ID] = true
			owners = append(owners, Owner{ID: u.ID, Name: u.Name})
		}
	}
	return e.store.seePlayers(ctx, owners, now, true)
}

// rankSamples orders players and keeps the top N, plus any pinned player that
// did not make the cut (with position 0, so their series stay available without
// claiming a place on the list).
func (e *exporter) rankSamples(samples []playerSample, pinned map[int64]bool) []playerSample {
	value, ok := rankFuncs[e.rank]
	if !ok {
		value = rankFuncs["points"]
	}
	sort.SliceStable(samples, func(i, j int) bool {
		vi, vj := value(samples[i]), value(samples[j])
		if vi != vj {
			return vi > vj
		}
		return samples[i].User.ID < samples[j].User.ID // stable tie-break
	})

	out := make([]playerSample, 0, min(len(samples), e.cfg.TopN)+len(pinned))
	for i := range samples {
		s := samples[i]
		s.Pinned = pinned[s.User.ID]
		inTop := i < e.cfg.TopN
		if inTop {
			s.Position = i + 1
		}
		if inTop || pinned[s.User.ID] {
			out = append(out, s)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Retention
// ---------------------------------------------------------------------------

// pruneLoop deletes rows that have aged out, so an unattended deployment does
// not grow without bound.
func (e *exporter) pruneLoop(ctx context.Context) error {
	if e.cfg.PruneInterval <= 0 {
		return nil
	}
	return e.everyTick(ctx, e.cfg.PruneInterval, "retention prune", func() error {
		now := time.Now()
		if e.cfg.TakeoverRetention > 0 {
			n, err := e.store.pruneTakeovers(ctx, now.Add(-e.cfg.TakeoverRetention))
			if err != nil {
				return err
			}
			if n > 0 {
				e.log.Info("pruned expired takeovers", "rows", n)
			}
		}
		// Players are kept for twice the roster TTL: dropping them the moment
		// they age off the roster would lose the first_seen history for anyone
		// who simply took a fortnight off.
		n, err := e.store.prunePlayers(ctx, now.Add(-2*e.cfg.RosterTTL))
		if err != nil {
			return err
		}
		if n > 0 {
			e.log.Info("pruned inactive players", "rows", n)
		}
		orphans, err := e.store.pruneOrphanedStats(ctx)
		if err != nil {
			return err
		}
		if orphans > 0 {
			e.log.Info("pruned stats for departed players", "rows", orphans)
		}
		// History is pruned by age only, deliberately not by roster membership:
		// the point of keeping it is to still have the trend after someone stops
		// playing.
		if e.cfg.HistoryRetention > 0 {
			h, err := e.store.pruneHistory(ctx, now.Add(-e.cfg.HistoryRetention))
			if err != nil {
				return err
			}
			if h > 0 {
				e.log.Info("pruned expired player history", "rows", h)
			}
		}
		return nil
	})
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

// publicMux is served on the address Fly publishes. Everything here is reachable
// from the internet, so /api/* carries the bearer token.
func (e *exporter) publicMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/api/players", e.requireToken(http.HandlerFunc(e.handleAPIPlayers)))
	mux.Handle("/api/history", e.requireToken(http.HandlerFunc(e.handleAPIHistory)))
	mux.HandleFunc("/healthz", handleHealthz)
	mux.HandleFunc("/zones", handleZoneMap)
	mux.HandleFunc("/status", e.handleStatus)
	mux.HandleFunc("/", e.handleLeaderboard)
	return mux
}

// internalMux carries telemetry and is served on a port Fly does not publish,
// so it is reachable only over the private network.
//
// Splitting the ports is what makes the authentication question disappear.
// Trying to serve /metrics on the public port meant guessing, per request,
// whether the caller was Fly's Prometheus or the internet — and getting that
// wrong fails silently, stopping ingestion with no error anywhere. A port that
// is simply never published cannot be reached from outside in the first place.
func (e *exporter) internalMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(e.registry, promhttp.HandlerOpts{
		// Serve what we have rather than failing the scrape, but say so — a
		// collector inconsistency is otherwise completely invisible.
		ErrorHandling: promhttp.ContinueOnError,
		ErrorLog:      gatherLogger{e.log},
	}))
	mux.HandleFunc("/healthz", handleHealthz)
	return mux
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ok\n"))
}

func (e *exporter) serve(ctx context.Context, name, address string, handler http.Handler) error {
	srv := &http.Server{
		Addr:              address,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	ln, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen on %s (%s): %w", address, name, err)
	}
	e.log.Info("serving http", "listener", name, "address", ln.Addr().String())

	errs := make(chan error, 1)
	go func() { errs <- srv.Serve(ln) }()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errs:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// leaderRow is one row of the leaderboard page.
type leaderRow struct {
	Position      int
	Name          string
	Points        int64
	PointsPerHour int64
	Zones         int
	Place         int64
	Pinned        bool
}

// handleLeaderboard renders the current top list. The snapshot it reads is the
// same one /metrics serves, so the page cannot disagree with the metrics.
func (e *exporter) handleLeaderboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	samples := e.players.snapshot()
	rows := make([]leaderRow, 0, len(samples))
	updated := time.Time{}
	for _, s := range samples {
		rows = append(rows, leaderRow{
			Position: s.Position, Name: s.User.Name, Points: s.User.Points,
			PointsPerHour: s.User.PointsPerHour, Zones: len(s.User.Zones),
			Place: s.User.Place, Pinned: s.Pinned,
		})
		if s.UpdatedAt.After(updated) {
			updated = s.UpdatedAt
		}
	}

	e.renderPage(w, "leaderboard.html", map[string]any{
		"Region":  e.regionTitle(),
		"RankBy":  rankLabel(e.rank),
		"TopN":    e.cfg.TopN,
		"Pinned":  strings.Join(e.cfg.Players, ", "),
		"Players": rows,
		"Updated": relativeTime(updated),
	})
}

// handleStatus renders operational state: how long the process has been up, how
// current the feed is, and how large the database has grown.
func (e *exporter) handleStatus(w http.ResponseWriter, r *http.Request) {
	cursor := e.cursor()
	feedAge := "—"
	if !cursor.IsZero() {
		feedAge = time.Since(cursor).Round(time.Second).String()
	}

	data := map[string]any{
		"Area":        e.sel.summary,
		"Uptime":      time.Since(e.startedAt).Round(time.Second).String(),
		"Started":     e.startedAt.UTC().Format("2006-01-02 15:04:05 MST"),
		"FeedCursor":  formatTime(cursor),
		"FeedAge":     feedAge,
		"DBSize":      "—",
		"Takeovers":   "—",
		"HistoryRows": "—",
		"Players":     "—",
		"DBMeasured":  "not yet measured",
	}
	// Database figures are sampled after each stats refresh rather than per
	// request, so loading this page costs no queries.
	if s := e.dbStats.Load(); s != nil {
		data["DBSize"] = humanBytes(s.Bytes)
		data["Takeovers"] = formatCount(s.Takeovers)
		data["HistoryRows"] = formatCount(s.HistoryRows)
		data["Players"] = formatCount(s.Players)
		data["DBMeasured"] = relativeTime(s.MeasuredAt)
	}
	e.renderPage(w, "status.html", data)
}

// handleZoneMap serves the embedded zone ownership map, the same page published
// to GitHub Pages. It calls the Turf API directly from the browser, so it needs
// nothing from this process beyond being handed to the client.
func handleZoneMap(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/zones" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=300")
	_, _ = w.Write(zoneMapPage)
}

// renderPage renders into a buffer first, so a template error surfaces as a 500
// rather than as a half-written page with a 200 already committed.
func (e *exporter) renderPage(w http.ResponseWriter, name string, data any) {
	var buf bytes.Buffer
	if err := pageTemplates.ExecuteTemplate(&buf, name, data); err != nil {
		e.log.Error("rendering page", "template", name, "err", err)
		http.Error(w, "template error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(buf.Bytes())
}

// regionTitle is the friendliest name available for the monitored area: the
// configured region name when one was given, otherwise the selector summary.
func (e *exporter) regionTitle() string {
	if len(e.cfg.Regions) == 1 {
		if _, err := strconv.ParseInt(e.cfg.Regions[0], 10, 64); err != nil {
			return e.cfg.Regions[0] // a name was configured, use it verbatim
		}
	}
	if name := e.sel.regionName; name != "" {
		return name
	}
	return e.sel.summary
}

func rankLabel(r rankBy) string {
	switch r {
	case "points":
		return "round points"
	case "totalPoints":
		return "all-time points"
	case "pointsPerHour":
		return "points per hour"
	case "taken":
		return "takeovers"
	case "areaZones":
		return "zones held here"
	default:
		return string(r)
	}
}

// humanBytes formats a byte count for people rather than for machines.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	value := float64(n)
	for _, suffix := range []string{"KiB", "MiB", "GiB", "TiB"} {
		value /= unit
		if value < unit {
			return fmt.Sprintf("%.1f %s", value, suffix)
		}
	}
	return fmt.Sprintf("%.1f PiB", value)
}

// formatCount groups thousands with thin spaces, which reads better than commas
// next to Danish and Swedish player names.
func formatCount(n int64) string {
	s := strconv.FormatInt(n, 10)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	for i, digit := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteString(" ")
		}
		b.WriteRune(digit)
	}
	return b.String()
}

// relativeTime renders an instant as an age, which is what a reader of a status
// page actually wants to know.
func relativeTime(t time.Time) string {
	if t.IsZero() {
		return "never"
	}
	d := time.Since(t)
	switch {
	case d < 0:
		return "just now"
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}

// requireToken wraps a handler with bearer token authentication, when a token is
// configured. With no token the handler is returned unchanged, which is correct
// while the app is only reachable on a private network.
//
// The token comes from the environment (TURF_API_TOKEN), never from a config
// file: fly.toml is committed, and a secret in git stays in git.
func (e *exporter) requireToken(next http.Handler) http.Handler {
	if e.cfg.APIToken == "" {
		return next
	}
	// Compare digests rather than the raw strings, so neither the token nor its
	// length leaks through timing.
	want := sha256.Sum256([]byte("Bearer " + e.cfg.APIToken))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := sha256.Sum256([]byte(r.Header.Get("Authorization")))
		if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
			e.metrics.unauthorized.Inc()
			w.Header().Set("WWW-Authenticate", `Bearer realm="turf-exporter"`)
			httpError(w, http.StatusUnauthorized, "missing or invalid bearer token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// apiPlayer is the current-standings row served by /api/players.
type apiPlayer struct {
	Player            string     `json:"player"`
	PlayerID          int64      `json:"player_id"`
	Position          *int       `json:"top_position"`
	Points            int64      `json:"points"`
	PointsPerHour     int64      `json:"points_per_hour"`
	Zones             int        `json:"zones"`
	AreaZones         *int       `json:"area_zones"`
	Takeovers         int64      `json:"takeovers"`
	ObservedTakeovers int64      `json:"observed_takeovers"`
	TotalPoints       int64      `json:"total_points"`
	Place             int64      `json:"place"`
	Rank              int64      `json:"rank"`
	Country           string     `json:"country"`
	Region            string     `json:"region"`
	Updated           *time.Time `json:"updated"`
}

// handleAPIPlayers serves the current top list as a flat JSON array, the shape
// Grafana's Infinity datasource consumes without any parsing configuration.
func (e *exporter) handleAPIPlayers(w http.ResponseWriter, r *http.Request) {
	out := []apiPlayer{}
	for _, s := range e.players.snapshot() {
		p := apiPlayer{
			Player: s.User.Name, PlayerID: s.User.ID,
			Points: s.User.Points, PointsPerHour: s.User.PointsPerHour,
			Zones: len(s.User.Zones), Takeovers: s.User.Taken,
			ObservedTakeovers: s.ObservedTakeovers, TotalPoints: s.User.TotalPoints,
			Place: s.User.Place, Rank: s.User.Rank, Country: s.User.Country,
		}
		// Null rather than a sentinel: a pinned player has no position, and an
		// unscanned player has no area zone count. Charting either as 0 would be
		// a lie.
		if s.Position > 0 {
			pos := s.Position
			p.Position = &pos
		}
		if s.AreaZones >= 0 {
			az := s.AreaZones
			p.AreaZones = &az
		}
		if s.User.Region != nil {
			p.Region = s.User.Region.Name
		}
		if !s.UpdatedAt.IsZero() {
			t := s.UpdatedAt.UTC()
			p.Updated = &t
		}
		out = append(out, p)
	}
	writeJSON(w, r, e.log, out)
}

// handleAPIHistory serves stored snapshots as a flat JSON array, oldest first.
//
// Parameters: from/to (RFC3339, Unix seconds, or a negative Go duration such as
// -7d... expressed as -168h), player (repeatable or comma-separated names),
// player_id (likewise), and limit.
func (e *exporter) handleAPIHistory(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	q := historyQuery{
		From:  now.Add(-7 * 24 * time.Hour),
		To:    now,
		Limit: 10000,
	}

	if v := r.URL.Query().Get("from"); v != "" {
		t, err := parseTimeParam(v, now)
		if err != nil {
			httpError(w, http.StatusBadRequest, "from: %v", err)
			return
		}
		q.From = t
	}
	if v := r.URL.Query().Get("to"); v != "" {
		t, err := parseTimeParam(v, now)
		if err != nil {
			httpError(w, http.StatusBadRequest, "to: %v", err)
			return
		}
		q.To = t
	}
	if q.To.Before(q.From) {
		httpError(w, http.StatusBadRequest, "to (%s) is before from (%s)",
			q.To.Format(time.RFC3339), q.From.Format(time.RFC3339))
		return
	}
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			httpError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		q.Limit = min(n, 200000)
	}
	q.Names = splitParams(r.URL.Query()["player"])
	for _, s := range splitParams(r.URL.Query()["player_id"]) {
		id, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			httpError(w, http.StatusBadRequest, "player_id %q is not a number", s)
			return
		}
		q.PlayerIDs = append(q.PlayerIDs, id)
	}

	rows, err := e.store.queryHistory(r.Context(), q)
	if err != nil {
		e.log.Error("history query failed", "err", err)
		httpError(w, http.StatusInternalServerError, "query failed")
		return
	}
	writeJSON(w, r, e.log, rows)
}

// parseTimeParam accepts RFC3339, Unix seconds, or a Go duration relative to
// now ("-24h"), which is what makes the endpoint pleasant to curl.
func parseTimeParam(v string, now time.Time) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return t, nil
	}
	if secs, err := strconv.ParseInt(v, 10, 64); err == nil {
		return time.Unix(secs, 0), nil
	}
	if d, err := time.ParseDuration(v); err == nil {
		return now.Add(d), nil
	}
	return time.Time{}, fmt.Errorf("want RFC3339, Unix seconds, or a duration like -24h; got %q", v)
}

// splitParams flattens repeated and comma-separated query parameters.
func splitParams(values []string) []string {
	var out []string
	for _, v := range values {
		for _, part := range strings.Split(v, ",") {
			if part = strings.TrimSpace(part); part != "" {
				out = append(out, part)
			}
		}
	}
	return out
}

func writeJSON(w http.ResponseWriter, r *http.Request, log *slog.Logger, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	if r.URL.Query().Get("pretty") != "" {
		enc.SetIndent("", "  ")
	}
	if err := enc.Encode(v); err != nil {
		log.Warn("writing json response", "err", err)
	}
}

func httpError(w http.ResponseWriter, code int, format string, args ...any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf(format, args...)})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// everyTick runs fn immediately and then on every tick, logging failures rather
// than tearing the process down: a transient API error should not take out the
// other loops.
func (e *exporter) everyTick(ctx context.Context, interval time.Duration, what string, fn func() error) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := fn(); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			e.log.Error(what+" failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// gatherLogger adapts slog to promhttp.Logger.
type gatherLogger struct{ log *slog.Logger }

func (g gatherLogger) Println(v ...any) {
	g.log.Error("error gathering metrics", "err", strings.TrimSpace(fmt.Sprintln(v...)))
}

// scanFinished releases anything waiting on the first discovery scan.
func (e *exporter) scanFinished() {
	e.firstScanOnce.Do(func() { close(e.firstScan) })
}

func outcomeLabel(err error) string {
	if err == nil {
		return "success"
	}
	return "error"
}

// splitPlayerSpecs divides always-track specs into numeric ids and names.
func splitPlayerSpecs(specs []string) (names []string, ids []int64, err error) {
	for _, spec := range specs {
		if spec = strings.TrimSpace(spec); spec == "" {
			continue
		}
		if id, convErr := strconv.ParseInt(spec, 10, 64); convErr == nil {
			if id <= 0 {
				return nil, nil, fmt.Errorf("invalid player id %q", spec)
			}
			ids = append(ids, id)
			continue
		}
		names = append(names, spec)
	}
	return names, ids, nil
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return "never"
	}
	// UTC throughout: the machine's zone is an accident of where it runs, and a
	// page mixing local and UTC timestamps invites misreading one for the other.
	return t.UTC().Format("2006-01-02 15:04:05 MST")
}
