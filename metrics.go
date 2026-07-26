package main

import (
	"cmp"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// ---------------------------------------------------------------------------
// Exporter's own metrics
// ---------------------------------------------------------------------------

// opsMetrics covers the exporter's behaviour: API traffic, feed volume, and the
// health of each background loop. These are ordinary registered collectors; the
// per-player series live in playerCollector instead.
type opsMetrics struct {
	apiRequests    *prometheus.CounterVec
	apiDuration    *prometheus.HistogramVec
	apiRateLimited *prometheus.CounterVec

	feedEvents    *prometheus.CounterVec
	feedTakeovers *prometheus.CounterVec
	feedStored    prometheus.Counter
	feedPolls     *prometheus.CounterVec
	feedLag       prometheus.Gauge
	feedCursor    prometheus.Gauge

	exposedPlayers  prometheus.Gauge
	storedTakeovers prometheus.Gauge

	refreshes   *prometheus.CounterVec
	refreshTime prometheus.Gauge

	unauthorized prometheus.Counter
	dbBytes      prometheus.Gauge

	historyWrites *prometheus.CounterVec
	historyRows   prometheus.Counter
	historyStored prometheus.Gauge

	scans     *prometheus.CounterVec
	scanTime  prometheus.Gauge
	scanTiles prometheus.Gauge
	scanZones *prometheus.GaugeVec
}

func newOpsMetrics(reg prometheus.Registerer) *opsMetrics {
	f := metricFactory{reg}
	return &opsMetrics{
		apiRequests: f.counterVec(prometheus.CounterOpts{
			Name: "turf_api_requests_total",
			Help: "Turf API requests by endpoint and outcome.",
		}, "endpoint", "outcome"),
		apiDuration: f.histogramVec(prometheus.HistogramOpts{
			Name:    "turf_api_request_duration_seconds",
			Help:    "Latency of Turf API requests, excluding time spent waiting on the client rate limiter.",
			Buckets: prometheus.DefBuckets,
		}, "endpoint"),
		apiRateLimited: f.counterVec(prometheus.CounterOpts{
			Name: "turf_api_rate_limited_total",
			Help: "Requests refused by the Turf API's one-per-second limit.",
		}, "endpoint"),

		feedEvents: f.counterVec(prometheus.CounterOpts{
			Name: "turf_feed_events_total",
			Help: "Feed events received, by event type.",
		}, "type"),
		feedTakeovers: f.counterVec(prometheus.CounterOpts{
			Name: "turf_feed_takeovers_total",
			Help: "Takeover events received, by country and whether they fall inside the monitored area.",
		}, "country", "monitored"),
		feedStored: f.counter(prometheus.CounterOpts{
			Name: "turf_feed_takeovers_stored_total",
			Help: "Takeover events stored, i.e. not already seen in an earlier poll.",
		}),
		feedPolls: f.counterVec(prometheus.CounterOpts{
			Name: "turf_feed_polls_total",
			Help: "Takeover feed polls, by outcome.",
		}, "outcome"),
		feedLag: f.gauge(prometheus.GaugeOpts{
			Name: "turf_feed_lag_seconds",
			Help: "Age of the newest event seen in the last successful poll.",
		}),
		feedCursor: f.gauge(prometheus.GaugeOpts{
			Name: "turf_feed_cursor_timestamp_seconds",
			Help: "Timestamp of the newest feed event processed so far.",
		}),

		exposedPlayers: f.gauge(prometheus.GaugeOpts{
			Name: "turf_exposed_players",
			Help: "Players currently exposed as per-player metric series, across all boards.",
		}),
		storedTakeovers: f.gauge(prometheus.GaugeOpts{
			Name: "turf_stored_takeovers",
			Help: "Takeover rows currently in the database.",
		}),

		refreshes: f.counterVec(prometheus.CounterOpts{
			Name: "turf_stats_refreshes_total",
			Help: "Player stats refresh cycles, by outcome.",
		}, "outcome"),
		refreshTime: f.gauge(prometheus.GaugeOpts{
			Name: "turf_stats_last_success_timestamp_seconds",
			Help: "When the player stats were last refreshed successfully.",
		}),

		dbBytes: f.gauge(prometheus.GaugeOpts{
			Name: "turf_db_size_bytes",
			Help: "Size of the SQLite database including its WAL, sampled after each stats refresh.",
		}),

		unauthorized: f.counter(prometheus.CounterOpts{
			Name: "turf_http_unauthorized_total",
			Help: "Requests to /api/* rejected for a missing or invalid bearer token.",
		}),

		historyWrites: f.counterVec(prometheus.CounterOpts{
			Name: "turf_history_writes_total",
			Help: "Attempts to store a player history snapshot, by outcome.",
		}, "outcome"),
		historyRows: f.counter(prometheus.CounterOpts{
			Name: "turf_history_rows_written_total",
			Help: "Player history rows actually inserted; zero when the current time bucket is already recorded.",
		}),
		historyStored: f.gauge(prometheus.GaugeOpts{
			Name: "turf_history_rows",
			Help: "Player history rows currently in the database.",
		}),

		scans: f.counterVec(prometheus.CounterOpts{
			Name: "turf_zone_scans_total",
			Help: "Zone discovery scans, by outcome.",
		}, "outcome"),
		scanTime: f.gauge(prometheus.GaugeOpts{
			Name: "turf_zone_scan_last_success_timestamp_seconds",
			Help: "When the zone discovery scan last completed successfully.",
		}),
		scanTiles: f.gauge(prometheus.GaugeOpts{
			Name: "turf_zone_scan_tiles",
			Help: "Number of bounding box tiles the discovery scan requests.",
		}),
		scanZones: f.gaugeVec(prometheus.GaugeOpts{
			Name: "turf_area_zones",
			Help: "Zones found by the last discovery scan, split by whether they fall inside the monitored area.",
		}, "monitored"),
	}
}

// observeRequest implements requestObserver.
func (m *opsMetrics) observeRequest(endpoint, outcome string, d time.Duration) {
	m.apiRequests.WithLabelValues(endpoint, outcome).Inc()
	m.apiDuration.WithLabelValues(endpoint).Observe(d.Seconds())
}

// observeRateLimited implements requestObserver.
func (m *opsMetrics) observeRateLimited(endpoint string) {
	m.apiRateLimited.WithLabelValues(endpoint).Inc()
}

// metricFactory registers each metric as it is built, so there is no separate
// list of MustRegister calls to keep in sync.
type metricFactory struct{ reg prometheus.Registerer }

func (f metricFactory) counter(o prometheus.CounterOpts) prometheus.Counter {
	c := prometheus.NewCounter(o)
	f.reg.MustRegister(c)
	return c
}

func (f metricFactory) counterVec(o prometheus.CounterOpts, labels ...string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(o, labels)
	f.reg.MustRegister(c)
	return c
}

func (f metricFactory) gauge(o prometheus.GaugeOpts) prometheus.Gauge {
	g := prometheus.NewGauge(o)
	f.reg.MustRegister(g)
	return g
}

func (f metricFactory) gaugeVec(o prometheus.GaugeOpts, labels ...string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(o, labels)
	f.reg.MustRegister(g)
	return g
}

func (f metricFactory) histogramVec(o prometheus.HistogramOpts, labels ...string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(o, labels)
	f.reg.MustRegister(h)
	return h
}

func boolLabel(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// ---------------------------------------------------------------------------
// Per-player metrics
// ---------------------------------------------------------------------------

// playerSample is everything exposed about one player at a point in time.
type playerSample struct {
	User User
	// Positions maps board key to the player's 1-based rank on that board. A
	// player can place on several (Hovedstaden and Denmark both), or on none if
	// they are only here because they were pinned.
	Positions map[string]int
	// AreaZones is how many zones inside the monitored area the player held at
	// the last discovery scan; -1 means there is no scan data yet.
	AreaZones int
	// ObservedTakeovers is how many takeovers we recorded for this player from
	// the feed, within the takeover retention window.
	ObservedTakeovers int64
	// Pinned means the player is tracked by configuration rather than by rank.
	Pinned    bool
	UpdatedAt time.Time
}

// bestPosition is the player's highest placing across all boards, or a sentinel
// that sorts last when they placed on none.
func (s playerSample) bestPosition() int {
	best := 1 << 30
	for _, p := range s.Positions {
		if p > 0 && p < best {
			best = p
		}
	}
	return best
}

var playerLabels = []string{"player", "player_id"}

// playerCollector publishes per-player series from a snapshot that the stats
// refresher replaces wholesale.
//
// It is a custom collector rather than a set of GaugeVecs on purpose: when a
// player drops off the top list their series must disappear, and reconciling a
// GaugeVec by hand (delete the ones that left, keep the ones that stayed) is
// exactly the bookkeeping a snapshot collector makes unnecessary.
type playerCollector struct {
	mu      sync.RWMutex
	samples []playerSample

	values   []playerValue
	position *prometheus.Desc
	updated  *prometheus.Desc
	info     *prometheus.Desc
	descs    []*prometheus.Desc
}

type playerValue struct {
	desc  *prometheus.Desc
	kind  prometheus.ValueType
	value func(playerSample) (float64, bool)
}

func newPlayerCollector() *playerCollector {
	c := &playerCollector{}
	gauge := func(name, help string, fn func(playerSample) (float64, bool)) {
		c.add(name, help, prometheus.GaugeValue, fn)
	}
	counter := func(name, help string, fn func(playerSample) (float64, bool)) {
		c.add(name, help, prometheus.CounterValue, fn)
	}
	always := func(fn func(playerSample) int64) func(playerSample) (float64, bool) {
		return func(s playerSample) (float64, bool) { return float64(fn(s)), true }
	}

	gauge("turf_player_points",
		"Points scored by the player in the current monthly round.",
		always(func(s playerSample) int64 { return s.User.Points }))
	gauge("turf_player_zones",
		"Zones the player currently holds, anywhere.",
		always(func(s playerSample) int64 { return int64(len(s.User.Zones)) }))
	gauge("turf_player_points_per_hour",
		"Points per hour the player currently earns from the zones they hold.",
		always(func(s playerSample) int64 { return s.User.PointsPerHour }))
	counter("turf_player_takeovers_total",
		"Takeovers the player has made all-time, as reported by the Turf user API.",
		always(func(s playerSample) int64 { return s.User.Taken }))
	counter("turf_player_observed_takeovers_total",
		"Takeovers by the player recorded from the takeover feed and still within the retention window.",
		always(func(s playerSample) int64 { return s.ObservedTakeovers }))
	counter("turf_player_total_points",
		"All-time points scored by the player.",
		always(func(s playerSample) int64 { return s.User.TotalPoints }))
	counter("turf_player_unique_zones_taken",
		"Distinct zones the player has ever taken.",
		always(func(s playerSample) int64 { return s.User.UniqueZonesTaken }))
	gauge("turf_player_rank",
		"The player's rank level (a title, not a leaderboard position).",
		always(func(s playerSample) int64 { return s.User.Rank }))
	gauge("turf_player_blocktime_seconds",
		"How long the player must wait before retaking a zone.",
		always(func(s playerSample) int64 { return s.User.Blocktime }))
	gauge("turf_player_medals",
		"Number of medals the player has earned.",
		always(func(s playerSample) int64 { return int64(len(s.User.Medals)) }))
	gauge("turf_player_place",
		"The player's position on the global Turf leaderboard.",
		func(s playerSample) (float64, bool) { return float64(s.User.Place), s.User.Place > 0 })
	gauge("turf_player_area_zones",
		"Zones inside the monitored area the player held at the last discovery scan.",
		func(s playerSample) (float64, bool) { return float64(s.AreaZones), s.AreaZones >= 0 })

	c.position = prometheus.NewDesc(
		"turf_player_top_position",
		"The player's position on a leaderboard, 1 being highest. Absent for pinned players who did not place.",
		[]string{"player", "player_id", "board"}, nil)
	c.descs = append(c.descs, c.position)
	c.updated = prometheus.NewDesc(
		"turf_player_stats_updated_timestamp_seconds",
		"When this player's stats were last fetched.", playerLabels, nil)
	c.info = prometheus.NewDesc(
		"turf_player_info",
		"Player metadata as labels; always 1. Join on player_id to add country and home region to the other player metrics.",
		[]string{"player", "player_id", "country", "region"}, nil)
	c.descs = append(c.descs, c.updated, c.info)
	return c
}

func (c *playerCollector) add(name, help string, kind prometheus.ValueType, fn func(playerSample) (float64, bool)) {
	d := prometheus.NewDesc(name, help, playerLabels, nil)
	c.descs = append(c.descs, d)
	c.values = append(c.values, playerValue{desc: d, kind: kind, value: fn})
}

// set replaces the exposed snapshot.
func (c *playerCollector) set(samples []playerSample) {
	c.mu.Lock()
	c.samples = samples
	c.mu.Unlock()
}

// snapshot returns the exposed samples ordered by top list position, with
// pinned players that did not place at the end.
func (c *playerCollector) snapshot() []playerSample {
	c.mu.RLock()
	out := slices.Clone(c.samples)
	c.mu.RUnlock()

	slices.SortStableFunc(out, func(a, b playerSample) int {
		return cmp.Or(
			cmp.Compare(a.bestPosition(), b.bestPosition()),
			cmp.Compare(b.User.Points, a.User.Points),
		)
	})
	return out
}

// Describe implements prometheus.Collector.
func (c *playerCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range c.descs {
		ch <- d
	}
}

// Collect implements prometheus.Collector.
func (c *playerCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	samples := c.samples
	c.mu.RUnlock()

	for _, s := range samples {
		name := s.User.Name
		id := strconv.FormatInt(s.User.ID, 10)
		for _, v := range c.values {
			if f, ok := v.value(s); ok {
				ch <- prometheus.MustNewConstMetric(v.desc, v.kind, f, name, id)
			}
		}
		for boardKey, pos := range s.Positions {
			ch <- prometheus.MustNewConstMetric(c.position, prometheus.GaugeValue,
				float64(pos), name, id, boardKey)
		}
		if !s.UpdatedAt.IsZero() {
			ch <- prometheus.MustNewConstMetric(c.updated, prometheus.GaugeValue,
				float64(s.UpdatedAt.Unix()), name, id)
		}
		region := ""
		if s.User.Region != nil {
			region = s.User.Region.Name
		}
		ch <- prometheus.MustNewConstMetric(c.info, prometheus.GaugeValue, 1,
			name, id, s.User.Country, region)
	}
}
