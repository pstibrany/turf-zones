// turf-exporter follows the Turf game API for one region and exposes the local
// player leaderboard as Prometheus metrics, logging every takeover as it happens.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// envPrefix is prepended to the upper-cased, underscore-separated flag name, so
// -top.n can also be set as TURF_TOP_N. Handy on Fly.io, where configuration is
// naturally environment variables.
const envPrefix = "TURF_"

// defaultCopenhagenBox covers the mainland part of region 172 (Hovedstaden):
// Copenhagen and Frederiksberg, the western suburbs out to Frederikssund, and
// North Zealand up to Helsingør. Bornholm also belongs to Hovedstaden but sits
// 200 km away, so it is left out by default — add a second box for it if you
// want it (see -scan.bbox).
var defaultCopenhagenBox = bbox{South: 55.55, West: 11.95, North: 56.12, East: 12.75}

// Config is the exporter's full configuration.
type Config struct {
	ListenAddress   string
	InternalAddress string
	LogLevel        string
	LogFormat       string

	APIBaseURL     string
	APIMinInterval time.Duration
	APIMaxRetries  int
	APIToken       string

	Countries stringList
	Regions   stringList
	Areas     stringList

	ScanEnabled  bool
	Boxes        bboxList
	ScanInterval time.Duration
	ScanTileLat  float64
	ScanTileLon  float64
	ScanMaxSplit int

	FeedInterval    time.Duration
	FeedOverlap     time.Duration
	FeedMaxLookback time.Duration
	FeedLog         scope
	FeedStore       scope
	FeedStoreRaw    bool

	StatsInterval time.Duration
	CountryBoard  bool
	TopN          int
	Players       stringList

	HistoryInterval  time.Duration
	HistoryRetention time.Duration

	DBPath            string
	TakeoverRetention time.Duration
	PruneInterval     time.Duration
}

// defaultConfig is the configuration used when no flags are given: region 172
// (Hovedstaden) in Denmark, top 50 by points scored this round.
func defaultConfig() Config {
	return Config{
		ListenAddress:   ":8080",
		InternalAddress: ":9090",
		LogLevel:        "info",
		LogFormat:       "json",
		APIBaseURL:      defaultAPIBaseURL,
		// Far more spacing than the API's one-per-second demands, deliberately.
		// The limiter spaces requests when they *leave*, but the API counts them
		// when they *arrive*, and variable latency compresses the gap — 1.1s drew
		// a 429 on the second request of a cold start. Steady state needs only
		// about one request per minute (a feed poll, plus one batched user refresh
		// every ten), so this uses under a tenth of the allowance on a free
		// service that warns about heavy use. The only thing it slows is the
		// discovery scan, which is sequential: 16 tiles take ~80s.
		APIMinInterval: 5 * time.Second,
		APIMaxRetries:  4,
		Countries:      stringList{"dk"},
		Regions:        stringList{"172"},

		ScanEnabled:  true,
		Boxes:        bboxList{defaultCopenhagenBox},
		ScanInterval: 6 * time.Hour,
		ScanTileLat:  0.15,
		ScanTileLon:  0.25,
		ScanMaxSplit: 3,

		FeedInterval:    time.Minute,
		FeedOverlap:     2 * time.Minute,
		FeedMaxLookback: 25 * time.Minute,
		FeedLog:         scopeMonitored,
		FeedStore:       scopeMonitored,

		StatsInterval: 10 * time.Minute,
		CountryBoard:  true,
		TopN:          50,

		HistoryInterval:  time.Hour,
		HistoryRetention: 90 * 24 * time.Hour,

		DBPath:            "turf.db",
		TakeoverRetention: 90 * 24 * time.Hour,
		PruneInterval:     6 * time.Hour,
	}
}

func (c *Config) registerFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.ListenAddress, "listen", c.ListenAddress,
		"Address for the public listener: the status page and the token-guarded /api/* endpoints.")
	fs.StringVar(&c.InternalAddress, "listen.internal", c.InternalAddress,
		"Address for the telemetry listener: /metrics and /healthz, unauthenticated. Do not publish this port — keeping it private is what makes the lack of authentication safe. Empty disables it.")
	fs.StringVar(&c.LogLevel, "log.level", c.LogLevel, "Log level: debug, info, warn or error.")
	fs.StringVar(&c.LogFormat, "log.format", c.LogFormat, "Log format: json or text.")

	fs.StringVar(&c.APIBaseURL, "api.url", c.APIBaseURL, "Base URL of the Turf API.")
	fs.DurationVar(&c.APIMinInterval, "api.min-interval", c.APIMinInterval,
		"Minimum spacing between Turf API requests. The API allows one per second globally, so this cannot go below 1s — and only one instance may run per source address.")
	fs.IntVar(&c.APIMaxRetries, "api.max-retries", c.APIMaxRetries, "Retries after a failed Turf API request.")
	fs.StringVar(&c.APIToken, "api.token", c.APIToken,
		"Bearer token required on /api/* requests; empty leaves them open, which is fine while the app is only reachable privately. Prefer setting TURF_API_TOKEN in the environment — a command-line flag is visible in the process list. /metrics is never guarded, so Fly's Prometheus can still scrape it.")

	fs.Var(&c.Countries, "turf.countries", "Comma-separated country codes to monitor.")
	fs.Var(&c.Regions, "turf.regions",
		"Comma-separated region ids or names to monitor; empty means every region in the selected countries. Denmark: 172 Hovedstaden, 173 Sjælland, 174 Syddanmark, 175 Midtjylland, 176 Nordjylland.")
	fs.Var(&c.Areas, "turf.areas",
		"Comma-separated area (municipality) ids or names to narrow the region further; empty means all of it.")

	fs.BoolVar(&c.ScanEnabled, "scan.enabled", c.ScanEnabled,
		"Periodically enumerate zones to discover players. Without it the roster only grows from the takeover feed, which is slow for a small country.")
	fs.Var(&c.Boxes, "scan.bbox",
		"Bounding boxes to scan for players, as south,west,north,east. Separate multiple boxes with ';'. Only zones matching the region filter are kept.")
	fs.DurationVar(&c.ScanInterval, "scan.interval", c.ScanInterval, "How often to run the zone discovery scan.")
	fs.Float64Var(&c.ScanTileLat, "scan.tile-lat", c.ScanTileLat, "Latitude span of one scan tile, in degrees.")
	fs.Float64Var(&c.ScanTileLon, "scan.tile-lon", c.ScanTileLon,
		"Longitude span of one scan tile, in degrees. The API rejects boxes above roughly 320 km².")
	fs.IntVar(&c.ScanMaxSplit, "scan.max-split", c.ScanMaxSplit,
		"How many times a tile may be quartered when the API still calls it too big.")

	fs.DurationVar(&c.FeedInterval, "feed.interval", c.FeedInterval, "How often to poll the takeover feed.")
	fs.DurationVar(&c.FeedOverlap, "feed.overlap", c.FeedOverlap,
		"How far behind the cursor to re-request on each poll. Duplicates are discarded on insert, so this only costs bandwidth.")
	fs.DurationVar(&c.FeedMaxLookback, "feed.max-lookback", c.FeedMaxLookback,
		"Furthest back to ever request. The takeover feed only retains about 30 minutes.")
	fs.Var(scopeValue{&c.FeedLog}, "feed.log", "Which takeovers to log: area, all or none.")
	fs.Var(scopeValue{&c.FeedStore}, "feed.store", "Which takeovers to store in the database: area, all or none.")
	fs.BoolVar(&c.FeedStoreRaw, "feed.store-raw", c.FeedStoreRaw,
		"Also keep the raw JSON of each takeover event, in the database and in the log line.")

	fs.DurationVar(&c.StatsInterval, "stats.interval", c.StatsInterval, "How often to refetch the leaderboards.")
	fs.BoolVar(&c.CountryBoard, "boards.country", c.CountryBoard,
		"Also show a country-wide leaderboard alongside the region one. Both come straight from /users/top, so both are complete.")
	fs.IntVar(&c.TopN, "top.n", c.TopN,
		"How many players per board to expose. The API returns at most 50, and windows past that renumber, so higher values have no effect.")
	fs.Var(&c.Players, "players",
		"Comma-separated player names or ids to always track, whether or not they make the top list.")

	fs.DurationVar(&c.HistoryInterval, "history.interval", c.HistoryInterval,
		"How often to append a player stats snapshot to the database. Snapshots are bucketed to this interval, so refreshes in between write nothing. Zero disables history.")
	fs.DurationVar(&c.HistoryRetention, "history.retention", c.HistoryRetention,
		"How long to keep player history snapshots. Zero keeps them forever.")

	fs.StringVar(&c.DBPath, "db.path", c.DBPath, "Path to the SQLite database. It is created if missing.")
	fs.DurationVar(&c.TakeoverRetention, "db.takeover-retention", c.TakeoverRetention, "How long to keep stored takeovers.")
	fs.DurationVar(&c.PruneInterval, "db.prune-interval", c.PruneInterval, "How often to delete expired rows.")
}

// validate rejects configurations that cannot work.
func (c *Config) validate() error {
	if c.APIMinInterval < time.Second {
		return fmt.Errorf("api.min-interval must be at least 1s (the API allows one request per second), got %s", c.APIMinInterval)
	}
	if c.TopN <= 0 {
		return fmt.Errorf("top.n must be positive, got %d", c.TopN)
	}
	if !c.FeedLog.valid() {
		return fmt.Errorf("feed.log must be area, all or none, got %q", c.FeedLog)
	}
	if !c.FeedStore.valid() {
		return fmt.Errorf("feed.store must be area, all or none, got %q", c.FeedStore)
	}
	if c.FeedInterval <= 0 {
		return fmt.Errorf("feed.interval must be positive")
	}
	// A feed poll needs one request slot per interval. If spacing is wider than
	// the interval, polls fall behind for good, and takeovers are lost as soon as
	// the lag passes the feed's ~30 minute retention — silently, since each
	// individual poll still succeeds.
	if c.APIMinInterval >= c.FeedInterval {
		return fmt.Errorf("api.min-interval (%s) must be shorter than feed.interval (%s), or takeover polls cannot keep up",
			c.APIMinInterval, c.FeedInterval)
	}
	if c.StatsInterval <= 0 {
		return fmt.Errorf("stats.interval must be positive")
	}
	// A token short enough to brute-force is worse than none, because it invites
	// the belief that the endpoint is protected.
	if c.APIToken != "" && len(c.APIToken) < 16 {
		return fmt.Errorf("api.token must be at least 16 characters; generate one with: openssl rand -hex 32")
	}
	if c.ListenAddress != "" && c.ListenAddress == c.InternalAddress {
		return fmt.Errorf("listen and listen.internal must differ; sharing a port would publish /metrics")
	}
	if c.DBPath == "" {
		return fmt.Errorf("db.path must be set")
	}
	if c.ScanEnabled {
		if len(c.Boxes) == 0 {
			return fmt.Errorf("scan.enabled requires at least one scan.bbox")
		}
		if c.ScanTileLat <= 0 || c.ScanTileLon <= 0 {
			return fmt.Errorf("scan.tile-lat and scan.tile-lon must be positive")
		}
	}
	return nil
}

func main() {
	cfg := defaultConfig()
	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	cfg.registerFlags(fs)

	if err := fs.Parse(os.Args[1:]); err != nil {
		if err == flag.ErrHelp {
			os.Exit(0)
		}
		os.Exit(2)
	}
	if err := applyEnv(fs, envPrefix); err != nil {
		fmt.Fprintf(os.Stderr, "invalid environment configuration: %v\n", err)
		os.Exit(2)
	}
	if err := cfg.validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid configuration: %v\n", err)
		os.Exit(2)
	}

	log, events, err := newLoggers(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid logging configuration: %v\n", err)
		os.Exit(2)
	}

	if err := run(cfg, log, events); err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run(cfg Config, log, events *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	e, err := newExporter(ctx, cfg, log, events)
	if err != nil {
		return err
	}
	defer func() {
		if err := e.Close(); err != nil {
			log.Warn("closing database", "err", err)
		}
	}()

	err = e.run(ctx)
	log.Info("shut down")
	return err
}

// newLoggers returns the application logger and the takeover event logger. They
// are separate so takeover lines can be told apart, and so raising the app log
// level never silences the event stream.
func newLoggers(cfg Config) (app, events *slog.Logger, err error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		return nil, nil, fmt.Errorf("log.level %q: %w", cfg.LogLevel, err)
	}

	newHandler := func(l slog.Level) (slog.Handler, error) {
		opts := &slog.HandlerOptions{Level: l}
		switch strings.ToLower(cfg.LogFormat) {
		case "json":
			return slog.NewJSONHandler(os.Stdout, opts), nil
		case "text", "logfmt":
			return slog.NewTextHandler(os.Stdout, opts), nil
		default:
			return nil, fmt.Errorf("log.format %q: want json or text", cfg.LogFormat)
		}
	}

	appHandler, err := newHandler(level)
	if err != nil {
		return nil, nil, err
	}
	// Takeovers are logged at info; the event logger stays at info regardless of
	// the app level, since emitting them is the point of the feed follower.
	eventHandler, err := newHandler(slog.LevelInfo)
	if err != nil {
		return nil, nil, err
	}

	app = slog.New(appHandler)
	slog.SetDefault(app)
	events = slog.New(eventHandler).With("event", "takeover")
	return app, events, nil
}

// applyEnv fills in flags that were not given on the command line from
// environment variables. Explicit flags win.
func applyEnv(fs *flag.FlagSet, prefix string) error {
	given := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { given[f.Name] = true })

	replacer := strings.NewReplacer(".", "_", "-", "_")
	var firstErr error
	fs.VisitAll(func(f *flag.Flag) {
		if given[f.Name] || firstErr != nil {
			return
		}
		key := prefix + strings.ToUpper(replacer.Replace(f.Name))
		value, ok := os.LookupEnv(key)
		if !ok {
			return
		}
		if err := f.Value.Set(value); err != nil {
			firstErr = fmt.Errorf("%s=%q: %w", key, value, err)
		}
	})
	return firstErr
}

// ---------------------------------------------------------------------------
// Flag value types
// ---------------------------------------------------------------------------

// stringList is a comma-separated flag value.
type stringList []string

func (l *stringList) String() string { return strings.Join(*l, ",") }

func (l *stringList) Set(s string) error {
	*l = nil
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			*l = append(*l, part)
		}
	}
	return nil
}

// bboxList is a ';'-separated list of bounding boxes, so a region split across
// distant places (Hovedstaden's mainland and Bornholm, say) can be covered
// without scanning the sea between them.
type bboxList []bbox

func (l *bboxList) String() string {
	parts := make([]string, len(*l))
	for i, b := range *l {
		parts[i] = b.String()
	}
	return strings.Join(parts, ";")
}

func (l *bboxList) Set(s string) error {
	*l = nil
	for _, part := range strings.Split(s, ";") {
		if part = strings.TrimSpace(part); part == "" {
			continue
		}
		var b bbox
		if err := b.Set(part); err != nil {
			return err
		}
		*l = append(*l, b)
	}
	return nil
}

// scopeValue adapts scope to flag.Value.
type scopeValue struct{ target *scope }

func (v scopeValue) String() string {
	if v.target == nil {
		return ""
	}
	return string(*v.target)
}

func (v scopeValue) Set(s string) error {
	sc := scope(strings.ToLower(strings.TrimSpace(s)))
	if !sc.valid() {
		return fmt.Errorf("must be area, all or none")
	}
	*v.target = sc
	return nil
}
