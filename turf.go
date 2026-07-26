// Turf API client: types, bounding-box arithmetic, and a rate-limited caller.
//
// See TURF-API.md for what the API does and does not offer. Two facts shape
// this code: there is a global limit of one request per second, and there is no
// leaderboard endpoint, so player names must be discovered before their stats
// can be fetched.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

const (
	// defaultAPIBaseURL is the current preferred API version.
	defaultAPIBaseURL = "https://api.turfgame.com/v5"

	// apiTimeLayout is the ISO-8601 variant the API uses, e.g. "2026-07-21T16:03:19+0000".
	apiTimeLayout = "2006-01-02T15:04:05-0700"

	// maxUsersPerRequest is how many players we batch into one POST /users. The
	// API accepted 500 in testing; 400 keeps headroom.
	maxUsersPerRequest = 400

	// maxResponseBytes caps how much we read from one response. A 500-user batch
	// is ~320 KB, a dense zone tile a few hundred KB.
	maxResponseBytes = 32 << 20
)

// Feed event types. Any other subtype returns an empty list.
const (
	eventTakeover = "takeover"
	eventZone     = "zone"
	eventMedal    = "medal"
	eventChat     = "chat"
)

// Error codes observed from the API. They arrive either with a matching HTTP
// status or inline in a 200 body, so both paths funnel into apiError.
const (
	errCodeAreaTooBig  = 195887106
	errCodeRateLimited = 195887108
)

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

// apiTime wraps time.Time with the API's timestamp format.
type apiTime struct {
	time.Time
}

func (t *apiTime) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	if s == "" {
		t.Time = time.Time{}
		return nil
	}
	p, err := time.Parse(apiTimeLayout, s)
	if err != nil {
		return fmt.Errorf("parse turf timestamp %q: %w", s, err)
	}
	t.Time = p
	return nil
}

func (t apiTime) MarshalJSON() ([]byte, error) { return json.Marshal(t.String()) }

func (t apiTime) String() string {
	if t.IsZero() {
		return ""
	}
	return t.Format(apiTimeLayout)
}

// Owner is the {name, id} pair the API uses wherever it refers to a player.
type Owner struct {
	Name string `json:"name"`
	ID   int64  `json:"id"`
}

// Area is a municipality ("kommune") inside a Region.
type Area struct {
	Name string `json:"name"`
	ID   int64  `json:"id"`
}

// Region is a province. Country is populated on zone objects; the /regions
// listing carries the country on the enclosing object instead.
type Region struct {
	Name    string `json:"name"`
	ID      int64  `json:"id"`
	Area    *Area  `json:"area,omitempty"`
	Country string `json:"country,omitempty"`
}

func (r *Region) areaID() int64 {
	if r == nil || r.Area == nil {
		return 0
	}
	return r.Area.ID
}

func (r *Region) areaName() string {
	if r == nil || r.Area == nil {
		return ""
	}
	return r.Area.Name
}

// ZoneType is set only on special zones (Holy, Grave, …).
type ZoneType struct {
	Name string `json:"name"`
	ID   int64  `json:"id"`
}

// Zone is a capturable location. PreviousOwner is only present on feed events.
type Zone struct {
	Name           string    `json:"name"`
	ID             int64     `json:"id"`
	Latitude       float64   `json:"latitude"`
	Longitude      float64   `json:"longitude"`
	CurrentOwner   *Owner    `json:"currentOwner,omitempty"`
	PreviousOwner  *Owner    `json:"previousOwner,omitempty"`
	Region         *Region   `json:"region,omitempty"`
	Type           *ZoneType `json:"type,omitempty"`
	PointsPerHour  int64     `json:"pointsPerHour"`
	TakeoverPoints int64     `json:"takeoverPoints"`
	TotalTakeovers int64     `json:"totalTakeovers"`
	DateCreated    apiTime   `json:"dateCreated"`
	DateLastTaken  apiTime   `json:"dateLastTaken"`
}

// User is a player's stats. Points is for the current monthly round;
// TotalPoints is all-time. Rank is a title level, not a leaderboard position —
// Place is the position.
type User struct {
	Name             string  `json:"name"`
	ID               int64   `json:"id"`
	Country          string  `json:"country"`
	Region           *Region `json:"region,omitempty"`
	Points           int64   `json:"points"`
	PointsPerHour    int64   `json:"pointsPerHour"`
	TotalPoints      int64   `json:"totalPoints"`
	Place            int64   `json:"place"`
	Rank             int64   `json:"rank"`
	Taken            int64   `json:"taken"`
	UniqueZonesTaken int64   `json:"uniqueZonesTaken"`
	Blocktime        int64   `json:"blocktime"`
	Medals           []int64 `json:"medals"`
	Zones            []int64 `json:"zones"`
}

// UserRef identifies a player for POST /users. Exactly one field is sent.
type UserRef struct {
	Name string `json:"name,omitempty"`
	ID   int64  `json:"id,omitempty"`
}

// CountryRegions is one entry of GET /regions.
type CountryRegions struct {
	Country string `json:"country"`
	Name    string `json:"name"`
	ID      int64  `json:"id"`
	Areas   []Area `json:"areas"`
}

// FeedEvent is one entry from /feeds. Which fields are populated depends on
// Type; Raw always holds the original JSON, so nothing the API sends is lost.
type FeedEvent struct {
	Type         string  `json:"type"`
	Time         apiTime `json:"time"`
	Zone         *Zone   `json:"zone,omitempty"`
	CurrentOwner *Owner  `json:"currentOwner,omitempty"`
	Latitude     float64 `json:"latitude"`
	Longitude    float64 `json:"longitude"`
	User         *Owner  `json:"user,omitempty"`
	Medal        int64   `json:"medal"`
	Sender       *Owner  `json:"sender,omitempty"`
	Message      string  `json:"message,omitempty"`
	Region       *Region `json:"region,omitempty"`

	Raw json.RawMessage `json:"-"`
}

func (e *FeedEvent) UnmarshalJSON(b []byte) error {
	type alias FeedEvent // sheds UnmarshalJSON, keeps tags
	var a alias
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	*e = FeedEvent(a)
	e.Raw = append(json.RawMessage(nil), b...)
	return nil
}

// taker returns the player who took the zone in a takeover event.
func (e *FeedEvent) taker() *Owner {
	if e.CurrentOwner != nil {
		return e.CurrentOwner
	}
	if e.Zone != nil {
		return e.Zone.CurrentOwner
	}
	return nil
}

// previousOwner returns who held the zone before the takeover, or nil if the
// zone was neutral (newly created, or start of round).
func (e *FeedEvent) previousOwner() *Owner {
	if e.Zone != nil {
		return e.Zone.PreviousOwner
	}
	return nil
}

// eventRegion returns the region the event happened in, wherever the API chose
// to put it for this event type.
func (e *FeedEvent) eventRegion() *Region {
	if e.Zone != nil && e.Zone.Region != nil {
		return e.Zone.Region
	}
	return e.Region
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// apiError is an error response from the API.
type apiError struct {
	Status  int    `json:"-"`
	Code    int64  `json:"errorCode"`
	Message string `json:"errorMessage"`
}

func (e *apiError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("turf api: http %d", e.Status)
	}
	return fmt.Sprintf("turf api: %s (code %d, http %d)", e.Message, e.Code, e.Status)
}

// retryable reports whether resending the same request could succeed.
func (e *apiError) retryable() bool {
	return e.Code == errCodeRateLimited ||
		e.Status == http.StatusTooManyRequests ||
		e.Status >= http.StatusInternalServerError
}

// isRateLimited reports whether err is the one-request-per-second refusal.
func isRateLimited(err error) bool {
	var ae *apiError
	if errors.As(err, &ae) {
		return ae.Code == errCodeRateLimited || ae.Status == http.StatusTooManyRequests
	}
	return false
}

// isAreaTooBig reports whether err is a POST /zones bounding box size rejection.
func isAreaTooBig(err error) bool {
	var ae *apiError
	if errors.As(err, &ae) {
		return ae.Code == errCodeAreaTooBig
	}
	return false
}

// ---------------------------------------------------------------------------
// Bounding boxes
// ---------------------------------------------------------------------------

// latLon is the API's coordinate shape.
type latLon struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// boxRequest is one element of the POST /zones body.
type boxRequest struct {
	NorthEast latLon `json:"northEast"`
	SouthWest latLon `json:"southWest"`
}

// bbox is a lat/lon rectangle. The API rejects boxes above roughly 320 km² with
// errorCode 195887106 ("The area is too big"), so large areas must be tiled.
type bbox struct {
	South, West, North, East float64
}

func (b bbox) request() boxRequest {
	return boxRequest{
		NorthEast: latLon{Latitude: b.North, Longitude: b.East},
		SouthWest: latLon{Latitude: b.South, Longitude: b.West},
	}
}

// String renders the box as "south,west,north,east", the form Set parses.
func (b bbox) String() string {
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
	return f(b.South) + "," + f(b.West) + "," + f(b.North) + "," + f(b.East)
}

// Set implements flag.Value.
func (b *bbox) Set(s string) error {
	parts := strings.Split(strings.TrimSpace(s), ",")
	if len(parts) != 4 {
		return fmt.Errorf("want 4 comma-separated values (south,west,north,east), got %d", len(parts))
	}
	var v [4]float64
	for i, p := range parts {
		parsed, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
		if err != nil {
			return fmt.Errorf("value %d: %w", i+1, err)
		}
		v[i] = parsed
	}
	nb := bbox{South: v[0], West: v[1], North: v[2], East: v[3]}
	if !nb.valid() {
		return fmt.Errorf("empty box: %s", nb)
	}
	*b = nb
	return nil
}

func (b bbox) valid() bool { return b.North > b.South && b.East > b.West }

// areaKm2 approximates the ground area, using the mean latitude for the
// longitude scale. Good enough to predict the API's size rejection.
func (b bbox) areaKm2() float64 {
	const degKm = 111.32
	midLat := (b.North + b.South) / 2
	return (b.North - b.South) * degKm * (b.East - b.West) * degKm * math.Cos(midLat*math.Pi/180)
}

// tiles splits the box into a grid of sub-boxes no larger than maxLat by maxLon
// degrees, ordered row by row from the south-west corner.
func (b bbox) tiles(maxLat, maxLon float64) []bbox {
	if !b.valid() {
		return nil
	}
	rows, cols := steps(b.North-b.South, maxLat), steps(b.East-b.West, maxLon)
	latStep := (b.North - b.South) / float64(rows)
	lonStep := (b.East - b.West) / float64(cols)

	out := make([]bbox, 0, rows*cols)
	for r := range rows {
		for c := range cols {
			out = append(out, bbox{
				South: b.South + float64(r)*latStep,
				North: b.South + float64(r+1)*latStep,
				West:  b.West + float64(c)*lonStep,
				East:  b.West + float64(c+1)*lonStep,
			})
		}
	}
	return out
}

// quarter splits the box into four, to recover from an "area is too big"
// rejection without knowing the exact limit.
func (b bbox) quarter() []bbox {
	midLat, midLon := (b.North+b.South)/2, (b.East+b.West)/2
	return []bbox{
		{South: b.South, West: b.West, North: midLat, East: midLon},
		{South: b.South, West: midLon, North: midLat, East: b.East},
		{South: midLat, West: b.West, North: b.North, East: midLon},
		{South: midLat, West: midLon, North: b.North, East: b.East},
	}
}

func steps(span, limit float64) int {
	if limit <= 0 {
		return 1
	}
	return max(int(math.Ceil(span/limit)), 1)
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

// requestObserver receives per-request telemetry. endpoint is a low-cardinality
// label ("zones", "users", "feeds/takeover"), not the full URL.
type requestObserver interface {
	observeRequest(endpoint, outcome string, duration time.Duration)
	observeRateLimited(endpoint string)
}

type nopObserver struct{}

func (nopObserver) observeRequest(string, string, time.Duration) {}
func (nopObserver) observeRateLimited(string)                    {}

// turfClient talks to the Turf API. It is safe for concurrent use: every request
// passes through one rate limiter, so callers never have to coordinate.
type turfClient struct {
	base       string
	hc         *http.Client
	limiter    *rate.Limiter
	log        *slog.Logger
	obs        requestObserver
	maxRetries int
}

func newTurfClient(baseURL string, minInterval time.Duration, maxRetries int, log *slog.Logger, obs requestObserver) *turfClient {
	if baseURL == "" {
		baseURL = defaultAPIBaseURL
	}
	if minInterval < time.Second {
		minInterval = time.Second
	}
	if maxRetries <= 0 {
		maxRetries = 4
	}
	if log == nil {
		log = slog.Default()
	}
	if obs == nil {
		obs = nopObserver{}
	}
	return &turfClient{
		base:       baseURL,
		hc:         &http.Client{Timeout: 60 * time.Second},
		limiter:    rate.NewLimiter(rate.Every(minInterval), 1),
		log:        log,
		obs:        obs,
		maxRetries: maxRetries,
	}
}

// zones returns every zone inside the box, splitting into quarters whenever the
// API rejects the box as too big. depth bounds the recursion.
func (c *turfClient) zones(ctx context.Context, box bbox, depth int) ([]Zone, error) {
	var out []Zone
	err := c.do(ctx, http.MethodPost, "/zones", "zones", []boxRequest{box.request()}, &out)
	if err == nil {
		return out, nil
	}
	if !isAreaTooBig(err) || depth <= 0 {
		return nil, fmt.Errorf("zones %s: %w", box, err)
	}
	c.log.Debug("splitting oversized bounding box", "box", box.String(), "area_km2", box.areaKm2())
	var all []Zone
	for _, q := range box.quarter() {
		sub, err := c.zones(ctx, q, depth-1)
		if err != nil {
			return nil, err
		}
		all = append(all, sub...)
	}
	return all, nil
}

// users returns stats for the given players, batching into as few requests as
// the API allows. Unknown players are simply absent from the result.
func (c *turfClient) users(ctx context.Context, refs []UserRef) ([]User, error) {
	out := make([]User, 0, len(refs))
	for start := 0; start < len(refs); start += maxUsersPerRequest {
		chunk := refs[start:min(start+maxUsersPerRequest, len(refs))]
		var users []User
		if err := c.do(ctx, http.MethodPost, "/users", "users", chunk, &users); err != nil {
			return out, fmt.Errorf("users (batch of %d): %w", len(chunk), err)
		}
		out = append(out, users...)
	}
	return out, nil
}

// topQuery selects a leaderboard. Exactly one of Region or Country is set;
// Region must be the region's *name* — a numeric id makes the endpoint return
// HTTP 500.
type topQuery struct {
	Region  string `json:"region,omitempty"`
	Country string `json:"country,omitempty"`
	From    int    `json:"from"`
	To      int    `json:"to"`
}

// maxTopUsers is how many entries one /users/top request returns. Windows past
// the first 50 come back with `place` values that do not continue the sequence,
// so paging beyond this is not trustworthy and is not attempted.
const maxTopUsers = 50

// usersTop returns a leaderboard in order, with `place` localized to the
// requested region or country — the same list and the same numbering the game
// shows in its own tabs.
//
// This is the endpoint that removes the need to discover players at all: the
// ranking is authoritative and complete, where anything assembled from zone
// ownership is necessarily partial.
func (c *turfClient) usersTop(ctx context.Context, q topQuery) ([]User, error) {
	q.From = 1
	q.To = maxTopUsers
	var out []User
	if err := c.do(ctx, http.MethodPost, "/users/top", "users/top", q, &out); err != nil {
		return nil, fmt.Errorf("users/top %s%s: %w", q.Region, q.Country, err)
	}
	return out, nil
}

// feed returns events of one type ("takeover", "zone", "medal", "chat"), or all
// types when kind is empty. A non-zero after limits results to newer events; the
// API silently returns whatever it retains if after predates its window.
func (c *turfClient) feed(ctx context.Context, kind string, after time.Time) ([]FeedEvent, error) {
	path, endpoint := "/feeds", "feeds"
	if kind != "" {
		path += "/" + kind
		endpoint += "/" + kind
	}
	if !after.IsZero() {
		path += "?afterDate=" + url.QueryEscape(after.UTC().Format(apiTimeLayout))
	}
	var events []FeedEvent
	if err := c.do(ctx, http.MethodGet, path, endpoint, nil, &events); err != nil {
		return nil, fmt.Errorf("feed %q: %w", endpoint, err)
	}
	return events, nil
}

// regions returns every region and area, grouped by country.
func (c *turfClient) regions(ctx context.Context) ([]CountryRegions, error) {
	var out []CountryRegions
	if err := c.do(ctx, http.MethodGet, "/regions", "regions", nil, &out); err != nil {
		return nil, fmt.Errorf("regions: %w", err)
	}
	return out, nil
}

func (c *turfClient) do(ctx context.Context, method, path, endpoint string, body, out any) error {
	var encoded []byte
	if body != nil {
		var err error
		if encoded, err = json.Marshal(body); err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
	}

	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			if err := sleepCtx(ctx, backoff(attempt)); err != nil {
				return err
			}
		}
		// The limiter, not the retry loop, is what keeps us under 1 req/s.
		if err := c.limiter.Wait(ctx); err != nil {
			return err
		}

		start := time.Now()
		err := c.attempt(ctx, method, path, encoded, out)
		c.obs.observeRequest(endpoint, outcomeOf(err), time.Since(start))

		switch {
		case err == nil:
			return nil
		case isRateLimited(err):
			c.obs.observeRateLimited(endpoint)
		case !isRetryable(err):
			return err
		}
		lastErr = err
		c.log.Warn("turf api request failed, retrying",
			"endpoint", endpoint, "attempt", attempt+1, "err", err)
	}
	return fmt.Errorf("after %d attempts: %w", c.maxRetries+1, lastErr)
}

func (c *turfClient) attempt(ctx context.Context, method, path string, body []byte, out any) error {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "turf-exporter (+https://github.com/pstibrany/turf-zones)")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	// Errors come back either with a matching status or inline in a 200 body.
	// Successful responses are arrays (or, for a single zone, an object without
	// errorCode), so sniffing for errorCode is unambiguous.
	if ae := decodeAPIError(raw, resp.StatusCode); ae != nil {
		return ae
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return &apiError{Status: resp.StatusCode, Message: snippet(raw)}
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode response (%s): %w", snippet(raw), err)
	}
	return nil
}

func decodeAPIError(raw []byte, status int) *apiError {
	trimmed := bytes.TrimLeft(raw, " \t\r\n")
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil
	}
	var ae apiError
	if err := json.Unmarshal(trimmed, &ae); err != nil {
		return nil
	}
	if ae.Code == 0 && ae.Message == "" {
		return nil // a legitimate object response
	}
	ae.Status = status
	return &ae
}

func isRetryable(err error) bool {
	var ae *apiError
	if errors.As(err, &ae) {
		return ae.retryable()
	}
	// Transport failures (connection reset, timeout, DNS) are worth another try.
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

func outcomeOf(err error) string {
	switch {
	case err == nil:
		return "success"
	case isRateLimited(err):
		return "rate_limited"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "canceled"
	default:
		var ae *apiError
		if errors.As(err, &ae) {
			return "api_error"
		}
		return "error"
	}
}

// backoff grows 1s, 2s, 4s, 8s… with jitter, on top of the limiter's spacing.
func backoff(attempt int) time.Duration {
	d := time.Second << min(attempt-1, 5)
	return d + rand.N(d/2)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func snippet(b []byte) string {
	const limit = 200
	if len(b) > limit {
		return string(b[:limit]) + "…"
	}
	return string(b)
}
