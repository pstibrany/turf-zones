package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver: no cgo, so the binary stays static
)

// store is the exporter's persistent state: which players we know about, their
// last known stats, and the takeover history harvested from the feed.
//
// It exists for three reasons. The API has no leaderboard, so the roster we rank
// has to be accumulated over time and must survive restarts. Feed events carry
// no unique id, so the primary key on takeovers doubles as the dedupe mechanism.
// And the takeover log is worth keeping — it is the one thing the API will not
// give you retroactively.
type store struct {
	db *sql.DB
}

// migrations are applied in order; the database's PRAGMA user_version records
// how many have run. Never edit an existing entry — append a new one.
var migrations = []string{
	// 1: initial schema.
	`
	CREATE TABLE players (
		id          INTEGER PRIMARY KEY,
		name        TEXT    NOT NULL,
		first_seen  INTEGER NOT NULL,
		last_seen   INTEGER NOT NULL,
		pinned      INTEGER NOT NULL DEFAULT 0
	);
	CREATE INDEX players_last_seen ON players(last_seen);

	CREATE TABLE player_stats (
		player_id          INTEGER PRIMARY KEY,
		updated_at         INTEGER NOT NULL,
		name               TEXT    NOT NULL,
		country            TEXT,
		region_id          INTEGER,
		region_name        TEXT,
		points             INTEGER,
		points_per_hour    INTEGER,
		total_points       INTEGER,
		place              INTEGER,
		rank               INTEGER,
		taken              INTEGER,
		unique_zones_taken INTEGER,
		blocktime          INTEGER,
		medals             INTEGER,
		zones              INTEGER
	);

	CREATE TABLE takeovers (
		time                 INTEGER NOT NULL,
		zone_id              INTEGER NOT NULL,
		taker_id             INTEGER NOT NULL,
		zone_name            TEXT,
		country              TEXT,
		region_id            INTEGER,
		region_name          TEXT,
		area_id              INTEGER,
		area_name            TEXT,
		latitude             REAL,
		longitude            REAL,
		taker_name           TEXT,
		previous_owner_id    INTEGER,
		previous_owner_name  TEXT,
		takeover_points      INTEGER,
		points_per_hour      INTEGER,
		zone_total_takeovers INTEGER,
		raw                  TEXT,
		PRIMARY KEY (time, zone_id, taker_id)
	);
	CREATE INDEX takeovers_time ON takeovers(time);
	CREATE INDEX takeovers_taker ON takeovers(taker_id, time);

	CREATE TABLE meta (
		key   TEXT PRIMARY KEY,
		value TEXT NOT NULL
	);
	`,
	// 2: player stat history. player_stats only ever holds the latest row per
	// player, so everything older was being discarded; this keeps a periodic
	// snapshot so trends outlive any Prometheus retention window.
	`
	CREATE TABLE player_history (
		ts                 INTEGER NOT NULL,
		player_id          INTEGER NOT NULL,
		name               TEXT    NOT NULL,
		points             INTEGER,
		points_per_hour    INTEGER,
		total_points       INTEGER,
		place              INTEGER,
		rank               INTEGER,
		taken              INTEGER,
		unique_zones_taken INTEGER,
		zones              INTEGER,
		area_zones         INTEGER,
		PRIMARY KEY (ts, player_id)
	);
	CREATE INDEX player_history_player ON player_history(player_id, ts);
	CREATE INDEX player_history_ts ON player_history(ts);
	`,
	// 3: /users/top returns each leaderboard already ranked and complete, so the
	// roster has nothing left to do — players existed only to accumulate an
	// approximation of a list the API serves directly.
	//
	// player_stats survives with a new shape and a new job: a cache of the last
	// leaderboard, so a restart can render the page before the first API call
	// returns — and can still render it if the API is down at that moment. That
	// means remembering which board and which position, not just the figures, so
	// the key becomes (board, player_id). Pinned players who placed nowhere are
	// stored under the empty board.
	`
	DROP TABLE IF EXISTS players;
	DROP TABLE IF EXISTS player_stats;

	CREATE TABLE player_stats (
		board              TEXT    NOT NULL,
		player_id          INTEGER NOT NULL,
		position           INTEGER,
		pinned             INTEGER NOT NULL DEFAULT 0,
		updated_at         INTEGER NOT NULL,
		name               TEXT    NOT NULL,
		country            TEXT,
		region_id          INTEGER,
		region_name        TEXT,
		points             INTEGER,
		points_per_hour    INTEGER,
		total_points       INTEGER,
		place              INTEGER,
		rank               INTEGER,
		taken              INTEGER,
		unique_zones_taken INTEGER,
		blocktime          INTEGER,
		medals             INTEGER,
		zones              INTEGER,
		area_zones         INTEGER,
		PRIMARY KEY (board, player_id)
	);
	`,
}

// openStore opens the database at path, creating and migrating it as needed.
// SQLite creates the file if it is missing, so a fresh volume needs no setup.
// Use ":memory:" for an ephemeral store.
func openStore(ctx context.Context, path string, log *slog.Logger) (*store, error) {
	dsn := path
	if path != ":memory:" && path != "" {
		if abs, err := filepath.Abs(path); err == nil {
			path = abs
		}
		// WAL keeps the single writer from blocking readers; busy_timeout stops
		// the occasional overlap from surfacing as SQLITE_BUSY.
		dsn = "file:" + url.PathEscape(path) +
			"?_pragma=journal_mode(WAL)" +
			"&_pragma=busy_timeout(5000)" +
			"&_pragma=synchronous(NORMAL)"
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	// Writes are tiny and infrequent; one connection sidesteps lock contention
	// entirely, and keeps an in-memory DB from being recreated per connection.
	db.SetMaxOpenConns(1)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	s := &store{db: db}
	if err := s.migrate(ctx, log); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate %s: %w", path, err)
	}
	return s, nil
}

// migrate applies any migrations the database has not seen yet.
func (s *store) migrate(ctx context.Context, log *slog.Logger) error {
	var version int
	if err := s.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if version > len(migrations) {
		return fmt.Errorf("database schema version %d is newer than this binary understands (%d)", version, len(migrations))
	}
	for i := version; i < len(migrations); i++ {
		log.Info("applying database migration", "version", i+1)
		if err := s.tx(ctx, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, migrations[i]); err != nil {
				return err
			}
			// PRAGMA does not accept a bound parameter.
			_, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", i+1))
			return err
		}); err != nil {
			return fmt.Errorf("migration %d: %w", i+1, err)
		}
	}
	return nil
}

func (s *store) Close() error { return s.db.Close() }

// takeover is one takeover event, flattened for storage.
type takeover struct {
	Time              time.Time
	ZoneID            int64
	ZoneName          string
	Country           string
	RegionID          int64
	RegionName        string
	AreaID            int64
	AreaName          string
	Latitude          float64
	Longitude         float64
	TakerID           int64
	TakerName         string
	PreviousOwnerID   int64
	PreviousOwnerName string
	TakeoverPoints    int64
	PointsPerHour     int64
	ZoneTotalTakeover int64
	Raw               string
}

// takeoverFromEvent flattens a feed event. It returns false for events that are
// not takeovers or that lack the fields we key on.
func takeoverFromEvent(e *FeedEvent) (takeover, bool) {
	taker := e.taker()
	if e.Type != eventTakeover || e.Zone == nil || taker == nil || e.Time.IsZero() {
		return takeover{}, false
	}
	t := takeover{
		Time:              e.Time.Time,
		ZoneID:            e.Zone.ID,
		ZoneName:          e.Zone.Name,
		Latitude:          orFallback(e.Latitude, e.Zone.Latitude),
		Longitude:         orFallback(e.Longitude, e.Zone.Longitude),
		TakerID:           taker.ID,
		TakerName:         taker.Name,
		TakeoverPoints:    e.Zone.TakeoverPoints,
		PointsPerHour:     e.Zone.PointsPerHour,
		ZoneTotalTakeover: e.Zone.TotalTakeovers,
		Raw:               string(e.Raw),
	}
	if r := e.eventRegion(); r != nil {
		t.Country, t.RegionID, t.RegionName = r.Country, r.ID, r.Name
		t.AreaID, t.AreaName = r.areaID(), r.areaName()
	}
	if prev := e.previousOwner(); prev != nil {
		t.PreviousOwnerID, t.PreviousOwnerName = prev.ID, prev.Name
	}
	return t, true
}

// insertTakeovers stores takeovers, ignoring ones already present, and returns
// the subset that was new. That subset is also what still needs logging: feed
// events have no id, so (time, zone, taker) is our identity.
func (s *store) insertTakeovers(ctx context.Context, takeovers []takeover, storeRaw bool) ([]takeover, error) {
	if len(takeovers) == 0 {
		return nil, nil
	}
	var fresh []takeover
	err := s.tx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT OR IGNORE INTO takeovers (
				time, zone_id, taker_id, zone_name, country, region_id, region_name,
				area_id, area_name, latitude, longitude, taker_name,
				previous_owner_id, previous_owner_name, takeover_points,
				points_per_hour, zone_total_takeovers, raw
			) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, t := range takeovers {
			raw := ""
			if storeRaw {
				raw = t.Raw
			}
			res, err := stmt.ExecContext(ctx,
				t.Time.Unix(), t.ZoneID, t.TakerID, t.ZoneName, t.Country, t.RegionID, t.RegionName,
				t.AreaID, t.AreaName, t.Latitude, t.Longitude, t.TakerName,
				t.PreviousOwnerID, t.PreviousOwnerName, t.TakeoverPoints,
				t.PointsPerHour, t.ZoneTotalTakeover, raw)
			if err != nil {
				return fmt.Errorf("insert takeover zone=%d: %w", t.ZoneID, err)
			}
			if n, err := res.RowsAffected(); err == nil && n > 0 {
				fresh = append(fresh, t)
			}
		}
		return nil
	})
	return fresh, err
}

// takeoverCounts returns stored takeovers per player since the cutoff. This is
// the feed-derived count, independent of the all-time figure the user API gives.
func (s *store) takeoverCounts(ctx context.Context, since time.Time) (map[int64]int64, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT taker_id, COUNT(*) FROM takeovers WHERE time >= ? GROUP BY taker_id`, since.Unix())
	if err != nil {
		return nil, fmt.Errorf("count takeovers: %w", err)
	}
	defer rows.Close()

	out := make(map[int64]int64)
	for rows.Next() {
		var id, n int64
		if err := rows.Scan(&id, &n); err != nil {
			return nil, err
		}
		out[id] = n
	}
	return out, rows.Err()
}

// countPlayers returns how many distinct players the cached leaderboards hold.
func (s *store) countPlayers(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT player_id) FROM player_stats`).Scan(&n)
	return n, err
}

func (s *store) countTakeovers(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM takeovers`).Scan(&n)
	return n, err
}

// pruneTakeovers deletes takeovers older than the cutoff.
func (s *store) pruneTakeovers(ctx context.Context, before time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM takeovers WHERE time < ?`, before.Unix())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// cacheLeaderboards replaces the cached copy of every board with the given
// samples. It is a whole-table swap rather than an upsert, because a player who
// has dropped off a board must disappear from the cache too — otherwise a
// restart would resurrect them.
func (s *store) cacheLeaderboards(ctx context.Context, samples []playerSample, at time.Time) error {
	return s.tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM player_stats`); err != nil {
			return err
		}
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO player_stats (
				board, player_id, position, pinned, updated_at, name, country,
				region_id, region_name, points, points_per_hour, total_points,
				place, rank, taken, unique_zones_taken, blocktime, medals, zones,
				area_zones
			) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, p := range samples {
			u := p.User
			var regionID int64
			var regionName string
			if u.Region != nil {
				regionID, regionName = u.Region.ID, u.Region.Name
			}
			var areaZones any
			if p.AreaZones >= 0 {
				areaZones = p.AreaZones
			}
			pinned := 0
			if p.Pinned {
				pinned = 1
			}
			// One row per board the player placed on; a pinned player who placed
			// nowhere still needs a row, stored under the empty board.
			rows := make(map[string]any, len(p.Positions)+1)
			for boardKey, pos := range p.Positions {
				rows[boardKey] = pos
			}
			if len(rows) == 0 {
				rows[""] = nil
			}
			for boardKey, pos := range rows {
				if _, err := stmt.ExecContext(ctx,
					boardKey, u.ID, pos, pinned, at.Unix(), u.Name, u.Country,
					regionID, regionName, u.Points, u.PointsPerHour, u.TotalPoints,
					u.Place, u.Rank, u.Taken, u.UniqueZonesTaken, u.Blocktime,
					len(u.Medals), len(u.Zones), areaZones); err != nil {
					return fmt.Errorf("cache %s/%d: %w", boardKey, u.ID, err)
				}
			}
		}
		return nil
	})
}

// cachedLeaderboards returns the last leaderboards written by
// cacheLeaderboards, so a restart can render the page before the first API call
// returns — and can still render it if the API happens to be down right then.
func (s *store) cachedLeaderboards(ctx context.Context) ([]playerSample, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT board, player_id, position, pinned, updated_at, name, country,
		       region_id, region_name, points, points_per_hour, total_points,
		       place, rank, taken, unique_zones_taken, blocktime, medals, zones,
		       area_zones
		FROM player_stats`)
	if err != nil {
		return nil, fmt.Errorf("load cached leaderboards: %w", err)
	}
	defer rows.Close()

	// Rows are per board, so several may describe the same player; merge them
	// back into one sample carrying every position.
	byPlayer := make(map[int64]*playerSample)
	var order []int64
	for rows.Next() {
		var (
			boardKey           string
			playerID           int64
			position, areaZone sql.NullInt64
			pinned, updated    int64
			regionID           int64
			medals, zoneCount  int64
			name               string
			country, regionNm  sql.NullString
			u                  User
		)
		if err := rows.Scan(&boardKey, &playerID, &position, &pinned, &updated, &name,
			&country, &regionID, &regionNm, &u.Points, &u.PointsPerHour, &u.TotalPoints,
			&u.Place, &u.Rank, &u.Taken, &u.UniqueZonesTaken, &u.Blocktime,
			&medals, &zoneCount, &areaZone); err != nil {
			return nil, err
		}

		p, ok := byPlayer[playerID]
		if !ok {
			u.ID, u.Name, u.Country = playerID, name, country.String
			if regionID != 0 || regionNm.String != "" {
				u.Region = &Region{ID: regionID, Name: regionNm.String, Country: country.String}
			}
			// Only counts were stored, not the id lists; slices of the right
			// length keep the collector's len() arithmetic honest.
			u.Medals = make([]int64, medals)
			u.Zones = make([]int64, zoneCount)
			p = &playerSample{
				User:      u,
				Positions: map[string]int{},
				Pinned:    pinned == 1,
				AreaZones: -1,
				UpdatedAt: time.Unix(updated, 0),
			}
			if areaZone.Valid {
				p.AreaZones = int(areaZone.Int64)
			}
			byPlayer[playerID] = p
			order = append(order, playerID)
		}
		if boardKey != "" && position.Valid {
			p.Positions[boardKey] = int(position.Int64)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]playerSample, 0, len(order))
	for _, id := range order {
		out = append(out, *byPlayer[id])
	}
	return out, nil
}

// insertHistory records a snapshot of each player at the given bucket time.
// The primary key on (ts, player_id) makes this idempotent: stats refresh more
// often than snapshots are wanted, so every refresh within the same bucket
// simply writes nothing. That means no separate "when did I last snapshot"
// state to keep, and it survives restarts for free. Returns how many rows were
// new.
func (s *store) insertHistory(ctx context.Context, samples []playerSample, bucket time.Time) (int, error) {
	if len(samples) == 0 {
		return 0, nil
	}
	written := 0
	err := s.tx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT OR IGNORE INTO player_history (
				ts, player_id, name, points, points_per_hour, total_points,
				place, rank, taken, unique_zones_taken, zones, area_zones
			) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		ts := bucket.Unix()
		for _, p := range samples {
			if p.User.ID == 0 {
				continue
			}
			// -1 means "no scan data yet"; store it as NULL rather than as a
			// number that would average into charts as if it were real.
			var areaZones any
			if p.AreaZones >= 0 {
				areaZones = p.AreaZones
			}
			res, err := stmt.ExecContext(ctx, ts, p.User.ID, p.User.Name,
				p.User.Points, p.User.PointsPerHour, p.User.TotalPoints,
				p.User.Place, p.User.Rank, p.User.Taken, p.User.UniqueZonesTaken,
				len(p.User.Zones), areaZones)
			if err != nil {
				return fmt.Errorf("insert history for %d: %w", p.User.ID, err)
			}
			if n, err := res.RowsAffected(); err == nil && n > 0 {
				written++
			}
		}
		return nil
	})
	return written, err
}

// historyRow is one stored snapshot.
type historyRow struct {
	Time             time.Time `json:"time"`
	PlayerID         int64     `json:"player_id"`
	Name             string    `json:"player"`
	Points           int64     `json:"points"`
	PointsPerHour    int64     `json:"points_per_hour"`
	TotalPoints      int64     `json:"total_points"`
	Place            int64     `json:"place"`
	Rank             int64     `json:"rank"`
	Taken            int64     `json:"takeovers"`
	UniqueZonesTaken int64     `json:"unique_zones_taken"`
	Zones            int64     `json:"zones"`
	AreaZones        *int64    `json:"area_zones"`
}

// historyQuery selects a slice of history.
type historyQuery struct {
	From      time.Time
	To        time.Time
	PlayerIDs []int64
	Names     []string
	Limit     int
}

// queryHistory returns snapshots matching the query, oldest first. An empty
// PlayerIDs and Names means every player.
func (s *store) queryHistory(ctx context.Context, q historyQuery) ([]historyRow, error) {
	sb := strings.Builder{}
	sb.WriteString(`
		SELECT ts, player_id, name, points, points_per_hour, total_points,
		       place, rank, taken, unique_zones_taken, zones, area_zones
		FROM player_history WHERE ts >= ? AND ts <= ?`)
	args := []any{q.From.Unix(), q.To.Unix()}

	if len(q.PlayerIDs) > 0 || len(q.Names) > 0 {
		var clauses []string
		if len(q.PlayerIDs) > 0 {
			clauses = append(clauses, "player_id IN ("+placeholders(len(q.PlayerIDs))+")")
			for _, id := range q.PlayerIDs {
				args = append(args, id)
			}
		}
		if len(q.Names) > 0 {
			clauses = append(clauses, "name COLLATE NOCASE IN ("+placeholders(len(q.Names))+")")
			for _, n := range q.Names {
				args = append(args, n)
			}
		}
		sb.WriteString(" AND (" + strings.Join(clauses, " OR ") + ")")
	}
	sb.WriteString(" ORDER BY ts ASC, player_id ASC LIMIT ?")
	args = append(args, q.Limit)

	rows, err := s.db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query history: %w", err)
	}
	defer rows.Close()

	out := []historyRow{}
	for rows.Next() {
		var r historyRow
		var ts int64
		var areaZones sql.NullInt64
		if err := rows.Scan(&ts, &r.PlayerID, &r.Name, &r.Points, &r.PointsPerHour,
			&r.TotalPoints, &r.Place, &r.Rank, &r.Taken, &r.UniqueZonesTaken,
			&r.Zones, &areaZones); err != nil {
			return nil, err
		}
		r.Time = time.Unix(ts, 0).UTC()
		if areaZones.Valid {
			r.AreaZones = &areaZones.Int64
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// pruneHistory deletes snapshots older than the cutoff.
func (s *store) pruneHistory(ctx context.Context, before time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM player_history WHERE ts < ?`, before.Unix())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// countHistory returns the number of stored snapshot rows.
func (s *store) countHistory(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM player_history`).Scan(&n)
	return n, err
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

// getMeta reads a string from the meta table; ok is false when absent.
func (s *store) getMeta(ctx context.Context, key string) (value string, ok bool, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

// setMeta writes a string to the meta table.
func (s *store) setMeta(ctx context.Context, key, value string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO meta (key, value) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

// getMetaTime reads a timestamp from the meta table.
func (s *store) getMetaTime(ctx context.Context, key string) (time.Time, bool, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, false, nil // corrupt value: treat as absent
	}
	return t, true, nil
}

// setMetaTime writes a timestamp to the meta table.
func (s *store) setMetaTime(ctx context.Context, key string, t time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO meta (key, value) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		key, t.UTC().Format(time.RFC3339))
	return err
}

func (s *store) tx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func orFallback(primary, fallback float64) float64 {
	if primary != 0 {
		return primary
	}
	return fallback
}
