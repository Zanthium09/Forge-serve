package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
)

// Errors the admin API maps to 404 / 409 / 400.
var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("conflict")
	ErrInvalid  = errors.New("invalid")
)

// classify turns constraint violations into the admin API's errors.
func classify(err error) error {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		switch pg.Code {
		case "23505": // unique_violation
			return fmt.Errorf("%w: %s", ErrConflict, pg.Detail)
		case "23503", "23514", "22P02": // foreign key, check, bad text repr
			return fmt.Errorf("%w: %s", ErrInvalid, pg.Message)
		}
	}
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

type User struct {
	ID                 int64     `json:"id"`
	Email              string    `json:"email"`
	DisplayName        string    `json:"display_name"`
	TeamID             *int64    `json:"team_id"`
	Role               string    `json:"role"`
	CanViewUsage       bool      `json:"can_view_usage"`
	Status             string    `json:"status"`
	RPMBurst           int       `json:"rpm_burst"`
	RPMRefillPerSec    float64   `json:"rpm_refill_per_sec"`
	TokensPerMinute    *int      `json:"tokens_per_minute"`
	DailyTokenBudget   *int64    `json:"daily_token_budget"`
	MonthlyTokenBudget *int64    `json:"monthly_token_budget"`
	Priority           *int      `json:"priority"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

const userCols = `id, email, display_name, team_id, role, can_view_usage, status, rpm_burst,
	rpm_refill_per_sec, tokens_per_minute, daily_token_budget, monthly_token_budget,
	priority, created_at, updated_at`

type scanner interface{ Scan(dest ...any) error }

func scanUser(r scanner) (User, error) {
	var u User
	err := r.Scan(&u.ID, &u.Email, &u.DisplayName, &u.TeamID, &u.Role, &u.CanViewUsage, &u.Status,
		&u.RPMBurst, &u.RPMRefillPerSec, &u.TokensPerMinute, &u.DailyTokenBudget,
		&u.MonthlyTokenBudget, &u.Priority, &u.CreatedAt, &u.UpdatedAt)
	return u, err
}

// UserColumns are the PATCH-able user columns (email is the identity and
// never changes). The bool says whether NULL is allowed.
var UserColumns = map[string]bool{
	"display_name": false, "team_id": true, "role": false, "can_view_usage": false,
	"rpm_burst": false, "rpm_refill_per_sec": false, "tokens_per_minute": true,
	"daily_token_budget": true, "monthly_token_budget": true, "priority": true,
}

// TeamColumns are the PATCH-able team columns.
var TeamColumns = map[string]bool{"name": false, "monthly_token_cap": true, "priority": true}

// tx runs fn and its audit row in one transaction, so every admin change
// is recorded or none is.
func (s *Store) tx(ctx context.Context, actor *int64, action, target string, detail any,
	fn func(tx *sql.Tx) (string, error)) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	t, err := fn(tx)
	if err != nil {
		return classify(err)
	}
	if t != "" {
		target = t
	}
	d, _ := json.Marshal(detail)
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO admin_audit (actor_user_id, action, target, detail) VALUES ($1, $2, $3, $4)`,
		actor, action, target, string(d)); err != nil {
		return err
	}
	return tx.Commit()
}

func target(kind string, id int64) string { return kind + ":" + strconv.FormatInt(id, 10) }

// CreateUser inserts a user; email uniqueness is case-insensitive.
func (s *Store) CreateUser(ctx context.Context, actor *int64, u User) (User, error) {
	var out User
	err := s.tx(ctx, actor, "user.create", "", map[string]any{"email": u.Email}, func(tx *sql.Tx) (string, error) {
		var err error
		out, err = scanUser(tx.QueryRowContext(ctx, `INSERT INTO users
			(email, display_name, team_id, role, can_view_usage, rpm_burst, rpm_refill_per_sec,
			 tokens_per_minute, daily_token_budget, monthly_token_budget, priority)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING `+userCols,
			u.Email, u.DisplayName, u.TeamID, u.Role, u.CanViewUsage, u.RPMBurst, u.RPMRefillPerSec,
			u.TokensPerMinute, u.DailyTokenBudget, u.MonthlyTokenBudget, u.Priority))
		return target("user", out.ID), err
	})
	return out, err
}

func (s *Store) GetUser(ctx context.Context, id int64) (User, error) {
	u, err := scanUser(s.db.QueryRowContext(ctx, `SELECT `+userCols+` FROM users WHERE id = $1`, id))
	return u, classify(err)
}

func (s *Store) ListUsers(ctx context.Context, teamID *int64, status string, limit, offset int) ([]User, error) {
	return s.queryUsers(ctx, `SELECT `+userCols+` FROM users
		WHERE ($1::bigint IS NULL OR team_id = $1) AND ($2 = '' OR status = $2)
		ORDER BY id LIMIT $3 OFFSET $4`, teamID, status, limit, offset)
}

// InactiveUsers: active accounts older than the window with no requests in
// it, so forgotten accounts can be offboarded (spec §7.7).
func (s *Store) InactiveUsers(ctx context.Context, days int) ([]User, error) {
	return s.queryUsers(ctx, `SELECT `+userCols+` FROM users u
		WHERE u.status = 'active' AND u.created_at < now() - make_interval(days => $1)
		  AND NOT EXISTS (SELECT 1 FROM usage_events e
		                  WHERE e.user_id = u.id AND e.created_at >= now() - make_interval(days => $1))
		ORDER BY u.email`, days)
}

func (s *Store) queryUsers(ctx context.Context, q string, args ...any) ([]User, error) {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []User{}
	for rows.Next() {
		u, err := scanUser(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// patch builds "col = $n, ..." from whitelisted columns; nil sets NULL.
func patch(set map[string]any, allowed map[string]bool, args []any) (string, []any, error) {
	var parts []string
	for col, v := range set {
		if _, ok := allowed[col]; !ok {
			return "", nil, fmt.Errorf("%w: unknown field %s", ErrInvalid, col)
		}
		args = append(args, v)
		parts = append(parts, col+" = $"+strconv.Itoa(len(args)))
	}
	return strings.Join(parts, ", "), args, nil
}

func (s *Store) UpdateUser(ctx context.Context, actor *int64, id int64, set map[string]any) (User, error) {
	sets, args, err := patch(set, UserColumns, []any{id})
	if err != nil {
		return User{}, err
	}
	if sets == "" {
		return s.GetUser(ctx, id)
	}
	var out User
	err = s.tx(ctx, actor, "user.update", target("user", id), set, func(tx *sql.Tx) (string, error) {
		var err error
		out, err = scanUser(tx.QueryRowContext(ctx,
			`UPDATE users SET `+sets+`, updated_at = now() WHERE id = $1 RETURNING `+userCols, args...))
		return "", err
	})
	return out, err
}

// SetUserStatus disables or re-enables a user; disabling blocks all keys.
func (s *Store) SetUserStatus(ctx context.Context, actor *int64, id int64, status string) (User, error) {
	var out User
	err := s.tx(ctx, actor, "user."+map[string]string{"active": "enable", "disabled": "disable"}[status],
		target("user", id), nil, func(tx *sql.Tx) (string, error) {
			var err error
			out, err = scanUser(tx.QueryRowContext(ctx,
				`UPDATE users SET status = $2, updated_at = now() WHERE id = $1 RETURNING `+userCols, id, status))
			return "", err
		})
	return out, err
}

type Team struct {
	ID              int64     `json:"id"`
	Name            string    `json:"name"`
	MonthlyTokenCap *int64    `json:"monthly_token_cap"`
	Priority        *int      `json:"priority"`
	CreatedAt       time.Time `json:"created_at"`
}

const teamCols = `id, name, monthly_token_cap, priority, created_at`

func scanTeam(r scanner) (Team, error) {
	var t Team
	err := r.Scan(&t.ID, &t.Name, &t.MonthlyTokenCap, &t.Priority, &t.CreatedAt)
	return t, err
}

func (s *Store) CreateTeam(ctx context.Context, actor *int64, t Team) (Team, error) {
	var out Team
	err := s.tx(ctx, actor, "team.create", "", map[string]any{"name": t.Name}, func(tx *sql.Tx) (string, error) {
		var err error
		out, err = scanTeam(tx.QueryRowContext(ctx,
			`INSERT INTO teams (name, monthly_token_cap, priority) VALUES ($1, $2, $3) RETURNING `+teamCols,
			t.Name, t.MonthlyTokenCap, t.Priority))
		return target("team", out.ID), err
	})
	return out, err
}

func (s *Store) ListTeams(ctx context.Context) ([]Team, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+teamCols+` FROM teams ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []Team{}
	for rows.Next() {
		t, err := scanTeam(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) UpdateTeam(ctx context.Context, actor *int64, id int64, set map[string]any) (Team, error) {
	sets, args, err := patch(set, TeamColumns, []any{id})
	if err != nil {
		return Team{}, err
	}
	var out Team
	err = s.tx(ctx, actor, "team.update", target("team", id), set, func(tx *sql.Tx) (string, error) {
		q := `SELECT ` + teamCols + ` FROM teams WHERE id = $1`
		if sets != "" {
			q = `UPDATE teams SET ` + sets + ` WHERE id = $1 RETURNING ` + teamCols
		}
		var err error
		out, err = scanTeam(tx.QueryRowContext(ctx, q, args...))
		return "", err
	})
	return out, err
}

// DeleteTeam refuses while the team still has users.
func (s *Store) DeleteTeam(ctx context.Context, actor *int64, id int64) error {
	return s.tx(ctx, actor, "team.delete", target("team", id), nil, func(tx *sql.Tx) (string, error) {
		var members int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM users WHERE team_id = $1`, id).Scan(&members); err != nil {
			return "", err
		}
		if members > 0 {
			return "", fmt.Errorf("%w: team has %d users", ErrConflict, members)
		}
		res, err := tx.ExecContext(ctx, `DELETE FROM teams WHERE id = $1`, id)
		if err != nil {
			return "", err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return "", ErrNotFound
		}
		return "", nil
	})
}

type Key struct {
	ID         int64      `json:"id"`
	UserID     int64      `json:"user_id"`
	KeyPrefix  string     `json:"key_prefix"`
	Label      string     `json:"label"`
	CreatedAt  time.Time  `json:"created_at"`
	ExpiresAt  *time.Time `json:"expires_at"`
	RevokedAt  *time.Time `json:"revoked_at"`
	LastUsedAt *time.Time `json:"last_used_at"`
}

const keyCols = `id, user_id, key_prefix, label, created_at, expires_at, revoked_at, last_used_at`

func scanKey(r scanner) (Key, error) {
	var k Key
	err := r.Scan(&k.ID, &k.UserID, &k.KeyPrefix, &k.Label, &k.CreatedAt, &k.ExpiresAt, &k.RevokedAt, &k.LastUsedAt)
	return k, err
}

// CreateKey mints a key for a user. The raw key is returned once and never
// stored; the audit row records only the prefix.
func (s *Store) CreateKey(ctx context.Context, actor *int64, userID int64, label string, expiresAt *time.Time) (Key, string, error) {
	raw, hash, prefix, err := auth.NewKey()
	if err != nil {
		return Key{}, "", err
	}
	var out Key
	err = s.tx(ctx, actor, "key.create", "", map[string]any{"user_id": userID, "key_prefix": prefix, "label": label},
		func(tx *sql.Tx) (string, error) {
			var err error
			out, err = scanKey(tx.QueryRowContext(ctx,
				`INSERT INTO api_keys (user_id, key_hash, key_prefix, label, expires_at)
				 VALUES ($1, $2, $3, $4, $5) RETURNING `+keyCols, userID, hash, prefix, label, expiresAt))
			return target("key", out.ID), err
		})
	if err != nil {
		return Key{}, "", err
	}
	return out, raw, nil
}

// InsertKeyHash stores a key whose raw value the caller already has (dev
// seed only). Idempotent on the hash.
func (s *Store) InsertKeyHash(ctx context.Context, userID int64, hash, prefix, label string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO api_keys (user_id, key_hash, key_prefix, label)
		VALUES ($1, $2, $3, $4) ON CONFLICT (key_hash) DO NOTHING`, userID, hash, prefix, label)
	return err
}

func (s *Store) ListKeys(ctx context.Context, userID int64) ([]Key, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+keyCols+` FROM api_keys WHERE user_id = $1 ORDER BY id`, userID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []Key{}
	for rows.Next() {
		k, err := scanKey(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// RevokeKey returns the key and its hash, so the caller can evict caches.
func (s *Store) RevokeKey(ctx context.Context, actor *int64, id int64) (Key, string, error) {
	var out Key
	var hash string
	err := s.tx(ctx, actor, "key.revoke", target("key", id), nil, func(tx *sql.Tx) (string, error) {
		row := tx.QueryRowContext(ctx, `UPDATE api_keys SET revoked_at = COALESCE(revoked_at, now())
			WHERE id = $1 RETURNING `+keyCols+`, key_hash`, id)
		err := row.Scan(&out.ID, &out.UserID, &out.KeyPrefix, &out.Label, &out.CreatedAt,
			&out.ExpiresAt, &out.RevokedAt, &out.LastUsedAt, &hash)
		return "", err
	})
	return out, hash, err
}

type Grant struct {
	ID     int64  `json:"id"`
	Model  string `json:"model"`
	UserID *int64 `json:"user_id"`
	TeamID *int64 `json:"team_id"`
}

func (s *Store) CreateGrant(ctx context.Context, actor *int64, g Grant) (Grant, error) {
	out := g
	err := s.tx(ctx, actor, "grant.create", "", g, func(tx *sql.Tx) (string, error) {
		err := tx.QueryRowContext(ctx, `INSERT INTO model_grants (model, user_id, team_id) VALUES ($1, $2, $3) RETURNING id`,
			g.Model, g.UserID, g.TeamID).Scan(&out.ID)
		return target("grant", out.ID), err
	})
	return out, err
}

func (s *Store) ListGrants(ctx context.Context) ([]Grant, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, model, user_id, team_id FROM model_grants ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []Grant{}
	for rows.Next() {
		var g Grant
		if err := rows.Scan(&g.ID, &g.Model, &g.UserID, &g.TeamID); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

func (s *Store) DeleteGrant(ctx context.Context, actor *int64, id int64) error {
	return s.tx(ctx, actor, "grant.delete", target("grant", id), nil, func(tx *sql.Tx) (string, error) {
		res, err := tx.ExecContext(ctx, `DELETE FROM model_grants WHERE id = $1`, id)
		if err != nil {
			return "", err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return "", ErrNotFound
		}
		return "", nil
	})
}

type UsageRow struct {
	Key              string `json:"key"`
	Requests         int64  `json:"requests"`
	PromptTokens     int64  `json:"prompt_tokens"`
	CompletionTokens int64  `json:"completion_tokens"`
	TotalTokens      int64  `json:"total_tokens"`
}

// UsageBy aggregates usage between from and to, grouped by user email,
// team name, or model.
func (s *Store) UsageBy(ctx context.Context, groupBy string, from, to time.Time) ([]UsageRow, error) {
	key := map[string]string{
		"user":  "u.email",
		"team":  "COALESCE(t.name, '(no team)')",
		"model": "e.model",
	}[groupBy]
	if key == "" {
		return nil, fmt.Errorf("%w: group_by must be user, team or model", ErrInvalid)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+key+`, COUNT(*), SUM(e.prompt_tokens), SUM(e.completion_tokens)
		FROM usage_events e
		JOIN users u ON u.id = e.user_id
		LEFT JOIN teams t ON t.id = e.team_id
		WHERE e.created_at >= $1 AND e.created_at < $2
		GROUP BY 1 ORDER BY 1`, from, to)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []UsageRow{}
	for rows.Next() {
		var r UsageRow
		if err := rows.Scan(&r.Key, &r.Requests, &r.PromptTokens, &r.CompletionTokens); err != nil {
			return nil, err
		}
		r.TotalTokens = r.PromptTokens + r.CompletionTokens
		out = append(out, r)
	}
	return out, rows.Err()
}

type AuditEntry struct {
	ID          int64           `json:"id"`
	ActorUserID *int64          `json:"actor_user_id"`
	Action      string          `json:"action"`
	Target      string          `json:"target"`
	Detail      json.RawMessage `json:"detail"`
	At          time.Time       `json:"at"`
}

func (s *Store) ListAudit(ctx context.Context, limit, offset int) ([]AuditEntry, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, actor_user_id, action, target, detail::text, at
		FROM admin_audit ORDER BY id DESC LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []AuditEntry{}
	for rows.Next() {
		var a AuditEntry
		var detail string
		if err := rows.Scan(&a.ID, &a.ActorUserID, &a.Action, &a.Target, &detail, &a.At); err != nil {
			return nil, err
		}
		a.Detail = json.RawMessage(detail)
		out = append(out, a)
	}
	return out, rows.Err()
}

// HasAdmin reports whether any admin exists (bootstrap runs only once).
func (s *Store) HasAdmin(ctx context.Context) (bool, error) {
	var ok bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM users WHERE role = 'admin')`).Scan(&ok)
	return ok, err
}

// ImportRow is one line of a user CSV import.
type ImportRow struct{ Email, DisplayName, Team string }

// ImportUsers upserts users by case-insensitive email, creating missing
// teams, in one transaction with one audit row (actor NULL: the CLI).
// New users get the given limits; existing users keep theirs.
func (s *Store) ImportUsers(ctx context.Context, rows []ImportRow, burst int, refill float64,
	tpm *int, daily, monthly *int64) (created, updated int, err error) {
	err = s.tx(ctx, nil, "users.import", "users", map[string]any{"rows": len(rows)}, func(tx *sql.Tx) (string, error) {
		teams := map[string]int64{}
		for _, r := range rows {
			var teamID *int64
			if r.Team != "" {
				id, ok := teams[r.Team]
				if !ok {
					if err := tx.QueryRowContext(ctx, `INSERT INTO teams (name) VALUES ($1)
						ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id`, r.Team).Scan(&id); err != nil {
						return "", err
					}
					teams[r.Team] = id
				}
				teamID = &id
			}
			var inserted bool
			if err := tx.QueryRowContext(ctx, `INSERT INTO users
				(email, display_name, team_id, rpm_burst, rpm_refill_per_sec,
				 tokens_per_minute, daily_token_budget, monthly_token_budget)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
				ON CONFLICT ((lower(email))) DO UPDATE
				  SET display_name = EXCLUDED.display_name, team_id = EXCLUDED.team_id, updated_at = now()
				RETURNING (xmax = 0)`,
				r.Email, r.DisplayName, teamID, burst, refill, tpm, daily, monthly).Scan(&inserted); err != nil {
				return "", err
			}
			if inserted {
				created++
			} else {
				updated++
			}
		}
		return "", nil
	})
	return created, updated, err
}

// EnsureUser creates or refreshes a user with fixed request limits and
// returns its id (dev seed only).
func (s *Store) EnsureUser(ctx context.Context, email string, burst int, refill float64) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx, `INSERT INTO users (email, rpm_burst, rpm_refill_per_sec) VALUES ($1, $2, $3)
		ON CONFLICT ((lower(email))) DO UPDATE SET rpm_burst = EXCLUDED.rpm_burst,
		  rpm_refill_per_sec = EXCLUDED.rpm_refill_per_sec, updated_at = now()
		RETURNING id`, email, burst, refill).Scan(&id)
	return id, err
}
