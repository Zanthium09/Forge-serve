package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
	"github.com/Zanthium09/forge-serve/gateway/internal/config"
	"github.com/Zanthium09/forge-serve/gateway/internal/store"
)

const usage = `usage:
  gateway                                serve (default)
  gateway admin bootstrap --email <e>    create the first admin and print its key once
  gateway users import <file.csv>        upsert users from email,display_name,team`

// runCLI handles subcommands; it returns the process exit code.
func runCLI(args []string, out, errOut io.Writer) int {
	cmd := strings.Join(args[:min(2, len(args))], " ")
	if cmd != "admin bootstrap" && cmd != "users import" {
		_, _ = fmt.Fprintln(errOut, usage)
		return 2
	}
	cfg, err := loadConfig()
	if err != nil {
		_, _ = fmt.Fprintln(errOut, "config invalid:", err)
		return 1
	}
	st, err := openStore()
	if err != nil {
		_, _ = fmt.Fprintln(errOut, err)
		return 1
	}
	defer func() { _ = st.Close() }()
	ctx := context.Background()

	switch cmd {
	case "admin bootstrap":
		fs := flag.NewFlagSet("bootstrap", flag.ContinueOnError)
		email := fs.String("email", "", "admin email")
		if err := fs.Parse(args[2:]); err != nil || *email == "" {
			_, _ = fmt.Fprintln(errOut, usage)
			return 2
		}
		err = runBootstrap(ctx, st, *email, cfg.Defaults, out)
	case "users import":
		if len(args) != 3 {
			_, _ = fmt.Fprintln(errOut, usage)
			return 2
		}
		f, ferr := os.Open(args[2])
		if ferr != nil {
			_, _ = fmt.Fprintln(errOut, ferr)
			return 1
		}
		defer func() { _ = f.Close() }()
		err = runImport(ctx, st, cfg.Defaults, f, out)
	}
	if err != nil {
		_, _ = fmt.Fprintln(errOut, "error:", err)
		return 1
	}
	return 0
}

// openStore connects and migrates, like the server does at boot.
func openStore() (*store.Store, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return nil, errors.New("DATABASE_URL is required")
	}
	st, err := store.New(dsn)
	if err != nil {
		return nil, err
	}
	if err := st.Migrate(context.Background()); err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return st, nil
}

// optional turns a config default (0 = unlimited) into a nullable column.
func optional[T int | int64](v T) *T {
	if v <= 0 {
		return nil
	}
	return &v
}

// runBootstrap creates the first admin. Production installs ship no seeded
// keys, so this is the only way in (spec §7.6); it refuses to run twice.
func runBootstrap(ctx context.Context, st *store.Store, email string, d config.UserDefaults, out io.Writer) error {
	has, err := st.HasAdmin(ctx)
	if err != nil {
		return err
	}
	if has {
		return errors.New("admin already exists; create further admins through the admin API")
	}
	u, err := st.CreateUser(ctx, nil, store.User{Email: email, Role: "admin",
		RPMBurst: d.RPMBurst, RPMRefillPerSec: d.RPMRefillPerSec,
		TokensPerMinute: optional(d.TokensPerMinute), DailyTokenBudget: optional(d.DailyTokenBudget),
		MonthlyTokenBudget: optional(d.MonthlyTokenBudget)})
	if err != nil {
		return err
	}
	_, raw, err := st.CreateKey(ctx, nil, u.ID, "bootstrap", nil)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "admin %s created. API key (shown once, store it now):\n%s\n", email, raw)
	return nil
}

// runImport upserts users from a CSV with header email,display_name,team.
func runImport(ctx context.Context, st *store.Store, d config.UserDefaults, r io.Reader, out io.Writer) error {
	cr := csv.NewReader(r)
	cr.TrimLeadingSpace = true
	records, err := cr.ReadAll()
	if err != nil {
		return fmt.Errorf("read csv: %w", err)
	}
	if len(records) == 0 || strings.Join(records[0], ",") != "email,display_name,team" {
		return errors.New(`csv header must be exactly "email,display_name,team"`)
	}
	var rows []store.ImportRow
	for i, rec := range records[1:] {
		email := strings.TrimSpace(rec[0])
		if !strings.Contains(email, "@") {
			return fmt.Errorf("line %d: invalid email %q", i+2, email)
		}
		rows = append(rows, store.ImportRow{Email: email, DisplayName: strings.TrimSpace(rec[1]), Team: strings.TrimSpace(rec[2])})
	}
	created, updated, err := st.ImportUsers(ctx, rows, d.RPMBurst, d.RPMRefillPerSec,
		optional(d.TokensPerMinute), optional(d.DailyTokenBudget), optional(d.MonthlyTokenBudget))
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "created %d, updated %d\n", created, updated)
	return nil
}

// devSeed recreates the two well-known development keys (FORGE_DEV_SEED=1
// only, never in production): sk-forge-dev-key with a generous quota and
// sk-forge-demo-key with a deliberately tiny one, so rate limiting is easy
// to demonstrate. Idempotent.
func devSeed(ctx context.Context, st *store.Store) error {
	for _, s := range []struct {
		email, key string
		burst      int
		refill     float64
	}{
		{"dev@forge.local", "sk-forge-dev-key", 60, 10},
		{"demo@forge.local", "sk-forge-demo-key", 5, 1},
	} {
		id, err := st.EnsureUser(ctx, s.email, s.burst, s.refill)
		if err != nil {
			return err
		}
		if err := st.InsertKeyHash(ctx, id, auth.HashKey(s.key), s.key[len("sk-forge-"):], "dev seed"); err != nil {
			return err
		}
	}
	return nil
}
