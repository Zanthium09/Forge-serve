package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
	"github.com/Zanthium09/forge-serve/gateway/internal/config"
	"github.com/Zanthium09/forge-serve/gateway/internal/store/storetest"
)

var defaults = config.UserDefaults{RPMBurst: 60, RPMRefillPerSec: 10}

func TestBootstrapOnce(t *testing.T) {
	st := storetest.Open(t)
	var out bytes.Buffer
	if err := runBootstrap(context.Background(), st, "root@x.com", defaults, &out); err != nil {
		t.Fatal(err)
	}
	raw := strings.TrimSpace(out.String()[strings.Index(out.String(), "sk-forge-"):])
	raw = strings.Fields(raw)[0]
	rec, err := st.LookupKey(context.Background(), auth.HashKey(raw))
	if err != nil || rec.Role != "admin" {
		t.Fatalf("bootstrap key does not authenticate as admin: %+v %v", rec, err)
	}
	err = runBootstrap(context.Background(), st, "other@x.com", defaults, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "admin already exists") {
		t.Fatalf("second bootstrap err = %v", err)
	}
}

func TestImportUpserts(t *testing.T) {
	st := storetest.Open(t)
	csv1 := "email,display_name,team\na@x.com,Ann,ml\nb@x.com,Bob,ml\n"
	var out bytes.Buffer
	if err := runImport(context.Background(), st, defaults, strings.NewReader(csv1), &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "created 2, updated 0") {
		t.Fatalf("first import: %q", out.String())
	}
	out.Reset()
	csv2 := "email,display_name,team\nA@X.com,Ann B,ml\nb@x.com,Bobby,ml\n"
	if err := runImport(context.Background(), st, defaults, strings.NewReader(csv2), &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "created 0, updated 2") {
		t.Fatalf("second import: %q", out.String())
	}
	var teams, users int
	var name string
	_ = st.DB().QueryRow(`SELECT count(*) FROM teams WHERE name = 'ml'`).Scan(&teams)
	_ = st.DB().QueryRow(`SELECT count(*) FROM users`).Scan(&users)
	_ = st.DB().QueryRow(`SELECT display_name FROM users WHERE lower(email) = 'a@x.com'`).Scan(&name)
	if teams != 1 || users != 2 || name != "Ann B" {
		t.Fatalf("teams = %d users = %d name = %q", teams, users, name)
	}
}

func TestImportRejectsBadHeader(t *testing.T) {
	st := storetest.Open(t)
	err := runImport(context.Background(), st, defaults, strings.NewReader("mail,name\na@x.com,A\n"), &bytes.Buffer{})
	if err == nil {
		t.Fatal("bad header must be rejected")
	}
}

func TestDevSeedIdempotent(t *testing.T) {
	st := storetest.Open(t)
	for i := 0; i < 2; i++ {
		if err := devSeed(context.Background(), st); err != nil {
			t.Fatal(err)
		}
	}
	var users int
	_ = st.DB().QueryRow(`SELECT count(*) FROM users`).Scan(&users)
	if users != 2 {
		t.Fatalf("users = %d, want 2", users)
	}
	rec, err := st.LookupKey(context.Background(), auth.HashKey("sk-forge-demo-key"))
	if err != nil || rec.RPMBurst != 5 || rec.RPMRefillPerSec != 1 {
		t.Fatalf("demo key = %+v %v", rec, err)
	}
	if _, err := st.LookupKey(context.Background(), auth.HashKey("sk-forge-dev-key")); err != nil {
		t.Fatal(err)
	}
}
