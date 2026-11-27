// Package testpostgres creates isolated databases for PostgreSQL-backed tests.
package testpostgres

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const TestURLEnv = "FUGUE_STORE_TEST_POSTGRES"

var fixtures struct {
	sync.Mutex
	sequence int
	byPath   map[string]string
}

// Database returns a dedicated test database. Reopening a named fixture within
// a test returns the same database; no test data is shared across tests.
// The supplied server must be a disposable loopback PostgreSQL instance.
func Database(t testing.TB, fixture string) string {
	t.Helper()
	address := strings.TrimSpace(os.Getenv(TestURLEnv))
	if address == "" {
		t.Skipf("set %s to a disposable loopback PostgreSQL instance (see make test-postgres)", TestURLEnv)
	}
	u, err := url.Parse(address)
	if err != nil || u.Scheme != "postgres" && u.Scheme != "postgresql" || u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" || !strings.HasPrefix(u.Path, "/fugue_test") || u.RawQuery != "sslmode=disable" {
		t.Fatal("test PostgreSQL URL must use loopback, a fugue_test database and sslmode=disable")
	}
	template := strings.TrimPrefix(u.Path, "/")
	adminURL := *u
	adminURL.Path = "/postgres"
	admin, err := sql.Open("pgx", adminURL.String())
	if err != nil {
		t.Fatal("open test postgres: ", err)
	}
	defer admin.Close()
	fixtures.Lock()
	defer fixtures.Unlock()
	if fixtures.byPath == nil {
		fixtures.byPath = make(map[string]string)
	}
	key := t.Name() + "\x00" + fixture
	if fixture != "" {
		if dsn := fixtures.byPath[key]; dsn != "" {
			return dsn
		}
	}
	fixtures.sequence++
	name := fmt.Sprintf("fugue_test_%d_%d", os.Getpid(), fixtures.sequence)
	quoted := pgx.Identifier{name}.Sanitize()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+quoted+" TEMPLATE "+pgx.Identifier{template}.Sanitize()); err != nil {
		t.Fatal("clone PostgreSQL test template: ", err)
	}
	t.Cleanup(func() {
		fixtures.Lock()
		delete(fixtures.byPath, key)
		fixtures.Unlock()
		db, err := sql.Open("pgx", adminURL.String())
		if err != nil {
			t.Error("open test postgres for cleanup: ", err)
			return
		}
		defer db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if _, err = db.ExecContext(ctx, "DROP DATABASE "+quoted+" WITH (FORCE)"); err != nil {
			t.Error("drop test database: ", err)
		}
	})
	u.Path = "/" + name
	dsn := u.String()
	if fixture != "" {
		fixtures.byPath[key] = dsn
	}
	return dsn
}
