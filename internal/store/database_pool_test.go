package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestDatabasePoolReusesConcurrentReadConnections(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{db: db}
	s.ConfigureDatabasePool(6, time.Minute)
	for round := 0; round < 2; round++ {
		var connections []*sql.Conn
		for i := 0; i < 6; i++ {
			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			connections = append(connections, conn)
		}
		for _, conn := range connections {
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if stats := db.Stats(); stats.Idle != 6 || stats.MaxIdleClosed != 0 || stats.MaxOpenConnections != 0 {
			t.Fatalf("read burst closed reusable connections or changed concurrency: %+v", stats)
		}
	}
	// Configuration recovery is independent of the binary and can restore the
	// earlier idle-pool size immediately.
	s.ConfigureDatabasePool(2, time.Minute)
	if stats := db.Stats(); stats.Idle != 2 || stats.MaxIdleClosed != 4 {
		t.Fatalf("pool configuration did not reduce retained connections: %+v", stats)
	}
}
