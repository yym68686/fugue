package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestDatabaseConcurrencyWaitsInsteadOfOpeningMoreConnections(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{db: db}
	s.ConfigureDatabaseConcurrency(2)
	s.ConfigureDatabasePool(2, time.Minute)
	first, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := db.Conn(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("saturated pool should wait with caller deadline: %v", err)
	}
	if stats := db.Stats(); stats.OpenConnections != 2 || stats.WaitCount != 1 {
		t.Fatalf("connection limit not enforced: %+v", stats)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	third, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	third.Close()
}

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
