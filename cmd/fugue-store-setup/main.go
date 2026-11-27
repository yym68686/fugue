// fugue-store-setup is a one-shot initializer for local development databases.
package main

import (
	"context"
	"log"
	"os"
	"time"

	"fugue/internal/schemamigrate"
	"fugue/internal/store"
)

func main() {
	dsn := os.Getenv("FUGUE_DATABASE_URL")
	s := store.New(dsn)
	if err := s.BootstrapSchema(); err != nil {
		log.Fatal(err)
	}
	if err := s.Close(); err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := schemamigrate.MigrateAll(ctx, dsn); err != nil {
		log.Fatal(err)
	}
}
