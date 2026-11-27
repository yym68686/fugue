package main

import (
	"context"
	"errors"
	"os"
	"time"

	"fugue/internal/productioncontrol"
	"fugue/internal/releaseguardian"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

const nativeJournalFileEnv = "FUGUE_RELEASE_NATIVE_JOURNAL_FILE"

// The native Guardian's database principal only needs to read approvals,
// releases and their fences and append execution_launches. It cannot publish
// candidates or settle executions.
func openNativeLaunchGate(ctx context.Context, store *releaseguardian.KubeStore) (releaseguardian.NativeRolloutGate, func(), error) {
	cfg, trusted, cluster, err := loadNativeJournalConfig(os.Getenv(nativeJournalFileEnv))
	if err != nil {
		return nil, nil, err
	}
	db := stdlib.OpenDB(*cfg)
	closeDB := func() { _ = db.Close() }
	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(2)
	journal, err := productioncontrol.NewJournal(db, trusted)
	if err != nil {
		closeDB()
		return nil, nil, err
	}
	gate, err := releaseguardian.NewNativeLaunchGate(store, journal, cluster)
	if err != nil {
		closeDB()
		return nil, nil, err
	}
	ping, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	if err := db.PingContext(ping); err != nil {
		closeDB()
		return nil, nil, errors.New("native Guardian journal endpoint is unavailable")
	}
	return gate, closeDB, nil
}

func loadNativeJournalConfig(path string) (*pgx.ConnConfig, productioncontrol.Approvers, string, error) {
	return productioncontrol.LoadJournalEndpoint(path)
}
