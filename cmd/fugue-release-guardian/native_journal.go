package main

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
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
	if !filepath.IsAbs(path) {
		return nil, nil, "", errors.New("native Guardian requires a mounted production journal configuration file")
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > 32<<10 {
		return nil, nil, "", errors.New("native Guardian journal configuration file is unavailable or invalid")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, "", errors.New("native Guardian could not open its journal configuration")
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, (32<<10)+1))
	var settings struct {
		Cluster  string            `json:"cluster"`
		Database string            `json:"database"`
		Owners   map[string][]byte `json:"ownerPublicKeys"`
	}
	if err != nil || len(raw) > 32<<10 || decodeStrictJSON(raw, &settings) != nil ||
		settings.Database == "" || settings.Cluster == "" || len(settings.Owners) == 0 {
		return nil, nil, "", errors.New("native Guardian journal settings require an independent database and owner public keys")
	}
	cfg, err := pgx.ParseConfig(settings.Database)
	if err != nil {
		return nil, nil, "", errors.New("native Guardian journal connection settings are invalid")
	}
	// Require authenticated TLS without permissive pgx fallback entries.
	if cfg.Host == "" || cfg.TLSConfig == nil || cfg.TLSConfig.InsecureSkipVerify || cfg.TLSConfig.ServerName != cfg.Host || len(cfg.Fallbacks) != 0 {
		return nil, nil, "", errors.New("native Guardian journal requires one hostname-verified PostgreSQL TLS endpoint")
	}
	cfg.ConnectTimeout = 5 * time.Second
	trusted := make(productioncontrol.Approvers, len(settings.Owners))
	for id, key := range settings.Owners {
		trusted[id] = productioncontrol.TrustedApprover{ClusterID: settings.Cluster, PublicKey: key}
	}
	return cfg, trusted, settings.Cluster, nil
}
