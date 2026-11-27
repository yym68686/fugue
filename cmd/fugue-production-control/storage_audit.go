package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"fugue/internal/storageaudit"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func observeStorageAudit(ctx context.Context, path string, out io.Writer) error {
	raw, err := readFile(path, 16<<10, true)
	if err != nil {
		return err
	}
	defer clear(raw)
	var settings struct {
		Provider storageaudit.MaintenanceObserver `json:"provider"`
		Database string                           `json:"databaseUrl"`
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&settings) != nil || d.Decode(new(any)) != io.EOF || settings.Database == "" {
		return errors.New("invalid private storage audit observer configuration")
	}
	cfg, err := pgx.ParseConfig(settings.Database)
	if err != nil || cfg.Host == "" || cfg.TLSConfig == nil || cfg.TLSConfig.InsecureSkipVerify ||
		cfg.TLSConfig.ServerName != cfg.Host || len(cfg.Fallbacks) != 0 ||
		cfg.Database != settings.Provider.Database || cfg.User != settings.Provider.Role {
		return errors.New("storage audit observer requires the exact role and database on one hostname-verified PostgreSQL endpoint")
	}
	cfg.ConnectTimeout = 5 * time.Second
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	cfg.RuntimeParams["default_transaction_read_only"] = "on"
	db := stdlib.OpenDB(*cfg)
	defer db.Close()
	db.SetMaxOpenConns(1)
	observed, err := settings.Provider.Observe(ctx, db)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(observed)
}
