package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"fugue/internal/productioncontrol"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func observeSampleRetention(ctx context.Context, path string, out io.Writer) error {
	raw, err := readFile(path, 16<<10, true)
	if err != nil {
		return err
	}
	defer clear(raw)
	var settings struct {
		Provider productioncontrol.SampleRetentionProvider `json:"provider"`
		Database string                                    `json:"databaseUrl"`
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&settings) != nil || d.Decode(new(any)) != io.EOF || settings.Database == "" {
		return errors.New("invalid private sample retention observer configuration")
	}
	cfg, err := pgx.ParseConfig(settings.Database)
	if err != nil || cfg.Host == "" || cfg.TLSConfig == nil || cfg.TLSConfig.InsecureSkipVerify ||
		cfg.TLSConfig.ServerName != cfg.Host || len(cfg.Fallbacks) != 0 ||
		cfg.Database != settings.Provider.Database || cfg.User != settings.Provider.Role {
		return errors.New("sample retention observer requires the exact database and role on one hostname-verified PostgreSQL endpoint")
	}
	cfg.ConnectTimeout = 5 * time.Second
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = make(map[string]string)
	}
	cfg.RuntimeParams["default_transaction_read_only"] = "on"
	db := stdlib.OpenDB(*cfg)
	defer db.Close()
	db.SetMaxOpenConns(1)
	observation, err := settings.Provider.Observe(ctx, db)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(observation)
}
