package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"time"

	"fugue/internal/productioncontrol"
	"fugue/internal/storageaudit"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func draftStorageAudit(ctx context.Context, journalPath, request, shadowPath string, out io.Writer) error {
	raw, err := readFile(shadowPath, 16<<10, false)
	if err != nil {
		return err
	}
	shadow, err := productioncontrol.DecodeStorageAuditShadow(raw)
	if err != nil {
		return err
	}
	return withJournal(ctx, journalPath, true, func(ctx context.Context, j *productioncontrol.Journal, cluster string) error {
		state, err := j.ReadResource(ctx, productioncontrol.Scope{ClusterID: cluster, ResourceID: productioncontrol.StorageAuditPolicyResource})
		if err != nil {
			return err
		}
		if state.Pending != "" {
			return productioncontrol.ErrResourceBusy
		}
		submission, err := productioncontrol.DraftStorageAuditPolicy(cluster, request, state.LastVerified, shadow)
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(submission)
	})
}

func observeStorageAudit(ctx context.Context, path string, out io.Writer) error {
	provider, db, err := openStorageAuditObserver(path)
	if err != nil {
		return err
	}
	defer db.Close()
	observed, err := provider.Observe(ctx, db)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(observed)
}

func openStorageAuditObserver(path string) (storageaudit.MaintenanceObserver, *sql.DB, error) {
	raw, err := readFile(path, 16<<10, true)
	if err != nil {
		return storageaudit.MaintenanceObserver{}, nil, err
	}
	defer clear(raw)
	var settings struct {
		Provider storageaudit.MaintenanceObserver `json:"provider"`
		Database string                           `json:"databaseUrl"`
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&settings) != nil || d.Decode(new(any)) != io.EOF || settings.Database == "" {
		return storageaudit.MaintenanceObserver{}, nil, errors.New("invalid private storage audit observer configuration")
	}
	cfg, err := pgx.ParseConfig(settings.Database)
	if err != nil || cfg.Host == "" || cfg.TLSConfig == nil || cfg.TLSConfig.InsecureSkipVerify ||
		cfg.TLSConfig.ServerName != cfg.Host || len(cfg.Fallbacks) != 0 ||
		cfg.Database != settings.Provider.Database || cfg.User != settings.Provider.Role {
		return storageaudit.MaintenanceObserver{}, nil, errors.New("storage audit observer requires the exact role and database on one hostname-verified PostgreSQL endpoint")
	}
	cfg.ConnectTimeout = 5 * time.Second
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	cfg.RuntimeParams["default_transaction_read_only"] = "on"
	db := stdlib.OpenDB(*cfg)
	db.SetMaxOpenConns(1)
	return settings.Provider, db, nil
}
