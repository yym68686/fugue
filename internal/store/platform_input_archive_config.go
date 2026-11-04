package store

import (
	"context"
	"encoding/json"
	"fmt"

	"fugue/internal/inputarchive"
	"fugue/internal/model"
)

// resolveCompilerInputArchiveConfig supports both explicitly configured legacy
// environments and the encrypted, platform-owned R2 configuration. Callers with
// an open reader transaction must pass that transaction, not the pool.
func resolveCompilerInputArchiveConfig(ctx context.Context, db inputQueryer, writer bool) (inputarchive.S3Config, bool, error) {
	cfg, configured, err := inputarchive.ConfigFromEnv()
	if err != nil || configured || db == nil {
		return cfg, configured, err
	}
	var raw []byte
	err = db.QueryRowContext(ctx, `SELECT body FROM fugue_object_storage_config WHERE id='platform'`).Scan(&raw)
	if absentInputManifest(err) {
		return cfg, false, nil
	}
	if err != nil {
		return cfg, false, err
	}
	var platform model.ObjectStorageConfig
	if err = json.Unmarshal(raw, &platform); err != nil {
		return cfg, false, err
	}
	v := platform.CompilerInputArchive
	if v == nil || v.State != "active" {
		return cfg, false, nil
	}
	if v.AccountID != platform.AccountID || !v.ReaderConfigured || !v.WriterConfigured || v.ProtectionVerifiedAt == nil || !compilerInputRetirementPlanHash.MatchString(v.RetentionPolicySHA256) {
		return cfg, false, fmt.Errorf("managed compiler input archive configuration incomplete: %w", ErrConflict)
	}
	sealed := v.Reader
	if writer {
		sealed = v.Writer
	}
	secret, err := decryptDataBackendSecret(sealed)
	if err != nil {
		return cfg, false, fmt.Errorf("managed compiler input archive credential unavailable: %w", ErrConflict)
	}
	cfg = inputarchive.S3Config{AccountID: v.AccountID, Endpoint: "https://" + v.AccountID + ".r2.cloudflarestorage.com", Region: "auto", Bucket: v.Bucket, Prefix: v.Prefix, AccessKeyID: secret.AccessKeyID, SecretAccessKey: secret.SecretAccessKey}
	if err = cfg.Validate(); err != nil {
		return cfg, false, err
	}
	return cfg, true, nil
}
