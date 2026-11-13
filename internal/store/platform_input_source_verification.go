package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"strings"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
	"fugue/internal/schemamigrate"
)

const CompilerInputSourceVerificationSchema = "fugue.compiler-input-source-verification/v1"

// This selects one already retained parent. It grants no publication, restore,
// pin, classification or retirement authority, and accepts no caller keys.
type CompilerInputSourceVerificationContract struct {
	Schema              string `json:"schema"`
	ArtifactID          string `json:"artifact_id"`
	ExpectedContentHash string `json:"expected_content_hash"`
}

func (c CompilerInputSourceVerificationContract) Validate() error {
	if c.Schema != CompilerInputSourceVerificationSchema || c.ArtifactID == "" || len(c.ArtifactID) > 512 || strings.TrimSpace(c.ArtifactID) != c.ArtifactID || !inputDigestPattern.MatchString(c.ExpectedContentHash) {
		return ErrInvalidInput
	}
	return nil
}

type CompilerInputVerifiedSource struct {
	ArtifactID   string `json:"artifact_id"`
	ArtifactKind string `json:"artifact_kind"`
	ScopeKey     string `json:"scope_key"`
	Generation   string `json:"generation"`
	ContentHash  string `json:"content_hash"`
	TypedDigest  string `json:"typed_digest"`
	SigningKeyID string `json:"signing_key_id"`
}

type CompilerInputSourceVerificationReport struct {
	Action                            string                        `json:"action"`
	Schema                            string                        `json:"schema"`
	ReadOnly                          bool                          `json:"read_only"`
	Verified                          bool                          `json:"verified"`
	ArtifactID                        string                        `json:"artifact_id"`
	ContentHash                       string                        `json:"content_hash"`
	Snapshot                          string                        `json:"database_snapshot"`
	ObservedAt                        time.Time                     `json:"observed_at"`
	Sources                           []CompilerInputVerifiedSource `json:"sources,omitempty"`
	OriginalExternalAuthorityVerified bool                          `json:"original_external_authority_verified"`
	ClosureComplete                   bool                          `json:"closure_complete"`
	RetirementAuthorized              bool                          `json:"retirement_authorized"`
}

// RunCompilerInputSourceVerification authenticates the parent and both exact
// compiler sources using the API role's current keyring/revocations. It reads
// exactly three bounded artifacts in one read-only RR snapshot, never input
// chunks, an archive, business state, keys from the caller, or a later source.
func RunCompilerInputSourceVerification(ctx context.Context, databaseURL string, keys bundleauth.Keyring, c CompilerInputSourceVerificationContract) (CompilerInputSourceVerificationReport, error) {
	r := CompilerInputSourceVerificationReport{Action: "compiler-source-verify", Schema: CompilerInputSourceVerificationSchema, ReadOnly: true, ArtifactID: c.ArtifactID, ContentHash: c.ExpectedContentHash}
	if err := c.Validate(); err != nil {
		return r, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return r, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return runCompilerInputSourceVerification(ctx, db, keys, c)
}

func runCompilerInputSourceVerification(ctx context.Context, db *sql.DB, keys bundleauth.Keyring, c CompilerInputSourceVerificationContract) (CompilerInputSourceVerificationReport, error) {
	r := CompilerInputSourceVerificationReport{Action: "compiler-source-verify", Schema: CompilerInputSourceVerificationSchema, ReadOnly: true, ArtifactID: c.ArtifactID, ContentHash: c.ExpectedContentHash}
	if err := c.Validate(); err != nil {
		return r, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return r, err
	}
	defer tx.Rollback()
	for _, statement := range []string{`SET LOCAL statement_timeout='5s'`, `SET LOCAL lock_timeout='250ms'`, `SET LOCAL work_mem='1MB'`, `SET LOCAL max_parallel_workers_per_gather=0`, `SET LOCAL jit=off`} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return r, err
		}
	}
	if err = schemamigrate.EnforceCompilerInputZeroTemp(ctx, tx); err != nil {
		return r, err
	}
	var coherent bool
	if err = tx.QueryRowContext(ctx, `SELECT transaction_timestamp(),pg_current_snapshot()::text,current_setting('transaction_isolation')='repeatable read' AND current_setting('transaction_read_only')='on'`).Scan(&r.ObservedAt, &r.Snapshot, &coherent); err != nil {
		return r, err
	}
	if !coherent {
		return r, ErrConflict
	}
	parent, err := readCompilerSourceArtifact(ctx, tx, "id=$1", c.ArtifactID)
	if err != nil {
		return r, err
	}
	if parent.ID != c.ArtifactID || parent.ContentHash != c.ExpectedContentHash {
		return r, ErrConflict
	}
	sources, err := verifyCompilerOriginalSources(parent, keys, func(refKind, scope, generation string) (model.PlatformArtifact, error) {
		return readCompilerSourceArtifact(ctx, tx, "artifact_kind=$1 AND scope_key=$2 AND generation=$3", refKind, scope, generation)
	})
	if err != nil {
		return r, err
	}
	if err = tx.Commit(); err != nil {
		return r, err
	}
	r.Sources, r.Verified = sources, true
	return r, nil
}

// All variable-length artifact fields together are capped at 8MiB before
// transfer/decoding. Three rows cannot exceed the existing 32MiB batch cap.
// Predicates are internal constants; all identities remain query parameters.
func readCompilerSourceArtifact(ctx context.Context, db platformStateDB, predicate string, args ...any) (model.PlatformArtifact, error) {
	if predicate != "id=$1" && predicate != "artifact_kind=$1 AND scope_key=$2 AND generation=$3" {
		return model.PlatformArtifact{}, ErrInvalidInput
	}
	return scanPlatformArtifact(db.QueryRowContext(ctx, `SELECT id,artifact_kind,scope_key,scope_json,schema_version,generation,generation_sequence,status,content_hash,
fugue_platform_artifact_body(content_hash,content_json),validation_results_json,compatibility_floor,metadata_json,created_by_type,created_by_id,provenance_json,created_at,updated_at
FROM fugue_platform_artifacts WHERE `+predicate+` AND
octet_length(scope_json::text)+octet_length(fugue_platform_artifact_body(content_hash,content_json)::text)+octet_length(validation_results_json::text)+octet_length(metadata_json::text)+octet_length(provenance_json::text)+octet_length(id)+octet_length(artifact_kind)+octet_length(scope_key)+octet_length(generation)+octet_length(compatibility_floor)+octet_length(created_by_type)+octet_length(created_by_id)+octet_length(schema_version)+octet_length(status)+octet_length(content_hash)<=8388608`, args...))
}

func verifyCompilerOriginalSources(parent model.PlatformArtifact, keys bundleauth.Keyring, read func(string, string, string) (model.PlatformArtifact, error)) ([]CompilerInputVerifiedSource, error) {
	if parent.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(parent, keys).Pass {
		return nil, ErrConflict
	}
	refs, declared, err := compilerInputRetainedLineageSources(parent)
	if err != nil || !declared || len(refs) != 2 {
		return nil, ErrConflict
	}
	lineage := platformconfig.LineageFromArtifact(parent)
	out := make([]CompilerInputVerifiedSource, 0, 2)
	for i, ref := range refs {
		a, err := read(ref.ArtifactKind, ref.ScopeKey, ref.ID)
		if err != nil {
			return nil, err
		}
		if a.ArtifactKind != ref.ArtifactKind || a.ScopeKey != ref.ScopeKey || a.Generation != ref.ID || a.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(a, keys).Pass {
			return nil, ErrConflict
		}
		var typed any
		var scope, generation string
		if i == 0 {
			var intent platformconfig.PlatformIntent
			if decodeCompilerSourceContent(a.Content, &intent) != nil || intent.SchemaVersion != platformconfig.SchemaVersion || platformconfig.ValidatePlatformIntent(intent) != nil {
				return nil, ErrConflict
			}
			typed, scope, generation = intent, intent.Scope, intent.Generation
		} else {
			var policy platformconfig.PolicySnapshot
			if decodeCompilerSourceContent(a.Content, &policy) != nil || policy.SchemaVersion != platformconfig.SchemaVersion || platformconfig.ValidatePolicySnapshot(policy) != nil {
				return nil, ErrConflict
			}
			typed, scope, generation = policy, policy.Scope, policy.Generation
		}
		normalized, scopeKey := NormalizePlatformArtifactScope(model.PlatformArtifactScope{ScopeType: "global", Key: scope})
		if scopeKey != a.ScopeKey || normalized != a.Scope || generation != a.Generation {
			return nil, ErrConflict
		}
		digest, err := platformconfig.Digest(typed)
		expected := lineage.IntentDigest
		metadataKey := "intent_digest"
		if i == 1 {
			expected = lineage.PolicyDigest
			metadataKey = "policy_digest"
		}
		if err != nil || digest != expected || a.Metadata[metadataKey] != digest {
			return nil, ErrConflict
		}
		out = append(out, CompilerInputVerifiedSource{ArtifactID: a.ID, ArtifactKind: a.ArtifactKind, ScopeKey: a.ScopeKey, Generation: a.Generation, ContentHash: a.ContentHash, TypedDigest: digest, SigningKeyID: a.Provenance.KeyID})
	}
	return out, nil
}

func decodeCompilerSourceContent(content map[string]any, dst any) error {
	raw, err := json.Marshal(content)
	if err != nil || len(raw) > 8<<20 {
		return ErrConflict
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(dst) != nil {
		return ErrConflict
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return ErrConflict
	}
	return nil
}
