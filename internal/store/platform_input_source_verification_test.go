package store

import (
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"testing"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
	"github.com/jackc/pgx/v5/pgconn"
)

func sourceVerificationFixture(t *testing.T, s *Store) (model.PlatformArtifact, []model.PlatformArtifact) {
	t.Helper()
	generation := model.NewID("source_verify")
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: platformconfig.PlatformIntent{Scope: "global", Generation: generation + "-intent"}, Policy: platformconfig.PolicySnapshot{Scope: "global", Generation: generation + "-policy"}})
	if err != nil {
		t.Fatal(err)
	}
	create := func(a model.PlatformArtifact) model.PlatformArtifact {
		t.Helper()
		a, err := s.CreatePlatformArtifact(a)
		if err != nil {
			t.Fatal(err)
		}
		a, err = s.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "neutral-source", Pass: true}})
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	return create(compiled.RouteArtifact), []model.PlatformArtifact{create(compiled.IntentArtifact), create(compiled.PolicyArtifact)}
}

func TestCompilerOriginalSourcesAuthenticateExactTypedSources(t *testing.T) {
	s := newPlatformArtifactEnsureTestStore(t)
	parent, sources := sourceVerificationFixture(t, s)
	keys := s.platformArtifactSigningKeyring()
	verify := func(parent model.PlatformArtifact, sources []model.PlatformArtifact, keys bundleauth.Keyring) ([]CompilerInputVerifiedSource, error) {
		return verifyCompilerOriginalSources(parent, keys, func(kind, scope, generation string) (model.PlatformArtifact, error) {
			for _, a := range sources {
				if a.ArtifactKind == kind && a.ScopeKey == scope && a.Generation == generation {
					return a, nil
				}
			}
			return model.PlatformArtifact{}, ErrNotFound
		})
	}
	got, err := verify(parent, sources, keys)
	if err != nil || len(got) != 2 || got[0].ArtifactID != sources[0].ID || got[1].ArtifactID != sources[1].ID || got[0].TypedDigest == got[0].ContentHash {
		t.Fatal("original typed hashes or exact sources not verified", got, err)
	}
	clone := func(a model.PlatformArtifact) model.PlatformArtifact {
		raw, _ := json.Marshal(a)
		var b model.PlatformArtifact
		if err := json.Unmarshal(raw, &b); err != nil {
			t.Fatal(err)
		}
		return b
	}
	resign := func(a model.PlatformArtifact) model.PlatformArtifact {
		var err error
		a.ContentHash, err = platformArtifactContentHash(a.Content)
		if err != nil {
			t.Fatal(err)
		}
		a, err = platformsafety.SignPlatformArtifact(a, keys)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	for _, name := range []string{"parent signature", "source signature", "missing intent", "revoked key", "wrong typed digest", "unknown typed field", "typed generation", "typed scope", "typed schema", "source metadata", "metadata lineage"} {
		t.Run(name, func(t *testing.T) {
			p := clone(parent)
			a := []model.PlatformArtifact{clone(sources[0]), clone(sources[1])}
			k := keys
			switch name {
			case "parent signature":
				p.Provenance.Signature = "invalid"
			case "source signature":
				a[0].Provenance.Signature = "invalid"
			case "missing intent":
				a = a[1:]
			case "revoked key":
				k.RevokedKeyIDs = map[string]struct{}{strings.ToLower(keys.PrimaryKeyID): {}}
			case "wrong typed digest":
				p.Metadata["intent_digest"] = "sha256:" + strings.Repeat("0", 64)
				p.Content["lineage"].(map[string]any)["intent_digest"] = p.Metadata["intent_digest"]
				p = resign(p)
			case "unknown typed field":
				a[0].Content["unrecognized_source_field"] = true
				a[0] = resign(a[0])
			case "typed generation":
				a[0].Content["generation"] = "different-generation"
				a[0] = resign(a[0])
			case "typed scope":
				a[0].Content["scope"] = "authority-cell:neutral-other"
				a[0] = resign(a[0])
			case "typed schema":
				delete(a[0].Content, "schema_version")
				a[0] = resign(a[0])
			case "source metadata":
				a[0].Metadata["intent_digest"] = "sha256:" + strings.Repeat("1", 64)
				a[0] = resign(a[0])
			case "metadata lineage":
				p.Metadata["policy_generation"] = "different-generation"
				p = resign(p)
			}
			out, err := verify(p, a, k)
			if err == nil || out != nil {
				t.Fatal("untrusted or unbound source accepted", out, err)
			}
		})
	}
}

func TestCompilerSourceVerificationPostgresReadOnlyAndMissingSource(t *testing.T) {
	db, address, schema := hotInventoryTestDatabase(t)
	s := &Store{db: db, databaseURL: address}
	configureTestPlatformArtifactSigning(s)
	parent, sources := sourceVerificationFixture(t, s)
	c := CompilerInputSourceVerificationContract{Schema: CompilerInputSourceVerificationSchema, ArtifactID: parent.ID, ExpectedContentHash: parent.ContentHash}
	roleName := model.NewID("source_verify_role")
	role := quotePostgresIntegrationIdentifier(t, roleName)
	if _, err := db.Exec("CREATE ROLE " + role + " LOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, q := range []string{"DROP OWNED BY " + role, "DROP ROLE " + role} {
			if _, err := db.Exec(q); err != nil {
				t.Error(err)
			}
		}
	})
	if _, err := db.Exec("GRANT USAGE ON SCHEMA " + quotePostgresIntegrationIdentifier(t, schema) + " TO " + role + "; GRANT SELECT ON ALL TABLES IN SCHEMA " + quotePostgresIntegrationIdentifier(t, schema) + " TO " + role); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(address)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.User(roleName)
	readerDB, err := openPostgresDatabase(u.String())
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{`SET temp_file_limit=-1`, `UPDATE fugue_platform_artifacts SET status=status`} {
		_, err := readerDB.Exec(q)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			readerDB.Close()
			t.Fatal("read-only source role acquired unrelated write or SET authority", err)
		}
	}
	readerDB.Close()
	report, err := RunCompilerInputSourceVerification(t.Context(), u.String(), s.platformArtifactSigningKeyring(), c)
	if err != nil || !report.Verified || !report.ReadOnly || report.Snapshot == "" || len(report.Sources) != 2 || report.ClosureComplete || report.RetirementAuthorized || report.OriginalExternalAuthorityVerified {
		t.Fatal("bounded read-only proof incomplete", report, err)
	}
	before, err := s.GetPlatformArtifact(parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`UPDATE fugue_platform_artifacts SET metadata_json=jsonb_build_object('oversized',$2::text) WHERE id=$1`, sources[0].ID, strings.Repeat("x", 8<<20)); err != nil {
		t.Fatal(err)
	}
	report, err = runCompilerInputSourceVerification(t.Context(), s.db, s.platformArtifactSigningKeyring(), c)
	if err == nil || report.Verified || len(report.Sources) != 0 {
		t.Fatal("oversized source supplied partial success", report, err)
	}
	if _, err = s.db.Exec(`DELETE FROM fugue_platform_artifacts WHERE id=$1`, sources[0].ID); err != nil {
		t.Fatal(err)
	}
	report, err = runCompilerInputSourceVerification(t.Context(), s.db, s.platformArtifactSigningKeyring(), c)
	if err == nil || report.Verified || len(report.Sources) != 0 {
		t.Fatal("missing source supplied partial success", report, err)
	}
	after, err := s.GetPlatformArtifact(parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(before)
	a, _ := json.Marshal(after)
	if string(a) != string(b) {
		t.Fatal("verification altered retained parent")
	}
	c.ExpectedContentHash = "invalid"
	if r, err := runCompilerInputSourceVerification(t.Context(), nil, bundleauth.Keyring{}, c); !errors.Is(err, ErrInvalidInput) || r.Verified {
		t.Fatal("invalid contract opened database", r, err)
	}
}
