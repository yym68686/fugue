package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/platformconfig"
	"fugue/internal/schemamigrate"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func separatedTestStore(t testing.TB) *Store {
	s := sharedInputTestStore(t)
	if err := schemamigrate.MigrateCompilerInputSeparated(t.Context(), s.databaseURL); err != nil {
		t.Fatal(err)
	}
	s.storagePolicy.shared.Enabled = false
	s.storagePolicy.separated = compilerInputCodecPolicy{Version: 1, Enabled: true}
	return s
}

func separatedFixture(t testing.TB) platformconfig.RuntimeSnapshot {
	s := platformconfig.RuntimeSnapshot{IntentGeneration: fmt.Sprintf("separated-%d", time.Now().UnixNano()), PolicyGeneration: "policy", Facts: map[string]any{"literal": "<中文>&\"physical_evidence\":{}"}}
	items := []map[string]any{}
	for i := 0; i < 80; i++ {
		items = append(items, map[string]any{"id": fmt.Sprintf("measurement-%d", i), "observed_at": "2026-01-01T00:00:00Z", "payload": strings.Repeat(fmt.Sprintf("fact-%d", i), 30)})
	}
	for i := 0; i < 8; i++ {
		raw, _ := json.Marshal(map[string]any{"snapshot": map[string]any{"network_samples": items, "node_id": fmt.Sprintf("node-%d", i%2)}, "captured_at": "2026-01-01T00:00:00Z"})
		s.DNSSelections = append(s.DNSSelections, platformconfig.DNSSelectionObservation{NodeID: fmt.Sprintf("node-%d", i), Hostname: "service.example.test", Type: "A", PhysicalEvidence: raw})
	}
	return s
}

func assertSeparatedEquivalent(t *testing.T, s *Store, snapshot platformconfig.RuntimeSnapshot, digest string) {
	t.Helper()
	raw, err := platformconfig.EncodeRuntimeSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.GetPlatformCompilerInput(digest)
	if err != nil {
		t.Fatal(err)
	}
	rebuilt, _ := platformconfig.EncodeRuntimeSnapshot(got)
	if !bytes.Equal(raw, rebuilt) {
		t.Fatal("original canonical bytes changed")
	}
	content, err := s.GetPlatformArtifactContent(digest)
	if err != nil {
		t.Fatal(err)
	}
	if d, err := platformconfig.RuntimeSnapshotContentDigest(content.Content); err != nil || d != digest {
		t.Fatal("public content/replay changed", err)
	}
	for _, needed := range []func(CompilerDNSSelectionSummary) bool{nil, func(f CompilerDNSSelectionSummary) bool { return false }, func(f CompilerDNSSelectionSummary) bool { return f.NodeID == "node-2" }} {
		selected, err := s.GetPlatformCompilerInputDNSSelections(digest, needed)
		if err != nil {
			t.Fatal(err)
		}
		expected := selectCompilerDNSObservations(got, needed)
		a, _ := json.Marshal(selected)
		b, _ := json.Marshal(expected)
		if !bytes.Equal(a, b) {
			t.Fatal("DNS semantics changed")
		}
	}
}

func TestSeparatedInputPostgresRoundtripSharingAndRollback(t *testing.T) {
	s := separatedTestStore(t)
	snapshot := separatedFixture(t)
	digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	assertSeparatedEquivalent(t, s, snapshot, digest)
	var components int
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_separated_refs WHERE content_hash=$1`, digest).Scan(&components); err != nil || components != 3 {
		t.Fatal("evidence copies weren't shared", components, err)
	}
	m, err := readInputManifest(t.Context(), s.db, digest)
	if err != nil || m.Format != inputFormatSeparatedV4 {
		t.Fatal(m, err)
	}
	s.storagePolicy.separated.Enabled = false
	assertSeparatedEquivalent(t, s, snapshot, digest)
	for _, action := range []string{"rollback", "migrate"} {
		report, err := s.maintainCompilerInput(t.Context(), CompilerInputMaintenanceOptions{Action: action, Digest: digest, MaxInputBytes: 128 << 20, Apply: true})
		if err != nil || !report.Applied {
			t.Fatal(action, report, err)
		}
		now, err := readInputManifest(t.Context(), s.db, digest)
		if err != nil || !now.Created.Equal(m.Created) {
			t.Fatal("input identity/time changed", err)
		}
		assertSeparatedEquivalent(t, s, snapshot, digest)
	}
	if _, err = s.maintainCompilerInput(t.Context(), CompilerInputMaintenanceOptions{Action: "collect", Apply: true}); err != nil {
		t.Fatal(err)
	}
	assertSeparatedEquivalent(t, s, snapshot, digest)
}

func TestSeparatedInputMigrationKeepsConcurrentReaderSnapshot(t *testing.T) {
	s := separatedTestStore(t)
	s.storagePolicy.separated.Enabled = false
	snapshot := separatedFixture(t)
	digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	reader, err := s.db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Rollback()
	m, err := readInputManifest(t.Context(), reader, digest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.maintainCompilerInput(t.Context(), CompilerInputMaintenanceOptions{Action: "migrate", Digest: digest, ExpectedFormat: m.Format, MaxInputBytes: 128 << 20, Apply: true}); err != nil {
		t.Fatal(err)
	}
	r, err := openInputChunks(t.Context(), reader, m)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	h := sha256.New()
	if _, err = io.Copy(h, r); err != nil || sharedHashBytes(h.Sum(nil)) != digest {
		t.Fatal("reader observed a mixed representation", err)
	}
	assertSeparatedEquivalent(t, s, snapshot, digest)
}

func TestSeparatedInputCorruptionCannotAuthorizeOmittedEvidence(t *testing.T) {
	for _, mutation := range []string{"layout", "evidence", "reference"} {
		t.Run(mutation, func(t *testing.T) {
			s := separatedTestStore(t)
			snapshot := separatedFixture(t)
			digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
			if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
				t.Fatal(err)
			}
			if mutation == "evidence" {
				rows, err := s.db.Query(`SELECT pack_id,data FROM fugue_platform_input_shared_packs WHERE pack_id IN(SELECT pack_id FROM fugue_platform_input_component_refs WHERE content_hash IN(SELECT component_hash FROM fugue_platform_input_separated_refs WHERE content_hash=$1))`, digest)
				if err != nil {
					t.Fatal(err)
				}
				original := map[int64][]byte{}
				for rows.Next() {
					var id int64
					var raw []byte
					if err = rows.Scan(&id, &raw); err != nil {
						t.Fatal(err)
					}
					original[id] = raw
				}
				rows.Close()
				t.Cleanup(func() {
					for id, raw := range original {
						h := sha256.Sum256(raw)
						if _, err := s.db.Exec(`UPDATE fugue_platform_input_shared_packs SET data=$2,stored_hash=$3 WHERE pack_id=$1`, id, raw, h[:]); err != nil {
							t.Error(err)
						}
					}
				})
			}
			query := `UPDATE fugue_platform_input_separated_roots SET layout=set_byte(layout,1,9) WHERE content_hash=$1`
			switch mutation {
			case "evidence":
				query = `UPDATE fugue_platform_input_shared_packs SET data=set_byte(data,0,9),stored_hash=sha256(set_byte(data,0,9)) WHERE pack_id IN(SELECT pack_id FROM fugue_platform_input_component_refs WHERE content_hash IN(SELECT component_hash FROM fugue_platform_input_separated_refs WHERE content_hash=$1))`
			case "reference":
				query = `DELETE FROM fugue_platform_input_separated_refs WHERE content_hash=$1`
			}
			if _, err := s.db.Exec(query, digest); err != nil {
				t.Fatal(err)
			}
			if _, err := s.GetPlatformCompilerInputDNSSelections(digest, func(CompilerDNSSelectionSummary) bool { return false }); err == nil {
				t.Fatal("corrupt omitted evidence accepted")
			}
			if _, err := s.maintainCompilerInput(t.Context(), CompilerInputMaintenanceOptions{Action: "rollback", Digest: digest, MaxInputBytes: 128 << 20, Apply: true}); err == nil {
				t.Fatal("corrupt evidence rewritten")
			}
		})
	}
}

func TestSeparatedLayoutRejectsMalformedInventory(t *testing.T) {
	parts := []separatedPart{{Size: 3}, {Evidence: true, Digest: sharedHash(sha256.Sum256([]byte("{}"))), Size: 2}}
	raw, err := encodeSeparatedLayout(parts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = decodeSeparatedLayout(raw); err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{nil, raw[:len(raw)-1], append(append([]byte{}, raw...), 0)} {
		if _, err = decodeSeparatedLayout(bad); err == nil {
			t.Fatal("bad layout accepted")
		}
	}
}

func TestSeparatedMigrationRehearsalGuardsAndPhysicalCollection(t *testing.T) {
	s := separatedTestStore(t)
	for _, old := range []string{inputFormatV1, inputFormatCodecV2, inputFormatSharedV3} {
		t.Run(old, func(t *testing.T) {
			s.storagePolicy.separated.Enabled = false
			s.storagePolicy.codec = compilerInputCodecPolicy{Version: 1, Enabled: old == inputFormatCodecV2}
			s.storagePolicy.shared.Enabled = old == inputFormatSharedV3
			snapshot := separatedFixture(t)
			digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
			if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
				t.Fatal(err)
			}
			m, err := readInputManifest(t.Context(), s.db, digest)
			if err != nil || m.Format != old {
				t.Fatal(m, err)
			}
			o := CompilerInputMaintenanceOptions{Action: "migrate", Digest: digest, ExpectedFormat: old, MaxInputBytes: 128 << 20}
			if report, err := s.maintainCompilerInput(t.Context(), o); err != nil || report.Applied {
				t.Fatal(report, err)
			}
			after, err := readInputManifest(t.Context(), s.db, digest)
			if err != nil || after != m {
				t.Fatal("rehearsal changed manifest", after, err)
			}
			o.Apply = true
			o.ExpectedFormat = "wrong"
			if _, err = s.maintainCompilerInput(t.Context(), o); err == nil {
				t.Fatal("stale format accepted")
			}
			o.ExpectedFormat = old
			o.MaxInputBytes = m.Size - 1
			if _, err = s.maintainCompilerInput(t.Context(), o); err == nil {
				t.Fatal("size budget ignored")
			}
			o.MaxInputBytes = 128 << 20
			if _, err = s.maintainCompilerInput(t.Context(), o); err != nil {
				t.Fatal(err)
			}
			assertSeparatedEquivalent(t, s, snapshot, digest)
			if _, err = s.maintainCompilerInput(t.Context(), CompilerInputMaintenanceOptions{Action: "rollback", Digest: digest, MaxInputBytes: 128 << 20, Apply: true}); err != nil {
				t.Fatal(err)
			}
			if _, err = s.maintainCompilerInput(t.Context(), CompilerInputMaintenanceOptions{Action: "collect", Apply: true}); err != nil {
				t.Fatal(err)
			}
			assertSeparatedEquivalent(t, s, snapshot, digest)
		})
	}
}

func TestSeparatedCollectorYieldsToWriterFenceAndRejectsMissingBarrier(t *testing.T) {
	s := separatedTestStore(t)
	tx, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`SELECT pg_advisory_xact_lock(315609238744290)`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err = s.maintainCompilerInput(ctx, CompilerInputMaintenanceOptions{Action: "collect", Apply: true}); err == nil {
		t.Fatal("collector crossed active writer fence")
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`ALTER TABLE fugue_platform_input_component_refs DISABLE TRIGGER ALL`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := s.db.Exec(`ALTER TABLE fugue_platform_input_component_refs ENABLE TRIGGER ALL`); err != nil {
			t.Error(err)
		}
	})
	if _, err = s.maintainCompilerInput(t.Context(), CompilerInputMaintenanceOptions{Action: "collect", Apply: true}); err == nil {
		t.Fatal("collector ignored disabled FK barrier")
	}
}

func TestSeparatedInputConcurrentSharedFacts(t *testing.T) {
	s := separatedTestStore(t)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		snapshot := separatedFixture(t)
		digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
				t.Error(err)
				return
			}
			if _, err := s.GetPlatformCompilerInput(digest); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}

// Representative volume with repeated evidence and slightly changed bodies.
// This is synthetic; it does not predict physical savings for production data.
func TestSeparatedInputSyntheticVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("volume integration")
	}
	s := separatedTestStore(t)
	snapshot := separatedFixture(t)
	evidence := snapshot.DNSSelections[0].PhysicalEvidence
	base := snapshot.DNSSelections[0]
	snapshot.DNSSelections = nil
	for i := 0; i < 1800; i++ {
		v := base
		v.NodeID = fmt.Sprintf("volume-node-%d", i)
		v.PhysicalEvidence = evidence
		snapshot.DNSSelections = append(snapshot.DNSSelections, v)
	}
	digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
	started := time.Now()
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	wrote := time.Since(started)
	started = time.Now()
	if _, err := s.GetPlatformCompilerInput(digest); err != nil {
		t.Fatal(err)
	}
	read := time.Since(started)
	started = time.Now()
	if _, err := s.GetPlatformCompilerInputDNSSelections(digest, func(CompilerDNSSelectionSummary) bool { return false }); err != nil {
		t.Fatal(err)
	}
	var logical, components, packs int64
	if err := s.db.QueryRow(`SELECT size_bytes FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest).Scan(&logical); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_separated_refs WHERE content_hash=$1`, digest).Scan(&components); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT COALESCE(sum(octet_length(data)),0) FROM fugue_platform_input_shared_packs WHERE pack_id IN(SELECT pack_id FROM fugue_platform_input_component_refs WHERE content_hash IN(SELECT component_hash FROM fugue_platform_input_separated_refs WHERE content_hash=$1))`, digest).Scan(&packs); err != nil {
		t.Fatal(err)
	}
	t.Logf("synthetic logical=%d component_count=%d referenced_pack_bytes=%d write=%s read=%s omitted=%s cache=%t", logical, components, packs, wrote, read, time.Since(started), s.compilerDNSInputs.get(digest) != nil)
	if components != 2 {
		t.Fatal("repeated evidence not shared", components)
	}
}

func TestSeparatedInputDistinctEvidenceProfile(t *testing.T) {
	if os.Getenv("FUGUE_SEPARATED_PROFILE") != "1" {
		t.Skip("explicit synthetic profile only")
	}
	s := separatedTestStore(t)
	snapshot := separatedFixture(t)
	base := snapshot.DNSSelections[0]
	var evidence map[string]any
	if err := json.Unmarshal(base.PhysicalEvidence, &evidence); err != nil {
		t.Fatal(err)
	}
	samples := evidence["snapshot"].(map[string]any)["network_samples"].([]any)
	for i := 0; i < 3; i++ {
		samples = append(samples, samples...)
	}
	evidence["snapshot"].(map[string]any)["network_samples"] = samples
	snapshot.DNSSelections = nil
	for i := 0; i < 530; i++ {
		evidence["captured_at"] = fmt.Sprintf("capture-%d", i)
		v := base
		v.NodeID = fmt.Sprintf("profile-node-%d", i)
		v.PhysicalEvidence, _ = json.Marshal(evidence)
		snapshot.DNSSelections = append(snapshot.DNSSelections, v)
	}
	digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
	started := time.Now()
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	wrote := time.Since(started)
	started = time.Now()
	if _, err := s.GetPlatformCompilerInput(digest); err != nil {
		t.Fatal(err)
	}
	read := time.Since(started)
	started = time.Now()
	if _, err := s.GetPlatformCompilerInputDNSSelections(digest, func(CompilerDNSSelectionSummary) bool { return false }); err != nil {
		t.Fatal(err)
	}
	t.Logf("synthetic distinct_evidence=530 logical_approx=%d write=%s full_read=%s omitted_read=%s", 530*len(base.PhysicalEvidence)*8, wrote, read, time.Since(started))
}

func TestSeparatedWriterBudgetFallbackIsAtomic(t *testing.T) {
	s := separatedTestStore(t)
	s.storagePolicy.codec = compilerInputCodecPolicy{Version: 1, Enabled: true}
	snapshot := separatedFixture(t)
	snapshot.DNSSelections[0].PhysicalEvidence = json.RawMessage(fmt.Sprintf(`{"unique":"budget-%d"}`, time.Now().UnixNano()))
	snapshot.Facts["large_config"] = strings.Repeat("config", separatedFrameLimit/6+1)
	digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
	var before, after int
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_components`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	m, err := readInputManifest(t.Context(), s.db, digest)
	if err != nil || m.Format != inputFormatCodecV2 {
		t.Fatal("budget fallback failed", m, err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_components`).Scan(&after); err != nil || after != before {
		t.Fatal("partial components leaked", before, after, err)
	}
	if _, err = s.GetPlatformCompilerInput(digest); err != nil {
		t.Fatal(err)
	}
}

func TestSeparatedWriterPolicyFailsClosed(t *testing.T) {
	s := &Store{}
	path := filepath.Join(t.TempDir(), "policy.json")
	for _, value := range []struct {
		raw     string
		enabled bool
	}{
		{`{"version":1,"enabled":true}`, true},
		{`{"version":1,"enabled":false}`, false},
		{`{"version":2,"enabled":true}`, false},
		{`{"version":1,"enabled":true,"delete_evidence":true}`, false},
		{`{"version":1,"enabled":true} {}`, false},
	} {
		if err := os.WriteFile(path, []byte(value.raw), 0600); err != nil {
			t.Fatal(err)
		}
		s.loadCompilerInputSeparatedPolicy(path)
		if s.compilerInputSeparatedEnabled() != value.enabled {
			t.Fatal("unexpected writer authorization")
		}
	}
	s.loadCompilerInputSeparatedPolicy(path + "-missing")
	if s.compilerInputSeparatedEnabled() {
		t.Fatal("missing declaration enabled writer")
	}
}

func TestSeparatedFrameDoesNotClassifyConfigurationByFieldName(t *testing.T) {
	snapshot := platformconfig.RuntimeSnapshot{IntentGeneration: "dns_selections", PolicyGeneration: "policy", Facts: map[string]any{"looks_like_evidence": map[string]any{"physical_evidence": map[string]any{"configuration": "retained"}}, "array": []any{map[string]any{"physical_evidence": true}}}}
	w := &separatedInputWriter{hash: sha256.New(), verified: map[string]bool{}}
	if err := platformconfig.WriteRuntimeSnapshot(w, snapshot); err != nil {
		t.Fatal(err)
	}
	raw, err := platformconfig.EncodeRuntimeSnapshot(snapshot)
	if err != nil || !bytes.Equal(raw, w.frame) || len(w.verified) != 0 || w.depth != 0 || w.quoted || w.dnsArray {
		t.Fatal("configuration field was reclassified", err)
	}
}

func TestSeparatedMaintenanceUsesOrdinaryDatabaseRole(t *testing.T) {
	s := separatedTestStore(t)
	var super bool
	if err := s.db.QueryRow(`SELECT rolsuper FROM pg_roles WHERE rolname=current_user`).Scan(&super); err != nil {
		t.Fatal(err)
	}
	if !super {
		t.Skip("fixture role creation requires a disposable database administrator")
	}
	// SET ROLE, not the fixture's superuser identity, executes every statement
	// in the maintenance transaction. No login, credential, or production role.
	const role = "fugue_test_input_maintenance"
	if _, err := s.db.Exec(`CREATE ROLE ` + role + ` NOLOGIN NOSUPERUSER`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.db.Exec(`RESET ROLE`)
		s.db.Exec(`DROP OWNED BY ` + role)
		if _, err := s.db.Exec(`DROP ROLE ` + role); err != nil {
			t.Error(err)
		}
	})
	if _, err := s.db.Exec(`GRANT USAGE ON SCHEMA public TO ` + role + `; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO ` + role + `; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO ` + role); err != nil {
		t.Fatal(err)
	}
	s.storagePolicy.separated.Enabled = false
	snapshot := separatedFixture(t)
	digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	s.db.SetMaxOpenConns(1)
	s.db.SetMaxIdleConns(1)
	if _, err := s.db.Exec(`SET ROLE ` + role); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"migrate", "rollback", "collect"} {
		if _, err := s.maintainCompilerInput(t.Context(), CompilerInputMaintenanceOptions{Action: action, Digest: digest, MaxInputBytes: 128 << 20, Apply: true}); err != nil {
			t.Fatal(action, err)
		}
	}
	assertSeparatedEquivalent(t, s, snapshot, digest)
}

func TestSeparatedReaderBatchesDistinctComponentMetadata(t *testing.T) {
	s := separatedTestStore(t)
	snapshot := separatedFixture(t)
	base := snapshot.DNSSelections[0]
	snapshot.DNSSelections = nil
	for i := 0; i < 80; i++ {
		v := base
		v.PhysicalEvidence = json.RawMessage(fmt.Sprintf(`{"capture":"capture-%d","shared":%s}`, i, base.PhysicalEvidence))
		snapshot.DNSSelections = append(snapshot.DNSSelections, v)
	}
	digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	config, err := pgx.ParseConfig(s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	tracer := &inputProfileTracer{}
	config.Tracer = tracer
	db := stdlib.OpenDB(*config)
	defer db.Close()
	reader := &Store{db: db, databaseURL: s.databaseURL, dbReady: true}
	got, err := reader.GetPlatformCompilerInput(digest)
	if err != nil {
		t.Fatal(err)
	}
	if actual, e := platformconfig.RuntimeSnapshotDigest(got); e != nil || actual != digest {
		t.Fatal("bytes changed", e)
	}
	if queries := tracer.metadata.Load(); queries >= 32 {
		t.Fatalf("per-component round trips returned: %d", queries)
	}
}

func TestSeparatedFinalReadbackRejectsForgedNewComponentRecipe(t *testing.T) {
	s := separatedTestStore(t)
	if _, err := s.db.Exec(`CREATE FUNCTION fugue_test_alter_component_recipe() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.recipe=set_byte(NEW.recipe,0,0); NEW.recipe_hash='sha256:'||encode(sha256(NEW.recipe),'hex'); RETURN NEW; END $$; CREATE TRIGGER fugue_test_alter_component_recipe BEFORE INSERT ON fugue_platform_input_component_recipes FOR EACH ROW EXECUTE FUNCTION fugue_test_alter_component_recipe()`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := s.db.Exec(`DROP TRIGGER fugue_test_alter_component_recipe ON fugue_platform_input_component_recipes; DROP FUNCTION fugue_test_alter_component_recipe()`)
		if err != nil {
			t.Error(err)
		}
	})
	snapshot := separatedFixture(t)
	snapshot.DNSSelections[0].PhysicalEvidence = json.RawMessage(fmt.Sprintf(`{"unique":"new-%d"}`, time.Now().UnixNano()))
	digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err == nil {
		t.Fatal("forged stored recipe committed")
	}
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest).Scan(&n); err != nil || n != 0 {
		t.Fatal("failed input leaked", n, err)
	}
}
