package store

import (
	"encoding/json"
	"regexp"
	"testing"

	"fugue/internal/platformconfig"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestCompilerInputPostgresRetainsFormatAndVerifiesActualStoredContent(t *testing.T) {
	for _, scenario := range []string{"inserted", "existing", "reordered", "corrupt", "unknown_field", "trailing", "wrong_digest"} {
		t.Run(scenario, func(t *testing.T) {
			database, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			state := &Store{databaseURL: "postgres://example", db: database, dbReady: true}
			snapshot := platformconfig.RuntimeSnapshot{IntentGeneration: "intent-a", DNSSelections: []platformconfig.DNSSelectionObservation{{NodeID: "dns-a", Hostname: "app.example.test",
				PhysicalEvidence: json.RawMessage(`{"snapshot":{"z":1,"a":{"z":2,"a":3}},"result":{"z":4,"a":5}}`)}}}
			digest, err := platformconfig.RuntimeSnapshotDigest(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := platformconfig.EncodeRuntimeSnapshot(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			stored := encoded
			if scenario == "reordered" {
				var value map[string]any
				if err := json.Unmarshal(encoded, &value); err != nil {
					t.Fatal(err)
				}
				stored, _ = json.MarshalIndent(value, "", " ")
			}
			if scenario == "corrupt" {
				stored = []byte(`{"intent_generation":"foreign"}`)
			}
			if scenario == "unknown_field" {
				stored = []byte(`{"unexpected":true}`)
			}
			if scenario == "trailing" {
				stored = append(append([]byte(nil), encoded...), []byte(` {}`)...)
			}
			if scenario == "wrong_digest" {
				digest = "sha256:unrelated"
			} else {
				affected := int64(1)
				if scenario == "existing" || scenario == "corrupt" {
					affected = 0
				}
				mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO fugue_platform_artifact_contents (content_hash, content_json, size_bytes, created_at, updated_at) VALUES ($1,$2::jsonb,$3,$4,$4) ON CONFLICT (content_hash) DO NOTHING`)).
					WithArgs(digest, encoded, int64(len(encoded)), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, affected))
				mock.ExpectQuery(regexp.QuoteMeta(`SELECT content_json FROM fugue_platform_artifact_contents WHERE content_hash = $1`)).WithArgs(digest).
					WillReturnRows(sqlmock.NewRows([]string{"content_json"}).AddRow(stored))
			}
			err = state.EnsurePlatformCompilerInput(snapshot, digest)
			valid := scenario == "inserted" || scenario == "existing" || scenario == "reordered"
			if (err == nil) != valid {
				t.Fatal("stored content was not independently verified", scenario, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
