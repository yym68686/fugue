package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"fugue/internal/inputarchive"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/testfixture/celldns"
)

func TestHotInventoryPostgresProducerGenerationMatchesLedger(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	installHotIndexTest(t, db, address)
	req, policies, _, _, _ := celldns.CompletePublication(t, true, true)
	p := platformproducer.Policy{SchemaVersion: platformproducer.Schema, Generation: "dns-source-generation", PublicationRole: platformconfig.PublicationRoleCellDNS,
		AuthorityCellID: "cell-dns", TargetScope: "authority-cell:cell-dns", Mode: "shadow", InputSource: platformproducer.DNSProducerInputSource,
		IntervalSeconds: 30, RefreshSeconds: 120, DNSSource: &platformproducer.DNSProducerSource{ProducerPolicy: *req.PreviousTrafficPublication.ProducerPolicy,
			PreviousTopology: req.Intent.RouteAuthorityTransition.PreviousTopology, DNSNodeIDs: []string{"dns-a"}}}
	raw, err := json.Marshal(p)
	var content map[string]any
	if err != nil || json.Unmarshal(raw, &content) != nil {
		t.Fatal(err)
	}
	scope, err := platformproducer.PolicyScopeForTarget(p.TargetScope)
	if err != nil {
		t.Fatal(err)
	}
	policies = append(policies, celldns.Sign(t, model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot,
		Scope: model.PlatformArtifactScope{Key: scope}, Generation: p.Generation, Content: content}, "neutral-dns-source", 1))
	for i, policy := range policies {
		if _, err := platformproducer.Decode(policy); err != nil {
			t.Fatal("complete fixture must have a valid exact policy identity", err)
		}
		for _, mutation := range []string{"valid", "missing", "mismatch"} {
			t.Run(fmt.Sprintf("policy-%d/%s", i, mutation), func(t *testing.T) {
				a := policy
				a.ID = fmt.Sprintf("neutral-producer-%d-%s", i, mutation)
				if mutation == "missing" {
					a.Generation = ""
				} else if mutation == "mismatch" {
					a.Generation += "-different"
				}
				// The independent oracle keeps the complete original envelope.
				// Comparing two truncated SQL read paths would hide this defect.
				want, wantErr := compilerInputArtifactHotNode(a)
				if (wantErr == nil) != (mutation == "valid") {
					t.Fatal("fixture did not exercise the exact generation guard", wantErr)
				}
				body, err := json.Marshal(a.Content)
				if err != nil {
					t.Fatal(err)
				}
				metadata, err := json.Marshal(a.Metadata)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = db.Exec(`INSERT INTO fugue_platform_artifacts(id,artifact_kind,scope_key,generation,status,content_hash,content_json,metadata_json,created_at,updated_at)
 VALUES($1,$2,$3,$4,'validated',$5,$6,$7,now(),now())`, a.ID, a.ArtifactKind, a.ScopeKey, a.Generation, a.ContentHash, body, metadata); err != nil {
					t.Fatal(err)
				}
				tx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if _, err = tx.Exec(`SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='250ms'; SET LOCAL work_mem='1MB'; SET LOCAL temp_file_limit=0; SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL jit=off`); err != nil {
					t.Fatal(err)
				}
				ref := inputarchive.HotReference{Kind: "artifact", ID: a.ID}
				got, singleErr := resolveCompilerInputHotNode(t.Context(), tx, ref)
				if (singleErr == nil) != (wantErr == nil) || singleErr == nil && !reflect.DeepEqual(got, want) {
					t.Errorf("single SQL read changed exact producer generation validation: %v", singleErr)
				}
				for _, indexed := range []bool{false, true} {
					batch, batchErr := resolveCompilerInputHotBatchWithIndex(t.Context(), tx, []inputarchive.HotReference{ref}, nil, indexed)
					node, found := batch[ref]
					if batchErr != nil || found != (wantErr == nil) || found && !reflect.DeepEqual(node, want) {
						t.Errorf("batch indexed=%t changed exact producer generation validation: found=%t err=%v", indexed, found, batchErr)
					}
				}
				if mutation == "valid" {
					// A concurrent update cannot replace the envelope inside the RR
					// snapshot or turn a mismatched later generation into a success.
					if _, err = db.Exec(`UPDATE fugue_platform_artifacts SET generation=$2 WHERE id=$1`, a.ID, a.Generation+"-changed"); err != nil {
						t.Fatal(err)
					}
					batch, err := resolveCompilerInputHotBatch(t.Context(), tx, []inputarchive.HotReference{ref})
					if err != nil || !reflect.DeepEqual(batch[ref], want) {
						t.Error("concurrent generation update escaped the original snapshot", err)
					}
					batch, err = resolveCompilerInputHotBatch(t.Context(), db, []inputarchive.HotReference{ref})
					if err != nil || len(batch) != 0 {
						t.Error("new snapshot accepted a mismatched ledger generation", err)
					}
				}
			})
		}
	}
}
