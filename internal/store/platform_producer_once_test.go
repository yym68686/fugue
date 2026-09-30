package store

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
)

func testProducerPublicationLedger(t *testing.T, address string) {
	f := activationFixture(t, address)
	r, err := f.release()
	if err != nil {
		t.Fatal(err)
	}
	if done, err := f.s.HasVerifiedProducerPublication(f.policy.TargetScope, r.ID); err != nil || done {
		t.Fatal("old baseline consumed new bound", err)
	}
	// Model a historical completed producer publication, subsequently superseded
	// by an operator. No current lane or LKG is needed to retain the bound.
	parent, err := f.s.GetPlatformArtifact(f.lkg.ArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	if parent.Metadata == nil {
		parent.Metadata = map[string]string{}
	}
	parent.Metadata[platformproducer.PolicyReleaseMetadata] = r.ID
	full, err := f.s.GetPlatformArtifactRelease(f.lkg.VerifiedByReleaseID)
	if err != nil {
		t.Fatal(err)
	}
	full.ReleasedByType, full.ReleasedByID = model.ActorTypeBootstrap, platformproducer.Actor
	full.Status = model.PlatformArtifactReleaseStatusSuperseded
	if f.s.db != nil {
		raw, _ := json.Marshal(parent.Metadata)
		_, err = f.s.db.Exec(`UPDATE fugue_platform_artifacts SET metadata_json=$2::jsonb WHERE id=$1`, parent.ID, raw)
		if err == nil {
			_, err = f.s.db.Exec(`UPDATE fugue_platform_artifact_releases SET released_by_type=$2,released_by_id=$3,status=$4 WHERE id=$1`, full.ID, full.ReleasedByType, full.ReleasedByID, full.Status)
		}
	} else {
		err = f.s.withLockedState(true, func(state *model.State) error {
			state.PlatformArtifacts[platformArtifactIndex(state.PlatformArtifacts, parent.ID)] = parent
			state.PlatformArtifactReleases[platformArtifactReleaseIndex(state.PlatformArtifactReleases, full.ID)] = full
			return nil
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []struct {
		scope, policy string
		want          bool
	}{{f.policy.TargetScope, r.ID, true}, {f.policy.TargetScope, "other-policy", false}, {"authority-cell:other", r.ID, false}} {
		done, err := f.s.HasVerifiedProducerPublication(query.scope, query.policy)
		if err != nil || done != query.want {
			t.Fatal("durable bound differs", query, done, err)
		}
		if f.s.db != nil {
			tx, err := f.s.db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			done, err = pgHasVerifiedProducerPublication(context.Background(), tx, query.scope, query.policy)
			tx.Rollback()
			if err != nil || done != query.want {
				t.Fatal("transaction bound differs", done, err)
			}
		}
	}
}

func TestProducerPublicationLedger(t *testing.T) { testProducerPublicationLedger(t, "") }
func TestProducerPublicationLedgerPostgres(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("disposable PostgreSQL not configured")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback database")
	}
	testProducerPublicationLedger(t, address)
}
