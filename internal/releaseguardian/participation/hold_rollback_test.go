package participation

import (
	"encoding/json"
	"maps"
	"testing"
	"time"

	"fugue/internal/model/edgecohort"
	"fugue/internal/productioncontrol"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"
)

func TestOfflineHoldRollbackPreservesPresenceAndUnownedAnnotations(t *testing.T) {
	at := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	s, n, after := draftFixtureOfflineHold(t, offlineFixtureTopology(at), at)
	p, err := decodeHold(s)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{participationAnnotation, edgecohort.ReleaseHoldAnnotation} {
		for _, existing := range []*string{nil, new(string)} {
			name := key + "/absent"
			if existing != nil {
				name = key + "/present-empty"
			}
			t.Run(name, func(t *testing.T) {
				original := p
				original.From.Annotations = maps.Clone(p.From.Annotations)
				original.From.Annotations[key] = existing
				q := n.DeepCopy()
				q.ResourceVersion = "11"
				q.Annotations = maps.Clone(after)
				q.Annotations["fixture.example/preserve"] = "unchanged"
				rb, err := encodeHoldRollback(s.Intent.ClusterID, "rollback-a", original, digest([]byte("original")),
					q.ResourceVersion, productioncontrol.Version{Generation: s.Intent.Generation + 1, Digest: s.Intent.TargetDigest},
					digest([]byte("checkpoint")))
				if err != nil {
					t.Fatal(err)
				}
				_, patchBytes, err := offlineHoldPatch(rb, q)
				if err != nil {
					t.Fatal(err)
				}
				patch, err := jsonpatch.DecodePatch(patchBytes)
				if err != nil {
					t.Fatal(err)
				}
				wire, err := json.Marshal(q)
				if err != nil {
					t.Fatal(err)
				}
				result, err := patch.Apply(wire)
				if err != nil {
					t.Fatal(err)
				}
				got := q.DeepCopy()
				got.Annotations = nil
				if err := json.Unmarshal(result, got); err != nil {
					t.Fatal(err)
				}
				value, present := got.Annotations[key]
				if present != (existing != nil) || value != "" || got.Annotations["fixture.example/preserve"] != "unchanged" ||
					HoldStateDigest(q.Name, string(q.UID), p.To.Group, got.Annotations) != rb.Intent.TargetDigest {
					t.Fatalf("exact rollback did not preserve annotation presence: %v", got.Annotations)
				}
			})
		}
	}
}
