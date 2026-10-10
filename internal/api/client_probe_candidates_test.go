package api

import (
	"context"
	"errors"
	"testing"

	"fugue/internal/model"
	"fugue/internal/routeprobe"
)

func TestClientProbePlanRequiresPerHostServingProofBeforeAdmittingTargets(t *testing.T) {
	nodes := []model.EdgeNode{{ID: "edge-a", EdgeGroupID: "group-a"}, {ID: "edge-b", EdgeGroupID: "group-a"}, {ID: "edge-c", EdgeGroupID: "group-b"}}
	for _, failure := range []string{"unavailable", "excluded", "foreign_identity"} {
		ready, proofs := clientProbeReadyCandidates(context.Background(), model.EdgeClientProbeRequest{Hostname: "app.example.test"}, nodes, func(ctx context.Context, node model.EdgeNode) (routeprobe.Proof, error) {
			proof := routeprobe.Proof{EdgeID: node.ID, GroupID: node.EdgeGroupID}
			if node.ID == "edge-c" {
				switch failure {
				case "unavailable":
					return proof, errors.New("route not serving")
				case "excluded":
					proof.State = "excluded"
				case "foreign_identity":
					proof.EdgeID = "edge-other"
				}
			}
			return proof, nil
		})
		if len(ready) != 2 || len(proofs) != 2 || ready[0].ID != "edge-a" || ready[1].ID != "edge-b" {
			t.Fatal("unavailable service candidate blocked valid comparison or became a probe target", failure, ready)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ready, _ := clientProbeReadyCandidates(ctx, model.EdgeClientProbeRequest{}, nodes, func(context.Context, model.EdgeNode) (routeprobe.Proof, error) {
		t.Fatal("cancelled plan issued a route proof request")
		return routeprobe.Proof{}, nil
	})
	if len(ready) != 0 {
		t.Fatal(ready)
	}
}
