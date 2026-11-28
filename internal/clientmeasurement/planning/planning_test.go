package planning

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/routeprobe"
)

func TestObserverBudgetRetainsCooldownAndCapacityUntilExpiry(t *testing.T) {
	var budget Budget
	now := time.Now()
	if !budget.Reserve("same", now) || budget.Reserve("same", now.Add(29*time.Second)) ||
		!budget.Reserve("same", now.Add(30*time.Second)) {
		t.Fatal("per-observer cooldown drifted")
	}
	for i := range 127 {
		if !budget.Reserve(strconv.Itoa(i), now) {
			t.Fatalf("identity %d did not fit", i)
		}
	}
	if budget.Reserve("extra", now.Add(4*time.Minute)) || !budget.Reserve("extra", now.Add(5*time.Minute)) {
		t.Fatal("observer capacity or expiry drifted")
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if budget.Reserve("contended", now.Add(5*time.Minute)) {
		t.Fatal("a contended request must fail closed")
	}
}

func TestClientProbePlanRequiresPerHostServingProofBeforeAdmittingTargets(t *testing.T) {
	nodes := []model.EdgeNode{{ID: "edge-a", EdgeGroupID: "group-a"}, {ID: "edge-b", EdgeGroupID: "group-a"}, {ID: "edge-c", EdgeGroupID: "group-b"}}
	for _, failure := range []string{"unavailable", "excluded", "foreign_identity"} {
		ready, proofs := ReadyCandidates(context.Background(), nodes, func(ctx context.Context, node model.EdgeNode) (routeprobe.Proof, error) {
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
	ready, _ := ReadyCandidates(ctx, nodes, func(context.Context, model.EdgeNode) (routeprobe.Proof, error) {
		t.Fatal("cancelled plan issued a route proof request")
		return routeprobe.Proof{}, nil
	})
	if len(ready) != 0 {
		t.Fatal(ready)
	}
}
