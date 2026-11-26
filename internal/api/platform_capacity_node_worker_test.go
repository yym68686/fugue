package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/edgegroupfront"
	"fugue/internal/releaseguardian"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNodeLocalCapacityRecoveryUsesExactCompletedRollback(t *testing.T) {
	const group = "edge-group-fixture"
	digest := func(value string) string { return "sha256:" + strings.Repeat(value, 64) }
	uid := func(value string) string { return "00000000-0000-4000-8000-00000000000" + value }
	start := time.Now().UTC()
	previous := edgegroupfront.ActivationState{Schema: edgegroupfront.ActivationStateSchemaV1, GroupID: group, Generation: 11,
		ActiveSlot: "a", BundleGeneration: "test.p1.r1", WorkerSourceCommit: strings.Repeat("a", 40),
		WorkerImageDigest: digest("a"), Authority: edgegroupfront.ActivationAuthority,
		Operation: edgegroupfront.ActivationOperationInit, Reason: "verified predecessor", UpdatedAt: start}
	j, err := (edgegroupfront.NodeWorkerJournal{
		Schema: edgegroupfront.NodeWorkerJournalSchema, Recipient: edgegroupfront.NativeRecipient{Node: "edge-fixture", Group: group, PodUID: uid("1"), Incarnation: digest("b")},
		NodeUID: uid("2"), ServiceUID: uid("3"), PredecessorWorkerUID: uid("4"), CandidateWorkerUID: uid("5"),
		PredecessorProcess: digest("c"), CandidateProcess: digest("d"), CandidateComponent: "worker-fixture", ReleaseDigest: digest("e"),
		Expected: previous, Candidate: edgegroupfront.ActivationRepairRelease{Slot: "b", BundleGeneration: "test.p2.r1", WorkerSourceCommit: strings.Repeat("f", 40), WorkerImageDigest: digest("f")},
		CreatedAt: start.Add(time.Second),
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	state := previous
	state.Generation, state.PreviousSlot, state.Operation, state.RollbackOfGeneration = 13, "b", edgegroupfront.ActivationOperationRollback, 12
	state.Reason, state.UpdatedAt = "restore node Worker using journal "+j.Digest, start.Add(2*time.Second)
	receipt := releaseguardian.NodeWorkerResult{Schema: "edge-node-worker-result/v1", JournalDigest: j.Digest, Outcome: "reverted", Activation: state}
	if !receipt.MatchesJournal(j) {
		t.Fatal("synthetic rollback must be a real Guardian outcome")
	}
	promoted := previous
	promoted.Generation, promoted.ActiveSlot, promoted.PreviousSlot = 12, "b", "a"
	promoted.Operation, promoted.Reason, promoted.UpdatedAt = edgegroupfront.ActivationOperationPromote, "advance node Worker using journal "+j.Digest, start.Add(2*time.Second)
	promoted.BundleGeneration, promoted.WorkerSourceCommit, promoted.WorkerImageDigest = j.Candidate.BundleGeneration, j.Candidate.WorkerSourceCommit, j.Candidate.WorkerImageDigest
	promotion := releaseguardian.NodeWorkerResult{Schema: receipt.Schema, JournalDigest: j.Digest, Outcome: "verified", Activation: promoted,
		Witness: &releaseguardian.NodeWorkerWitness{Schema: releaseguardian.NodeWorkerWitnessSchema, JournalDigest: j.Digest, ObservedAt: promoted.UpdatedAt.Add(time.Second),
			Held:  releaseguardian.NodeWorkerConnection{FrontUID: j.Recipient.PodUID, ID: "retained", StartedAt: j.CreatedAt, Slot: "a", Target: "192.0.2.1:18443"},
			Fresh: releaseguardian.NodeWorkerConnection{FrontUID: j.Recipient.PodUID, ID: "fresh", StartedAt: promoted.UpdatedAt, Slot: "b", Target: "192.0.2.1:38443"},
		}}
	if !promotion.MatchesJournal(j) {
		t.Fatal("synthetic cutover must be a real Guardian outcome")
	}
	record := func(name, kind, key string, data any) corev1.ConfigMap {
		raw, err := declarativerelease.CanonicalJSON(data)
		if err != nil {
			t.Fatal(err)
		}
		immutable := true
		return corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, UID: "fixture-uid", ResourceVersion: "1", Labels: map[string]string{
			"app.kubernetes.io/managed-by": "fugue-release-guardian", "fugue.pro/authority-store": "true", "fugue.pro/group": group,
			"fugue.pro/authority-kind": kind}}, Immutable: &immutable, Data: map[string]string{key: string(raw)}}
	}
	const base = "/api/v1/namespaces/system"
	node := corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: j.Recipient.Node, UID: "00000000-0000-4000-8000-000000000002"}}
	service := corev1.Service{ObjectMeta: metav1.ObjectMeta{UID: "00000000-0000-4000-8000-000000000003"}}
	t.Run("independent Worker journal does not block recovery of another node", func(t *testing.T) {
		name := releaseguardian.FrontRepairFenceName(group)
		cm := record(name, "node-worker-journal", "journal.json", j)
		journal, err := json.Marshal(j)
		if err != nil {
			t.Fatal(err)
		}
		cm.Data["journal.json"] = string(journal)
		kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.URL.Path != base+"/configmaps/"+name {
				http.NotFound(w, req)
				return
			}
			_ = json.NewEncoder(w).Encode(cm)
		}))
		defer kube.Close()
		client := &clusterNodeClient{baseURL: kube.URL, client: kube.Client()}
		other := corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "edge-peer", UID: "peer-uid"}}
		replacement := corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: node.Name, UID: "replacement-uid"}}
		for _, tc := range []struct {
			node    *corev1.Node
			pending bool
		}{{&other, false}, {&node, true}, {&replacement, true}, {nil, true}} {
			got, err := edgeAuthorityTransitionPending(context.Background(), client, "system", group, tc.node)
			if err != nil || got != tc.pending {
				t.Fatalf("node %v: pending=%t, want %t: %v", tc.node, got, tc.pending, err)
			}
		}
	})
	for _, scenario := range []string{"verified", "reverted", "missing index", "changed index", "wrong generation", "tampered rollback", "replaced ingress", "foreign node UID"} {
		t.Run(scenario, func(t *testing.T) {
			a := record(releaseguardian.NodeWorkerArchiveName(j.Digest), "node-worker-archive", "journal.json", j)
			name := releaseguardian.NodeWorkerResultName(j.Digest)
			indexName := releaseguardian.NodeWorkerIndexName(group)
			r := receipt
			if scenario == "verified" {
				r = promotion
			}
			generation := r.Activation.Generation
			if scenario == "wrong generation" {
				generation++
			}
			index := record(indexName, "node-worker-index", "index.json", releaseguardian.NodeWorkerIndex{
				Schema: "edge-node-worker-index/v1", Group: group, Nodes: map[string]releaseguardian.NodeWorkerIndexHead{
					j.NodeUID: {JournalDigest: j.Digest, Generation: generation},
				},
			})
			index.Immutable = nil
			indexReads := 0
			if scenario == "tampered rollback" {
				r.Activation.WorkerImageDigest = digest("0")
			}
			kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				var reply any
				switch req.URL.Path {
				case base + "/configmaps/" + releaseguardian.NodeWorkerModeName(group):
					reply = record(releaseguardian.NodeWorkerModeName(group), "node-worker-mode", "mode.json", map[string]string{"schema": "edge-node-worker-mode/v1", "group": group})
				case base + "/configmaps/" + indexName:
					if scenario == "missing index" {
						http.NotFound(w, req)
						return
					}
					indexReads++
					if scenario == "changed index" && indexReads > 1 {
						index.ResourceVersion = "2"
					}
					reply = index
				case base + "/configmaps/" + a.Name:
					reply = a
				case base + "/configmaps/" + name:
					reply = record(name, "node-worker-result", "result.json", r)
				default:
					t.Errorf("unexpected read: %s", req.URL)
					http.NotFound(w, req)
					return
				}
				_ = json.NewEncoder(w).Encode(reply)
			}))
			defer kube.Close()
			n, svc := node, service
			if scenario == "replaced ingress" {
				svc.UID = "replacement-ingress"
			}
			if scenario == "foreign node UID" {
				n.UID = "replacement-node"
			}
			client := &clusterNodeClient{client: kube.Client(), baseURL: kube.URL, bearerToken: "metadata-reader"}
			got, err := (&Server{}).capacityNodeWorkerTarget(t.Context(), client, base, n, svc, group)
			switch scenario {
			case "verified":
				if err != nil || got.journalDigest != j.Digest || got.releaseDigest != j.ReleaseDigest || got.target.FrontGeneration != 12 || got.target.Slot != "b" {
					t.Fatal("did not recover the committed release", got, err)
				}
			case "reverted":
				if err != nil || got.journalDigest != j.Digest || got.releaseDigest != "" || got.target.FrontGeneration != 13 || got.target.Slot != "a" {
					t.Fatal("rollback did not restore the predecessor or incorrectly attested the candidate", got, err)
				}
			case "foreign node UID":
				if err != nil || got != (nodeWorkerRecoveryAuthority{}) {
					t.Fatal("old physical node must not grant authority", got, err)
				}
			default:
				if err == nil {
					t.Fatal("unproven recovery history admitted")
				}
			}
		})
	}
}
