package releaseguardian

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type standbyRestoreActivator struct {
	testFrontActivator
	restore *testFrontTransaction
}

func (activator standbyRestoreActivator) BeginRestore(context.Context, CurrentAuthority) (FrontAuthorityTransaction, error) {
	return activator.restore, nil
}

func TestCompletedStandbyRollbackStillRequiresExactAuthorityIdentity(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*FrontAuthorityReceipt)
	}{
		{"exact standby", nil},
		{"wrong source", func(r *FrontAuthorityReceipt) { r.TargetWorkerSourceSHA = strings.Repeat("9", 40) }},
		{"wrong image", func(r *FrontAuthorityReceipt) { r.TargetWorkerImageDigest = "sha256:" + strings.Repeat("9", 64) }},
		{"wrong slot", func(r *FrontAuthorityReceipt) { r.TargetSlot = r.PreviousSlot }},
		{"wrong generation", func(r *FrontAuthorityReceipt) { r.TargetGeneration++ }},
		{"wrong previous bundle", func(r *FrontAuthorityReceipt) { r.PreviousBundleGeneration = "other.p23.r7" }},
		{"old standby", func(r *FrontAuthorityReceipt) { r.TargetBundleGeneration = "standby.p19.r0" }},
		{"regressed recovery", func(r *FrontAuthorityReceipt) { r.TargetBundleGeneration = "standby.p24.r6" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			group := "edge-group-restore-test"
			client := fake.NewSimpleClientset()
			store, err := NewAuthorityStore(client, "control")
			if err != nil {
				t.Fatal(err)
			}
			current := CurrentAuthority{APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: group,
				CurrentRecordDigest: otherDigest, CurrentWorkerSlot: AuthoritySlotA, CurrentFrontGeneration: 11,
				CurrentBundleGeneration: "candidate.p23.r7", CurrentWorkerSourceSHA: strings.Repeat("2", 40), CurrentWorkerImageDigest: otherDigest,
				PreviousRecordDigest: testDigest, PreviousWorkerSlot: AuthoritySlotB, PreviousFrontGeneration: 10,
				PreviousBundleGeneration: "previous.p20.r7", PreviousWorkerSourceSHA: testSHA, PreviousWorkerImageDigest: testDigest, AuthorityEpoch: 5}
			if _, _, err := store.SwitchCurrent(ctx, current, "", ""); err != nil {
				t.Fatal(err)
			}
			// fake clients do not assign Kubernetes UID/resourceVersion themselves.
			cm, err := client.CoreV1().ConfigMaps("control").Get(ctx, currentAuthorityName(group), metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			cm.UID, cm.ResourceVersion = "restore-current", "30"
			if _, err := client.CoreV1().ConfigMaps("control").Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			tx := &testFrontTransaction{receipt: FrontAuthorityReceipt{GroupID: group,
				PreviousSlot: current.CurrentWorkerSlot, PreviousGeneration: 11, PreviousBundleGeneration: current.CurrentBundleGeneration,
				PreviousWorkerSourceSHA: current.CurrentWorkerSourceSHA, PreviousWorkerImageDigest: current.CurrentWorkerImageDigest,
				TargetSlot: current.PreviousWorkerSlot, TargetGeneration: 12, TargetBundleGeneration: "standby.p24.r0",
				TargetWorkerSourceSHA: current.PreviousWorkerSourceSHA, TargetWorkerImageDigest: current.PreviousWorkerImageDigest}}
			if test.change != nil {
				test.change(&tx.receipt)
			}
			controller, err := NewAuthorityControllerWithActivators(testAuthorityDecisionStore{AuthorityStore: store},
				map[string]CandidateCanaryVerifier{group: {KeyID: "candidate-canary-v1", Key: make([]byte, 32)}},
				map[string]FrontAuthorityActivator{group: standbyRestoreActivator{restore: tx}})
			if err != nil {
				t.Fatal(err)
			}
			receipt, restoreErr := controller.Revert(ctx, group, current.CurrentRecordDigest, testDigest)
			live, _, _, err := store.LoadCurrent(ctx, group)
			if err != nil {
				t.Fatal(err)
			}
			if test.change == nil {
				if restoreErr != nil || receipt.Validate() != nil || !tx.committed || tx.rolledBack || live.CurrentWorkerSlot != AuthoritySlotB || live.CurrentBundleGeneration != tx.receipt.TargetBundleGeneration {
					t.Fatalf("completed standby rollback not recorded: live=%+v receipt=%+v err=%v", live, receipt, restoreErr)
				}
			} else if restoreErr == nil || live != current || tx.committed || !tx.rolledBack {
				t.Fatalf("invalid rollback changed authority: live=%+v err=%v", live, restoreErr)
			}
		})
	}
}
