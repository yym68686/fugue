package diagnosticadmission

import (
	"context"
	"errors"
	"fmt"
	"fugue/internal/livediagnostics"
	"sync"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestAdmissionSerializesReplicasAndRespectsGlobalBudget(t *testing.T) {
	client := fake.NewSimpleClientset()
	var mu sync.Mutex
	var lease *coordinationv1.Lease
	revision := 0
	client.PrependReactor("*", "leases", func(action ktesting.Action) (bool, runtime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		resource := schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}
		switch action.GetVerb() {
		case "get":
			if lease == nil {
				return true, nil, apierrors.NewNotFound(resource, admissionLease)
			}
			return true, lease.DeepCopy(), nil
		case "create":
			if lease != nil {
				return true, nil, apierrors.NewAlreadyExists(resource, admissionLease)
			}
			lease = action.(ktesting.CreateAction).GetObject().(*coordinationv1.Lease).DeepCopy()
		case "update":
			next := action.(ktesting.UpdateAction).GetObject().(*coordinationv1.Lease)
			if lease == nil || next.ResourceVersion != lease.ResourceVersion {
				return true, nil, apierrors.NewConflict(resource, admissionLease, errors.New("revision changed"))
			}
			lease = next.DeepCopy()
		default:
			return false, nil, nil
		}
		revision++
		lease.ResourceVersion = fmt.Sprint(revision)
		return true, lease.DeepCopy(), nil
	})
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("diagnostic-%d", i), Labels: map[string]string{livediagnostics.ManagedByLabel: livediagnostics.ManagedByValue, livediagnostics.TargetTypeLabel: string(livediagnostics.TargetNode)}, Annotations: map[string]string{livediagnostics.TargetNodeAnnotation: fmt.Sprintf("node-%d", i)}}}
			for attempt := 0; attempt < 12; attempt++ {
				if _, err := AdmitJob(context.Background(), client, "system-test", job); err == nil {
					return
				} else if !errors.Is(err, ErrAdmissionBusy) {
					t.Errorf("unexpected admission error: %v", err)
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(i)
	}
	wg.Wait()
	jobs, err := client.BatchV1().Jobs("system-test").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != livediagnostics.MaxActiveGlobal {
		t.Fatalf("expected exactly %d admitted sessions, got %d", livediagnostics.MaxActiveGlobal, len(jobs.Items))
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "" {
		t.Fatal("admission lease remained held")
	}
}
