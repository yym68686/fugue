package diagnosticadmission

import (
	"context"
	"errors"
	"fmt"
	"fugue/internal/livediagnostics"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

var ErrAdmissionBusy = errors.New("diagnostic admission or target budget is occupied")

const admissionLease = "fugue-diagnostic-admission"

// AdmitJob serializes the bounded list-and-create operation across API replicas
// and direct CLI clients. The local request deadline is shorter than the lease;
// a failed holder cannot indefinitely block subsequent observation sessions.
func AdmitJob(parent context.Context, client kubernetes.Interface, namespace string, job *batchv1.Job) (*batchv1.Job, error) {
	ctx, cancel := context.WithTimeout(parent, 8*time.Second)
	defer cancel()
	leases := client.CoordinationV1().Leases(namespace)
	now := metav1.NewMicroTime(time.Now().UTC())
	holder := job.Name
	ttl := int32(20)
	current, err := leases.Get(ctx, admissionLease, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		current, err = leases.Create(ctx, &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: admissionLease}, Spec: coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: &ttl, RenewTime: &now}}, metav1.CreateOptions{})
	} else if err == nil {
		if current.Spec.HolderIdentity != nil && *current.Spec.HolderIdentity != "" && current.Spec.RenewTime != nil && current.Spec.LeaseDurationSeconds != nil && current.Spec.RenewTime.Add(time.Duration(*current.Spec.LeaseDurationSeconds)*time.Second).After(now.Time) {
			return nil, ErrAdmissionBusy
		}
		current.Spec.HolderIdentity = &holder
		current.Spec.RenewTime = &now
		current.Spec.LeaseDurationSeconds = &ttl
		current, err = leases.Update(ctx, current, metav1.UpdateOptions{})
	}
	if apierrors.IsAlreadyExists(err) || apierrors.IsConflict(err) {
		return nil, ErrAdmissionBusy
	}
	if err != nil {
		return nil, fmt.Errorf("reserve diagnostic admission: %w", err)
	}
	defer func() {
		releaseCtx, stop := context.WithTimeout(context.Background(), 2*time.Second)
		defer stop()
		if current == nil {
			return
		}
		empty := ""
		current.Spec.HolderIdentity = &empty
		// ResourceVersion prevents an expired owner from releasing a newer lease.
		_, _ = leases.Update(releaseCtx, current, metav1.UpdateOptions{})
	}()
	jobs, err := client.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{LabelSelector: livediagnostics.ManagedByLabel + "=" + livediagnostics.ManagedByValue})
	if err != nil {
		return nil, err
	}
	active := 0
	for _, existing := range jobs.Items {
		if existing.Status.Succeeded > 0 || existing.Status.Failed > 0 {
			continue
		}
		active++
		if sameDiagnosticTarget(existing, *job) {
			return nil, ErrAdmissionBusy
		}
	}
	if active >= livediagnostics.MaxActiveGlobal {
		return nil, ErrAdmissionBusy
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return client.BatchV1().Jobs(namespace).Create(ctx, job, metav1.CreateOptions{})
}
func sameDiagnosticTarget(a, b batchv1.Job) bool {
	if a.Labels[livediagnostics.AppIDLabel] != "" && a.Labels[livediagnostics.AppIDLabel] == b.Labels[livediagnostics.AppIDLabel] {
		return true
	}
	if a.Labels[livediagnostics.TargetTypeLabel] != b.Labels[livediagnostics.TargetTypeLabel] {
		return false
	}
	switch livediagnostics.TargetType(a.Labels[livediagnostics.TargetTypeLabel]) {
	case livediagnostics.TargetNode:
		return a.Annotations[livediagnostics.TargetNodeAnnotation] == b.Annotations[livediagnostics.TargetNodeAnnotation]
	case livediagnostics.TargetNodeProcess:
		return a.Annotations[livediagnostics.TargetNodeAnnotation] == b.Annotations[livediagnostics.TargetNodeAnnotation] && a.Annotations[livediagnostics.TargetProcessAnnotation] == b.Annotations[livediagnostics.TargetProcessAnnotation]
	default:
		return a.Annotations[livediagnostics.TargetPodUIDAnnotation] != "" && a.Annotations[livediagnostics.TargetPodUIDAnnotation] == b.Annotations[livediagnostics.TargetPodUIDAnnotation] && a.Annotations[livediagnostics.TargetContainerAnnotation] == b.Annotations[livediagnostics.TargetContainerAnnotation]
	}
}
