package controller

import (
	"context"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"fugue/internal/model"
	"fugue/internal/releaseproof"
	"fugue/internal/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
)

type rolloutCapacityDomainLock struct {
	slot   chan struct{}
	mu     sync.Mutex
	holder string
}

func (s *Service) lockManagedRolloutCapacityDomain(scheduling runtime.SchedulingConstraints) func() {
	unlock, _ := s.lockManagedRolloutCapacityDomainContext(context.Background(), scheduling, "")
	return unlock
}

func (s *Service) lockManagedRolloutCapacityDomainContext(ctx context.Context, scheduling runtime.SchedulingConstraints, operationID string) (func(), error) {
	key := managedRolloutCapacityDomainKey(scheduling)
	value, _ := s.managedRolloutCapacityLocks.LoadOrStore(key, &rolloutCapacityDomainLock{slot: make(chan struct{}, 1)})
	lock := value.(*rolloutCapacityDomainLock)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case lock.slot <- struct{}{}:
	default:
		started := time.Now()
		lock.mu.Lock()
		holder := lock.holder
		lock.mu.Unlock()
		if s.Logger != nil {
			s.Logger.Printf("rollout capacity lock waiting operation=%s observed_holder=%s", operationID, holder)
		}
		select {
		case lock.slot <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if s.Logger != nil {
			s.Logger.Printf("rollout capacity lock acquired operation=%s wait_ms=%d", operationID, time.Since(started).Milliseconds())
		}
	}
	lock.mu.Lock()
	if err := ctx.Err(); err != nil {
		<-lock.slot
		lock.mu.Unlock()
		return nil, err
	}
	lock.holder = operationID
	lock.mu.Unlock()
	return sync.OnceFunc(func() {
		lock.mu.Lock()
		lock.holder = ""
		<-lock.slot
		lock.mu.Unlock()
	}), nil
}

type rolloutCapacityHandoffKey struct{}

type rolloutCapacityHandoff struct {
	release              func(string)
	done                 bool
	lastInventoryAttempt time.Time
}

func withRolloutCapacityHandoff(ctx context.Context, release func(string)) context.Context {
	return context.WithValue(ctx, rolloutCapacityHandoffKey{}, &rolloutCapacityHandoff{release: release})
}

func canHandOffRolloutCapacity(op model.Operation, previous, next model.App) bool {
	return op.Type == model.OperationTypeDeploy &&
		releaseproof.OrdinarySingletonDrainEligible(previous.Spec) && releaseproof.OrdinarySingletonDrainEligible(next.Spec) &&
		previous.Spec.RuntimeID == next.Spec.RuntimeID
}

// Handoff is an optimization, never a readiness or capacity override. Only
// release after a full singleton cohort is bound and present in the fresh
// inventory that the next contender's capacity preflight uses. Old terminating
// Pods remain chargeable in that inventory.
func (h *rolloutCapacityHandoff) try(ctx context.Context, client *kubeClient, namespace string, dep kubeDeployment, snapshot kubePodList) {
	if h.done || dep.Metadata.UID == "" || dep.Metadata.ResourceVersion == "" || dep.Metadata.Namespace != namespace ||
		dep.Metadata.DeletionTimestamp != "" || dep.Metadata.Generation <= 0 || dep.Status.ObservedGeneration < dep.Metadata.Generation ||
		dep.Spec.Replicas == nil || *dep.Spec.Replicas != 1 || dep.Status.Replicas != 1 || dep.Status.UpdatedReplicas != 1 ||
		dep.Status.AvailableReplicas != 1 || dep.Status.UnavailableReplicas != 0 ||
		!deploymentNeedsZeroDowntimeSurgeCapacity(dep) || !kubeIntOrStringEqual(dep.Spec.Strategy.RollingUpdate.MaxSurge, 1) ||
		snapshot.Metadata.Continue != "" || snapshot.Metadata.ResourceVersion == "" {
		return
	}
	var pod kubePod
	key := deploymentReleaseKey(dep)
	if key == "" || dep.Spec.Template.Metadata.Annotations[runtime.FugueAnnotationReleaseKey] != key {
		return
	}
	for _, p := range snapshot.Items {
		if p.Metadata.Annotations[runtime.FugueAnnotationReleaseKey] != key || managedPostgresPodFinished(p) {
			continue
		}
		if pod.ObservedUID != "" || p.ObservedUID == "" || p.Spec.NodeName == "" || p.Spec.HostNetwork ||
			!podMatchesDeploymentTemplate(p, dep) {
			return
		}
		pod = p
	}
	var owner kubePodOwnerReference
	for _, ref := range pod.ObservedOwnerReferences {
		if !ref.Controller {
			continue
		}
		if owner.UID != "" || ref.APIVersion != "apps/v1" || ref.Kind != "ReplicaSet" ||
			ref.UID == "" || len(validation.IsDNS1123Subdomain(ref.Name)) != 0 {
			return
		}
		owner = ref
	}
	if owner.UID == "" {
		return
	}
	// This proof is optional. Do not repeatedly fetch a cluster-wide list
	// on every watch event when the latest inventory was inconclusive.
	now := time.Now()
	if !h.lastInventoryAttempt.IsZero() && now.Sub(h.lastInventoryAttempt) < 5*time.Second {
		return
	}
	h.lastInventoryAttempt = now
	proofCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	path := "/apis/apps/v1/namespaces/" + url.PathEscape(namespace) + "/replicasets/" + url.PathEscape(owner.Name)
	var rs releaseproof.ReplicaSet
	if _, err := client.doJSON(proofCtx, http.MethodGet, path, nil, &rs); err != nil {
		return
	}
	ref, ok := releaseproof.SingleControllerOwner(rs.Metadata.OwnerReferences, "apps/v1", "Deployment")
	if !ok || ref.UID != dep.Metadata.UID || ref.Name != dep.Metadata.Name || rs.Metadata.Namespace != namespace ||
		rs.Metadata.Name != owner.Name || rs.Metadata.UID != owner.UID || rs.Metadata.DeletionTimestamp != "" ||
		rs.Spec.Template.Metadata.Annotations.ReleaseKey != key {
		return
	}
	inventory, ok, err := client.listAllPods(proofCtx)
	if err != nil || !ok {
		return
	}
	count := 0
	for _, observed := range inventory {
		if observed.ObservedUID == pod.ObservedUID && observed.Spec.NodeName == pod.Spec.NodeName &&
			!observed.Spec.HostNetwork && podMatchesDeploymentTemplate(observed, dep) {
			count++
		}
	}
	if count != 1 {
		return
	}
	confirmed, found, err := client.getDeployment(proofCtx, namespace, dep.Metadata.Name)
	if err != nil || !found || confirmed.Metadata.UID != dep.Metadata.UID ||
		confirmed.Metadata.ResourceVersion != dep.Metadata.ResourceVersion || proofCtx.Err() != nil {
		return
	}
	h.done = true
	h.release("surge_accounted")
}

func managedRolloutCapacityDomainKey(scheduling runtime.SchedulingConstraints) string {
	parts := make([]string, 0, len(scheduling.NodeSelector)+len(scheduling.Tolerations))
	for key, value := range scheduling.NodeSelector {
		parts = append(parts, "selector:"+strings.TrimSpace(key)+"="+strings.TrimSpace(value))
	}
	for _, toleration := range scheduling.Tolerations {
		parts = append(parts, "toleration:"+strings.Join([]string{
			strings.TrimSpace(toleration.Key),
			strings.TrimSpace(toleration.Operator),
			strings.TrimSpace(toleration.Value),
			strings.TrimSpace(toleration.Effect),
		}, "="))
	}
	if len(parts) == 0 {
		return "cluster-default"
	}
	sort.Strings(parts)
	return strings.Join(parts, "\x00")
}
