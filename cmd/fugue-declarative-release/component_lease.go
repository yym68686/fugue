package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordinationclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// The longest bounded executor is the 12-minute Edge A/B transition. Keep the
// lease valid for that entire process lifetime without introducing a second
// renewal state machine; a crashed runner becomes reclaimable after 15 minutes.
const componentLeaseDurationSeconds int64 = 900

type heldComponentLease struct {
	Namespace       string
	Name            string
	UID             string
	ResourceVersion string
	Holder          string
}

type componentLeaseCoordinator struct {
	client coordinationclient.CoordinationV1Interface
	now    func() time.Time
}

func newComponentLeaseCoordinator() (*componentLeaseCoordinator, error) {
	config, err := loadComponentLeaseClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load Kubernetes client config for component lease: %w", err)
	}
	client, err := coordinationclient.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes client for component lease: %w", err)
	}
	return &componentLeaseCoordinator{client: client, now: time.Now}, nil
}

func loadComponentLeaseClientConfig() (*rest.Config, error) {
	if config, err := rest.InClusterConfig(); err == nil && config != nil && strings.TrimSpace(config.Host) != "" {
		return config, nil
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, err
	}
	if config == nil || strings.TrimSpace(config.Host) == "" {
		return nil, errors.New("Kubernetes client config is empty")
	}
	return config, nil
}

func componentLeaseHolder(release declarativerelease.PlanRelease, configSHA string) (string, error) {
	if strings.TrimSpace(os.Getenv("FUGUE_COMPONENT_LEASE_OWNER")) == "guardian" {
		podUID := strings.TrimSpace(os.Getenv("FUGUE_RELEASE_GUARDIAN_POD_UID"))
		recordDigest := strings.TrimSpace(os.Getenv("FUGUE_RELEASE_GUARDIAN_RECORD_DIGEST"))
		if podUID == "" || len(podUID) > 80 || strings.ContainsAny(podUID, "\r\n:\x00") ||
			!strings.HasPrefix(recordDigest, "sha256:") || len(recordDigest) != 71 || strings.Trim(recordDigest[7:], "0123456789abcdef") != "" ||
			len(configSHA) != 40 || strings.Trim(configSHA, "0123456789abcdef") != "" {
			return "", errors.New("Guardian component lease identity is invalid")
		}
		holder := "guardian:" + podUID + ":" + recordDigest[7:23] + ":" + release.ComponentID
		if len(holder) > 253 {
			return "", errors.New("Guardian component lease identity is oversized")
		}
		return holder, nil
	}
	repository := strings.TrimSpace(os.Getenv("GITHUB_REPOSITORY"))
	runID := strings.TrimSpace(os.Getenv("GITHUB_RUN_ID"))
	attempt := strings.TrimSpace(os.Getenv("GITHUB_RUN_ATTEMPT"))
	if repository == "" || strings.ContainsAny(repository, "\r\n:\x00") ||
		!decimalIdentity(runID) || !decimalIdentity(attempt) || len(configSHA) != 40 || strings.Trim(configSHA, "0123456789abcdef") != "" {
		return "", errors.New("GitHub component lease identity is invalid")
	}
	holder := "github:" + repository + ":" + runID + ":" + attempt + ":" + configSHA + ":" + release.ComponentID
	if len(holder) > 253 {
		return "", errors.New("GitHub component lease identity is oversized")
	}
	return holder, nil
}

func decimalIdentity(value string) bool {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

func (coordinator *componentLeaseCoordinator) acquire(ctx context.Context, release declarativerelease.PlanRelease, configSHA string) (heldComponentLease, error) {
	if coordinator == nil || coordinator.client == nil || coordinator.now == nil {
		return heldComponentLease{}, errors.New("component lease coordinator is unavailable")
	}
	holder, err := componentLeaseHolder(release, configSHA)
	if err != nil {
		return heldComponentLease{}, err
	}
	resource := coordinator.client.Leases(release.Workload.Namespace)
	current, err := resource.Get(ctx, release.Concurrency, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		created, createErr := resource.Create(ctx, newComponentLease(release, holder, coordinator.now().UTC()), metav1.CreateOptions{})
		if createErr != nil {
			return heldComponentLease{}, fmt.Errorf("create component lease: %w", createErr)
		}
		return heldLeaseFromObject(created, holder)
	}
	if err != nil {
		return heldComponentLease{}, fmt.Errorf("read component lease: %w", err)
	}
	currentHolder, _, renewTime, parseErr := parseComponentLease(current)
	if parseErr != nil {
		return heldComponentLease{}, parseErr
	}
	if currentHolder == holder {
		return heldLeaseFromObject(current, holder)
	}
	if currentHolder != "" && coordinator.now().UTC().Before(renewTime.Add(time.Duration(componentLeaseDurationSeconds)*time.Second)) {
		return heldComponentLease{}, fmt.Errorf("component lease is held by another release: %s", currentHolder)
	}
	updated := current.DeepCopy()
	setComponentLeaseSpec(updated, holder, coordinator.now().UTC())
	updated, err = resource.Update(ctx, updated, metav1.UpdateOptions{})
	if err != nil {
		return heldComponentLease{}, fmt.Errorf("acquire expired component lease with resourceVersion CAS: %w", err)
	}
	return heldLeaseFromObject(updated, holder)
}

func (coordinator *componentLeaseCoordinator) release(ctx context.Context, held heldComponentLease) error {
	if coordinator == nil || coordinator.client == nil || coordinator.now == nil || held.Namespace == "" || held.Name == "" || held.UID == "" || held.ResourceVersion == "" || held.Holder == "" {
		return errors.New("held component lease identity is invalid")
	}
	resource := coordinator.client.Leases(held.Namespace)
	current, err := resource.Get(ctx, held.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read component lease before release: %w", err)
	}
	currentHolder, _, _, err := parseComponentLease(current)
	if err != nil {
		return err
	}
	if string(current.GetUID()) != held.UID || current.GetResourceVersion() != held.ResourceVersion || currentHolder != held.Holder {
		return errors.New("component lease changed before release")
	}
	updated := current.DeepCopy()
	setComponentLeaseSpec(updated, "", coordinator.now().UTC())
	if _, err := resource.Update(ctx, updated, metav1.UpdateOptions{}); err == nil {
		return nil
	}
	reconciled, getErr := resource.Get(ctx, held.Name, metav1.GetOptions{})
	if getErr != nil {
		return fmt.Errorf("reconcile component lease release: %w", getErr)
	}
	reconciledHolder, _, _, parseErr := parseComponentLease(reconciled)
	if parseErr == nil && reconciledHolder == "" {
		return nil
	}
	return errors.New("component lease release result is unknown")
}

func newComponentLease(release declarativerelease.PlanRelease, holder string, now time.Time) *coordinationv1.Lease {
	value := &coordinationv1.Lease{
		TypeMeta: metav1.TypeMeta{APIVersion: coordinationv1.SchemeGroupVersion.String(), Kind: "Lease"},
		ObjectMeta: metav1.ObjectMeta{
			Name: release.Concurrency, Namespace: release.Workload.Namespace,
			Labels: map[string]string{"app.kubernetes.io/managed-by": "fugue-declarative-release", "fugue.pro/component": release.ComponentID},
		},
	}
	setComponentLeaseSpec(value, holder, now)
	return value
}

func setComponentLeaseSpec(value *coordinationv1.Lease, holder string, now time.Time) {
	duration := int32(componentLeaseDurationSeconds)
	renewTime := metav1.NewMicroTime(now.UTC().Truncate(time.Microsecond))
	spec := coordinationv1.LeaseSpec{
		HolderIdentity:       &holder,
		LeaseDurationSeconds: &duration,
		RenewTime:            &renewTime,
	}
	if holder != "" {
		acquireTime := renewTime
		spec.AcquireTime = &acquireTime
	}
	value.Spec = spec
}

func parseComponentLease(value *coordinationv1.Lease) (string, int64, time.Time, error) {
	if value == nil || value.GetUID() == "" || value.GetResourceVersion() == "" {
		return "", 0, time.Time{}, errors.New("component lease metadata is invalid")
	}
	if value.Spec.LeaseDurationSeconds == nil {
		return "", 0, time.Time{}, errors.New("component lease spec is invalid")
	}
	duration := int64(*value.Spec.LeaseDurationSeconds)
	if duration != componentLeaseDurationSeconds {
		return "", 0, time.Time{}, errors.New("component lease spec is invalid")
	}
	holder := ""
	if value.Spec.HolderIdentity != nil {
		holder = *value.Spec.HolderIdentity
	}
	// A released Lease has no holder and may have no timestamps.  The holder is
	// the concurrency authority; timestamps are required only while it is held.
	if holder == "" {
		return "", duration, time.Time{}, nil
	}
	if value.Spec.RenewTime == nil {
		return "", 0, time.Time{}, errors.New("component lease spec is invalid")
	}
	return holder, duration, value.Spec.RenewTime.Time, nil
}

func heldLeaseFromObject(value *coordinationv1.Lease, holder string) (heldComponentLease, error) {
	actual, _, _, err := parseComponentLease(value)
	if err != nil || actual != holder {
		return heldComponentLease{}, errors.New("acquired component lease readback is invalid")
	}
	return heldComponentLease{Namespace: value.GetNamespace(), Name: value.GetName(), UID: string(value.GetUID()), ResourceVersion: value.GetResourceVersion(), Holder: holder}, nil
}
