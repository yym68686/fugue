package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

// The longest bounded executor is the 12-minute Edge A/B transition. Keep the
// lease valid for that entire process lifetime without introducing a second
// renewal state machine; a crashed runner becomes reclaimable after 15 minutes.
const componentLeaseDurationSeconds int64 = 900

var componentLeaseGVR = schema.GroupVersionResource{Group: "coordination.k8s.io", Version: "v1", Resource: "leases"}

type heldComponentLease struct {
	Namespace       string
	Name            string
	UID             string
	ResourceVersion string
	Holder          string
}

type componentLeaseCoordinator struct {
	client dynamic.Interface
	now    func() time.Time
}

func newComponentLeaseCoordinator() (*componentLeaseCoordinator, error) {
	config, err := clientcmd.BuildConfigFromFlags("", "")
	if err != nil {
		return nil, fmt.Errorf("load Kubernetes client config for component lease: %w", err)
	}
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes client for component lease: %w", err)
	}
	return &componentLeaseCoordinator{client: client, now: time.Now}, nil
}

func componentLeaseHolder(release declarativerelease.PlanRelease, configSHA string) (string, error) {
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
	resource := coordinator.client.Resource(componentLeaseGVR).Namespace(release.Workload.Namespace)
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
	resource := coordinator.client.Resource(componentLeaseGVR).Namespace(held.Namespace)
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

func newComponentLease(release declarativerelease.PlanRelease, holder string, now time.Time) *unstructured.Unstructured {
	value := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "coordination.k8s.io/v1",
		"kind":       "Lease",
		"metadata": map[string]any{
			"name": release.Concurrency, "namespace": release.Workload.Namespace,
			"labels": map[string]any{"app.kubernetes.io/managed-by": "fugue-declarative-release", "fugue.pro/component": release.ComponentID},
		},
	}}
	setComponentLeaseSpec(value, holder, now)
	return value
}

func setComponentLeaseSpec(value *unstructured.Unstructured, holder string, now time.Time) {
	spec := map[string]any{
		"holderIdentity":       holder,
		"leaseDurationSeconds": componentLeaseDurationSeconds,
		"renewTime":            now.UTC().Format(time.RFC3339Nano),
	}
	if holder != "" {
		spec["acquireTime"] = now.UTC().Format(time.RFC3339Nano)
	}
	value.Object["spec"] = spec
}

func parseComponentLease(value *unstructured.Unstructured) (string, int64, time.Time, error) {
	if value == nil || value.GetUID() == "" || value.GetResourceVersion() == "" {
		return "", 0, time.Time{}, errors.New("component lease metadata is invalid")
	}
	holder, _, _ := unstructured.NestedString(value.Object, "spec", "holderIdentity")
	duration, _, _ := unstructured.NestedInt64(value.Object, "spec", "leaseDurationSeconds")
	if duration == 0 {
		raw, _, _ := unstructured.NestedString(value.Object, "spec", "leaseDurationSeconds")
		duration, _ = strconv.ParseInt(raw, 10, 64)
	}
	renewRaw, _, _ := unstructured.NestedString(value.Object, "spec", "renewTime")
	renew, err := time.Parse(time.RFC3339Nano, renewRaw)
	if err != nil || duration != componentLeaseDurationSeconds {
		return "", 0, time.Time{}, errors.New("component lease spec is invalid")
	}
	return holder, duration, renew, nil
}

func heldLeaseFromObject(value *unstructured.Unstructured, holder string) (heldComponentLease, error) {
	actual, _, _, err := parseComponentLease(value)
	if err != nil || actual != holder {
		return heldComponentLease{}, errors.New("acquired component lease readback is invalid")
	}
	return heldComponentLease{Namespace: value.GetNamespace(), Name: value.GetName(), UID: string(value.GetUID()), ResourceVersion: value.GetResourceVersion(), Holder: holder}, nil
}
