package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"fugue/internal/declarativerelease"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
)

const (
	frontRepairComponentLeaseSeconds = int32(900) // the declarative executor's component lease protocol
	frontRepairLeaseSafetyMargin     = 15 * time.Second
	frontRepairMutationTimeout       = 10 * time.Second
	frontRepairLeaseClockAllowance   = 5 * time.Second
)

type frontRepairLeaseKey struct {
	name, label, value string
	maxDuration        int32
}

type frontRepairLeaseClaim struct {
	key frontRepairLeaseKey
	uid string
	rv  string
}

type frontRepairLeasePair struct {
	client           kubernetes.Interface
	namespace, group string
	holder           string
	now              func() time.Time
	claims           []frontRepairLeaseClaim
}

// Acquire the existing Worker release lock first, then group authority, just
// like member recovery. The selected release must come from the verified
// immutable production plan, not free-form lock names in the repair request.
// Leases authorize serialization, never membership or repair intent.
func acquireFrontRepairLeases(ctx context.Context, client kubernetes.Interface, release declarativerelease.PlanRelease, group string, now func() time.Time) (*frontRepairLeasePair, error) {
	namespace := release.Workload.Namespace
	transition := release.Transition
	if client == nil || now == nil || len(validation.IsDNS1123Label(namespace)) != 0 ||
		len(validation.IsDNS1123Label(group)) != 0 || len(validation.IsDNS1123Label(release.ComponentID)) != 0 ||
		release.Concurrency != "fugue-production-"+release.ComponentID || transition == nil ||
		transition.Type != "edge-group-ab" || transition.EdgeGroupAB == nil || transition.EdgeGroupAB.GroupID != group {
		return nil, errors.New("Front repair lock names do not bind the selected Edge release")
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	pair := &frontRepairLeasePair{client: client, namespace: namespace, group: group, holder: "front-repair:" + hex.EncodeToString(nonce[:]), now: now}
	for _, key := range []frontRepairLeaseKey{
		{name: release.Concurrency, label: "fugue.pro/component", value: release.ComponentID, maxDuration: frontRepairComponentLeaseSeconds},
		{name: "fugue-authority-" + group, label: "fugue.pro/group", value: group, maxDuration: authorityLeaseSeconds},
	} {
		if err := pair.claim(ctx, key); err != nil {
			return nil, errors.Join(err, pair.release(ctx))
		}
	}
	if err := pair.requireHeld(ctx, group); err != nil {
		return nil, errors.Join(err, pair.release(ctx))
	}
	return pair, nil
}

func (pair *frontRepairLeasePair) claim(ctx context.Context, key frontRepairLeaseKey) error {
	leases := pair.client.CoordinationV1().Leases(pair.namespace)
	current, err := leases.Get(ctx, key.name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read existing Front repair Lease %s: %w", key.name, err)
	}
	now := pair.now().UTC()
	holder, expiry, err := frontRepairLeaseState(current, pair.namespace, key, now)
	if err != nil {
		return err
	}
	if holder != "" && !now.After(expiry.Add(frontRepairLeaseClockAllowance)) {
		return fmt.Errorf("Front repair Lease %s is held by another operation", key.name)
	}
	wanted := current.DeepCopy()
	stamp := metav1.NewMicroTime(now.Truncate(time.Microsecond))
	wanted.Spec.HolderIdentity, wanted.Spec.AcquireTime, wanted.Spec.RenewTime = stringPtr(pair.holder), &stamp, &stamp
	wanted.Spec.LeaseDurationSeconds = int32Ptr(key.maxDuration)
	actual, updateErr := leases.Update(ctx, wanted, metav1.UpdateOptions{})
	if updateErr != nil {
		// Do not retry Update. Read back our unique holder to reconcile a lost
		// acknowledgement, without claiming someone else's replacement object.
		readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		actual, err = leases.Get(readCtx, key.name, metav1.GetOptions{})
		if err != nil {
			return errors.Join(updateErr, err)
		}
	}
	holder, _, err = frontRepairLeaseState(actual, pair.namespace, key, pair.now().UTC())
	if err != nil || actual.UID != current.UID || holder != pair.holder || *actual.Spec.LeaseDurationSeconds != key.maxDuration {
		return errors.Join(fmt.Errorf("Front repair Lease %s acquisition is unproven", key.name), updateErr, err)
	}
	pair.claims = append(pair.claims, frontRepairLeaseClaim{key: key, uid: string(actual.UID), rv: actual.ResourceVersion})
	return nil
}

func frontRepairLeaseState(object *coordinationv1.Lease, namespace string, key frontRepairLeaseKey, now time.Time) (string, time.Time, error) {
	if object == nil || object.Name != key.name || object.Namespace != namespace || object.DeletionTimestamp != nil ||
		!frontRepairIdentity(string(object.UID)) || !frontRepairIdentity(object.ResourceVersion) || object.Labels[key.label] != key.value ||
		object.Spec.LeaseDurationSeconds == nil || *object.Spec.LeaseDurationSeconds <= 0 || *object.Spec.LeaseDurationSeconds > key.maxDuration {
		return "", time.Time{}, fmt.Errorf("Front repair Lease %s identity is invalid", key.name)
	}
	holder := valueOrEmpty(object.Spec.HolderIdentity)
	if holder == "" {
		return holder, time.Time{}, nil
	}
	renew := object.Spec.RenewTime
	if renew == nil || renew.IsZero() || renew.After(now.Add(frontRepairLeaseClockAllowance)) {
		return "", time.Time{}, fmt.Errorf("Front repair Lease %s has no reliable expiry", key.name)
	}
	return holder, renew.Add(time.Duration(*object.Spec.LeaseDurationSeconds) * time.Second), nil
}

func (pair *frontRepairLeasePair) requireHeld(ctx context.Context, group string) error {
	if pair == nil || pair.client == nil || pair.now == nil || pair.group != group || pair.holder == "" || len(pair.claims) != 2 {
		return errors.New("Front repair does not hold the required Lease pair")
	}
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	leases := pair.client.CoordinationV1().Leases(pair.namespace)
	var earliest time.Time
	for _, claim := range pair.claims {
		actual, err := leases.Get(readCtx, claim.key.name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		holder, expiry, err := frontRepairLeaseState(actual, pair.namespace, claim.key, pair.now().UTC())
		if err != nil || string(actual.UID) != claim.uid || actual.ResourceVersion != claim.rv || holder != pair.holder {
			return errors.Join(fmt.Errorf("Front repair Lease %s changed", claim.key.name), err)
		}
		if earliest.IsZero() || expiry.Before(earliest) {
			earliest = expiry
		}
	}
	if !pair.now().UTC().Add(frontRepairLeaseSafetyMargin).Before(earliest) {
		return errors.New("Front repair Leases lack mutation time budget")
	}
	return ctx.Err()
}

func (pair *frontRepairLeasePair) mutationContext(ctx context.Context, group string) (context.Context, context.CancelFunc, error) {
	if err := pair.requireHeld(ctx, group); err != nil {
		return nil, nil, err
	}
	operation, cancel := context.WithTimeout(ctx, frontRepairMutationTimeout)
	return operation, cancel, nil
}

func (pair *frontRepairLeasePair) release(ctx context.Context) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	leases := pair.client.CoordinationV1().Leases(pair.namespace)
	var errs error
	for i := len(pair.claims) - 1; i >= 0; i-- {
		claim := pair.claims[i]
		current, err := leases.Get(cleanup, claim.key.name, metav1.GetOptions{})
		if err != nil || current == nil || string(current.UID) != claim.uid || current.ResourceVersion != claim.rv || valueOrEmpty(current.Spec.HolderIdentity) != pair.holder {
			errs = errors.Join(errs, fmt.Errorf("Front repair Lease %s cleanup CAS changed", claim.key.name), err)
			continue
		}
		current.Spec.HolderIdentity, current.Spec.AcquireTime, current.Spec.RenewTime = stringPtr(""), nil, nil
		if _, err := leases.Update(cleanup, current, metav1.UpdateOptions{}); err != nil {
			errs = errors.Join(errs, fmt.Errorf("Front repair Lease %s cleanup is unproven: %w", claim.key.name, err))
		}
	}
	return errs
}
