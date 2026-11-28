package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"fugue/internal/productioncontrol"
	"fugue/internal/releaseguardian/storageauditcontrol"
	"fugue/internal/storageaudit"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

func shadowStorageAudit(ctx context.Context, paths []string, retired bool, out io.Writer) error {
	raw, err := readFile(paths[3], 16<<10, false)
	if err != nil {
		return err
	}
	var spec productioncontrol.StorageAuditShadowBinding
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&spec) != nil || d.Decode(new(any)) != io.EOF {
		return errors.New("invalid storage audit shadow specification")
	}
	if err := spec.Validate(); err != nil {
		return err
	}
	raw, err = readFile(paths[4], 4096, false)
	if err != nil {
		return err
	}
	target, err := parseShadowPolicy(raw)
	if err != nil {
		return err
	}
	cfg, err := readKubeObservationConfig(paths[1], paths[2])
	if err != nil {
		return err
	}
	client, err := storageauditcontrol.NewReader(cfg, spec)
	if err != nil {
		return err
	}
	provider, db, err := openStorageAuditObserver(paths[0])
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	report, err := readStorageAuditShadow(ctx, client, spec, target, retired, func(ctx context.Context) (storageaudit.OwnerObservation, error) {
		return provider.Observe(ctx, db)
	})
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(report)
}

func parseShadowPolicy(raw []byte) (storageaudit.Policy, error) {
	policy, err := storageaudit.ParsePolicy(raw)
	// The legacy publisher and the PostgreSQL policy contract agree at whole seconds.
	if err != nil || policy.EffectiveFrom.Nanosecond() != 0 {
		return storageaudit.Policy{}, errors.New("invalid storage audit shadow policy or effective time precision")
	}
	return policy, nil
}

func readStorageAuditShadow(ctx context.Context, client corev1.CoreV1Interface, spec productioncontrol.StorageAuditShadowBinding,
	target storageaudit.Policy, retired bool, observe func(context.Context) (storageaudit.OwnerObservation, error)) (productioncontrol.StorageAuditShadow, error) {
	readPublished := func() (storageaudit.Policy, string, error) {
		raw, present, version, err := storageauditcontrol.ReadProjection(ctx, client, spec)
		if err != nil {
			return storageaudit.Policy{}, "", err
		}
		if retired {
			if present {
				return storageaudit.Policy{}, "", errors.New("legacy storage audit policy remains in the ConfigMap")
			}
			return storageaudit.Policy{}, version, nil
		}
		policy, err := parseShadowPolicy([]byte(raw))
		return policy, version, err
	}
	before, version, err := readPublished()
	if err != nil {
		return productioncontrol.StorageAuditShadow{}, err
	}
	database, err := observe(ctx)
	if err != nil {
		return productioncontrol.StorageAuditShadow{}, err
	}
	if database.Owner != "worker" || database.Activation == nil || database.PolicyDigest != database.Policy.Digest() ||
		(!retired && database.Policy != before) {
		return productioncontrol.StorageAuditShadow{}, errors.New("PostgreSQL and the published storage audit policy disagree")
	}
	if target == database.Policy {
		return productioncontrol.StorageAuditShadow{}, errors.New("storage audit candidate matches the live policy")
	}
	after, afterVersion, err := readPublished()
	if err != nil {
		return productioncontrol.StorageAuditShadow{}, err
	}
	if version != afterVersion || before != after {
		return productioncontrol.StorageAuditShadow{}, errors.New("published storage audit policy changed during PostgreSQL readback")
	}
	current := productioncontrol.StorageAuditPolicyVersion{Document: database.Policy, Digest: "sha256:" + database.PolicyDigest}
	return productioncontrol.StorageAuditShadow{
		Schema: productioncontrol.StorageAuditShadowSchema, Binding: spec, ConfigMapResourceVersion: version, PublishedPolicyRetired: retired,
		Provider: database.Provider, Owner: database.Owner, Activation: *database.Activation, Current: current,
		Target: productioncontrol.StorageAuditPolicyVersion{Document: target, Digest: "sha256:" + target.Digest()}, Rollback: current,
	}, nil
}
