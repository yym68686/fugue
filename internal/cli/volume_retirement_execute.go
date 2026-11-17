package cli

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"strconv"
	"strings"
	"time"
)

func retirementReceiptName(pvUID string) string {
	digest := sha256.Sum256([]byte(pvUID))
	return "fugue-volume-retire-" + hex.EncodeToString(digest[:12])
}

func retirementReadReceipt(cm map[string]any) (map[string]any, error) {
	decoder := json.NewDecoder(strings.NewReader(physicalString(physicalMap(cm, "data"), "receipt.json")))
	// Inode/device identities and block counts must survive a read/resume cycle
	// exactly, including integers larger than float64's exact range.
	decoder.UseNumber()
	var receipt map[string]any
	if err := decoder.Decode(&receipt); err != nil || receipt == nil {
		return nil, fmt.Errorf("invalid retirement receipt")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return nil, fmt.Errorf("unexpected trailing retirement receipt data")
	}
	return receipt, nil
}

func retirementDatabaseHealthy(cluster map[string]any) bool {
	status := physicalMap(cluster, "status")
	ready, err := strconv.ParseFloat(fmt.Sprint(status["readyInstances"]), 64)
	return err == nil && ready >= 1 && physicalString(status, "phase") == "Cluster in healthy state" && physicalString(status, "currentPrimary") != "" && physicalString(status, "currentPrimary") == physicalString(status, "targetPrimary")
}

func retirementLease(ctx context.Context, kube physicalKubeFunc, name string) (func(), error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	holder := hex.EncodeToString(nonce[:])
	lease, err := kube(ctx, nil, "get", "lease", name, "-o", "json", "--ignore-not-found")
	if err != nil {
		return nil, err
	}
	spec := physicalMap(lease, "spec")
	if physicalString(spec, "holderIdentity") != "" {
		renewed, e := time.Parse(time.RFC3339Nano, physicalString(spec, "renewTime"))
		duration, conversion := strconv.ParseFloat(fmt.Sprint(spec["leaseDurationSeconds"]), 64)
		if e != nil || conversion != nil || duration <= 0 || renewed.Add(time.Duration(duration)*time.Second).After(time.Now()) {
			return nil, fmt.Errorf("retirement is locked by another operation; read its receipt before retrying")
		}
	}
	verb := "replace"
	if len(lease) == 0 {
		verb = "create"
		lease = map[string]any{"apiVersion": "coordination.k8s.io/v1", "kind": "Lease", "metadata": map[string]any{"name": name}}
	}
	// The complete CLI operation is bounded to 15 minutes. Node work has its
	// own 10-minute alarm and a nonblocking flock, including after SSH loss.
	lease["spec"] = map[string]any{"holderIdentity": holder, "leaseDurationSeconds": 1800, "renewTime": time.Now().UTC().Format(time.RFC3339Nano)}
	if _, err = kube(ctx, lease, verb, "-f", "-", "-o", "json"); err != nil {
		return nil, fmt.Errorf("cannot acquire retirement lease: %w", err)
	}
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		current, err := kube(ctx, nil, "get", "lease", name, "-o", "json")
		if err != nil || physicalString(physicalMap(current, "spec"), "holderIdentity") != holder {
			return
		}
		physicalMap(current, "spec")["holderIdentity"] = ""
		_, _ = kube(ctx, current, "replace", "-f", "-", "-o", "json")
	}, nil
}

func retirementDelete(ctx context.Context, kube physicalKubeFunc, resource, name, namespace string, object map[string]any) error {
	meta := physicalMap(object, "metadata")
	uid, rv := physicalString(meta, "uid"), physicalString(meta, "resourceVersion")
	if uid == "" || rv == "" {
		return fmt.Errorf("refusing delete without UID and resourceVersion")
	}
	url := "/api/v1/"
	if namespace != "" {
		url += "namespaces/" + namespace + "/"
	}
	url += resource + "/" + name
	_, err := kube(ctx, map[string]any{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": map[string]any{"uid": uid, "resourceVersion": rv}}, "delete", "--raw", url, "-f", "-")
	return err
}

func retirementWait(ctx context.Context, check func() (bool, error)) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	for {
		ready, err := check()
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("retirement is waiting for normal Kubernetes protection; no finalizers were forced: %w", ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

func executeVolumeRetirement(ctx context.Context, kube physicalKubeFunc, host volumeHostFunc, preview retirementPreview, reason string) (result map[string]any, resultErr error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	p := preview.Plan
	if p.Schema != volumeRetirementSchema || retirementDigest(p) != preview.SHA256 || !preview.CanExecute || len(preview.Blockers) != 0 || strings.TrimSpace(reason) == "" {
		return nil, fmt.Errorf("invalid retirement preview or reason")
	}
	name := retirementReceiptName(p.PVUID)
	cm, err := kube(ctx, nil, "get", "configmap", name, "-o", "json", "--ignore-not-found")
	if err != nil {
		return nil, err
	}
	var receipt map[string]any
	if len(cm) != 0 {
		receipt, err = retirementReadReceipt(cm)
		if err != nil || physicalString(receipt, "plan_sha256") != preview.SHA256 {
			return nil, fmt.Errorf("an existing retirement owns this PV; resume with its original plan file")
		}
		if physicalString(receipt, "phase") == "completed" {
			receipt["already_completed"] = true
			return receipt, nil
		}
	} else if age := time.Since(p.ObservedAt); age < 0 || age > time.Hour {
		return nil, fmt.Errorf("initial execution requires a preview from the last hour")
	}
	release, err := retirementLease(ctx, kube, name)
	if err != nil {
		return nil, err
	}
	defer release()
	// Re-read under the lease, including after a concurrent creator's failure.
	cm, err = kube(ctx, nil, "get", "configmap", name, "-o", "json", "--ignore-not-found")
	if err != nil {
		return nil, err
	}
	if len(cm) != 0 {
		receipt, err = retirementReadReceipt(cm)
		if err != nil || physicalString(receipt, "plan_sha256") != preview.SHA256 {
			return nil, fmt.Errorf("retirement receipt identity changed")
		}
		if physicalString(receipt, "phase") == "completed" {
			return receipt, nil
		}
	} else {
		blockers, err := reviewRetirement(ctx, kube, p, false)
		if err != nil {
			return nil, err
		}
		if len(blockers) != 0 {
			return nil, fmt.Errorf("retirement blocked: %s", strings.Join(blockers, "; "))
		}
		observed, err := host(ctx, map[string]any{"mode": "inspect", "path": p.Path, "storage_root": p.StorageRoot, "hostname": p.Node, "machine_id": p.Host["machine_id"]})
		if err != nil {
			return nil, err
		}
		raw, _ := json.Marshal(observed)
		var check struct {
			Host     map[string]string `json:"host"`
			Tree     retirementTree    `json:"tree"`
			Blockers []string          `json:"blockers"`
		}
		_ = json.Unmarshal(raw, &check)
		if check.Tree != p.Tree || !maps.Equal(check.Host, p.Host) || len(check.Blockers) != 0 {
			return nil, fmt.Errorf("node data or activity changed since preview; preview again")
		}
		receipt = map[string]any{"schema": volumeRetirementSchema, "plan_sha256": preview.SHA256, "plan": p, "reason": reason, "phase": "prepared", "started_at": time.Now().UTC().Format(time.RFC3339Nano)}
		raw, _ = json.Marshal(receipt)
		cm, err = kube(ctx, map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": name, "namespace": p.Namespace, "labels": map[string]any{"app.kubernetes.io/managed-by": "fugue-cli", "fugue.pro/volume-retirement": "v1"}}, "data": map[string]any{"receipt.json": string(raw)}}, "create", "-f", "-", "-o", "json")
		if err != nil {
			return nil, err
		}
	}
	result = receipt
	result["receipt_name"] = name
	save := func(saveCtx context.Context) error {
		receipt["updated_at"] = time.Now().UTC().Format(time.RFC3339Nano)
		raw, err := json.Marshal(receipt)
		if err != nil {
			return err
		}
		physicalMap(cm, "data")["receipt.json"] = string(raw)
		saved, err := kube(saveCtx, cm, "replace", "-f", "-", "-o", "json")
		if err == nil {
			cm = saved
		}
		return err
	}
	defer func() {
		if resultErr != nil {
			receipt["last_error"] = resultErr.Error()
			failureCtx, failureCancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer failureCancel()
			if err := save(failureCtx); err != nil {
				resultErr = fmt.Errorf("%w; receipt update also failed; read status before retry", resultErr)
			}
		}
	}()
	// A prior attempt may have deleted the PV and then lost the final ConfigMap
	// write. The node's durable receipt lets this same plan finish honestly.
	if physicalString(receipt, "phase") == "data_deleted" {
		pv, err := kube(ctx, nil, "get", "pv", p.PV, "-o", "json", "--ignore-not-found")
		if err != nil {
			return result, err
		}
		if len(pv) == 0 {
			blockers, err := reviewRetirement(ctx, kube, p, true, true)
			if err != nil || len(blockers) > 0 {
				return result, fmt.Errorf("post-retirement verification blocked: %v %s", err, strings.Join(blockers, "; "))
			}
			nodeReceipt, err := host(ctx, map[string]any{"mode": "retire", "plan": p, "plan_sha256": preview.SHA256})
			if err != nil || !retirementNodeVerified(nodeReceipt, preview) {
				return result, fmt.Errorf("cannot verify completed node retirement: %v", err)
			}
			receipt["node_receipt"], receipt["phase"], receipt["verified_pv_absent"] = nodeReceipt, "completed", true
			receipt["completed_at"] = time.Now().UTC().Format(time.RFC3339Nano)
			delete(receipt, "last_error")
			return result, save(ctx)
		}
	}
	pvc, err := kube(ctx, nil, "get", "pvc", p.PVC, "-o", "json", "--ignore-not-found")
	if err != nil {
		return result, err
	}
	if len(pvc) > 0 {
		if physicalString(physicalMap(pvc, "metadata"), "uid") != p.PVCUID {
			return result, fmt.Errorf("PVC name was reused; refusing retirement")
		}
		if physicalString(physicalMap(pvc, "metadata"), "deletionTimestamp") == "" {
			blockers, err := reviewRetirement(ctx, kube, p, false)
			if err != nil {
				return result, err
			}
			if len(blockers) > 0 {
				return result, fmt.Errorf("retirement blocked: %s", strings.Join(blockers, "; "))
			}
			if err := retirementDelete(ctx, kube, "persistentvolumeclaims", p.PVC, p.Namespace, pvc); err != nil {
				return result, err
			}
		}
	}
	err = retirementWait(ctx, func() (bool, error) {
		claim, err := kube(ctx, nil, "get", "pvc", p.PVC, "-o", "json", "--ignore-not-found")
		if err != nil {
			return false, err
		}
		if len(claim) != 0 {
			if physicalString(physicalMap(claim, "metadata"), "uid") != p.PVCUID {
				return false, fmt.Errorf("PVC name was reused during retirement")
			}
			return false, nil
		}
		pv, err := kube(ctx, nil, "get", "pv", p.PV, "-o", "json", "--ignore-not-found")
		return physicalString(physicalMap(pv, "status"), "phase") == "Released", err
	})
	if err != nil {
		return result, err
	}
	blockers, err := reviewRetirement(ctx, kube, p, true)
	if err != nil {
		return result, err
	}
	if len(blockers) > 0 {
		return result, fmt.Errorf("released volume is blocked: %s", strings.Join(blockers, "; "))
	}
	receipt["phase"] = "claim_deleted"
	if err := save(ctx); err != nil {
		return result, err
	}
	nodeReceipt, err := host(ctx, map[string]any{"mode": "retire", "plan": p, "plan_sha256": preview.SHA256})
	if err != nil {
		return result, fmt.Errorf("node retirement incomplete; retry this same plan after inspecting status: %w", err)
	}
	if !retirementNodeVerified(nodeReceipt, preview) {
		return result, fmt.Errorf("node did not return a matching verified deletion receipt")
	}
	receipt["node_receipt"], receipt["phase"] = nodeReceipt, "data_deleted"
	if err := save(ctx); err != nil {
		return result, err
	}
	blockers, err = reviewRetirement(ctx, kube, p, true)
	if err != nil {
		return result, err
	}
	if len(blockers) > 0 {
		return result, fmt.Errorf("data removed but PV retirement is blocked: %s", strings.Join(blockers, "; "))
	}
	pv, err := kube(ctx, nil, "get", "pv", p.PV, "-o", "json")
	if err != nil {
		return result, err
	}
	if !retirementPVMatches(pv, p) || physicalString(physicalMap(pv, "status"), "phase") != "Released" {
		return result, fmt.Errorf("PV identity changed before final retirement")
	}
	if err := retirementDelete(ctx, kube, "persistentvolumes", p.PV, "", pv); err != nil {
		return result, err
	}
	if err := retirementWait(ctx, func() (bool, error) {
		pv, err := kube(ctx, nil, "get", "pv", p.PV, "-o", "json", "--ignore-not-found")
		return len(pv) == 0, err
	}); err != nil {
		return result, err
	}
	blockers, err = reviewRetirement(ctx, kube, p, true, true)
	if err != nil || len(blockers) > 0 {
		return result, fmt.Errorf("data and PV removed but final verification blocked: %v %s", err, strings.Join(blockers, "; "))
	}
	receipt["phase"], receipt["completed_at"], receipt["verified_pv_absent"] = "completed", time.Now().UTC().Format(time.RFC3339Nano), true
	delete(receipt, "last_error")
	if err := save(ctx); err != nil {
		return result, fmt.Errorf("data and PV removed, but completion receipt failed; inspect saved state: %w", err)
	}
	return result, nil
}

func retirementNodeVerified(receipt map[string]any, preview retirementPreview) bool {
	return receipt["verified_absent"] == true && physicalString(receipt, "plan_sha256") == preview.SHA256 && physicalString(receipt, "pv_uid") == preview.Plan.PVUID && physicalString(receipt, "state") == "data_deleted"
}
