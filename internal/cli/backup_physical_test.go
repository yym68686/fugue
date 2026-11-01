package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

type physicalFake struct {
	cm, cluster, lease map[string]any
	writes             []string
	failPatch          bool
}

func physicalFixture() (*physicalFake, physicalBackupOptions) {
	opts := physicalBackupOptions{namespace: "platform-test", policy: "sample-retention"}
	p := physicalBackupPolicy{Schema: "fugue.cnpg-count-retention/v1", Namespace: opts.namespace, Cluster: "sample-db", ClusterUID: "cluster-uid", SystemID: "12345", Destination: "s3://sample/archive", Endpoint: "https://objects.example.test", ServerName: "sample-db", RetainCount: 7}
	raw, _ := json.Marshal(p)
	f := &physicalFake{
		cm:      map[string]any{"metadata": map[string]any{"name": opts.policy, "namespace": opts.namespace, "uid": "policy-uid", "resourceVersion": "1"}, "data": map[string]any{"policy.json": string(raw)}},
		cluster: map[string]any{"metadata": map[string]any{"name": p.Cluster, "namespace": opts.namespace, "uid": p.ClusterUID, "resourceVersion": "2"}, "spec": map[string]any{"storage": map[string]any{"size": "100Gi"}, "backup": map[string]any{"target": "prefer-standby", "retentionPolicy": "7d", "barmanObjectStore": map[string]any{"destinationPath": p.Destination, "endpointURL": p.Endpoint}}}, "status": map[string]any{"systemID": p.SystemID, "phase": "Cluster in healthy state", "readyInstances": float64(1), "currentPrimary": "sample-db-1", "targetPrimary": "sample-db-1"}},
		lease:   map[string]any{"metadata": map[string]any{"resourceVersion": "1"}, "spec": map[string]any{}},
	}
	return f, opts
}

func physicalClone(v map[string]any) map[string]any {
	raw, _ := json.Marshal(v)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return out
}

func (f *physicalFake) kube(_ context.Context, body any, args ...string) (map[string]any, error) {
	switch args[0] {
	case "get":
		switch args[1] {
		case "configmap":
			return physicalClone(f.cm), nil
		case "clusters.postgresql.cnpg.io":
			return physicalClone(f.cluster), nil
		case "lease":
			return physicalClone(f.lease), nil
		}
	case "replace":
		v := physicalClone(body.(map[string]any))
		if physicalMap(v, "data")["policy.json"] != nil {
			f.writes = append(f.writes, "intent")
			f.cm = v
		} else {
			f.lease = v
		}
		return physicalClone(v), nil
	case "patch":
		if f.failPatch {
			return nil, fmt.Errorf("synthetic CAS conflict")
		}
		f.writes = append(f.writes, "activation")
		var patch []map[string]any
		_ = json.Unmarshal([]byte(args[5]), &patch)
		for _, op := range patch {
			switch op["path"] {
			case "/metadata/uid":
				if op["value"] != physicalMap(f.cluster, "metadata")["uid"] {
					return nil, fmt.Errorf("UID test failed")
				}
			case "/metadata/resourceVersion":
				if op["value"] != physicalMap(f.cluster, "metadata")["resourceVersion"] {
					return nil, fmt.Errorf("RV test failed")
				}
			case "/metadata/annotations":
				physicalMap(f.cluster, "metadata")["annotations"] = op["value"]
			case "/metadata/annotations/fugue.pro~1cnpg-count-policy":
				physicalMap(physicalMap(f.cluster, "metadata"), "annotations")[physicalBackupPolicyAnnotation] = op["value"]
			case "/spec/backup/retentionPolicy":
				delete(physicalMap(physicalMap(f.cluster, "spec"), "backup"), "retentionPolicy")
			default:
				return nil, fmt.Errorf("unexpected patch path %v", op["path"])
			}
		}
		return physicalClone(f.cluster), nil
	}
	return nil, fmt.Errorf("unexpected Kubernetes operation %v", args)
}

func TestPhysicalConfigureCountNotDaysPreservesOtherFields(t *testing.T) {
	f, opts := physicalFixture()
	out, err := configurePhysicalBackup(context.Background(), f.kube, opts, 3)
	if err != nil {
		t.Fatal(err)
	}
	if out["configured"] != true || out["cleanup_blocked"] != false || !physicalBackupActivated(f.cm, f.cluster) {
		t.Fatalf("not activated: %+v", out)
	}
	if strings.Join(f.writes, ",") != "intent,activation" {
		t.Fatalf("unsafe write order: %v", f.writes)
	}
	if physicalString(physicalMap(physicalMap(f.cluster, "spec"), "storage"), "size") != "100Gi" || physicalString(physicalMap(physicalMap(f.cluster, "spec"), "backup"), "target") != "prefer-standby" {
		t.Fatal("changed unrelated database settings")
	}
	_, _, p, err := loadPhysicalBackup(context.Background(), f.kube, opts)
	if err != nil || p.RetainCount != 3 || !p.Enabled {
		t.Fatalf("policy not saved: %+v %v", p, err)
	}
}

func TestPhysicalRefusesIdentityAndOwnershipBeforeWritingIntent(t *testing.T) {
	for _, field := range []string{"uid", "namespace", "systemID", "endpoint", "owner", "unknown-policy-field"} {
		t.Run(field, func(t *testing.T) {
			f, opts := physicalFixture()
			switch field {
			case "uid", "namespace":
				physicalMap(f.cluster, "metadata")[field] = "other"
			case "systemID":
				physicalMap(f.cluster, "status")[field] = "999"
			case "endpoint":
				physicalMap(physicalMap(physicalMap(f.cluster, "spec"), "backup"), "barmanObjectStore")["endpointURL"] = "https://other.example.test"
			case "owner":
				physicalMap(f.cluster, "metadata")["annotations"] = map[string]any{physicalBackupPolicyAnnotation: "other-policy"}
			default:
				raw := physicalString(physicalMap(f.cm, "data"), "policy.json")
				physicalMap(f.cm, "data")["policy.json"] = strings.Replace(raw, "{", `{"unknown":true,`, 1)
			}
			if _, err := configurePhysicalBackup(context.Background(), f.kube, opts, 3); err == nil || len(f.writes) != 0 {
				t.Fatalf("expected refusal with no policy writes: %v %v", err, f.writes)
			}
		})
	}
}

func TestPhysicalCanSaveIntentWhileUnhealthyButCannotClaimCleanup(t *testing.T) {
	f, opts := physicalFixture()
	physicalMap(f.cluster, "status")["phase"] = "Not enough disk space"
	out, err := configurePhysicalBackup(context.Background(), f.kube, opts, 3)
	if err != nil || out["cleanup_blocked"] != true {
		t.Fatalf("expected saved intent with cleanup blocked: %+v %v", out, err)
	}
}

func TestPhysicalActivationConflictDoesNotReportSuccessAndRetryWorks(t *testing.T) {
	f, opts := physicalFixture()
	f.failPatch = true
	if _, err := configurePhysicalBackup(context.Background(), f.kube, opts, 3); err == nil || !strings.Contains(err.Error(), "intent saved but activation failed") {
		t.Fatalf("expected explicit partial configuration result: %v", err)
	}
	if physicalBackupActivated(f.cm, f.cluster) {
		t.Fatal("failed activation reported active")
	}
	f.failPatch = false
	if _, err := configurePhysicalBackup(context.Background(), f.kube, opts, 3); err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalInvalidCountsAndSSHArguments(t *testing.T) {
	for _, n := range []int{0, -1, 101} {
		f, opts := physicalFixture()
		if _, err := configurePhysicalBackup(context.Background(), f.kube, opts, n); err == nil || len(f.writes) > 0 {
			t.Fatalf("invalid count accepted: %d", n)
		}
	}
	for _, host := range []string{"-oProxyCommand=bad", "host;command", "$(command)", "host alias"} {
		_, opts := physicalFixture()
		opts.ssh = host
		if _, err := physicalKubeRunner(opts); err == nil {
			t.Fatalf("unsafe SSH alias accepted: %q", host)
		}
	}
}

func TestPhysicalConfigureRequiresExplicitAcknowledgementWithoutAPI(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runWithStreams([]string{"--base-url", "http://127.0.0.1:1", "admin", "backup", "physical", "configure", "--retain-count", "3"}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "--ack-reduced-recovery") {
		t.Fatalf("expected recovery acknowledgement, not an API request: %v", err)
	}
}
