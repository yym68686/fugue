package declarativerelease

import (
	"bytes"
	"os"
	"testing"
)

func TestObservationWorkerManifestUsesOnlyItsDatabaseCredential(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/releases/observation-worker/resources.json")
	if err != nil {
		t.Fatal(err)
	}
	set, err := DecodeResourceSet(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(set.Items) != 1 {
		t.Fatal("the standby release must not install any permissions or extra resources")
	}
	pod := apiTLSObject(t, apiTLSObject(t, apiTLSObject(t, set.Items[0], "spec"), "template"), "spec")
	if pod["automountServiceAccountToken"] != false || pod["serviceAccountName"] != nil {
		t.Fatalf("worker must not inherit service account or API credential mounts: %#v", pod)
	}
	container := apiTLSNamedObject(t, apiTLSArray(t, pod, "containers"), "name", "observation-worker")
	startup := apiTLSObject(t, apiTLSObject(t, container, "startupProbe"), "httpGet")
	if stringField(startup, "path") != "/sample-retention/readyz" || stringField(startup, "port") != "metrics" {
		t.Fatal("the new worker must prove sample retention is ready before rolling out")
	}
	registry, err := os.ReadFile("../../deploy/releases/components.json")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(registry, []byte("/sample-retention/readyz")) {
		t.Fatal("release health checks must be supported by the previous worker")
	}
	env := apiTLSArray(t, container, "env")
	if len(env) != 4 || container["envFrom"] != nil {
		t.Fatalf("unexpected extra worker environment: %#v", env)
	}
	memory := apiTLSNamedObject(t, env, "name", "GOMEMLIMIT")
	if len(memory) != 2 || stringField(memory, "value") != "384MiB" {
		t.Fatalf("unexpected worker memory setting: %#v", memory)
	}
	retention := apiTLSNamedObject(t, env, "name", "FUGUE_OBSERVATION_QUALITY_EVIDENCE_RETENTION")
	if len(retention) != 2 || stringField(retention, "value") != "enabled" {
		t.Fatalf("worker retention setting must be a literal, not a secret reference: %#v", retention)
	}
	dsn := apiTLSNamedObject(t, env, "name", "FUGUE_OBSERVATION_DATABASE_URL")
	key := apiTLSObject(t, apiTLSObject(t, dsn, "valueFrom"), "secretKeyRef")
	if stringField(key, "name") != "fugue-observation-worker-database" || stringField(key, "key") != "database-url" {
		t.Fatalf("worker must not reuse the API database credential: %#v", key)
	}
	samplePolicy := apiTLSNamedObject(t, env, "name", "FUGUE_OBSERVATION_SAMPLE_RETENTION_POLICY_FILE")
	if len(samplePolicy) != 2 || stringField(samplePolicy, "value") != "/etc/fugue-observation/observation-retention.json" {
		t.Fatalf("sample retention must use only the projected policy file: %#v", samplePolicy)
	}
	volumes := apiTLSArray(t, pod, "volumes")
	mounts := apiTLSArray(t, container, "volumeMounts")
	if len(volumes) != 1 || len(mounts) != 1 {
		t.Fatal("only the sample retention policy projection may be mounted")
	}
	volume := apiTLSNamedObject(t, volumes, "name", "sample-retention-policy")
	mount := apiTLSNamedObject(t, mounts, "name", "sample-retention-policy")
	config := apiTLSObject(t, volume, "configMap")
	items := apiTLSArray(t, config, "items")
	if len(volume) != 2 || stringField(config, "name") != "fugue-storage-policy" || len(items) != 1 ||
		stringField(mount, "mountPath") != "/etc/fugue-observation" || mount["readOnly"] != true || mount["subPath"] != nil {
		t.Fatal("sample retention must receive only its hot-reloadable read-only ConfigMap")
	}
	item := apiTLSNamedObject(t, items, "key", "observation-retention.json")
	if stringField(item, "path") != "observation-retention.json" {
		t.Fatal("sample retention policy projection has the wrong path")
	}
}
