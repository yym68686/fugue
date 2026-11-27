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
	if pod["automountServiceAccountToken"] != false || pod["serviceAccountName"] != nil || pod["volumes"] != nil {
		t.Fatalf("worker must not inherit service account or API credential mounts: %#v", pod)
	}
	container := apiTLSNamedObject(t, apiTLSArray(t, pod, "containers"), "name", "observation-worker")
	env := apiTLSArray(t, container, "env")
	if len(env) != 2 || container["envFrom"] != nil {
		t.Fatalf("unexpected extra worker environment: %#v", env)
	}
	memory := apiTLSNamedObject(t, env, "name", "GOMEMLIMIT")
	if len(memory) != 2 || stringField(memory, "value") != "384MiB" {
		t.Fatalf("unexpected worker memory setting: %#v", memory)
	}
	dsn := apiTLSNamedObject(t, env, "name", "FUGUE_OBSERVATION_DATABASE_URL")
	key := apiTLSObject(t, apiTLSObject(t, dsn, "valueFrom"), "secretKeyRef")
	if stringField(key, "name") != "fugue-observation-worker-database" || stringField(key, "key") != "database-url" {
		t.Fatalf("worker must not reuse the API database credential: %#v", key)
	}
}
