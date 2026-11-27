package declarativerelease

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestStorageAuditWorkerManifestDoesNotInheritAPIPrivileges(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/releases/storage-audit-worker/resources.json")
	if err != nil {
		t.Fatal(err)
	}
	set, err := DecodeResourceSet(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(set.Items) != 1 || set.Items[0]["kind"] != "Deployment" {
		t.Fatal("storage audit deploy must not install additional resources or privileges")
	}
	template := apiTLSObject(t, apiTLSObject(t, set.Items[0], "spec"), "template")
	pod := apiTLSObject(t, template, "spec")
	if pod["automountServiceAccountToken"] != false || pod["serviceAccountName"] != nil || pod["volumes"] != nil || pod["hostNetwork"] != nil {
		t.Fatal("storage audit may not mount API secrets, a token or the host network")
	}
	containers := apiTLSArray(t, pod, "containers")
	if len(containers) != 1 {
		t.Fatal("unexpected storage audit sidecars")
	}
	container := apiTLSNamedObject(t, containers, "name", "storage-audit-worker")
	env := apiTLSArray(t, container, "env")
	if len(env) != 2 || container["envFrom"] != nil || container["volumeMounts"] != nil {
		t.Fatalf("storage audit uses a broader credential surface than declared: %#v", env)
	}
	dsn := apiTLSNamedObject(t, env, "name", "FUGUE_STORAGE_AUDIT_DATABASE_URL")
	key := apiTLSObject(t, apiTLSObject(t, dsn, "valueFrom"), "secretKeyRef")
	if stringField(key, "name") != "fugue-storage-audit-worker-database" || stringField(key, "key") != "database-url" {
		t.Fatal("storage audit must not reuse the API database credential")
	}
	sec := apiTLSObject(t, container, "securityContext")
	if sec["allowPrivilegeEscalation"] != false || sec["readOnlyRootFilesystem"] != true {
		t.Fatal("storage audit must be non-privileged with a read-only root")
	}

	raw, err = os.ReadFile("../../deploy/environments/production/observability/scrape-policy.json")
	if err != nil {
		t.Fatal(err)
	}
	var policy struct {
		Jobs []struct {
			Job    string `json:"job_name"`
			Path   string `json:"metrics_path"`
			Labels []struct {
				Action string `json:"action"`
				Regex  string `json:"regex"`
				Target string `json:"target_label"`
			} `json:"relabel_configs"`
		} `json:"additionalScrapeConfigs"`
	}
	if err = json.Unmarshal(raw, &policy); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, job := range policy.Jobs {
		if job.Job != "fugue-storage-audit-worker" {
			continue
		}
		labels := apiTLSObject(t, apiTLSObject(t, template, "metadata"), "labels")
		port := apiTLSNamedObject(t, apiTLSArray(t, container, "ports"), "name", "metrics")
		expected := strings.Join([]string{stringField(labels, "app.kubernetes.io/name"), stringField(container, "name"), stringField(port, "name"), "Running"}, ";")
		if job.Path != "/metrics" || len(job.Labels) != 5 || job.Labels[0].Action != "keep" || job.Labels[0].Regex != expected ||
			job.Labels[1].Target != "pod" || job.Labels[2].Target != "pod_uid" || job.Labels[3].Target != "namespace" {
			t.Fatal("storage audit scrape is not limited to its exact Pod or does not preserve capacity alert labels")
		}
		count++
	}
	if count != 1 {
		t.Fatal("expected one separate storage audit metrics source")
	}
}
