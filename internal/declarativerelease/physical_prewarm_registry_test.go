package declarativerelease

import (
	"encoding/json"
	batchv1 "k8s.io/api/batch/v1"
	"os"
	"strings"
	"testing"

	"fugue/internal/prewarmconfig"
)

func TestProductionPhysicalPrewarmOwnsOnlyBoundedPrivateJob(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/releases/edge-worker-us-primary-prewarm/resources.json")
	var set struct{ Items []batchv1.Job }
	if err != nil || json.Unmarshal(raw, &set) != nil || len(set.Items) != 1 {
		t.Fatal("expected exactly one Job")
	}
	j := set.Items[0]
	// An API round trip omits zero-valued optional fields. The declarative
	// writer must see its own Job as converged after successful execution,
	// including the read-only flags protecting the source certificates.
	var declared struct{ Items []map[string]any }
	var roundTripped map[string]any
	live, err := json.Marshal(j)
	if err != nil || json.Unmarshal(live, &roundTripped) != nil || json.Unmarshal(raw, &declared) != nil {
		t.Fatal("failed Job API round trip", err)
	}
	if path := ResourceDesiredMismatch(declared.Items[0], roundTripped); path != "" {
		t.Fatal("Job diverges after Kubernetes serialization at", path)
	}
	p := j.Spec.Template.Spec
	if j.Kind != "Job" || j.Spec.ActiveDeadlineSeconds == nil || *j.Spec.ActiveDeadlineSeconds > 240 || j.Spec.BackoffLimit == nil || *j.Spec.BackoffLimit != 0 ||
		j.Spec.Parallelism == nil || *j.Spec.Parallelism != 1 || p.RestartPolicy != "Never" || p.NodeSelector["kubernetes.io/hostname"] == "" ||
		p.AutomountServiceAccountToken == nil || *p.AutomountServiceAccountToken || p.HostNetwork || p.HostPID || len(p.InitContainers) != 0 || len(p.Containers) != 1 {
		t.Fatal("unbounded or publicly exposed prewarm")
	}
	c := p.Containers[0]
	hosts, err := prewarmconfig.Parse(j.Spec.Template.Annotations[prewarmconfig.Annotation])
	if err != nil || len(c.Args) != 1 || strings.Contains(c.Args[0], "FUGUE_EDGE_PRIVATE_PREWARM_HOSTS") {
		t.Fatal("targets must be typed data, not command input", err)
	}
	for _, h := range hosts {
		if strings.Contains(c.Args[0], h) {
			t.Fatal("executable contains a hostname")
		}
	}
	projected := false
	for _, env := range c.Env {
		if env.Name == "FUGUE_EDGE_PRIVATE_PREWARM_HOSTS" && env.ValueFrom != nil && env.ValueFrom.FieldRef != nil && env.ValueFrom.FieldRef.FieldPath == "metadata.annotations['"+prewarmconfig.Annotation+"']" {
			projected = true
		}
	}
	if !projected {
		t.Fatal("private target list projection is missing")
	}
	if len(c.Ports) != 0 || c.SecurityContext == nil || c.SecurityContext.ReadOnlyRootFilesystem == nil || !*c.SecurityContext.ReadOnlyRootFilesystem || len(c.EnvFrom) != 0 {
		t.Fatal("non-isolated runner")
	}
	mounts := map[string]bool{}
	for _, m := range c.VolumeMounts {
		mounts[m.Name] = m.ReadOnly
	}
	for _, v := range p.Volumes {
		switch {
		case v.HostPath != nil:
			if !mounts[v.Name] || !(strings.HasSuffix(v.HostPath.Path, "/front") || strings.HasSuffix(v.HostPath.Path, "/caddy-data/caddy/certificates")) {
				t.Fatal("unexpected or writable host mount", v.Name)
			}
		case v.Secret != nil:
			if !mounts[v.Name] || !(v.Name == "reader" || v.Name == "verifier" || v.Name == "static-tls") {
				t.Fatal("unexpected credential", v.Name)
			}
		case v.EmptyDir != nil:
			if v.EmptyDir.SizeLimit == nil || v.EmptyDir.Medium != "Memory" {
				t.Fatal("unbounded scratch", v.Name)
			}
		default:
			t.Fatal("unexpected private Job volume", v.Name)
		}
	}
}
