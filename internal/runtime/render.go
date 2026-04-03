package runtime

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"fugue/internal/model"

	"gopkg.in/yaml.v3"
)

type Renderer struct {
	BaseDir string
}

type Bundle struct {
	TenantNamespace string
	ManifestPath    string
	Manifest        []byte
}

func NamespaceForTenant(tenantID string) string {
	tenantID = model.Slugify(strings.ReplaceAll(tenantID, "_", "-"))
	if len(tenantID) > 32 {
		tenantID = tenantID[:32]
	}
	return "fg-" + tenantID
}

func (r Renderer) RenderAppBundle(app model.App, constraints ...SchedulingConstraints) (Bundle, error) {
	var scheduling SchedulingConstraints
	if len(constraints) > 0 {
		scheduling = constraints[0]
	}
	return r.RenderAppBundleWithPlacements(app, scheduling, nil)
}

func (r Renderer) RenderAppBundleWithPlacements(app model.App, scheduling SchedulingConstraints, postgresPlacements map[string][]SchedulingConstraints) (Bundle, error) {
	namespace := NamespaceForTenant(app.TenantID)
	path := filepath.Join(r.BaseDir, namespace, fmt.Sprintf("%s.yaml", model.Slugify(app.Name)))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return Bundle{}, fmt.Errorf("create render directory: %w", err)
	}

	objects := buildAppObjectsWithPlacements(app, scheduling, postgresPlacements)
	manifest, err := marshalObjectsToManifest(objects)
	if err != nil {
		return Bundle{}, fmt.Errorf("render manifest: %w", err)
	}
	if err := os.WriteFile(path, manifest, 0o644); err != nil {
		return Bundle{}, fmt.Errorf("write manifest: %w", err)
	}

	return Bundle{
		TenantNamespace: namespace,
		ManifestPath:    path,
		Manifest:        manifest,
	}, nil
}

func (r Renderer) RenderManagedAppBundle(app model.App, constraints ...SchedulingConstraints) (Bundle, error) {
	var scheduling SchedulingConstraints
	if len(constraints) > 0 {
		scheduling = constraints[0]
	}
	return r.RenderManagedAppBundleWithPlacements(app, scheduling, nil)
}

func (r Renderer) RenderManagedAppBundleWithPlacements(app model.App, scheduling SchedulingConstraints, postgresPlacements map[string][]SchedulingConstraints) (Bundle, error) {
	namespace := NamespaceForTenant(app.TenantID)
	path := filepath.Join(r.BaseDir, namespace, fmt.Sprintf("%s-managedapp.yaml", ManagedAppResourceName(app)))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return Bundle{}, fmt.Errorf("create render directory: %w", err)
	}

	objects := BuildManagedAppStateObjectsWithPlacements(app, scheduling, postgresPlacements)
	manifest, err := marshalObjectsToManifest(objects)
	if err != nil {
		return Bundle{}, fmt.Errorf("render managed app manifest: %w", err)
	}
	if err := os.WriteFile(path, manifest, 0o644); err != nil {
		return Bundle{}, fmt.Errorf("write manifest: %w", err)
	}

	return Bundle{
		TenantNamespace: namespace,
		ManifestPath:    path,
		Manifest:        manifest,
	}, nil
}

func ApplyKubectl(manifestPath string) error {
	cmd := exec.Command("kubectl", "apply", "-f", manifestPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl apply: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func DeleteKubectl(manifestPath string) error {
	cmd := exec.Command("kubectl", "delete", "--ignore-not-found=true", "-f", manifestPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl delete: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func marshalObjectsToManifest(objects []map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	for index, obj := range objects {
		doc, err := yaml.Marshal(obj)
		if err != nil {
			return nil, fmt.Errorf("marshal object %d: %w", index, err)
		}
		if index > 0 {
			buf.WriteString("---\n")
		}
		buf.Write(doc)
	}
	return buf.Bytes(), nil
}
