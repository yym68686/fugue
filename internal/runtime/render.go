package runtime

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"fugue/internal/model"
	"fugue/internal/workloadidentity"

	"gopkg.in/yaml.v3"
)

type Renderer struct {
	BaseDir          string
	WorkloadIdentity WorkloadIdentityConfig
}

type WorkloadIdentityConfig struct {
	APIBaseURL string
	SigningKey string
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
	app = r.PrepareApp(app)
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
	app = r.PrepareApp(app)
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

func (r Renderer) PrepareApp(app model.App) model.App {
	return r.withWorkloadIdentity(app)
}

func (r Renderer) withWorkloadIdentity(app model.App) model.App {
	env := make(map[string]string)
	for key, value := range app.Spec.Env {
		env[key] = value
	}
	injected := map[string]string{
		"FUGUE_TENANT_ID":  strings.TrimSpace(app.TenantID),
		"FUGUE_PROJECT_ID": strings.TrimSpace(app.ProjectID),
		"FUGUE_APP_ID":     strings.TrimSpace(app.ID),
		"FUGUE_APP_NAME":   strings.TrimSpace(app.Name),
		"FUGUE_RUNTIME_ID": strings.TrimSpace(app.Spec.RuntimeID),
	}
	if app.Route != nil {
		if hostname := strings.TrimSpace(app.Route.Hostname); hostname != "" {
			injected["FUGUE_APP_HOSTNAME"] = hostname
			if publicURL := strings.TrimSpace(app.Route.PublicURL); publicURL != "" {
				injected["FUGUE_APP_URL"] = publicURL
			} else {
				injected["FUGUE_APP_URL"] = "https://" + hostname
			}
		}
	}
	if apiBaseURL := NormalizeWorkloadIdentityAPIBaseURL(r.WorkloadIdentity.APIBaseURL); apiBaseURL != "" {
		injected["FUGUE_API_URL"] = apiBaseURL
		injected["FUGUE_BASE_URL"] = apiBaseURL
	}
	if signingKey := strings.TrimSpace(r.WorkloadIdentity.SigningKey); signingKey != "" &&
		strings.TrimSpace(app.TenantID) != "" &&
		strings.TrimSpace(app.ProjectID) != "" {
		token, err := workloadidentity.Issue(signingKey, workloadidentity.Claims{
			TenantID:  app.TenantID,
			ProjectID: app.ProjectID,
			AppID:     app.ID,
			Scopes:    []string{"app.write", "app.deploy", "app.delete"},
		})
		if err == nil {
			injected["FUGUE_TOKEN"] = token
		}
	}
	for key, value := range injected {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		env[key] = value
	}
	app.Spec.Env = env
	return app
}

func NormalizeWorkloadIdentityAPIBaseURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if strings.Contains(raw, "://") {
		return strings.TrimRight(raw, "/")
	}
	return "https://" + strings.TrimRight(raw, "/")
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
