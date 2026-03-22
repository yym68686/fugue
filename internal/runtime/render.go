package runtime

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/template"

	"fugue/internal/model"
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

func (r Renderer) RenderAppBundle(app model.App) (Bundle, error) {
	namespace := NamespaceForTenant(app.TenantID)
	path := filepath.Join(r.BaseDir, namespace, fmt.Sprintf("%s.yaml", model.Slugify(app.Name)))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return Bundle{}, fmt.Errorf("create render directory: %w", err)
	}

	data := renderData{
		Namespace: namespace,
		AppName:   sanitizeName(app.Name),
		Image:     app.Spec.Image,
		Command:   app.Spec.Command,
		Args:      app.Spec.Args,
		Replicas:  app.Spec.Replicas,
		Ports:     app.Spec.Ports,
		Env:       sortedEnv(app.Spec.Env),
	}

	var buf bytes.Buffer
	if err := appManifestTemplate.Execute(&buf, data); err != nil {
		return Bundle{}, fmt.Errorf("render manifest: %w", err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		return Bundle{}, fmt.Errorf("write manifest: %w", err)
	}

	return Bundle{
		TenantNamespace: namespace,
		ManifestPath:    path,
		Manifest:        buf.Bytes(),
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

type renderData struct {
	Namespace string
	AppName   string
	Image     string
	Command   []string
	Args      []string
	Replicas  int
	Ports     []int
	Env       [][2]string
}

var appManifestTemplate = template.Must(template.New("app-manifest").Funcs(template.FuncMap{
	"quote": strconv.Quote,
}).Parse(`apiVersion: v1
kind: Namespace
metadata:
  name: {{ .Namespace }}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ .AppName }}
  namespace: {{ .Namespace }}
  labels:
    app.kubernetes.io/name: {{ .AppName }}
    app.kubernetes.io/managed-by: fugue
spec:
  replicas: {{ .Replicas }}
  selector:
    matchLabels:
      app.kubernetes.io/name: {{ .AppName }}
  template:
    metadata:
      labels:
        app.kubernetes.io/name: {{ .AppName }}
        app.kubernetes.io/managed-by: fugue
    spec:
      containers:
      - name: {{ .AppName }}
        image: {{ .Image }}
{{- if .Command }}
        command:
{{- range .Command }}
        - {{ quote . }}
{{- end }}
{{- end }}
{{- if .Args }}
        args:
{{- range .Args }}
        - {{ quote . }}
{{- end }}
{{- end }}
{{- if .Ports }}
        ports:
{{- range .Ports }}
        - containerPort: {{ . }}
{{- end }}
{{- end }}
{{- if .Env }}
        env:
{{- range .Env }}
        - name: {{ index . 0 }}
          value: {{ quote (index . 1) }}
{{- end }}
{{- end }}
---
apiVersion: v1
kind: Service
metadata:
  name: {{ .AppName }}
  namespace: {{ .Namespace }}
spec:
  selector:
    app.kubernetes.io/name: {{ .AppName }}
{{- if .Ports }}
  ports:
{{- range .Ports }}
  - name: tcp-{{ . }}
    port: {{ . }}
    targetPort: {{ . }}
{{- end }}
{{- else }}
  ports:
  - name: tcp-80
    port: 80
    targetPort: 80
{{- end }}
`))

func sanitizeName(name string) string {
	name = model.Slugify(name)
	if len(name) > 50 {
		return name[:50]
	}
	return name
}

func sortedEnv(env map[string]string) [][2]string {
	if len(env) == 0 {
		return nil
	}
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([][2]string, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, [2]string{key, env[key]})
	}
	return pairs
}
