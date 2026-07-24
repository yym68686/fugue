package platformsafety

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestControlPlanePodTemplatesBindSourceCommitToExistingImageTags(t *testing.T) {
	tests := []struct {
		name       string
		template   string
		annotation string
	}{
		{
			name:       "api",
			template:   "deployment.yaml",
			annotation: `fugue.pro/source-commit: {{ .Values.api.image.tag | quote }}`,
		},
		{
			name:       "controller",
			template:   "controller-deployment.yaml",
			annotation: `fugue.pro/source-commit: {{ .Values.controller.image.tag | quote }}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join("..", "..", "deploy", "helm", "fugue", "templates", test.template)
			encoded, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			body := string(encoded)
			if strings.Count(body, test.annotation) != 1 {
				t.Fatalf("%s must contain exactly one source-commit binding", path)
			}
			templateStart := strings.Index(body, "  template:\n")
			if templateStart < 0 {
				t.Fatalf("%s has no pod template", path)
			}
			podSpecStart := strings.Index(body[templateStart:], "    spec:\n")
			annotationIndex := strings.Index(body, test.annotation)
			if podSpecStart < 0 || annotationIndex < templateStart ||
				annotationIndex >= templateStart+podSpecStart {
				t.Fatalf("%s source-commit binding is not in pod template metadata", path)
			}
			if strings.Contains(test.annotation, "digest") || strings.Contains(test.annotation, "latest") {
				t.Fatalf("%s source identity must remain bound to the formal image tag", path)
			}
		})
	}
}

func TestControlPlanePodTemplatesRejectReservedSourceCommitOverride(t *testing.T) {
	chartDir := filepath.Join("..", "..", "deploy", "helm", "fugue")
	valuesPath := filepath.Join(t.TempDir(), "reserved-annotation.yaml")
	values := []byte("podAnnotations:\n  fugue.pro/source-commit: attacker\n")
	if err := os.WriteFile(valuesPath, values, 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("helm", "template", "fugue", chartDir, "-f", valuesPath)
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("helm template accepted a caller override of reserved fugue.pro/source-commit")
	}
	const rejection = "podAnnotations must not override reserved fugue.pro/source-commit annotation"
	if !strings.Contains(string(output), rejection) {
		t.Fatalf("helm template failed without reserved-key rejection: %v\n%s", err, output)
	}
}
