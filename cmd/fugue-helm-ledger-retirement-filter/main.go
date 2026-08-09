package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type resourceIdentity struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace"`
	Name       string `json:"name"`
}

func (id resourceIdentity) key() string {
	return strings.Join([]string{id.APIVersion, id.Kind, id.Namespace, id.Name}, "\x00")
}

type retiredResource struct {
	resourceIdentity
	UID             string `json:"uid"`
	ResourceVersion string `json:"resourceVersion"`
	Generation      int64  `json:"generation"`
	Source          string `json:"source"`
	Image           string `json:"image"`
	Desired         int    `json:"desired"`
	Updated         int    `json:"updated"`
	Ready           int    `json:"ready"`
	Available       int    `json:"available"`
	Unavailable     int    `json:"unavailable"`
}

type retirementIntent struct {
	APIVersion                  string            `json:"apiVersion"`
	Kind                        string            `json:"kind"`
	Release                     string            `json:"release"`
	Namespace                   string            `json:"namespace"`
	ExpectedSourceParent        string            `json:"expectedSourceParent"`
	ExpectedHelmRevision        int               `json:"expectedHelmRevision"`
	ExpectedHelmStatus          string            `json:"expectedHelmStatus"`
	ExpectedManifestDigest      string            `json:"expectedManifestDigest"`
	ExpectedFinalManifestDigest string            `json:"expectedFinalManifestDigest"`
	SourceChartCommit           string            `json:"sourceChartCommit"`
	SourceChartTree             string            `json:"sourceChartTree"`
	SourceChartTemplateDigest   string            `json:"sourceChartTemplateDigest"`
	PatchDigest                 string            `json:"patchDigest"`
	RetiredResources            []retiredResource `json:"retiredResources"`
}

type manifestDocument struct {
	Raw      []byte
	Identity resourceIdentity
}

func decodeIntent(path string) (retirementIntent, error) {
	f, err := os.Open(path)
	if err != nil {
		return retirementIntent{}, err
	}
	defer f.Close()
	dec := json.NewDecoder(io.LimitReader(f, 1<<20))
	dec.DisallowUnknownFields()
	var intent retirementIntent
	if err := dec.Decode(&intent); err != nil {
		return retirementIntent{}, err
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		return retirementIntent{}, errors.New("retirement intent has trailing JSON")
	}
	if intent.APIVersion != "release.fugue.dev/v1" || intent.Kind != "HelmLedgerRetirementIntent" ||
		intent.Release == "" || intent.Namespace == "" || len(intent.RetiredResources) == 0 {
		return retirementIntent{}, errors.New("retirement intent identity is invalid")
	}
	seen := map[string]struct{}{}
	for _, resource := range intent.RetiredResources {
		if resource.APIVersion == "" || resource.Kind == "" || resource.Namespace != intent.Namespace || resource.Name == "" {
			return retirementIntent{}, errors.New("retired resource identity is invalid")
		}
		if _, exists := seen[resource.key()]; exists {
			return retirementIntent{}, errors.New("retired resource identity is duplicated")
		}
		seen[resource.key()] = struct{}{}
	}
	return intent, nil
}

func splitManifest(raw []byte, defaultNamespace string) ([]manifestDocument, error) {
	lines := bytes.Split(raw, []byte("\n"))
	var chunks [][]byte
	var current bytes.Buffer
	flush := func() {
		if len(bytes.TrimSpace(current.Bytes())) != 0 {
			chunks = append(chunks, append([]byte(nil), current.Bytes()...))
		}
		current.Reset()
	}
	for _, line := range lines {
		if string(bytes.TrimSpace(line)) == "---" {
			flush()
			continue
		}
		current.Write(line)
		current.WriteByte('\n')
	}
	flush()
	documents := make([]manifestDocument, 0, len(chunks))
	seen := map[string]struct{}{}
	for _, chunk := range chunks {
		var header struct {
			APIVersion string `yaml:"apiVersion"`
			Kind       string `yaml:"kind"`
			Metadata   struct {
				Namespace string `yaml:"namespace"`
				Name      string `yaml:"name"`
			} `yaml:"metadata"`
		}
		if err := yaml.Unmarshal(chunk, &header); err != nil {
			return nil, fmt.Errorf("decode manifest document: %w", err)
		}
		if header.APIVersion == "" && header.Kind == "" && header.Metadata.Name == "" {
			continue
		}
		if header.APIVersion == "" || header.Kind == "" || header.Metadata.Name == "" {
			return nil, errors.New("manifest document identity is incomplete")
		}
		namespace := header.Metadata.Namespace
		if namespace == "" {
			namespace = defaultNamespace
		}
		identity := resourceIdentity{APIVersion: header.APIVersion, Kind: header.Kind, Namespace: namespace, Name: header.Metadata.Name}
		if _, exists := seen[identity.key()]; exists {
			return nil, fmt.Errorf("manifest identity %s/%s is duplicated", identity.Kind, identity.Name)
		}
		seen[identity.key()] = struct{}{}
		documents = append(documents, manifestDocument{Raw: chunk, Identity: identity})
	}
	return documents, nil
}

func filter(currentRaw, targetRaw []byte, intent retirementIntent) ([]byte, error) {
	current, err := splitManifest(currentRaw, intent.Namespace)
	if err != nil {
		return nil, fmt.Errorf("decode current manifest: %w", err)
	}
	target, err := splitManifest(targetRaw, intent.Namespace)
	if err != nil {
		return nil, fmt.Errorf("decode target manifest: %w", err)
	}
	retired := map[string]struct{}{}
	for _, resource := range intent.RetiredResources {
		retired[resource.key()] = struct{}{}
	}
	currentSet := map[string]struct{}{}
	for _, document := range current {
		currentSet[document.Identity.key()] = struct{}{}
	}
	for key := range retired {
		if _, exists := currentSet[key]; !exists {
			return nil, errors.New("retired resource is absent from the current Helm manifest")
		}
	}
	expected := map[string]struct{}{}
	for key := range currentSet {
		if _, remove := retired[key]; !remove {
			expected[key] = struct{}{}
		}
	}
	targetSet := map[string]struct{}{}
	for _, document := range target {
		targetSet[document.Identity.key()] = struct{}{}
	}
	if len(targetSet) != len(expected) {
		return nil, fmt.Errorf("target manifest object count=%d, want %d", len(targetSet), len(expected))
	}
	for key := range expected {
		if _, exists := targetSet[key]; !exists {
			return nil, errors.New("target manifest changed an object identity outside retirement")
		}
	}

	kept := make([]manifestDocument, 0, len(expected))
	for _, document := range current {
		if _, remove := retired[document.Identity.key()]; !remove {
			kept = append(kept, document)
		}
	}
	var output bytes.Buffer
	for _, document := range kept {
		output.WriteString("---\n")
		output.Write(bytes.TrimSpace(document.Raw))
		output.WriteByte('\n')
	}
	return output.Bytes(), nil
}

func run() error {
	if len(os.Args) != 1 {
		return errors.New("helm ledger retirement filter accepts no arguments")
	}
	currentPath := os.Getenv("FUGUE_HELM_CURRENT_MANIFEST")
	intentPath := os.Getenv("FUGUE_HELM_RETIREMENT_INTENT")
	if currentPath == "" || intentPath == "" {
		return errors.New("helm ledger retirement filter environment is incomplete")
	}
	intent, err := decodeIntent(intentPath)
	if err != nil {
		return fmt.Errorf("decode retirement intent: %w", err)
	}
	current, err := os.ReadFile(currentPath)
	if err != nil {
		return fmt.Errorf("read current Helm manifest: %w", err)
	}
	target, err := io.ReadAll(io.LimitReader(os.Stdin, 8<<20))
	if err != nil {
		return fmt.Errorf("read target Helm manifest: %w", err)
	}
	filtered, err := filter(current, target, intent)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(filtered)
	return err
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "helm-ledger-retirement-filter:", err)
		os.Exit(1)
	}
}
