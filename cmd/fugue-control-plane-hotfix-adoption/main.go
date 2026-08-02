package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"

	"fugue/internal/releasedomain"
	"gopkg.in/yaml.v3"
)

const maxCanonicalPlanBytes = 1 << 20

type runtimeFactory func(releasedomain.ControlPlaneHotfixAdoptionPlan) (releasedomain.ControlPlaneHotfixRuntime, error)

func main() {
	if os.Getenv("FUGUE_CONTROL_PLANE_HOTFIX_CONFIG_VALIDATE_ONLY") == "true" {
		os.Exit(runConfigValidation(
			os.Args[1:], os.Stderr,
			os.Getenv("FUGUE_CONTROL_PLANE_HOTFIX_VALIDATION_DIR"),
			os.Getenv("FUGUE_CONTROL_PLANE_HOTFIX_CONFIG_SECRET_NAME"),
			os.Getenv("FUGUE_CONTROL_PLANE_HOTFIX_CONFIG_SOURCE"),
			os.Getenv("FUGUE_CONTROL_PLANE_HOTFIX_CONFIG_API_IMAGE"),
		))
	}
	if os.Getenv("FUGUE_CONTROL_PLANE_HOTFIX_BUILD_PLAN") == "true" {
		os.Exit(runBuildPlan(
			os.Args[1:], os.Stdout, os.Stderr,
			os.Getenv("FUGUE_CONTROL_PLANE_HOTFIX_BUILD_DIR"),
		))
	}
	if os.Getenv("FUGUE_CONTROL_PLANE_HOTFIX_VALIDATE_ONLY") == "true" {
		os.Exit(runValidation(
			os.Args[1:], os.Stdin, os.Stderr,
			os.Getenv("FUGUE_CONTROL_PLANE_HOTFIX_VALIDATION_DIR"),
		))
	}
	os.Exit(run(
		os.Args[1:],
		os.Stdin,
		os.Stdout,
		os.Stderr,
		os.Getenv("FUGUE_CONTROL_PLANE_HOTFIX_DRY_RUN"),
		func(releasedomain.ControlPlaneHotfixAdoptionPlan) (releasedomain.ControlPlaneHotfixRuntime, error) {
			return nil, fmt.Errorf("bounded production runtime is not injected")
		},
	))
}

var configSHA = regexp.MustCompile(`^[0-9a-f]{40}$`)
var configDigestImage = regexp.MustCompile(`^[a-z0-9]+(?:[._/-][a-z0-9]+)*@sha256:[0-9a-f]{64}$`)
var configSecretName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`)

func runConfigValidation(args []string, stderr io.Writer, evidenceDir, secretName, source, apiImage string) int {
	if len(args) != 0 {
		return fail(stderr, "arguments are not supported")
	}
	if evidenceDir == "" || !filepath.IsAbs(evidenceDir) || filepath.Clean(evidenceDir) != evidenceDir {
		return fail(stderr, "validation directory is invalid")
	}
	if !configSecretName.MatchString(secretName) || !configSHA.MatchString(source) || !configDigestImage.MatchString(apiImage) {
		return fail(stderr, "config transaction identity is invalid")
	}
	if err := verifyEdgeActivationConfigRenderSet(evidenceDir, secretName, source, apiImage); err != nil {
		return fail(stderr, "config render-set verification failed: "+err.Error())
	}
	return 0
}

func verifyEdgeActivationConfigRenderSet(directory, secretName, source, apiImage string) error {
	base, err := readConfigManifest(filepath.Join(directory, "base.yaml"))
	if err != nil {
		return fmt.Errorf("base manifest: %w", err)
	}
	target, err := readConfigManifest(filepath.Join(directory, "target.yaml"))
	if err != nil {
		return fmt.Errorf("target manifest: %w", err)
	}
	repeated, err := readConfigManifest(filepath.Join(directory, "repeated-target.yaml"))
	if err != nil {
		return fmt.Errorf("repeated target manifest: %w", err)
	}
	hybrid, err := readConfigManifest(filepath.Join(directory, "hybrid.yaml"))
	if err != nil {
		return fmt.Errorf("hybrid manifest: %w", err)
	}
	if len(base) == 0 || len(target) != len(base) || !reflect.DeepEqual(target, repeated) || !reflect.DeepEqual(base, hybrid) {
		return fmt.Errorf("manifest inventory is missing, nondeterministic, or not compensatable")
	}
	const apiKey = "apps/v1\tDeployment\tfugue-system\tfugue-fugue-api"
	baseAPI, baseOK := base[apiKey]
	targetAPI, targetOK := target[apiKey]
	if !baseOK || !targetOK {
		return fmt.Errorf("exact API Deployment is absent")
	}
	for key, object := range base {
		if key != apiKey && !reflect.DeepEqual(object, target[key]) {
			return fmt.Errorf("non-API object changed: %s", key)
		}
	}
	stripped, err := stripExactActivationProjection(targetAPI, secretName, source, apiImage)
	if err != nil {
		return err
	}
	if _, err := stripExactActivationProjection(baseAPI, secretName, source, apiImage); err == nil {
		return fmt.Errorf("base API already contains the activation projection")
	}
	if !reflect.DeepEqual(baseAPI, stripped) {
		return fmt.Errorf("API Deployment changed outside the exact activation projection")
	}
	if err := verifyConfigValues(directory, secretName); err != nil {
		return err
	}
	return nil
}

func readConfigManifest(path string) (map[string]map[string]any, error) {
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 || len(data) > maxCanonicalPlanBytes*32 {
		return nil, fmt.Errorf("manifest evidence is invalid")
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	objects := map[string]map[string]any{}
	for {
		var object map[string]any
		err := decoder.Decode(&object)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(object) == 0 {
			continue
		}
		metadata, _ := object["metadata"].(map[string]any)
		apiVersion, _ := object["apiVersion"].(string)
		kind, _ := object["kind"].(string)
		name, _ := metadata["name"].(string)
		namespace, _ := metadata["namespace"].(string)
		if namespace == "" {
			namespace = "fugue-system"
		}
		if apiVersion == "" || kind == "" || name == "" {
			return nil, fmt.Errorf("manifest object identity is incomplete")
		}
		normalized, err := normalizeConfigValue(object)
		if err != nil {
			return nil, err
		}
		key := strings.Join([]string{apiVersion, kind, namespace, name}, "\t")
		if _, exists := objects[key]; exists {
			return nil, fmt.Errorf("duplicate manifest object %s", key)
		}
		objects[key] = normalized
	}
	return objects, nil
}

func normalizeConfigValue(value map[string]any) (map[string]any, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var normalized map[string]any
	if err := decoder.Decode(&normalized); err != nil {
		return nil, err
	}
	return normalized, nil
}

func stripExactActivationProjection(object map[string]any, secretName, source, apiImage string) (map[string]any, error) {
	raw, err := json.Marshal(object)
	if err != nil {
		return nil, err
	}
	var copy map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&copy); err != nil {
		return nil, err
	}
	metadata, _ := copy["metadata"].(map[string]any)
	if metadata["name"] != "fugue-fugue-api" || (metadata["namespace"] != nil && metadata["namespace"] != "fugue-system") {
		return nil, fmt.Errorf("API Deployment identity drifted")
	}
	spec, _ := copy["spec"].(map[string]any)
	template, _ := spec["template"].(map[string]any)
	templateMetadata, _ := template["metadata"].(map[string]any)
	annotations, _ := templateMetadata["annotations"].(map[string]any)
	if annotations["fugue.pro/source-commit"] != source {
		return nil, fmt.Errorf("API source annotation drifted")
	}
	podSpec, _ := template["spec"].(map[string]any)
	containers, _ := podSpec["containers"].([]any)
	apiIndex := -1
	for index, item := range containers {
		container, _ := item.(map[string]any)
		if container["name"] == "api" {
			if apiIndex != -1 {
				return nil, fmt.Errorf("API container is duplicated")
			}
			apiIndex = index
		}
	}
	if apiIndex == -1 {
		return nil, fmt.Errorf("API container is absent")
	}
	apiContainer := containers[apiIndex].(map[string]any)
	if apiContainer["image"] != apiImage {
		return nil, fmt.Errorf("API immutable image drifted")
	}
	env, ok := removeExactNamedEntry(apiContainer["env"], "FUGUE_EDGE_ACTIVATION_PLAN_SIGNING_PROJECTION_DIR", map[string]any{
		"name": "FUGUE_EDGE_ACTIVATION_PLAN_SIGNING_PROJECTION_DIR", "value": "/var/run/secrets/fugue-edge-activation",
	})
	if !ok {
		return nil, fmt.Errorf("activation projection env is absent or drifted")
	}
	apiContainer["env"] = env
	mounts, ok := removeExactNamedEntry(apiContainer["volumeMounts"], "edge-activation-plan-signing-key", map[string]any{
		"mountPath": "/var/run/secrets/fugue-edge-activation", "name": "edge-activation-plan-signing-key", "readOnly": true,
	})
	if !ok {
		return nil, fmt.Errorf("activation projection mount is absent or drifted")
	}
	apiContainer["volumeMounts"] = mounts
	containers[apiIndex] = apiContainer
	podSpec["containers"] = containers
	volumes, ok := removeExactNamedEntry(podSpec["volumes"], "edge-activation-plan-signing-key", map[string]any{
		"name": "edge-activation-plan-signing-key",
		"secret": map[string]any{
			"defaultMode": json.Number("256"), "secretName": secretName,
			"items": []any{
				map[string]any{"key": "FUGUE_EDGE_ACTIVATION_PLAN_SIGNING_KEY", "path": "plan-signing-key"},
				map[string]any{"key": "FUGUE_EDGE_ACTIVATION_PLAN_SIGNING_KEY_ID", "path": "key-id"},
				map[string]any{"key": "FUGUE_EDGE_ACTIVATION_PLAN_SIGNING_KEY_GENERATION", "path": "key-generation"},
			},
		},
	})
	if !ok {
		return nil, fmt.Errorf("activation projection volume is absent or drifted")
	}
	podSpec["volumes"] = volumes
	template["spec"] = podSpec
	spec["template"] = template
	copy["spec"] = spec
	return copy, nil
}

func removeExactNamedEntry(raw any, name string, expected map[string]any) ([]any, bool) {
	items, ok := raw.([]any)
	if !ok {
		return nil, false
	}
	result := make([]any, 0, len(items)-1)
	found := 0
	for _, item := range items {
		object, _ := item.(map[string]any)
		if object["name"] == name {
			if !reflect.DeepEqual(object, expected) {
				return nil, false
			}
			found++
			continue
		}
		result = append(result, item)
	}
	return result, found == 1
}

func verifyConfigValues(directory, secretName string) error {
	base, err := readJSONObject(filepath.Join(directory, "base-values.json"))
	if err != nil {
		return fmt.Errorf("base values: %w", err)
	}
	target, err := readHelmEnvelopeConfig(filepath.Join(directory, "target.yaml.json"))
	if err != nil {
		return fmt.Errorf("target values: %w", err)
	}
	repeated, err := readHelmEnvelopeConfig(filepath.Join(directory, "repeated-target.yaml.json"))
	if err != nil {
		return fmt.Errorf("repeated target values: %w", err)
	}
	hybrid, err := readHelmEnvelopeConfig(filepath.Join(directory, "hybrid.yaml.json"))
	if err != nil {
		return fmt.Errorf("hybrid values: %w", err)
	}
	if !reflect.DeepEqual(target, repeated) || !reflect.DeepEqual(base, hybrid) {
		return fmt.Errorf("Helm values are nondeterministic or not compensatable")
	}
	edge, _ := target["edgeActivation"].(map[string]any)
	if !reflect.DeepEqual(edge, map[string]any{"enabled": true, "signingSecretName": secretName}) {
		return fmt.Errorf("target activation values are not exact")
	}
	target["edgeActivation"] = map[string]any{"enabled": false, "signingSecretName": ""}
	if !reflect.DeepEqual(base, target) {
		return fmt.Errorf("Helm values changed outside edgeActivation")
	}
	return nil
}

func readJSONObject(path string) (map[string]any, error) {
	raw, err := os.ReadFile(path)
	if err != nil || len(raw) == 0 || len(raw) > maxCanonicalPlanBytes*8 {
		return nil, fmt.Errorf("JSON evidence is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, err
	}
	return value, nil
}

func readHelmEnvelopeConfig(path string) (map[string]any, error) {
	envelope, err := readJSONObject(path)
	if err != nil {
		return nil, err
	}
	config, ok := envelope["config"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("Helm envelope config is absent")
	}
	return config, nil
}

func runBuildPlan(args []string, stdout, stderr io.Writer, evidenceDir string) int {
	if len(args) != 0 {
		return fail(stderr, "arguments are not supported")
	}
	if evidenceDir == "" || !filepath.IsAbs(evidenceDir) || filepath.Clean(evidenceDir) != evidenceDir {
		return fail(stderr, "build directory is invalid")
	}
	raw, err := readFixedEvidenceFile(evidenceDir, "input.json", maxCanonicalPlanBytes)
	if err != nil {
		return fail(stderr, "build input is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var input releasedomain.ControlPlaneHotfixAdoptionInput
	if err := decoder.Decode(&input); err != nil {
		return fail(stderr, "build input decode failed")
	}
	if err := requireJSONEOF(decoder); err != nil {
		return fail(stderr, err.Error())
	}
	for index, name := range []string{"base.yaml", "target.yaml", "repeated-target.yaml", "hybrid.yaml"} {
		data, readErr := readFixedEvidenceFile(evidenceDir, name, maxCanonicalPlanBytes*32)
		if readErr != nil {
			return fail(stderr, "build evidence is invalid")
		}
		switch index {
		case 0:
			input.BaseManifest = data
		case 1:
			input.TargetManifest = data
		case 2:
			input.RepeatedTarget = data
		case 3:
			input.HybridManifest = data
		}
	}
	plan, err := releasedomain.BuildControlPlaneHotfixAdoptionPlanFromRenderSet(input)
	if err != nil {
		return fail(stderr, "plan construction failed: "+err.Error())
	}
	if err := writeCanonicalJSON(stdout, plan); err != nil {
		return fail(stderr, "plan write failed")
	}
	return 0
}

func runValidation(args []string, stdin io.Reader, stderr io.Writer, evidenceDir string) int {
	if len(args) != 0 {
		return fail(stderr, "arguments are not supported")
	}
	plan, err := readCanonicalPlan(stdin)
	if err != nil {
		return fail(stderr, err.Error())
	}
	if evidenceDir == "" || !filepath.IsAbs(evidenceDir) || filepath.Clean(evidenceDir) != evidenceDir {
		return fail(stderr, "validation directory is invalid")
	}
	manifests := make([][]byte, 0, 4)
	for _, name := range []string{"base.yaml", "target.yaml", "repeated-target.yaml", "hybrid.yaml"} {
		data, readErr := readFixedEvidenceFile(evidenceDir, name, maxCanonicalPlanBytes*32)
		if readErr != nil {
			return fail(stderr, "validation evidence is invalid")
		}
		manifests = append(manifests, data)
	}
	if err := releasedomain.VerifyControlPlaneHotfixRenderSet(plan, manifests[0], manifests[1], manifests[2], manifests[3]); err != nil {
		return fail(stderr, "render-set verification failed: "+err.Error())
	}
	return 0
}

func readFixedEvidenceFile(directory, name string, limit int64) ([]byte, error) {
	path := filepath.Join(directory, name)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() < 1 || info.Size() > limit {
		return nil, fmt.Errorf("evidence file is invalid")
	}
	return os.ReadFile(path)
}

func run(
	args []string,
	stdin io.Reader,
	stdout io.Writer,
	stderr io.Writer,
	dryRunValue string,
	newRuntime runtimeFactory,
) int {
	if len(args) != 0 {
		return fail(stderr, "arguments are not supported")
	}
	dryRun := false
	switch dryRunValue {
	case "", "false":
	case "true":
		dryRun = true
	default:
		return fail(stderr, "dry-run binding is invalid")
	}
	if newRuntime == nil {
		return fail(stderr, "runtime factory is nil")
	}
	plan, err := readCanonicalPlan(stdin)
	if err != nil {
		return fail(stderr, err.Error())
	}
	runtime, err := newRuntime(plan)
	if err != nil {
		return fail(stderr, "runtime injection failed: "+err.Error())
	}
	result, err := releasedomain.ExecuteControlPlaneHotfixAdoption(
		context.Background(),
		plan,
		runtime,
		releasedomain.ControlPlaneHotfixExecutionOptions{DryRun: dryRun},
	)
	if writeErr := writeCanonicalJSON(stdout, result); writeErr != nil {
		return fail(stderr, "result write failed")
	}
	if err != nil {
		return fail(stderr, err.Error())
	}
	return 0
}

func readCanonicalPlan(input io.Reader) (releasedomain.ControlPlaneHotfixAdoptionPlan, error) {
	raw, err := io.ReadAll(io.LimitReader(input, maxCanonicalPlanBytes+1))
	if err != nil || len(raw) == 0 || len(raw) > maxCanonicalPlanBytes {
		return releasedomain.ControlPlaneHotfixAdoptionPlan{}, fmt.Errorf("canonical plan input is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var plan releasedomain.ControlPlaneHotfixAdoptionPlan
	if err := decoder.Decode(&plan); err != nil {
		return releasedomain.ControlPlaneHotfixAdoptionPlan{}, fmt.Errorf("canonical plan decode failed")
	}
	if err := requireJSONEOF(decoder); err != nil {
		return releasedomain.ControlPlaneHotfixAdoptionPlan{}, err
	}
	if err := releasedomain.VerifyControlPlaneHotfixAdoptionPlan(plan); err != nil {
		return releasedomain.ControlPlaneHotfixAdoptionPlan{}, fmt.Errorf("canonical plan verification failed: %w", err)
	}
	canonical, err := json.Marshal(plan)
	if err != nil || !bytes.Equal(bytes.TrimSpace(raw), canonical) {
		return releasedomain.ControlPlaneHotfixAdoptionPlan{}, fmt.Errorf("plan input is not canonical JSON")
	}
	return plan, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("canonical plan contains trailing data")
	}
	return nil
}

func writeCanonicalJSON(output io.Writer, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = output.Write(data)
	return err
}

func fail(stderr io.Writer, message string) int {
	_, _ = fmt.Fprintln(stderr, "fugue-control-plane-hotfix-adoption:", message)
	return 1
}
