package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"fugue/internal/releasedomain"
)

const maxCanonicalPlanBytes = 1 << 20

type runtimeFactory func(releasedomain.ControlPlaneHotfixAdoptionPlan) (releasedomain.ControlPlaneHotfixRuntime, error)

func main() {
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
