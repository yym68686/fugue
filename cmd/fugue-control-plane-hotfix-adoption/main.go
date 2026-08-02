package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"fugue/internal/releasedomain"
)

const maxCanonicalPlanBytes = 1 << 20

type runtimeFactory func(releasedomain.ControlPlaneHotfixAdoptionPlan) (releasedomain.ControlPlaneHotfixRuntime, error)

func main() {
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
