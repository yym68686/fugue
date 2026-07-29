// Command fugue-component-plan emits a verified, side-effect-free component
// change plan.  It is intentionally an input-boundary tool: callers provide
// paths from an independently trusted diff/evidence step rather than asking
// this command to infer or mutate Git/Kubernetes state.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"fugue/internal/componentmanifest"
)

type stringList []string

func (values *stringList) String() string {
	if values == nil {
		return ""
	}
	return fmt.Sprintf("%v", []string(*values))
}

func (values *stringList) Set(value string) error {
	if value == "" {
		return errors.New("--path must not be empty")
	}
	*values = append(*values, value)
	return nil
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "fugue-component-plan:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("fugue-component-plan", flag.ContinueOnError)
	flags.SetOutput(stderr)
	manifestPath := flags.String("manifest", "docs/architecture/component-ownership-v1.yaml", "component ownership manifest")
	coordination := flags.Bool("coordination", false, "emit the observation-only coordination plan")
	var changedPaths stringList
	flags.Var(&changedPaths, "path", "repository-relative changed path (repeat for each path)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if len(changedPaths) == 0 {
		return errors.New("at least one --path is required")
	}
	file, err := os.Open(*manifestPath)
	if err != nil {
		return fmt.Errorf("open manifest: %w", err)
	}
	defer file.Close()
	manifest, err := componentmanifest.Load(file)
	if err != nil {
		return err
	}
	plan, err := componentmanifest.PlanChanges(manifest, changedPaths)
	if err != nil {
		return err
	}
	var output any = plan
	if *coordination {
		coordinationPlan, coordinationErr := componentmanifest.BuildShadowCoordinationPlan(plan)
		if coordinationErr != nil {
			return coordinationErr
		}
		output = coordinationPlan
	}
	encoded, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return fmt.Errorf("encode component change plan: %w", err)
	}
	encoded = append(encoded, '\n')
	if _, err := stdout.Write(encoded); err != nil {
		return fmt.Errorf("write component change plan: %w", err)
	}
	return nil
}
