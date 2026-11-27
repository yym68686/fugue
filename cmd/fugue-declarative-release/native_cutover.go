package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"fugue/internal/releaseguardian"
)

func runRenderNativeCutoverPolicy(args []string, output io.Writer) error {
	if len(args) != 2 {
		return errors.New("usage: fugue-declarative-release render-native-cutover-policy CUTOVER_SPEC_JSON")
	}
	file, err := os.Open(args[1])
	if err != nil {
		return err
	}
	defer file.Close()
	return renderNativeCutoverPolicy(file, output)
}

func renderNativeCutoverPolicy(input io.Reader, output io.Writer) error {
	const maximum = 16 << 10
	bounded := &io.LimitedReader{R: input, N: maximum + 1}
	decoder := json.NewDecoder(bounded)
	decoder.DisallowUnknownFields()
	var inputSpec releaseguardian.KubeDeploymentCutover
	if err := decoder.Decode(&inputSpec); err != nil {
		return fmt.Errorf("read Kubernetes cutover spec: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) || bounded.N == 0 {
		return errors.New("Kubernetes cutover spec must be one bounded JSON document")
	}
	policies, err := releaseguardian.DeploymentCutoverPolicies(inputSpec)
	if err != nil {
		return err
	}
	list := struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Items      []any  `json:"items"`
	}{APIVersion: "v1", Kind: "List"}
	for _, pair := range policies {
		list.Items = append(list.Items, pair.Policy, pair.Binding)
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(list)
}
