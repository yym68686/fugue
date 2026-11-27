package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"fugue/internal/releaseguardian"
	"k8s.io/client-go/kubernetes"
)

func frontWorkerCutover(ctx context.Context, args []string, out io.Writer) error {
	observe := args[0] == "observe-front-worker-cutover"
	if observe && len(args) != 4 || !observe && len(args) != 2 {
		return errors.New(usage)
	}
	raw, err := readFile(args[1], 16<<10, false)
	if err != nil {
		return err
	}
	var config releaseguardian.FrontWorkerCutover
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return fmt.Errorf("read Front/Worker cutover specification: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("Front/Worker cutover specification must contain one JSON document")
	}
	emit := json.NewEncoder(out).Encode
	if !observe {
		policies, err := releaseguardian.FrontWorkerCutoverPolicies(config)
		if err != nil {
			return err
		}
		items := make([]any, 0, 2*len(policies))
		for _, policy := range policies {
			items = append(items, policy.Policy, policy.Binding)
		}
		return emit(struct {
			APIVersion string `json:"apiVersion"`
			Kind       string `json:"kind"`
			Items      []any  `json:"items"`
		}{"v1", "List", items})
	}
	cfg, err := readKubeObservationConfig(args[2], args[3])
	if err != nil {
		return err
	}
	cfg.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return frontWorkerCutoverReader{next}
	})
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if config.Phase == releaseguardian.KubeCutoverFrozen {
		boundary, err := releaseguardian.ObserveFrozenFrontWorkerBoundary(ctx, client, config)
		if err != nil {
			return err
		}
		return emit(struct {
			Config releaseguardian.FrontWorkerCutover `json:"config"`
			releaseguardian.FrozenFrontWorkerObservation
		}{config, boundary})
	}
	policies, err := releaseguardian.ObserveFrontWorkerCutover(ctx, client, config)
	if err != nil {
		return err
	}
	policyDigest, err := releaseguardian.FrontWorkerPolicyDigest(config, policies)
	if err != nil {
		return err
	}
	return emit(struct {
		Config       releaseguardian.FrontWorkerCutover             `json:"config"`
		Policies     []releaseguardian.FrontWorkerPolicyObservation `json:"policies"`
		PolicyDigest string                                         `json:"policyDigest"`
	}{config, policies, policyDigest})
}

type frontWorkerCutoverReader struct{ next http.RoundTripper }

func (reader frontWorkerCutoverReader) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet || req.Header.Get("Upgrade") != "" {
		return nil, errors.New("Front/Worker cutover observation only accepts plain Kubernetes GET")
	}
	return reader.next.RoundTrip(req)
}
