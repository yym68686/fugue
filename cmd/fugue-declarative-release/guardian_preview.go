package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/releaseguardian"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func guardianSubmissionTarget(files map[string][]byte) (releaseguardian.TargetConfig, error) {
	plan, err := declarativerelease.DecodePlan(bytes.NewReader(files["release-plan.json"]))
	if err != nil {
		return releaseguardian.TargetConfig{}, err
	}
	artifact, err := declarativerelease.DecodeArtifactReceipt(bytes.NewReader(files["artifact-receipt.json"]))
	if err != nil {
		return releaseguardian.TargetConfig{}, err
	}
	release, err := selectedRelease(plan, artifact.Component)
	if err != nil {
		return releaseguardian.TargetConfig{}, err
	}
	if release.Delivery == nil || release.Delivery.Writer != "guardian" {
		return releaseguardian.TargetConfig{}, errors.New("component is not enrolled in Guardian delivery")
	}
	return releaseguardian.TargetConfig{
		Key:       releaseguardian.Key{Component: release.ComponentID, Group: release.Delivery.Group},
		Namespace: release.Workload.Namespace, MonitorComponent: release.ComponentID,
		DependencyService: release.Delivery.DependencyService,
	}, nil
}

func runGuardianPreview(parent context.Context, args []string, output io.Writer) error {
	compare := len(args) > 0 && args[0] == "guardian-native-shadow"
	if (!compare && len(args) != 4) || (compare && len(args) != 6) || strings.TrimSpace(args[1]) == "" || strings.TrimSpace(args[2]) == "" {
		return errors.New("usage: guardian-preview KUBECONFIG CONTEXT PLAN_DIR; guardian-native-shadow KUBECONFIG CONTEXT PLAN_DIR CLUSTER_ID REQUEST_ID")
	}
	files, err := readPlanDirectory(args[3])
	if err != nil {
		return err
	}
	target, err := guardianSubmissionTarget(files)
	if err != nil {
		return err
	}
	rules := &clientcmd.ClientConfigLoadingRules{ExplicitPath: args[1]}
	overrides := &clientcmd.ConfigOverrides{CurrentContext: args[2]}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
	if err != nil {
		return fmt.Errorf("load explicit Guardian shadow context: %w", err)
	}
	config.Wrap(func(transport http.RoundTripper) http.RoundTripper {
		return guardianPreviewTransport{transport}
	})
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create Guardian shadow client: %w", err)
	}
	store, err := releaseguardian.NewKubeStore(client, []releaseguardian.TargetConfig{target})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	if compare {
		observation, err := store.CompareNativeReleaseShadow(ctx, target.Key, args[4], args[5], files)
		if err != nil {
			return err
		}
		raw, err := declarativerelease.CanonicalJSON(observation)
		if err != nil {
			return err
		}
		_, err = output.Write(append(raw, '\n'))
		return err
	}
	preview, err := store.PreviewDesired(ctx, target.Key, files)
	if err != nil {
		return err
	}
	raw, err := declarativerelease.CanonicalJSON(struct {
		Kind    string                             `json:"kind"`
		Preview releaseguardian.PublicationPreview `json:"preview"`
	}{"GuardianPublicationShadowPreview", preview})
	if err != nil {
		return err
	}
	_, err = output.Write(append(raw, '\n'))
	return err
}

// The preview needs only non-upgraded Kubernetes GETs. Maintain this
// independent guard even when callers supply a read-only service account.
type guardianPreviewTransport struct{ next http.RoundTripper }

func (t guardianPreviewTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet || req.Header.Get("Upgrade") != "" {
		return nil, errors.New("Guardian shadow transport only allows non-upgraded GET requests")
	}
	return t.next.RoundTrip(req)
}
