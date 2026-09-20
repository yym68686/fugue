package cli

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"text/tabwriter"
	"time"

	"fugue/internal/diagnosticadmission"
	"fugue/internal/livediagnostics"
	"fugue/internal/model"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type diagnosticProbeCatalogResponse struct {
	SourceRevision string                            `json:"source_revision,omitempty"`
	CatalogDigest  string                            `json:"catalog_digest"`
	CatalogStatus  string                            `json:"catalog_status"`
	RunnerImage    string                            `json:"runner_image"`
	Probes         []livediagnostics.ProbeDescriptor `json:"probes"`
}

func (c *CLI) newDiagnosticsProbesCommand(opts *platformDiagnosticCommandOptions) *cobra.Command {
	parent := &cobra.Command{Use: "probes", Short: "Discover independently published diagnostic probes"}
	run := func(cmd *cobra.Command, args []string) error {
		var response diagnosticProbeCatalogResponse
		if opts.direct {
			direct, err := newDirectPlatformDiagnosticClient(*opts)
			if err != nil {
				return err
			}
			catalog, err := direct.diagnosticCatalog(cmd.Context())
			if err != nil {
				return err
			}
			response = diagnosticProbeCatalogResponse{catalog.SourceRevision, catalog.Digest, catalog.Status, catalog.RunnerImage, catalog.Descriptors()}
		} else {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			if err := client.doJSON(http.MethodGet, "/v1/admin/diagnostics/probes", nil, &response); err != nil {
				return err
			}
		}
		if len(args) > 0 {
			for _, p := range response.Probes {
				if p.ID == args[0] || p.ID+"@"+p.Digest == args[0] {
					return c.writeJSON(p)
				}
			}
			return fmt.Errorf("probe is not registered")
		}
		if c.wantsJSON() {
			return c.writeJSON(response)
		}
		tw := tabwriter.NewWriter(c.stdout, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "PROBE\tPROFILE\tMAX_SECONDS\tDESCRIPTION")
		for _, p := range response.Probes {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%d\t%s\n", p.ID, p.Profile, p.MaxDurationSeconds, p.Description)
		}
		return tw.Flush()
	}
	parent.AddCommand(&cobra.Command{Use: "list", Aliases: []string{"ls"}, Short: "List verified probe definitions", Args: cobra.NoArgs, RunE: run}, &cobra.Command{Use: "inspect <probe-ref>", Short: "Show a probe input contract and immutable identity", Args: cobra.ExactArgs(1), RunE: run})
	return parent
}

var errDirectDiagnosticCatalogAbsent = errors.New("diagnostic catalog has not been published")

func (c *directPlatformDiagnosticClient) diagnosticCatalog(ctx context.Context) (livediagnostics.VerifiedCatalog, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	catalog, err := c.client.CoreV1().ConfigMaps(c.controlNS).Get(ctx, livediagnostics.CatalogConfigMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return livediagnostics.VerifiedCatalog{}, errDirectDiagnosticCatalogAbsent
	}
	if err != nil {
		return livediagnostics.VerifiedCatalog{}, err
	}
	trust, err := c.client.CoreV1().ConfigMaps(c.controlNS).Get(ctx, livediagnostics.TrustConfigMap, metav1.GetOptions{})
	if err != nil {
		return livediagnostics.VerifiedCatalog{}, errors.New("diagnostic trust configuration is unavailable")
	}
	keys := map[string]string{}
	if err := livediagnostics.DecodeStrict([]byte(trust.Data["keys.json"]), &keys); err != nil {
		return livediagnostics.VerifiedCatalog{}, err
	}
	return livediagnostics.LoadPublishedCatalog([]byte(catalog.Data["catalog.json"]), []byte(catalog.Data["previous-catalog.json"]), keys, nil)
}
func (c *directPlatformDiagnosticClient) startRegisteredProbe(ctx context.Context, req platformDiagnosticStartRequest) (platformDiagnosticSessionResponse, error) {
	if req.Kind != "" && req.Kind != livediagnostics.ProbeRegistered {
		return platformDiagnosticSessionResponse{}, errors.New("probe_ref cannot be combined with a built-in kind")
	}
	catalog, err := c.diagnosticCatalog(ctx)
	if err != nil {
		return platformDiagnosticSessionResponse{}, err
	}
	probe, parameters, err := catalog.Resolve(req.ProbeRef, req.Parameters)
	if err != nil {
		return platformDiagnosticSessionResponse{}, err
	}
	target, err := c.resolveRegisteredTarget(ctx, catalog, req.Target)
	if err != nil {
		return platformDiagnosticSessionResponse{}, err
	}
	jobs, err := c.client.BatchV1().Jobs(c.controlNS).List(ctx, metav1.ListOptions{LabelSelector: livediagnostics.ManagedByLabel + "=" + livediagnostics.ManagedByValue})
	if err != nil {
		return platformDiagnosticSessionResponse{}, err
	}
	if directActiveDiagnosticCount(jobs.Items) >= livediagnostics.MaxActiveGlobal || directActiveTargetCount(jobs.Items, target) > 0 {
		return platformDiagnosticSessionResponse{}, errors.New("diagnostic concurrency budget is exhausted")
	}
	id := model.DNS1035Label(model.NewID("diagnostic"), "diagnostic")
	options := livediagnostics.StartRequest{Kind: livediagnostics.ProbeRegistered, DurationSeconds: req.DurationSeconds, FrequencyHz: req.FrequencyHz, SampleIntervalMilliseconds: req.SampleIntervalMilliseconds}
	job, err := livediagnostics.BuildProbeJob(catalog, probe, target, id, c.controlNS, "direct-kubernetes", options, parameters)
	if err != nil {
		return platformDiagnosticSessionResponse{}, err
	}
	created, err := diagnosticadmission.AdmitJob(ctx, c.client, c.controlNS, &job)
	if err != nil {
		return platformDiagnosticSessionResponse{}, err
	}
	return platformDiagnosticSessionResponse{Session: directPlatformDiagnosticSession(*created)}, nil
}
func (c *directPlatformDiagnosticClient) resolveRegisteredTarget(ctx context.Context, catalog livediagnostics.VerifiedCatalog, req platformDiagnosticTargetRequest) (livediagnostics.Target, error) {
	if req.Type == livediagnostics.TargetNode {
		node, err := c.client.CoreV1().Nodes().Get(ctx, req.Node, metav1.GetOptions{})
		if err != nil {
			return livediagnostics.Target{}, err
		}
		ready := false
		for _, condition := range node.Status.Conditions {
			if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
				ready = true
			}
		}
		if !ready {
			return livediagnostics.Target{}, errors.New("node is not Ready for a bounded diagnostic Job")
		}
		return livediagnostics.Target{Type: req.Type, Node: node.Name}, nil
	}
	if req.Type == livediagnostics.TargetNodeProcess {
		return c.resolveNodeProcessTarget(ctx, req)
	}
	if req.Type != livediagnostics.TargetPlatformComponent {
		return livediagnostics.Target{}, errors.New("unsupported diagnostic target type")
	}
	namespace := strings.TrimSpace(req.Namespace)
	if namespace == "" {
		namespace = c.controlNS
	}
	if !catalog.AllowsNamespace(namespace) {
		return livediagnostics.Target{}, errors.New("target namespace is not authorized by diagnostic catalog")
	}
	if req.Pod == "" {
		return c.resolveComponentTarget(ctx, req)
	}
	pod, err := c.client.CoreV1().Pods(namespace).Get(ctx, req.Pod, metav1.GetOptions{})
	if err != nil {
		return livediagnostics.Target{}, err
	}
	if pod.Status.Phase != corev1.PodRunning || pod.UID == "" || pod.Spec.NodeName == "" {
		return livediagnostics.Target{}, errors.New("target does not have a running Pod identity")
	}
	container := req.Container
	if container == "" && len(pod.Spec.Containers) == 1 {
		container = pod.Spec.Containers[0].Name
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == container && status.ContainerID != "" && status.State.Running != nil {
			return livediagnostics.Target{Type: req.Type, Component: pod.Labels["app.kubernetes.io/component"], Namespace: namespace, Pod: pod.Name, PodUID: string(pod.UID), Node: pod.Spec.NodeName, Container: container, ContainerID: status.ContainerID, ImageDigest: firstNonEmptyTrimmed(status.ImageID, status.Image)}, nil
		}
	}
	return livediagnostics.Target{}, errors.New("target container is not running")
}
