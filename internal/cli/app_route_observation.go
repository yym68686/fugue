package cli

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"fugue/internal/model"
)

type appRouteShowResult struct {
	AppID             string                        `json:"app_id"`
	Route             *model.AppRoute               `json:"route,omitempty"`
	Namespace         string                        `json:"namespace,omitempty"`
	Deployment        string                        `json:"deployment,omitempty"`
	ExpectedRevision  string                        `json:"expected_revision,omitempty"`
	ReadyPods         []string                      `json:"ready_pods,omitempty"`
	ServingPods       []string                      `json:"serving_pods,omitempty"`
	ServingRevisions  []string                      `json:"serving_revisions,omitempty"`
	Service           *model.ClusterServiceDetail   `json:"service,omitempty"`
	Rollout           *model.ClusterRolloutStatus   `json:"rollout,omitempty"`
	PodInventory      *model.AppRuntimePodInventory `json:"pod_inventory,omitempty"`
	EndpointsSwitched bool                          `json:"endpoints_switched"`
	ConclusionCode    string                        `json:"conclusion_code,omitempty"`
	Conclusion        string                        `json:"conclusion,omitempty"`
	Warnings          []string                      `json:"warnings,omitempty"`
}

func (c *CLI) loadAppRouteShowResult(client *Client, app model.App) appRouteShowResult {
	result := appRouteShowResult{
		AppID:    strings.TrimSpace(app.ID),
		Route:    app.Route,
		Warnings: []string{},
	}

	inventory, err := client.GetAppRuntimePods(app.ID, "app")
	if err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("runtime pod inventory unavailable: %v", err))
		finalizeAppRouteShowResult(&result)
		return result
	}
	result.PodInventory = &inventory
	result.Namespace = strings.TrimSpace(inventory.Namespace)
	result.Deployment = deploymentNameFromInventory(inventory)
	result.ExpectedRevision = latestRuntimeRevision(inventory.Groups)
	result.ReadyPods = readyPodNames(inventory)

	servingPods, service, err := c.loadServingPodsForVerification(client, app, inventory)
	if err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("service endpoints unavailable: %v", err))
	} else {
		result.Service = service
		result.ServingPods = servingPods
		result.ServingRevisions = endpointPodRevisions(*service, inventory)
		if result.ExpectedRevision != "" && len(result.ServingRevisions) > 0 && allStringsEqual(result.ServingRevisions, result.ExpectedRevision) {
			result.EndpointsSwitched = true
		}
	}

	if result.Namespace != "" && result.Deployment != "" {
		rollout, err := client.GetClusterRolloutStatus(result.Namespace, "deployment", result.Deployment)
		if err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("rollout status unavailable: %v", err))
		} else {
			result.Rollout = &rollout
		}
	}

	finalizeAppRouteShowResult(&result)
	return result
}

func finalizeAppRouteShowResult(result *appRouteShowResult) {
	if result == nil {
		return
	}
	switch {
	case result.Service == nil || result.PodInventory == nil:
		result.ConclusionCode = "inconclusive"
		result.Conclusion = "the CLI could not load enough runtime/service evidence to prove which endpoints the public route currently reaches"
	case result.Rollout != nil && strings.EqualFold(strings.TrimSpace(result.Rollout.Status), "ready") && !result.EndpointsSwitched:
		result.ConclusionCode = "endpoints_not_switched"
		result.Conclusion = "the deployment rollout is ready, but the service endpoints have not fully switched to the serving revision"
	case result.EndpointsSwitched:
		result.ConclusionCode = "in_sync"
		result.Conclusion = "the public route service endpoints are serving the expected ready revision"
	case len(result.ServingPods) == 0:
		result.ConclusionCode = "no_endpoints"
		result.Conclusion = "the app service currently has no ready endpoints"
	default:
		result.ConclusionCode = "rolling"
		result.Conclusion = "the service endpoints are still converging on the latest ready revision"
	}
}

func renderAppRouteShowResult(w io.Writer, result appRouteShowResult) error {
	pairs := []kvPair{
		{Key: "app_id", Value: result.AppID},
	}
	if result.Route != nil {
		pairs = append(pairs,
			kvPair{Key: "hostname", Value: result.Route.Hostname},
			kvPair{Key: "base_domain", Value: result.Route.BaseDomain},
			kvPair{Key: "public_url", Value: result.Route.PublicURL},
		)
		if result.Route.ServicePort > 0 {
			pairs = append(pairs, kvPair{Key: "service_port", Value: fmt.Sprintf("%d", result.Route.ServicePort)})
		}
	}
	pairs = append(pairs,
		kvPair{Key: "namespace", Value: result.Namespace},
		kvPair{Key: "deployment", Value: result.Deployment},
		kvPair{Key: "expected_revision", Value: result.ExpectedRevision},
		kvPair{Key: "serving_revisions", Value: strings.Join(result.ServingRevisions, ",")},
		kvPair{Key: "endpoints_switched", Value: fmt.Sprintf("%t", result.EndpointsSwitched)},
		kvPair{Key: "conclusion_code", Value: result.ConclusionCode},
		kvPair{Key: "conclusion", Value: result.Conclusion},
	)
	if err := writeKeyValues(w, pairs...); err != nil {
		return err
	}
	if len(result.ReadyPods) > 0 {
		if _, err := fmt.Fprintf(w, "ready_pods=%s\n", strings.Join(result.ReadyPods, ",")); err != nil {
			return err
		}
	}
	if len(result.ServingPods) > 0 {
		if _, err := fmt.Fprintf(w, "serving_pods=%s\n", strings.Join(result.ServingPods, ",")); err != nil {
			return err
		}
	}
	if result.Rollout != nil {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if err := writeKeyValues(w,
			kvPair{Key: "rollout_status", Value: result.Rollout.Status},
			kvPair{Key: "rollout_message", Value: result.Rollout.Message},
			kvPair{Key: "desired_replicas", Value: formatOptionalInt32(result.Rollout.DesiredReplicas)},
			kvPair{Key: "ready_replicas", Value: formatOptionalInt32(result.Rollout.ReadyReplicas)},
			kvPair{Key: "updated_replicas", Value: formatOptionalInt32(result.Rollout.UpdatedReplicas)},
			kvPair{Key: "available_replicas", Value: formatOptionalInt32(result.Rollout.AvailableReplicas)},
		); err != nil {
			return err
		}
	}
	if result.Service != nil {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if err := writeKeyValues(w,
			kvPair{Key: "service_name", Value: result.Service.Name},
			kvPair{Key: "service_type", Value: result.Service.Type},
			kvPair{Key: "service_cluster_ip", Value: result.Service.ClusterIP},
		); err != nil {
			return err
		}
		if err := writeAppRouteEndpointTable(w, result.Service.Endpoints); err != nil {
			return err
		}
	}
	if result.PodInventory != nil {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "pods"); err != nil {
			return err
		}
		if err := renderAppRuntimePodInventory(w, *result.PodInventory); err != nil {
			return err
		}
	}
	for _, warning := range result.Warnings {
		if _, err := fmt.Fprintf(w, "warning=%s\n", warning); err != nil {
			return err
		}
	}
	return nil
}

func writeAppRouteEndpointTable(w io.Writer, endpoints []model.ClusterServiceEndpoint) error {
	if len(endpoints) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w, "service_endpoints"); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "POD\tREADY\tIP\tNODE\tTARGET"); err != nil {
		return err
	}
	for _, endpoint := range endpoints {
		target := firstNonEmptyTrimmed(endpoint.TargetName, endpoint.TargetKind)
		if _, err := fmt.Fprintf(tw, "%s\t%t\t%s\t%s\t%s\n", endpoint.Pod, endpoint.Ready, endpoint.IP, endpoint.NodeName, target); err != nil {
			return err
		}
	}
	return tw.Flush()
}
