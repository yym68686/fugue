package cli

import (
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
)

type clusterPodsOptions struct {
	Namespace         string
	Node              string
	LabelSelector     string
	IncludeTerminated bool
}

type clusterEventsOptions struct {
	Namespace string
	Kind      string
	Name      string
	Type      string
	Reason    string
	Limit     int
}

type clusterLogsOptions struct {
	Namespace string
	Pod       string
	Container string
	TailLines int
	Previous  bool
}

type clusterLogsResponse struct {
	Namespace string `json:"namespace"`
	Pod       string `json:"pod"`
	Container string `json:"container,omitempty"`
	Logs      string `json:"logs"`
}

type clusterExecRequest struct {
	Namespace    string        `json:"namespace"`
	Pod          string        `json:"pod"`
	Container    string        `json:"container,omitempty"`
	Command      []string      `json:"command"`
	Retries      int           `json:"retries,omitempty"`
	RetryDelay   time.Duration `json:"-"`
	Timeout      time.Duration `json:"-"`
	RetryDelayMS int           `json:"retry_delay_ms,omitempty"`
	TimeoutMS    int           `json:"timeout_ms,omitempty"`
}

type clusterExecResponse struct {
	Namespace    string   `json:"namespace"`
	Pod          string   `json:"pod"`
	Container    string   `json:"container,omitempty"`
	Command      []string `json:"command"`
	Output       string   `json:"output"`
	AttemptCount int      `json:"attempt_count,omitempty"`
}

type clusterWebSocketProbeRequest struct {
	AppID     string            `json:"app_id"`
	Path      string            `json:"path,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"`
	TimeoutMS int               `json:"timeout_ms,omitempty"`
}

func (c *Client) ListClusterPods(opts clusterPodsOptions) ([]model.ClusterPod, error) {
	query := url.Values{}
	if value := strings.TrimSpace(opts.Namespace); value != "" {
		query.Set("namespace", value)
	}
	if value := strings.TrimSpace(opts.Node); value != "" {
		query.Set("node", value)
	}
	if value := strings.TrimSpace(opts.LabelSelector); value != "" {
		query.Set("label_selector", value)
	}
	if opts.IncludeTerminated {
		query.Set("include_terminated", "true")
	}

	relative := "/v1/cluster/pods"
	if encoded := query.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response struct {
		ClusterPods []model.ClusterPod `json:"cluster_pods"`
	}
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return nil, err
	}
	return response.ClusterPods, nil
}

func (c *Client) ListClusterEvents(opts clusterEventsOptions) ([]model.ClusterEvent, error) {
	query := url.Values{}
	if value := strings.TrimSpace(opts.Namespace); value != "" {
		query.Set("namespace", value)
	}
	if value := strings.TrimSpace(opts.Kind); value != "" {
		query.Set("kind", value)
	}
	if value := strings.TrimSpace(opts.Name); value != "" {
		query.Set("name", value)
	}
	if value := strings.TrimSpace(opts.Type); value != "" {
		query.Set("type", value)
	}
	if value := strings.TrimSpace(opts.Reason); value != "" {
		query.Set("reason", value)
	}
	if opts.Limit > 0 {
		query.Set("limit", strconv.Itoa(opts.Limit))
	}

	relative := "/v1/cluster/events"
	if encoded := query.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response struct {
		Events []model.ClusterEvent `json:"events"`
	}
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return nil, err
	}
	return response.Events, nil
}

func (c *Client) GetClusterLogs(opts clusterLogsOptions) (clusterLogsResponse, error) {
	query := url.Values{}
	query.Set("namespace", strings.TrimSpace(opts.Namespace))
	query.Set("pod", strings.TrimSpace(opts.Pod))
	if value := strings.TrimSpace(opts.Container); value != "" {
		query.Set("container", value)
	}
	if opts.TailLines > 0 {
		query.Set("tail_lines", strconv.Itoa(opts.TailLines))
	}
	if opts.Previous {
		query.Set("previous", "true")
	}

	var response clusterLogsResponse
	if err := c.doJSON(http.MethodGet, "/v1/cluster/logs?"+query.Encode(), nil, &response); err != nil {
		return clusterLogsResponse{}, err
	}
	return response, nil
}

func (c *Client) ExecClusterPod(req clusterExecRequest) (clusterExecResponse, error) {
	if req.RetryDelayMS == 0 && req.RetryDelay > 0 {
		req.RetryDelayMS = int(req.RetryDelay.Milliseconds())
	}
	if req.TimeoutMS == 0 && req.Timeout > 0 {
		req.TimeoutMS = int(req.Timeout.Milliseconds())
	}
	var response clusterExecResponse
	if err := c.doJSON(http.MethodPost, "/v1/cluster/exec", req, &response); err != nil {
		return clusterExecResponse{}, err
	}
	return response, nil
}

func (c *Client) ResolveClusterDNS(name, server, recordType string) (model.ClusterDNSResolveResult, error) {
	query := url.Values{}
	query.Set("name", strings.TrimSpace(name))
	if value := strings.TrimSpace(server); value != "" {
		query.Set("server", value)
	}
	if value := strings.TrimSpace(recordType); value != "" {
		query.Set("type", value)
	}
	var response model.ClusterDNSResolveResult
	if err := c.doJSON(http.MethodGet, "/v1/cluster/dns/resolve?"+query.Encode(), nil, &response); err != nil {
		return model.ClusterDNSResolveResult{}, err
	}
	return response, nil
}

func (c *Client) ConnectClusterNetwork(target string, timeout time.Duration) (model.ClusterNetworkConnectResult, error) {
	query := url.Values{}
	query.Set("target", strings.TrimSpace(target))
	if timeout > 0 {
		query.Set("timeout_ms", strconv.FormatInt(timeout.Milliseconds(), 10))
	}
	var response model.ClusterNetworkConnectResult
	if err := c.doJSON(http.MethodGet, "/v1/cluster/net/connect?"+query.Encode(), nil, &response); err != nil {
		return model.ClusterNetworkConnectResult{}, err
	}
	return response, nil
}

func (c *Client) ProbeClusterWebSocket(req clusterWebSocketProbeRequest, timeout time.Duration) (model.ClusterWebSocketProbeResult, error) {
	if req.TimeoutMS == 0 && timeout > 0 {
		req.TimeoutMS = int(timeout.Milliseconds())
	}
	var response model.ClusterWebSocketProbeResult
	if err := c.doJSON(http.MethodPost, "/v1/cluster/net/websocket", req, &response); err != nil {
		return model.ClusterWebSocketProbeResult{}, err
	}
	return response, nil
}

func (c *Client) ProbeClusterTLS(target, serverName string, timeout time.Duration) (model.ClusterTLSProbeResult, error) {
	query := url.Values{}
	query.Set("target", strings.TrimSpace(target))
	if value := strings.TrimSpace(serverName); value != "" {
		query.Set("server_name", value)
	}
	if timeout > 0 {
		query.Set("timeout_ms", strconv.FormatInt(timeout.Milliseconds(), 10))
	}
	var response model.ClusterTLSProbeResult
	if err := c.doJSON(http.MethodGet, "/v1/cluster/tls/probe?"+query.Encode(), nil, &response); err != nil {
		return model.ClusterTLSProbeResult{}, err
	}
	return response, nil
}

func (c *Client) GetClusterWorkload(namespace, kind, name string) (model.ClusterWorkloadDetail, error) {
	var response struct {
		Workload model.ClusterWorkloadDetail `json:"workload"`
	}
	if err := c.doJSON(
		http.MethodGet,
		path.Join("/v1/cluster/workloads", strings.TrimSpace(namespace), strings.TrimSpace(kind), strings.TrimSpace(name)),
		nil,
		&response,
	); err != nil {
		return model.ClusterWorkloadDetail{}, err
	}
	return response.Workload, nil
}

func (c *Client) GetClusterRolloutStatus(namespace, kind, name string) (model.ClusterRolloutStatus, error) {
	var response struct {
		Rollout model.ClusterRolloutStatus `json:"rollout"`
	}
	if err := c.doJSON(
		http.MethodGet,
		path.Join("/v1/cluster/rollouts", strings.TrimSpace(namespace), strings.TrimSpace(kind), strings.TrimSpace(name)),
		nil,
		&response,
	); err != nil {
		return model.ClusterRolloutStatus{}, err
	}
	return response.Rollout, nil
}
