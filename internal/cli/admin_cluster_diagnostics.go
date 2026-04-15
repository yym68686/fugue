package cli

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAdminClusterPodsCommand() *cobra.Command {
	opts := clusterPodsOptions{}
	cmd := &cobra.Command{
		Use:   "pods",
		Short: "List cluster pods across system and managed workloads",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			pods, err := client.ListClusterPods(opts)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"cluster_pods": pods})
			}
			return writeClusterPodTable(c.stdout, pods)
		},
	}
	cmd.Flags().StringVar(&opts.Namespace, "namespace", "", "Limit pods to one namespace")
	cmd.Flags().StringVar(&opts.Node, "node", "", "Limit pods to one node")
	cmd.Flags().StringVar(&opts.LabelSelector, "selector", "", "Kubernetes label selector")
	cmd.Flags().BoolVar(&opts.IncludeTerminated, "include-terminated", false, "Include succeeded and failed pods")
	return cmd
}

func (c *CLI) newAdminClusterEventsCommand() *cobra.Command {
	opts := clusterEventsOptions{Limit: 50}
	cmd := &cobra.Command{
		Use:   "events",
		Short: "List cluster events for system and managed workloads",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			events, err := client.ListClusterEvents(opts)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"events": events})
			}
			return writeClusterEventTable(c.stdout, events)
		},
	}
	cmd.Flags().StringVar(&opts.Namespace, "namespace", "", "Limit events to one namespace")
	cmd.Flags().StringVar(&opts.Kind, "kind", "", "Limit events to one involved object kind")
	cmd.Flags().StringVar(&opts.Name, "name", "", "Limit events to one involved object name")
	cmd.Flags().StringVar(&opts.Type, "type", "", "Limit events to one event type")
	cmd.Flags().StringVar(&opts.Reason, "reason", "", "Limit events to one reason")
	cmd.Flags().IntVar(&opts.Limit, "limit", opts.Limit, "Maximum events to return")
	return cmd
}

func (c *CLI) newAdminClusterLogsCommand() *cobra.Command {
	opts := clusterLogsOptions{TailLines: 200}
	cmd := &cobra.Command{
		Use:   "logs",
		Short: "Read pod logs from any cluster namespace",
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.Namespace) == "" || strings.TrimSpace(opts.Pod) == "" {
				return fmt.Errorf("--namespace and --pod are required")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			logs, err := client.GetClusterLogs(opts)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, logs)
			}
			_, err = fmt.Fprintln(c.stdout, strings.TrimRight(logs.Logs, "\n"))
			return err
		},
	}
	cmd.Flags().StringVar(&opts.Namespace, "namespace", "", "Pod namespace")
	cmd.Flags().StringVar(&opts.Pod, "pod", "", "Pod name")
	cmd.Flags().StringVar(&opts.Container, "container", "", "Container name")
	cmd.Flags().IntVar(&opts.TailLines, "tail", opts.TailLines, "Lines to read from the end of the log")
	cmd.Flags().BoolVar(&opts.Previous, "previous", false, "Read the previous container instance logs")
	return cmd
}

func (c *CLI) newAdminClusterExecCommand() *cobra.Command {
	opts := clusterExecRequest{
		Retries:    2,
		RetryDelay: 250 * time.Millisecond,
		Timeout:    60 * time.Second,
	}
	cmd := &cobra.Command{
		Use:   "exec --namespace <namespace> --pod <pod> -- <command...>",
		Short: "Run a diagnostic command inside a pod",
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.Namespace) == "" || strings.TrimSpace(opts.Pod) == "" {
				return fmt.Errorf("--namespace and --pod are required")
			}
			opts.Command = trimCommandArgs(args)
			if len(opts.Command) == 0 {
				return fmt.Errorf("command is required after --")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			result, err := client.ExecClusterPod(opts)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, result)
			}
			if result.AttemptCount > 1 {
				c.progressf("cluster_exec_attempts=%d", result.AttemptCount)
			}
			_, err = fmt.Fprintln(c.stdout, strings.TrimRight(result.Output, "\n"))
			return err
		},
	}
	cmd.Flags().StringVar(&opts.Namespace, "namespace", "", "Pod namespace")
	cmd.Flags().StringVar(&opts.Pod, "pod", "", "Pod name")
	cmd.Flags().StringVar(&opts.Container, "container", "", "Container name")
	cmd.Flags().IntVar(&opts.Retries, "retries", opts.Retries, "Retry count for transient EOF or stream failures")
	cmd.Flags().DurationVar(&opts.RetryDelay, "retry-delay", opts.RetryDelay, "Delay between retry attempts")
	cmd.Flags().DurationVar(&opts.Timeout, "timeout", opts.Timeout, "Per-attempt exec timeout")
	return cmd
}

func (c *CLI) newAdminClusterDNSCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dns",
		Short: "Run DNS diagnostics from the control plane",
	}
	cmd.AddCommand(c.newAdminClusterDNSResolveCommand())
	return cmd
}

func (c *CLI) newAdminClusterDNSResolveCommand() *cobra.Command {
	opts := struct {
		Server     string
		RecordType string
	}{RecordType: "A"}
	cmd := &cobra.Command{
		Use:   "resolve <name>",
		Short: "Resolve a DNS record with an optional explicit DNS server",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			result, err := client.ResolveClusterDNS(args[0], opts.Server, opts.RecordType)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, result)
			}
			return renderClusterDNSResolveResult(c.stdout, result)
		},
	}
	cmd.Flags().StringVar(&opts.Server, "server", "", "Explicit DNS server host or IP[:port]")
	cmd.Flags().StringVar(&opts.RecordType, "type", opts.RecordType, "Record type: A, AAAA, CNAME, TXT, MX, NS")
	return cmd
}

func (c *CLI) newAdminClusterNetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "net",
		Short: "Run network reachability probes from the control plane",
	}
	cmd.AddCommand(
		c.newAdminClusterNetConnectCommand(),
		c.newAdminClusterNetWebSocketCommand(),
	)
	return cmd
}

func (c *CLI) newAdminClusterNetConnectCommand() *cobra.Command {
	opts := struct {
		Timeout time.Duration
	}{Timeout: 5 * time.Second}
	cmd := &cobra.Command{
		Use:   "connect <target>",
		Short: "Open a TCP connection to a host:port from the control plane",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			result, err := client.ConnectClusterNetwork(args[0], opts.Timeout)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, result)
			}
			return renderClusterNetworkConnectResult(c.stdout, result)
		},
	}
	cmd.Flags().DurationVar(&opts.Timeout, "timeout", opts.Timeout, "Connection timeout")
	return cmd
}

func (c *CLI) newAdminClusterNetWebSocketCommand() *cobra.Command {
	opts := struct {
		Path    string
		Headers []string
		Timeout time.Duration
	}{
		Path:    "/",
		Timeout: 10 * time.Second,
	}
	cmd := &cobra.Command{
		Use:   "websocket <app>",
		Short: "Compare a direct app-service websocket handshake with the public route handshake",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			tenantID, projectID, err := c.resolveFilterSelections(client)
			if err != nil {
				return err
			}
			app, err := resolveAppReference(client, args[0], projectID, tenantID)
			if err != nil {
				return err
			}
			headers, err := parseHeaderArgs(opts.Headers)
			if err != nil {
				return err
			}
			result, err := client.ProbeClusterWebSocket(clusterWebSocketProbeRequest{
				AppID:   app.ID,
				Path:    strings.TrimSpace(opts.Path),
				Headers: headers,
			}, opts.Timeout)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, result)
			}
			return renderClusterWebSocketProbeResult(c.stdout, result)
		},
	}
	cmd.Flags().StringVar(&opts.Path, "path", opts.Path, "WebSocket request path, optionally including a query string")
	cmd.Flags().StringArrayVar(&opts.Headers, "header", nil, "Additional request header: Key=Value")
	cmd.Flags().DurationVar(&opts.Timeout, "timeout", opts.Timeout, "Per-handshake timeout")
	return cmd
}

func (c *CLI) newAdminClusterTLSCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tls",
		Short: "Run TLS handshake probes from the control plane",
	}
	cmd.AddCommand(c.newAdminClusterTLSProbeCommand())
	return cmd
}

func (c *CLI) newAdminClusterTLSProbeCommand() *cobra.Command {
	opts := struct {
		ServerName string
		Timeout    time.Duration
	}{Timeout: 5 * time.Second}
	cmd := &cobra.Command{
		Use:   "probe <target>",
		Short: "Probe a TLS endpoint, optionally with an explicit SNI server name",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			result, err := client.ProbeClusterTLS(args[0], opts.ServerName, opts.Timeout)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, result)
			}
			return renderClusterTLSProbeResult(c.stdout, result)
		},
	}
	cmd.Flags().StringVar(&opts.ServerName, "server-name", "", "Explicit TLS server name / SNI")
	cmd.Flags().DurationVar(&opts.Timeout, "timeout", opts.Timeout, "Handshake timeout")
	return cmd
}

func (c *CLI) newAdminClusterWorkloadCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workload",
		Short: "Inspect cluster workload manifests and pod placement",
	}
	cmd.AddCommand(c.newAdminClusterWorkloadShowCommand())
	return cmd
}

func (c *CLI) newAdminClusterWorkloadShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show <namespace> <kind> <name>",
		Short: "Show a Deployment, DaemonSet, StatefulSet, or Pod",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			workload, err := client.GetClusterWorkload(args[0], args[1], args[2])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"workload": workload})
			}
			return renderClusterWorkload(c.stdout, workload)
		},
	}
}

func (c *CLI) newAdminClusterRolloutCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rollout",
		Short: "Inspect rollout state for cluster workloads",
	}
	cmd.AddCommand(c.newAdminClusterRolloutStatusCommand())
	return cmd
}

func (c *CLI) newAdminClusterRolloutStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status <namespace> <kind> <name>",
		Short: "Show rollout status for a Deployment, DaemonSet, StatefulSet, or Pod",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			rollout, err := client.GetClusterRolloutStatus(args[0], args[1], args[2])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"rollout": rollout})
			}
			return renderClusterRolloutStatus(c.stdout, rollout)
		},
	}
}

func trimCommandArgs(args []string) []string {
	out := make([]string, 0, len(args))
	for _, arg := range args {
		arg = strings.TrimSpace(arg)
		if arg == "" {
			continue
		}
		out = append(out, arg)
	}
	return out
}

func parseHeaderArgs(values []string) (map[string]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	headers := make(map[string]string, len(values))
	for _, raw := range values {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		key, value, ok := strings.Cut(raw, "=")
		if !ok {
			return nil, fmt.Errorf("header %q must be Key=Value", raw)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("header %q is missing a key", raw)
		}
		headers[key] = strings.TrimSpace(value)
	}
	if len(headers) == 0 {
		return nil, nil
	}
	return headers, nil
}

func writeClusterPodTable(w io.Writer, pods []model.ClusterPod) error {
	sorted := append([]model.ClusterPod(nil), pods...)
	sortClusterPodsForCLI(sorted)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "NAMESPACE\tPOD\tPHASE\tREADY\tNODE\tOWNER\tCONTAINERS"); err != nil {
		return err
	}
	for _, pod := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%t\t%s\t%s\t%d\n",
			pod.Namespace,
			pod.Name,
			pod.Phase,
			pod.Ready,
			pod.NodeName,
			clusterPodOwnerLabel(pod.Owner),
			len(pod.Containers),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeClusterEventTable(w io.Writer, events []model.ClusterEvent) error {
	sorted := append([]model.ClusterEvent(nil), events...)
	sort.Slice(sorted, func(i, j int) bool {
		left := clusterEventSortTimeForCLI(sorted[i])
		right := clusterEventSortTimeForCLI(sorted[j])
		if !left.Equal(right) {
			return left.After(right)
		}
		if sorted[i].Namespace != sorted[j].Namespace {
			return sorted[i].Namespace < sorted[j].Namespace
		}
		return sorted[i].Name < sorted[j].Name
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "TIME\tTYPE\tREASON\tOBJECT\tMESSAGE"); err != nil {
		return err
	}
	for _, event := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s/%s\t%s\n",
			formatTime(clusterEventSortTimeForCLI(event)),
			event.Type,
			event.Reason,
			event.ObjectKind,
			event.ObjectName,
			event.Message,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeClusterWorkloadContainerTable(w io.Writer, title string, containers []model.ClusterWorkloadContainer) error {
	if len(containers) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w, title); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "NAME\tIMAGE"); err != nil {
		return err
	}
	for _, container := range containers {
		if _, err := fmt.Fprintf(tw, "%s\t%s\n", container.Name, container.Image); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeClusterWorkloadConditionTable(w io.Writer, conditions []model.ClusterWorkloadCondition) error {
	if len(conditions) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w, "conditions"); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "TYPE\tSTATUS\tREASON\tMESSAGE"); err != nil {
		return err
	}
	for _, condition := range conditions {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", condition.Type, condition.Status, condition.Reason, condition.Message); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeClusterTLSPeerCertificateTable(w io.Writer, certificates []model.ClusterTLSPeerCertificate) error {
	if len(certificates) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w, "peer_certificates"); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "SUBJECT\tISSUER\tNOT_AFTER\tSHA256"); err != nil {
		return err
	}
	for _, certificate := range certificates {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\n",
			certificate.Subject,
			certificate.Issuer,
			formatTime(certificate.NotAfter),
			certificate.SHA256,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func renderClusterDNSResolveResult(w io.Writer, result model.ClusterDNSResolveResult) error {
	if err := writeKeyValues(w,
		kvPair{Key: "name", Value: result.Name},
		kvPair{Key: "server", Value: result.Server},
		kvPair{Key: "record_type", Value: result.RecordType},
		kvPair{Key: "answers", Value: strconv.Itoa(len(result.Answers))},
		kvPair{Key: "error", Value: result.Error},
		kvPair{Key: "observed_at", Value: formatTime(result.ObservedAt)},
	); err != nil {
		return err
	}
	if len(result.Answers) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "TYPE\tVALUE"); err != nil {
		return err
	}
	for _, answer := range result.Answers {
		if _, err := fmt.Fprintf(tw, "%s\t%s\n", answer.Type, answer.Value); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func renderClusterNetworkConnectResult(w io.Writer, result model.ClusterNetworkConnectResult) error {
	return writeKeyValues(w,
		kvPair{Key: "target", Value: result.Target},
		kvPair{Key: "network", Value: result.Network},
		kvPair{Key: "success", Value: fmt.Sprintf("%t", result.Success)},
		kvPair{Key: "remote_addr", Value: result.RemoteAddr},
		kvPair{Key: "resolved_addresses", Value: strings.Join(result.ResolvedAddresses, ",")},
		kvPair{Key: "duration_ms", Value: strconv.FormatInt(result.DurationMillis, 10)},
		kvPair{Key: "error", Value: result.Error},
		kvPair{Key: "observed_at", Value: formatTime(result.ObservedAt)},
	)
}

func renderClusterWebSocketProbeResult(w io.Writer, result model.ClusterWebSocketProbeResult) error {
	if err := writeKeyValues(w,
		kvPair{Key: "app", Value: result.AppName},
		kvPair{Key: "app_id", Value: result.AppID},
		kvPair{Key: "path", Value: result.Path},
		kvPair{Key: "route_configured", Value: fmt.Sprintf("%t", result.RouteConfigured)},
		kvPair{Key: "conclusion_code", Value: result.ConclusionCode},
		kvPair{Key: "conclusion", Value: result.Conclusion},
		kvPair{Key: "observed_at", Value: formatTime(result.ObservedAt)},
	); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if err := renderClusterWebSocketProbeAttempt(w, "service", result.Service); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return renderClusterWebSocketProbeAttempt(w, "public_route", result.PublicRoute)
}

func renderClusterWebSocketProbeAttempt(w io.Writer, label string, attempt model.ClusterWebSocketProbeAttempt) error {
	if _, err := fmt.Fprintln(w, label); err != nil {
		return err
	}
	if err := writeKeyValues(w,
		kvPair{Key: "target", Value: attempt.Target},
		kvPair{Key: "url", Value: attempt.URL},
		kvPair{Key: "status", Value: attempt.Status},
		kvPair{Key: "status_code", Value: strconv.Itoa(attempt.StatusCode)},
		kvPair{Key: "upgraded", Value: fmt.Sprintf("%t", attempt.Upgraded)},
		kvPair{Key: "duration_ms", Value: strconv.FormatInt(attempt.DurationMillis, 10)},
		kvPair{Key: "error", Value: attempt.Error},
	); err != nil {
		return err
	}
	if strings.TrimSpace(attempt.BodyPreview) != "" {
		if _, err := fmt.Fprintf(w, "body_preview=%s\n", attempt.BodyPreview); err != nil {
			return err
		}
	}
	if len(attempt.Headers) > 0 {
		if _, err := fmt.Fprintln(w, "headers"); err != nil {
			return err
		}
		if err := writeStringMap(w, attempt.Headers); err != nil {
			return err
		}
	}
	return nil
}

func renderClusterTLSProbeResult(w io.Writer, result model.ClusterTLSProbeResult) error {
	if err := writeKeyValues(w,
		kvPair{Key: "target", Value: result.Target},
		kvPair{Key: "server_name", Value: result.ServerName},
		kvPair{Key: "success", Value: fmt.Sprintf("%t", result.Success)},
		kvPair{Key: "version", Value: result.Version},
		kvPair{Key: "cipher_suite", Value: result.CipherSuite},
		kvPair{Key: "negotiated_protocol", Value: result.NegotiatedProtocol},
		kvPair{Key: "verified", Value: fmt.Sprintf("%t", result.Verified)},
		kvPair{Key: "verification_error", Value: result.VerificationError},
		kvPair{Key: "duration_ms", Value: strconv.FormatInt(result.DurationMillis, 10)},
		kvPair{Key: "error", Value: result.Error},
		kvPair{Key: "observed_at", Value: formatTime(result.ObservedAt)},
	); err != nil {
		return err
	}
	if len(result.PeerCertificates) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return writeClusterTLSPeerCertificateTable(w, result.PeerCertificates)
}

func renderClusterWorkload(w io.Writer, workload model.ClusterWorkloadDetail) error {
	if err := writeKeyValues(w,
		kvPair{Key: "api_version", Value: workload.APIVersion},
		kvPair{Key: "kind", Value: workload.Kind},
		kvPair{Key: "namespace", Value: workload.Namespace},
		kvPair{Key: "name", Value: workload.Name},
		kvPair{Key: "selector", Value: workload.Selector},
		kvPair{Key: "desired_replicas", Value: formatOptionalInt32(workload.DesiredReplicas)},
		kvPair{Key: "ready_replicas", Value: formatOptionalInt32(workload.ReadyReplicas)},
		kvPair{Key: "updated_replicas", Value: formatOptionalInt32(workload.UpdatedReplicas)},
		kvPair{Key: "available_replicas", Value: formatOptionalInt32(workload.AvailableReplicas)},
		kvPair{Key: "current_replicas", Value: formatOptionalInt32(workload.CurrentReplicas)},
		kvPair{Key: "pods", Value: strconv.Itoa(len(workload.Pods))},
	); err != nil {
		return err
	}
	if len(workload.NodeSelector) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "node_selector"); err != nil {
			return err
		}
		if err := writeStringMap(w, workload.NodeSelector); err != nil {
			return err
		}
	}
	if len(workload.Tolerations) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "tolerations"); err != nil {
			return err
		}
		for _, toleration := range workload.Tolerations {
			if _, err := fmt.Fprintln(w, toleration); err != nil {
				return err
			}
		}
	}
	if len(workload.Containers) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if err := writeClusterWorkloadContainerTable(w, "containers", workload.Containers); err != nil {
			return err
		}
	}
	if len(workload.InitContainers) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if err := writeClusterWorkloadContainerTable(w, "init_containers", workload.InitContainers); err != nil {
			return err
		}
	}
	if len(workload.Conditions) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if err := writeClusterWorkloadConditionTable(w, workload.Conditions); err != nil {
			return err
		}
	}
	if len(workload.Pods) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		return writeClusterPodTable(w, workload.Pods)
	}
	return nil
}

func renderClusterRolloutStatus(w io.Writer, rollout model.ClusterRolloutStatus) error {
	if err := writeKeyValues(w,
		kvPair{Key: "kind", Value: rollout.Kind},
		kvPair{Key: "namespace", Value: rollout.Namespace},
		kvPair{Key: "name", Value: rollout.Name},
		kvPair{Key: "status", Value: rollout.Status},
		kvPair{Key: "desired_replicas", Value: formatOptionalInt32(rollout.DesiredReplicas)},
		kvPair{Key: "ready_replicas", Value: formatOptionalInt32(rollout.ReadyReplicas)},
		kvPair{Key: "updated_replicas", Value: formatOptionalInt32(rollout.UpdatedReplicas)},
		kvPair{Key: "available_replicas", Value: formatOptionalInt32(rollout.AvailableReplicas)},
		kvPair{Key: "message", Value: rollout.Message},
		kvPair{Key: "observed_at", Value: formatTime(rollout.ObservedAt)},
	); err != nil {
		return err
	}
	if len(rollout.Conditions) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return writeClusterWorkloadConditionTable(w, rollout.Conditions)
}

func sortClusterPodsForCLI(pods []model.ClusterPod) {
	sort.Slice(pods, func(i, j int) bool {
		if pods[i].Namespace != pods[j].Namespace {
			return pods[i].Namespace < pods[j].Namespace
		}
		if pods[i].NodeName != pods[j].NodeName {
			return pods[i].NodeName < pods[j].NodeName
		}
		return pods[i].Name < pods[j].Name
	})
}

func clusterPodOwnerLabel(owner *model.ClusterPodOwner) string {
	if owner == nil {
		return ""
	}
	return strings.TrimSpace(owner.Kind) + "/" + strings.TrimSpace(owner.Name)
}

func clusterEventSortTimeForCLI(event model.ClusterEvent) time.Time {
	for _, candidate := range []*time.Time{event.EventTime, event.LastTimestamp, event.FirstTimestamp} {
		if candidate != nil {
			return candidate.UTC()
		}
	}
	return time.Time{}
}

func formatOptionalInt32(value *int32) string {
	if value == nil {
		return ""
	}
	return strconv.FormatInt(int64(*value), 10)
}
