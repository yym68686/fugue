package observability

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	kubernetesLogDedupWindow = 5 * time.Minute

	kubernetesLabelName               = "app.kubernetes.io/name"
	kubernetesLabelComponent          = "app.kubernetes.io/component"
	kubernetesLabelFugueAppID         = "fugue.pro/app-id"
	kubernetesLabelFugueOwnerAppID    = "fugue.pro/owner-app-id"
	kubernetesLabelFugueTenantID      = "fugue.pro/tenant-id"
	kubernetesLabelFugueProjectID     = "fugue.pro/project-id"
	kubernetesLabelFugueRuntimeID     = "fugue.io/runtime-id"
	kubernetesLabelBackingServiceType = "fugue.pro/backing-service-type"
	kubernetesLabelRolloutSubsystem   = "fugue.io/rollout-subsystem"
	kubernetesPublicDataPlane         = "public-data-plane"
)

type kubernetesLogCollector struct {
	pipeline *Pipeline
	client   kubernetes.Interface
	deduper  *logLineDeduper
}

type kubernetesLogTarget struct {
	pod       corev1.Pod
	container string
}

type kubernetesLogIngestResult struct {
	ingested int
	scanned  int
}

func (p *Pipeline) runKubernetesLogCollection() {
	defer p.wg.Done()
	collector, err := newKubernetesLogCollector(p)
	if err != nil {
		p.kubernetesLogErrors.Add(1)
		p.recordError(fmt.Errorf("initialize Kubernetes log collector: %w", err))
		return
	}
	collector.run()
}

func newKubernetesLogCollector(p *Pipeline) (*kubernetesLogCollector, error) {
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	restConfig.UserAgent = "fugue-telemetry-agent"
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, err
	}
	return newKubernetesLogCollectorWithClient(p, client), nil
}

func newKubernetesLogCollectorWithClient(p *Pipeline, client kubernetes.Interface) *kubernetesLogCollector {
	maxEntries := p.cfg.KubernetesLogMaxLinesPerCycle * 2
	if maxEntries < 2048 {
		maxEntries = 2048
	}
	if maxEntries > 40000 {
		maxEntries = 40000
	}
	return &kubernetesLogCollector{
		pipeline: p,
		client:   client,
		deduper:  newLogLineDeduper(maxEntries),
	}
}

func (c *kubernetesLogCollector) run() {
	c.runCollectionLoop(c.collectOnce)
}

func (c *kubernetesLogCollector) runCollectionLoop(collect func(context.Context)) {
	collect(c.pipeline.ctx)
	ticker := time.NewTicker(c.pipeline.cfg.KubernetesLogPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.pipeline.ctx.Done():
			return
		case <-ticker.C:
			collect(c.pipeline.ctx)
		}
	}
}

func (c *kubernetesLogCollector) collectOnce(ctx context.Context) {
	targets, podCount, ok := c.kubernetesLogTargets(ctx, c.pipeline.cfg.KubernetesLogLabelSelector, true)
	if !ok {
		return
	}
	linesPerContainer := c.kubernetesLogLinesPerContainer(len(targets))
	priorityTargets := 0
	for _, target := range targets {
		if ctx.Err() != nil {
			return
		}
		priority := kubernetesLogPriorityTarget(target)
		if priority {
			priorityTargets++
		}
		result := c.collectContainerLogs(ctx, target.pod, target.container, linesPerContainer)
		if priority {
			tailLines := kubernetesLogTailLinesForRequest(c.pipeline.cfg, linesPerContainer)
			if tailLines > 0 && result.scanned >= int(tailLines) {
				c.pipeline.kubernetesPriorityTruncations.Add(1)
			}
		}
	}
	c.pipeline.kubernetesPriorityTargets.Store(int64(priorityTargets))
	c.pipeline.kubernetesLogPods.Store(int64(podCount))
}

func (c *kubernetesLogCollector) kubernetesLogTargets(ctx context.Context, labelSelector string, enforcePodLimit bool) ([]kubernetesLogTarget, int, bool) {
	cfg := c.pipeline.cfg
	listOptions := metav1.ListOptions{}
	if labelSelector = strings.TrimSpace(labelSelector); labelSelector != "" {
		listOptions.LabelSelector = labelSelector
	}
	pods, err := c.client.CoreV1().Pods("").List(ctx, listOptions)
	if err != nil {
		c.pipeline.kubernetesLogErrors.Add(1)
		c.pipeline.recordError(fmt.Errorf("list Kubernetes pods for log collection: %w", err))
		return nil, 0, false
	}
	sort.Slice(pods.Items, func(i, j int) bool {
		left := pods.Items[i].Namespace + "/" + pods.Items[i].Name
		right := pods.Items[j].Namespace + "/" + pods.Items[j].Name
		return left < right
	})

	c.deduper.Prune(time.Now().UTC().Add(-kubernetesLogDedupWindow))
	podCount := 0
	targets := []kubernetesLogTarget{}
	for i := range pods.Items {
		pod := pods.Items[i]
		if !kubernetesLogNamespaceAllowed(pod.Namespace, cfg.KubernetesLogNamespaces, cfg.KubernetesLogNamespacePrefixes) {
			continue
		}
		podCount++
		if enforcePodLimit && podCount > cfg.KubernetesLogMaxPods {
			c.pipeline.kubernetesLogErrors.Add(1)
			c.pipeline.recordError(fmt.Errorf("Kubernetes log collection reached pod limit %d", cfg.KubernetesLogMaxPods))
			break
		}
		for _, container := range kubernetesLogContainerNames(pod) {
			if !kubernetesContainerHasLogs(pod, container) {
				continue
			}
			targets = append(targets, kubernetesLogTarget{pod: pod, container: container})
		}
	}
	return targets, podCount, true
}

func (c *kubernetesLogCollector) kubernetesLogLinesPerContainer(targetCount int) int {
	cfg := c.pipeline.cfg
	tailLines := int(cfg.KubernetesLogTailLines)
	if tailLines <= 0 {
		tailLines = int(DefaultKubernetesLogTailLines)
	}
	if targetCount <= 0 || cfg.KubernetesLogMaxLinesPerCycle <= 0 {
		return tailLines
	}
	fairShare := cfg.KubernetesLogMaxLinesPerCycle / targetCount
	if cfg.KubernetesLogMaxLinesPerCycle%targetCount != 0 {
		fairShare++
	}
	if fairShare < 1 {
		fairShare = 1
	}
	if fairShare > tailLines {
		return tailLines
	}
	return fairShare
}

func (c *kubernetesLogCollector) collectContainerLogs(ctx context.Context, pod corev1.Pod, container string, maxLines int) kubernetesLogIngestResult {
	if maxLines <= 0 {
		return kubernetesLogIngestResult{}
	}
	cfg := c.pipeline.cfg
	sinceSeconds := int64((cfg.KubernetesLogPollInterval * 2).Seconds())
	if sinceSeconds < 5 {
		sinceSeconds = 5
	}
	tailLines := kubernetesLogTailLinesForRequest(cfg, maxLines)
	options := &corev1.PodLogOptions{
		Container:    container,
		Timestamps:   true,
		SinceSeconds: &sinceSeconds,
		TailLines:    &tailLines,
	}
	request := c.client.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, options)
	logCtx, cancel := context.WithTimeout(ctx, cfg.KubernetesLogPollInterval)
	defer cancel()
	stream, err := request.Stream(logCtx)
	if err != nil {
		if isBenignKubernetesLogReadError(err) {
			return kubernetesLogIngestResult{}
		}
		if ctx.Err() != nil {
			return kubernetesLogIngestResult{}
		}
		c.pipeline.kubernetesLogErrors.Add(1)
		c.pipeline.recordError(fmt.Errorf("read Kubernetes logs for %s/%s/%s: %w", pod.Namespace, pod.Name, container, err))
		return kubernetesLogIngestResult{}
	}
	defer stream.Close()

	return c.ingestLogStreamResult(logCtx, stream, pod, container, maxLines)
}

func kubernetesLogTailLinesForRequest(cfg Config, maxLines int) int64 {
	tailLines := cfg.KubernetesLogTailLines
	if tailLines <= 0 {
		tailLines = DefaultKubernetesLogTailLines
	}
	if maxLines <= 0 {
		return 0
	}
	if int64(maxLines) < tailLines {
		return int64(maxLines)
	}
	return tailLines
}

func (c *kubernetesLogCollector) ingestLogStream(ctx context.Context, stream io.Reader, pod corev1.Pod, container string, maxLines int) int {
	return c.ingestLogStreamResult(ctx, stream, pod, container, maxLines).ingested
}

func (c *kubernetesLogCollector) ingestLogStreamResult(ctx context.Context, stream io.Reader, pod corev1.Pod, container string, maxLines int) kubernetesLogIngestResult {
	scanner := bufio.NewScanner(stream)
	scanner.Buffer(make([]byte, 0, 64*1024), int(c.pipeline.cfg.MaxPayloadBytes))
	attrs := kubernetesLogAttributes(pod, container)
	source := "kubernetes://" + pod.Namespace + "/" + pod.Name + "/" + container
	ingested := 0
	scanned := 0
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return kubernetesLogIngestResult{scanned: scanned}
		default:
		}
		if scanned >= maxLines {
			break
		}
		scanned++
		timestamp, message := splitKubernetesLogLine(scanner.Text())
		priority := kubernetesLogPriorityMessage(message)
		key := kubernetesLogDedupKey(pod.Namespace, pod.Name, container, timestamp, message)
		if c.deduper.Seen(key, time.Now().UTC()) {
			continue
		}
		if c.pipeline.IngestLogLineWithAttributes(ctx, source, message, attrs, timestamp) {
			c.pipeline.kubernetesLogLines.Add(1)
			if priority {
				c.pipeline.kubernetesPriorityLines.Add(1)
			}
			ingested++
		}
	}
	if err := scanner.Err(); err != nil {
		if ctx.Err() != nil {
			return kubernetesLogIngestResult{ingested: ingested, scanned: scanned}
		}
		c.pipeline.kubernetesLogErrors.Add(1)
		c.pipeline.recordError(fmt.Errorf("scan Kubernetes logs for %s/%s/%s: %w", pod.Namespace, pod.Name, container, err))
	}
	return kubernetesLogIngestResult{ingested: ingested, scanned: scanned}
}

func kubernetesLogPriorityTarget(target kubernetesLogTarget) bool {
	if strings.EqualFold(strings.TrimSpace(target.pod.Labels[kubernetesLabelComponent]), "api") {
		return strings.EqualFold(strings.TrimSpace(target.container), "api")
	}
	if !strings.EqualFold(strings.TrimSpace(target.pod.Labels[kubernetesLabelRolloutSubsystem]), kubernetesPublicDataPlane) {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(target.container)) {
	case "edge", "edge-front":
		return true
	default:
		return false
	}
}

func kubernetesLogPriorityMessage(message string) bool {
	payload, ok := structuredLogJSONPayload(message)
	if !ok {
		return false
	}
	fields := map[string]any{}
	if err := json.Unmarshal([]byte(payload), &fields); err != nil {
		return false
	}
	for _, key := range []string{"fugue_table", "table"} {
		if table, ok := fields[key].(string); ok {
			switch strings.ToLower(strings.TrimSpace(table)) {
			case "request_fact", "request_facts":
				return true
			}
		}
	}
	eventType, _ := fields["event_type"].(string)
	switch strings.ToLower(strings.TrimSpace(eventType)) {
	case "request_fact", "request_summary", "edge_route_decision", "edge_route_decision_material_missing", "edge_request_body_buffer_slow", "edge_request_body_buffer_progress", "edge_front_tcp_connection":
		return true
	default:
		return false
	}
}

func kubernetesLogNamespaceAllowed(namespace string, exact []string, prefixes []string) bool {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return false
	}
	if len(exact) == 0 && len(prefixes) == 0 {
		return true
	}
	for _, candidate := range exact {
		if namespace == candidate {
			return true
		}
	}
	for _, prefix := range prefixes {
		if prefix != "" && strings.HasPrefix(namespace, prefix) {
			return true
		}
	}
	return false
}

func kubernetesLogContainerNames(pod corev1.Pod) []string {
	seen := map[string]struct{}{}
	names := make([]string, 0, len(pod.Spec.InitContainers)+len(pod.Spec.Containers)+len(pod.Spec.EphemeralContainers))
	add := func(name string) {
		name = strings.TrimSpace(name)
		if name == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	for _, container := range pod.Spec.InitContainers {
		add(container.Name)
	}
	for _, container := range pod.Spec.Containers {
		add(container.Name)
	}
	for _, container := range pod.Spec.EphemeralContainers {
		add(container.Name)
	}
	sort.Strings(names)
	return names
}

func kubernetesContainerHasLogs(pod corev1.Pod, container string) bool {
	statuses := make([]corev1.ContainerStatus, 0, len(pod.Status.InitContainerStatuses)+len(pod.Status.ContainerStatuses)+len(pod.Status.EphemeralContainerStatuses))
	statuses = append(statuses, pod.Status.InitContainerStatuses...)
	statuses = append(statuses, pod.Status.ContainerStatuses...)
	statuses = append(statuses, pod.Status.EphemeralContainerStatuses...)
	if len(statuses) == 0 {
		return true
	}
	for _, status := range statuses {
		if status.Name != container {
			continue
		}
		return status.State.Running != nil || status.State.Terminated != nil
	}
	return false
}

func isBenignKubernetesLogReadError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "not found") ||
		strings.Contains(message, "is terminated") ||
		strings.Contains(message, "is waiting to start")
}

func kubernetesLogAttributes(pod corev1.Pod, container string) map[string]string {
	labels := pod.Labels
	attrs := map[string]string{
		"namespace": pod.Namespace,
		"pod":       pod.Name,
		"container": container,
	}
	copyLabel := func(labelKey, attrKey string) {
		if value := strings.TrimSpace(labels[labelKey]); value != "" {
			attrs[attrKey] = value
		}
	}
	copyLabel(kubernetesLabelFugueTenantID, "tenant_id")
	copyLabel(kubernetesLabelFugueProjectID, "project_id")
	copyLabel(kubernetesLabelFugueAppID, "app_id")
	if attrs["app_id"] == "" {
		copyLabel(kubernetesLabelFugueOwnerAppID, "app_id")
	}
	copyLabel(kubernetesLabelFugueOwnerAppID, "owner_app_id")
	copyLabel(kubernetesLabelFugueRuntimeID, "runtime_id")
	if component := strings.TrimSpace(labels[kubernetesLabelComponent]); component != "" {
		attrs["component"] = component
	} else if component := strings.TrimSpace(labels[kubernetesLabelBackingServiceType]); component != "" {
		attrs["component"] = component
	} else if name := strings.TrimSpace(labels[kubernetesLabelName]); name != "" {
		attrs["component"] = name
	} else {
		attrs["component"] = container
	}
	return attrs
}

func splitKubernetesLogLine(line string) (time.Time, string) {
	line = strings.TrimRight(line, "\r\n")
	rawTimestamp, message, ok := strings.Cut(line, " ")
	if !ok {
		return time.Time{}, line
	}
	timestamp, err := time.Parse(time.RFC3339Nano, rawTimestamp)
	if err != nil {
		return time.Time{}, line
	}
	return timestamp.UTC(), message
}

func kubernetesLogDedupKey(namespace, pod, container string, timestamp time.Time, message string) [sha256.Size]byte {
	digest := sha256.New()
	for _, value := range []string{namespace, pod, container, timestamp.UTC().Format(time.RFC3339Nano), message} {
		_, _ = io.WriteString(digest, value)
		_, _ = digest.Write([]byte{0})
	}
	var key [sha256.Size]byte
	copy(key[:], digest.Sum(nil))
	return key
}

type logLineDeduper struct {
	mu      sync.Mutex
	maxSize int
	seen    map[[sha256.Size]byte]time.Time
	order   []logLineDedupEntry
}

type logLineDedupEntry struct {
	key    [sha256.Size]byte
	seenAt time.Time
}

func newLogLineDeduper(maxSize int) *logLineDeduper {
	if maxSize < 1 {
		maxSize = 1
	}
	return &logLineDeduper{maxSize: maxSize, seen: map[[sha256.Size]byte]time.Time{}, order: make([]logLineDedupEntry, 0, maxSize)}
}

func (d *logLineDeduper) Seen(key [sha256.Size]byte, now time.Time) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.seen[key]; ok {
		return true
	}
	if len(d.seen) >= d.maxSize {
		d.pruneLocked(now.Add(-kubernetesLogDedupWindow))
	}
	for len(d.seen) >= d.maxSize && len(d.order) > 0 {
		oldest := d.order[0]
		d.order = d.order[1:]
		if seenAt, ok := d.seen[oldest.key]; ok && seenAt.Equal(oldest.seenAt) {
			delete(d.seen, oldest.key)
		}
	}
	d.seen[key] = now
	d.order = append(d.order, logLineDedupEntry{key: key, seenAt: now})
	return false
}

func (d *logLineDeduper) Prune(cutoff time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pruneLocked(cutoff)
}

func (d *logLineDeduper) pruneLocked(cutoff time.Time) {
	retained := d.order[:0]
	for _, entry := range d.order {
		if entry.seenAt.Before(cutoff) {
			if seenAt, ok := d.seen[entry.key]; ok && seenAt.Equal(entry.seenAt) {
				delete(d.seen, entry.key)
			}
			continue
		}
		retained = append(retained, entry)
	}
	d.order = retained
}
