package cli

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	buildEvidenceDefaultControlPlaneNamespace = "fugue-system"
	buildEvidenceLogTailLines                 = 400
	buildEvidenceLineLimit                    = 8
)

func (c *CLI) enrichBuildLogsArtifactReport(client *Client, appID string, report *appBuildArtifactReport) {
	if report == nil {
		return
	}

	namespace, err := c.detectBuildEvidenceControlPlaneNamespace(client)
	if err != nil {
		if !shouldIgnoreOptionalBuildEvidenceError(err) {
			report.Warnings = appendUniqueString(report.Warnings, fmt.Sprintf("control-plane namespace unavailable: %v", err))
		}
		namespace = buildEvidenceDefaultControlPlaneNamespace
	}

	if jobName := strings.TrimSpace(report.BuildJobName); jobName != "" {
		if err := c.collectBuilderIdentityEvidence(client, namespace, jobName, report); err != nil && !shouldIgnoreOptionalBuildEvidenceError(err) {
			report.Warnings = appendUniqueString(report.Warnings, fmt.Sprintf("builder identity unavailable: %v", err))
		}
	}

	controllerTerms := buildControllerEvidenceTerms(report)
	if len(controllerTerms) > 0 {
		pod, matches, err := c.collectComponentLogEvidence(
			client,
			namespace,
			"app.kubernetes.io/component=controller",
			"controller",
			controllerTerms,
		)
		switch {
		case err == nil:
			report.ControllerPod = strings.TrimSpace(pod)
			report.ControllerLogEvidence = append([]string(nil), matches...)
			summarizeBuilderJobControllerEvidence(report)
			if strings.TrimSpace(report.BuildJobName) != "" && len(report.BuilderPods) == 0 {
				if err := c.collectBuilderIdentityEvidence(client, namespace, report.BuildJobName, report); err != nil && !shouldIgnoreOptionalBuildEvidenceError(err) {
					report.Warnings = appendUniqueString(report.Warnings, fmt.Sprintf("builder identity unavailable: %v", err))
				}
			}
		case !shouldIgnoreOptionalBuildEvidenceError(err):
			report.Warnings = appendUniqueString(report.Warnings, fmt.Sprintf("controller evidence unavailable: %v", err))
		}
	}

	if err := c.collectBuilderOperationEvidence(client, namespace, report); err != nil && !shouldIgnoreOptionalBuildEvidenceError(err) {
		report.Warnings = appendUniqueString(report.Warnings, fmt.Sprintf("builder pod evidence unavailable: %v", err))
	}

	registryTerms := buildRegistryEvidenceTerms(report.ManagedImageRef)
	if len(registryTerms) > 0 {
		pod, matches, err := c.collectComponentLogEvidence(
			client,
			namespace,
			"app.kubernetes.io/component=registry",
			"registry",
			registryTerms,
		)
		switch {
		case err == nil:
			report.RegistryPod = strings.TrimSpace(pod)
			report.RegistryLogEvidence = append([]string(nil), matches...)
			report.RegistryPublishEvidence = append([]string(nil), registryManifestPutEvidence(matches)...)
		case !shouldIgnoreOptionalBuildEvidenceError(err):
			report.Warnings = appendUniqueString(report.Warnings, fmt.Sprintf("registry evidence unavailable: %v", err))
		}
	}

	events, err := client.ListAuditEvents()
	switch {
	case err == nil:
		summarizeRegistryLifecycleEvidence(appID, report, events)
	case shouldIgnoreOptionalBuildEvidenceError(err):
		summarizeRegistryLifecycleEvidence(appID, report, nil)
	default:
		report.Warnings = appendUniqueString(report.Warnings, fmt.Sprintf("registry lifecycle audit unavailable: %v", err))
		summarizeRegistryLifecycleEvidence(appID, report, nil)
	}
	finalizeBuilderJobState(report)
}

func (c *CLI) detectBuildEvidenceControlPlaneNamespace(client *Client) (string, error) {
	status, err := client.GetControlPlaneStatus()
	if err != nil {
		return buildEvidenceDefaultControlPlaneNamespace, err
	}
	return firstNonEmptyTrimmed(strings.TrimSpace(status.Namespace), buildEvidenceDefaultControlPlaneNamespace), nil
}

func (c *CLI) collectBuilderIdentityEvidence(client *Client, namespace, jobName string, report *appBuildArtifactReport) error {
	pods, err := client.ListClusterPods(clusterPodsOptions{
		Namespace:         strings.TrimSpace(namespace),
		LabelSelector:     "job-name=" + strings.TrimSpace(jobName),
		IncludeTerminated: true,
	})
	if err != nil {
		return err
	}
	if len(pods) == 0 {
		if report != nil {
			report.BuilderNamespace = firstNonEmptyTrimmed(strings.TrimSpace(report.BuilderNamespace), strings.TrimSpace(namespace))
			report.BuilderJobState = firstNonEmptyTrimmed(strings.TrimSpace(report.BuilderJobState), "created-no-pods-visible")
			report.BuilderJobEvidence = appendUniqueString(report.BuilderJobEvidence, fmt.Sprintf("builder job %s is recorded, but no builder pods are still visible", strings.TrimSpace(jobName)))
		}
		return nil
	}

	sortClusterPodsForEvidence(pods)
	report.BuilderNamespace = strings.TrimSpace(namespace)
	for _, pod := range pods {
		report.BuilderPods = appendUniqueString(report.BuilderPods, strings.TrimSpace(pod.Name))
		if nodeName := strings.TrimSpace(pod.NodeName); nodeName != "" {
			report.BuilderNodes = appendUniqueString(report.BuilderNodes, nodeName)
		}
		for _, container := range pod.Containers {
			if name := strings.TrimSpace(container.Name); name != "" {
				report.BuilderContainers = appendUniqueString(report.BuilderContainers, name)
			}
		}
		report.BuilderJobEvidence = appendUniqueString(report.BuilderJobEvidence, summarizeBuilderPodEvidence(pod))
		c.collectBuilderSchedulingEvidence(client, namespace, pod, report)
	}
	sort.Strings(report.BuilderPods)
	sort.Strings(report.BuilderNodes)
	sort.Strings(report.BuilderContainers)
	if strings.TrimSpace(report.BuilderJobState) == "" {
		report.BuilderJobState = "pod-observed"
	}
	return nil
}

func (c *CLI) collectBuilderOperationEvidence(client *Client, namespace string, report *appBuildArtifactReport) error {
	if report == nil || !buildArtifactUsesBuilder(report) || strings.TrimSpace(report.BuildOperationID) == "" {
		return nil
	}
	if len(report.BuilderPods) > 0 && strings.TrimSpace(report.BuildJobName) != "" {
		return nil
	}

	pods, err := client.ListClusterPods(clusterPodsOptions{
		Namespace:         strings.TrimSpace(namespace),
		LabelSelector:     "fugue.pro/operation-id=" + strings.TrimSpace(report.BuildOperationID),
		IncludeTerminated: true,
	})
	if err != nil {
		return err
	}
	if len(pods) == 0 {
		return nil
	}

	sortClusterPodsForEvidence(pods)
	report.BuilderNamespace = firstNonEmptyTrimmed(strings.TrimSpace(report.BuilderNamespace), strings.TrimSpace(namespace))
	if strings.TrimSpace(report.BuildJobName) == "" {
		for _, pod := range pods {
			if pod.Owner != nil && strings.EqualFold(strings.TrimSpace(pod.Owner.Kind), "Job") {
				report.BuildJobName = firstNonEmptyTrimmed(strings.TrimSpace(report.BuildJobName), strings.TrimSpace(pod.Owner.Name))
			}
		}
	}
	for _, pod := range pods {
		report.BuilderPods = appendUniqueString(report.BuilderPods, strings.TrimSpace(pod.Name))
		if nodeName := strings.TrimSpace(pod.NodeName); nodeName != "" {
			report.BuilderNodes = appendUniqueString(report.BuilderNodes, nodeName)
		}
		for _, container := range pod.Containers {
			if name := strings.TrimSpace(container.Name); name != "" {
				report.BuilderContainers = appendUniqueString(report.BuilderContainers, name)
			}
		}
		report.BuilderJobEvidence = appendUniqueString(report.BuilderJobEvidence, summarizeBuilderPodEvidence(pod))
		c.collectBuilderSchedulingEvidence(client, namespace, pod, report)
	}
	sort.Strings(report.BuilderPods)
	sort.Strings(report.BuilderNodes)
	sort.Strings(report.BuilderContainers)
	if strings.TrimSpace(report.BuilderJobState) == "" {
		report.BuilderJobState = "pod-observed"
	}
	return nil
}

func (c *CLI) collectComponentLogEvidence(client *Client, namespace, selector, preferredContainer string, terms []string) (string, []string, error) {
	pods, err := client.ListClusterPods(clusterPodsOptions{
		Namespace:         strings.TrimSpace(namespace),
		LabelSelector:     strings.TrimSpace(selector),
		IncludeTerminated: false,
	})
	if err != nil {
		return "", nil, err
	}
	if len(pods) == 0 {
		return "", nil, fmt.Errorf("no pods matched %q", selector)
	}
	sortClusterPodsForEvidence(pods)

	var (
		firstPodName string
		lastErr      error
	)
	for _, pod := range pods {
		podName := strings.TrimSpace(pod.Name)
		if podName == "" {
			continue
		}
		if firstPodName == "" {
			firstPodName = podName
		}
		logs, err := client.GetClusterLogs(clusterLogsOptions{
			Namespace: strings.TrimSpace(namespace),
			Pod:       podName,
			Container: selectEvidenceContainer(pod, preferredContainer),
			TailLines: buildEvidenceLogTailLines,
		})
		if err != nil {
			lastErr = err
			continue
		}
		matches := extractRelevantLogLines(logs.Logs, terms, buildEvidenceLineLimit)
		if len(matches) > 0 {
			return podName, matches, nil
		}
	}
	if lastErr != nil && firstPodName == "" {
		return "", nil, lastErr
	}
	if firstPodName == "" {
		return "", nil, fmt.Errorf("no readable pods matched %q", selector)
	}
	return firstPodName, nil, nil
}

func sortClusterPodsForEvidence(pods []model.ClusterPod) {
	sort.Slice(pods, func(i, j int) bool {
		left := clusterPodEvidenceScore(pods[i])
		right := clusterPodEvidenceScore(pods[j])
		if left != right {
			return left > right
		}
		leftTime := clusterPodEvidenceTimestamp(pods[i])
		rightTime := clusterPodEvidenceTimestamp(pods[j])
		if !leftTime.Equal(rightTime) {
			return leftTime.After(rightTime)
		}
		return strings.TrimSpace(pods[i].Name) < strings.TrimSpace(pods[j].Name)
	})
}

func clusterPodEvidenceScore(pod model.ClusterPod) int {
	score := 0
	if pod.Ready {
		score += 4
	}
	if strings.EqualFold(strings.TrimSpace(pod.Phase), "Running") {
		score += 2
	}
	if pod.StartTime != nil && !pod.StartTime.IsZero() {
		score++
	}
	return score
}

func clusterPodEvidenceTimestamp(pod model.ClusterPod) time.Time {
	if pod.StartTime != nil && !pod.StartTime.IsZero() {
		return pod.StartTime.UTC()
	}
	return time.Time{}
}

func selectEvidenceContainer(pod model.ClusterPod, preferred string) string {
	preferred = strings.TrimSpace(preferred)
	if preferred == "" {
		if len(pod.Containers) == 1 {
			return strings.TrimSpace(pod.Containers[0].Name)
		}
		return ""
	}
	for _, container := range pod.Containers {
		if strings.EqualFold(strings.TrimSpace(container.Name), preferred) {
			return strings.TrimSpace(container.Name)
		}
	}
	if len(pod.Containers) == 1 {
		return strings.TrimSpace(pod.Containers[0].Name)
	}
	return preferred
}

func buildControllerEvidenceTerms(report *appBuildArtifactReport) []string {
	if report == nil {
		return nil
	}
	terms := make([]string, 0, 5)
	for _, value := range []string{
		report.BuildOperationID,
		report.BuildJobName,
		report.ManagedImageRef,
		report.RuntimeImageRef,
		report.LinkedDeployOperationID,
	} {
		if value = strings.TrimSpace(value); value != "" {
			terms = appendUniqueString(terms, value)
		}
	}
	return terms
}

func buildRegistryEvidenceTerms(imageRef string) []string {
	imageRef = strings.TrimSpace(imageRef)
	if imageRef == "" {
		return nil
	}
	terms := []string{imageRef}
	if repository := registryRepositoryPath(imageRef); repository != "" {
		terms = appendUniqueString(terms, repository)
	}
	if tag := registryTagOrDigest(imageRef); tag != "" {
		terms = appendUniqueString(terms, tag)
	}
	return terms
}

func registryRepositoryPath(imageRef string) string {
	imageRef = strings.TrimSpace(imageRef)
	if imageRef == "" {
		return ""
	}
	if at := strings.Index(imageRef, "@"); at >= 0 {
		imageRef = imageRef[:at]
	}
	lastSlash := strings.LastIndex(imageRef, "/")
	lastColon := strings.LastIndex(imageRef, ":")
	if lastColon > lastSlash {
		imageRef = imageRef[:lastColon]
	}
	if slash := strings.Index(imageRef, "/"); slash >= 0 && slash+1 < len(imageRef) {
		return strings.TrimSpace(imageRef[slash+1:])
	}
	return strings.TrimSpace(imageRef)
}

func registryTagOrDigest(imageRef string) string {
	imageRef = strings.TrimSpace(imageRef)
	if imageRef == "" {
		return ""
	}
	if at := strings.LastIndex(imageRef, "@"); at >= 0 && at+1 < len(imageRef) {
		return strings.TrimSpace(imageRef[at+1:])
	}
	lastSlash := strings.LastIndex(imageRef, "/")
	lastColon := strings.LastIndex(imageRef, ":")
	if lastColon > lastSlash && lastColon+1 < len(imageRef) {
		return strings.TrimSpace(imageRef[lastColon+1:])
	}
	return ""
}

func extractRelevantLogLines(logs string, terms []string, limit int) []string {
	if strings.TrimSpace(logs) == "" || len(terms) == 0 || limit <= 0 {
		return nil
	}
	normalizedTerms := make([]string, 0, len(terms))
	for _, term := range terms {
		term = strings.ToLower(strings.TrimSpace(term))
		if term == "" {
			continue
		}
		normalizedTerms = appendUniqueString(normalizedTerms, term)
	}
	if len(normalizedTerms) == 0 {
		return nil
	}

	matches := make([]string, 0, limit)
	for _, line := range strings.Split(logs, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		normalizedLine := strings.ToLower(line)
		for _, term := range normalizedTerms {
			if !strings.Contains(normalizedLine, term) {
				continue
			}
			matches = appendUniqueString(matches, line)
			break
		}
		if len(matches) >= limit {
			break
		}
	}
	return matches
}

func summarizeRegistryLifecycleEvidence(appID string, report *appBuildArtifactReport, events []model.AuditEvent) {
	if report == nil {
		return
	}
	if normalizeImageInventoryStatus(report.RegistryImageStatus) != "missing" {
		return
	}

	imageRef := strings.TrimSpace(report.ManagedImageRef)
	if imageRef == "" {
		return
	}

	var lifecycleEvents []string
	registryPutEvidence := len(report.RegistryPublishEvidence) > 0
	controllerPublishEvidence := controllerEvidenceSuggestsPublished(report.ControllerLogEvidence)
	publishEvidence := registryPutEvidence || controllerPublishEvidence
	deleteEvidence := registryEvidenceSuggestsDelete(report.RegistryLogEvidence)
	deleteAudit := latestImageDeleteAuditEvent(events, appID, imageRef)

	if registryPutEvidence {
		lifecycleEvents = appendUniqueString(lifecycleEvents, "registry logs include a matching manifest PUT")
	}
	if controllerPublishEvidence {
		lifecycleEvents = appendUniqueString(lifecycleEvents, fmt.Sprintf("controller still records the image as published for build %s", firstNonEmptyTrimmed(report.BuildOperationID, report.BuildJobName, imageRef)))
	}
	if deleteEvidence {
		lifecycleEvents = appendUniqueString(lifecycleEvents, "registry logs include a matching delete or manifest-miss line")
	}
	if deleteAudit != nil {
		lifecycleEvents = appendUniqueString(lifecycleEvents, fmt.Sprintf("audit recorded app.image.delete at %s", formatTime(deleteAudit.CreatedAt.UTC())))
	}

	if !publishEvidence && !deleteEvidence && deleteAudit == nil {
		switch strings.TrimSpace(report.BuilderJobState) {
		case "not-observed":
			report.RegistryLifecycleState = "push-not-observed"
			report.RegistryLifecycleHint = fmt.Sprintf("no builder job or registry manifest PUT was observed for managed image %q", imageRef)
		case "failed":
			report.RegistryLifecycleState = "push-not-observed"
			report.RegistryLifecycleHint = fmt.Sprintf("builder job failed before any registry manifest PUT was observed for managed image %q", imageRef)
		case "started", "applied", "pod-observed", "created-no-pods-visible":
			report.RegistryLifecycleState = "push-not-observed"
			report.RegistryLifecycleHint = fmt.Sprintf("builder activity was observed, but no registry manifest PUT was observed for managed image %q", imageRef)
		}
		if report.RegistryLifecycleState != "" {
			report.RegistryLifecycleEvents = append([]string(nil), lifecycleEvents...)
		}
		return
	}

	switch {
	case deleteAudit != nil && publishEvidence:
		report.RegistryLifecycleState = "deleted-after-publish"
		report.RegistryLifecycleHint = fmt.Sprintf("managed image %q was published earlier and later deleted from registry inventory", imageRef)
	case deleteAudit != nil:
		report.RegistryLifecycleState = "deleted"
		report.RegistryLifecycleHint = fmt.Sprintf("managed image %q appears to have been deleted from registry inventory", imageRef)
	case deleteEvidence && publishEvidence:
		report.RegistryLifecycleState = "deleted-after-publish"
		report.RegistryLifecycleHint = fmt.Sprintf("managed image %q existed earlier and later disappeared from registry inventory", imageRef)
	case deleteEvidence:
		report.RegistryLifecycleState = "push-not-observed"
		switch strings.TrimSpace(report.BuilderJobState) {
		case "failed":
			report.RegistryLifecycleHint = fmt.Sprintf("builder job failed before any registry manifest PUT was observed for managed image %q", imageRef)
		case "started", "applied", "pod-observed", "created-no-pods-visible":
			report.RegistryLifecycleHint = fmt.Sprintf("registry reported manifest misses for managed image %q, but no registry manifest PUT was observed for this import", imageRef)
		default:
			report.RegistryLifecycleHint = fmt.Sprintf("managed image %q is missing and no publish evidence was observed for this import", imageRef)
		}
	case publishEvidence:
		report.RegistryLifecycleState = "previously-published-now-missing"
		report.RegistryLifecycleHint = fmt.Sprintf("managed image %q was published earlier but is now missing from registry inventory", imageRef)
	default:
		return
	}
	report.RegistryLifecycleEvents = append([]string(nil), lifecycleEvents...)
}

func registryEvidenceSuggestsDelete(lines []string) bool {
	for _, line := range lines {
		normalized := strings.ToLower(strings.TrimSpace(line))
		if strings.Contains(normalized, " delete ") || strings.HasPrefix(normalized, "delete ") || strings.Contains(normalized, " manifest unknown") || strings.Contains(normalized, "blob unknown") {
			return true
		}
		if strings.Contains(normalized, "delete /v2/") || strings.Contains(normalized, "\"delete\"") {
			return true
		}
	}
	return false
}

func registryManifestPutEvidence(lines []string) []string {
	matches := make([]string, 0, len(lines))
	for _, line := range lines {
		normalized := strings.ToLower(strings.TrimSpace(line))
		if strings.Contains(normalized, "/manifests/") && (strings.Contains(normalized, " put ") || strings.HasPrefix(normalized, "put ") || strings.Contains(normalized, "\"put ") || strings.Contains(normalized, "http.request.method=put") || strings.Contains(normalized, "msg=\"put /v2/")) {
			matches = appendUniqueString(matches, strings.TrimSpace(line))
		}
	}
	return matches
}

func controllerEvidenceSuggestsPublished(lines []string) bool {
	for _, line := range lines {
		normalized := strings.ToLower(strings.TrimSpace(line))
		if normalized == "" {
			continue
		}
		if strings.Contains(normalized, "completed import build") {
			return true
		}
		if strings.Contains(normalized, "managed_image=") && strings.Contains(normalized, "deploy=") {
			return true
		}
	}
	return false
}

func summarizeBuilderJobControllerEvidence(report *appBuildArtifactReport) {
	if report == nil {
		return
	}
	for _, line := range report.ControllerLogEvidence {
		normalized := strings.ToLower(strings.TrimSpace(line))
		if !strings.Contains(normalized, "builder job ") {
			continue
		}
		report.BuilderJobEvidence = appendUniqueString(report.BuilderJobEvidence, strings.TrimSpace(line))
		if name := logfmtField(line, "name"); name != "" {
			report.BuildJobName = firstNonEmptyTrimmed(strings.TrimSpace(report.BuildJobName), name)
		}
		if namespace := logfmtField(line, "namespace"); namespace != "" {
			report.BuilderNamespace = firstNonEmptyTrimmed(strings.TrimSpace(report.BuilderNamespace), namespace)
		}
		switch {
		case strings.Contains(normalized, "builder job apply failed"), strings.Contains(normalized, "builder job failed"):
			report.BuilderJobState = "failed"
		case strings.Contains(normalized, "waiting for placement"), strings.Contains(normalized, "failedscheduling"), strings.Contains(normalized, "unschedulable"):
			report.BuilderJobState = "waiting-placement"
		case strings.Contains(normalized, "builder job completed"):
			if report.BuilderJobState != "failed" {
				report.BuilderJobState = "completed"
			}
		case strings.Contains(normalized, "builder job applied"):
			if report.BuilderJobState == "" || report.BuilderJobState == "started" {
				report.BuilderJobState = "applied"
			}
		case strings.Contains(normalized, "builder job start"):
			if report.BuilderJobState == "" {
				report.BuilderJobState = "started"
			}
		default:
			if report.BuilderJobState == "" {
				report.BuilderJobState = "observed"
			}
		}
	}
}

func finalizeBuilderJobState(report *appBuildArtifactReport) {
	if report == nil {
		return
	}
	if !buildArtifactUsesBuilder(report) {
		if report.BuilderJobState == "" {
			report.BuilderJobState = "not-required"
		}
		return
	}
	if report.BuilderJobState != "" {
		return
	}
	if strings.TrimSpace(report.BuildJobName) != "" {
		report.BuilderJobState = "name-recorded"
		return
	}
	report.BuilderJobState = "not-observed"
	report.BuilderJobEvidence = appendUniqueString(report.BuilderJobEvidence, fmt.Sprintf("no builder job was observed for import %s", strings.TrimSpace(report.BuildOperationID)))
}

func buildArtifactUsesBuilder(report *appBuildArtifactReport) bool {
	if report == nil {
		return false
	}
	strategy := strings.TrimSpace(strings.ToLower(report.BuildStrategy))
	if strategy == strings.ToLower(model.AppSourceTypeDockerImage) {
		return false
	}
	return strategy != "" ||
		strings.TrimSpace(report.BuildOperationID) != "" ||
		strings.TrimSpace(report.ManagedImageRef) != "" ||
		strings.TrimSpace(report.BuildJobName) != "" ||
		len(report.BuilderPods) > 0
}

func summarizeBuilderPodEvidence(pod model.ClusterPod) string {
	detail := builderPodDetail(pod)
	if pod.Owner != nil && strings.EqualFold(strings.TrimSpace(pod.Owner.Kind), "Job") && strings.TrimSpace(pod.Owner.Name) != "" {
		summary := fmt.Sprintf(
			"builder pod %s for job %s phase=%s node=%s",
			strings.TrimSpace(pod.Name),
			strings.TrimSpace(pod.Owner.Name),
			firstNonEmptyTrimmed(strings.TrimSpace(pod.Phase), "unknown"),
			firstNonEmptyTrimmed(strings.TrimSpace(pod.NodeName), "-"),
		)
		if detail != "" {
			summary += " " + detail
		}
		return summary
	}
	summary := fmt.Sprintf(
		"builder pod %s phase=%s node=%s",
		strings.TrimSpace(pod.Name),
		firstNonEmptyTrimmed(strings.TrimSpace(pod.Phase), "unknown"),
		firstNonEmptyTrimmed(strings.TrimSpace(pod.NodeName), "-"),
	)
	if detail != "" {
		summary += " " + detail
	}
	return summary
}

func builderPodDetail(pod model.ClusterPod) string {
	for _, container := range pod.Containers {
		detail := strings.TrimSpace(container.Reason)
		if message := strings.TrimSpace(container.Message); message != "" {
			if detail != "" {
				detail += ": "
			}
			detail += message
		}
		if detail != "" {
			return detail
		}
	}
	return ""
}

func (c *CLI) collectBuilderSchedulingEvidence(client *Client, namespace string, pod model.ClusterPod, report *appBuildArtifactReport) {
	if report == nil || !strings.EqualFold(strings.TrimSpace(pod.Phase), "Pending") {
		return
	}

	switch {
	case strings.TrimSpace(pod.NodeName) == "":
		switch strings.TrimSpace(report.BuilderJobState) {
		case "", "pod-observed", "started", "applied", "observed", "name-recorded", "created-no-pods-visible":
			report.BuilderJobState = "waiting-placement"
		}
		report.BuilderJobEvidence = appendUniqueString(report.BuilderJobEvidence, fmt.Sprintf("builder pod %s is pending with no assigned node yet", strings.TrimSpace(pod.Name)))
	default:
		if strings.TrimSpace(report.BuilderJobState) == "" || strings.TrimSpace(report.BuilderJobState) == "pod-observed" {
			report.BuilderJobState = "scheduled-pending"
		}
	}

	events, err := client.ListClusterEvents(clusterEventsOptions{
		Namespace: strings.TrimSpace(namespace),
		Kind:      "Pod",
		Name:      strings.TrimSpace(pod.Name),
		Limit:     6,
	})
	if err != nil {
		if !shouldIgnoreOptionalBuildEvidenceError(err) {
			report.Warnings = appendUniqueString(report.Warnings, fmt.Sprintf("builder scheduling evidence unavailable: %v", err))
		}
		return
	}
	for _, event := range events {
		reason := strings.TrimSpace(event.Reason)
		message := strings.TrimSpace(event.Message)
		if reason == "" && message == "" {
			continue
		}
		report.BuilderJobEvidence = appendUniqueString(report.BuilderJobEvidence, fmt.Sprintf("builder pod %s event %s: %s", strings.TrimSpace(pod.Name), firstNonEmptyTrimmed(reason, "Unknown"), message))
		normalized := strings.ToLower(reason + " " + message)
		if strings.Contains(normalized, "failedscheduling") ||
			strings.Contains(normalized, "unschedulable") ||
			strings.Contains(normalized, "didn't match") ||
			strings.Contains(normalized, "0/") {
			report.BuilderJobState = "waiting-placement"
		}
	}
}

func logfmtField(line, key string) string {
	line = strings.TrimSpace(line)
	key = strings.TrimSpace(key)
	if line == "" || key == "" {
		return ""
	}
	token := key + "="
	index := strings.Index(line, token)
	if index < 0 {
		return ""
	}
	value := line[index+len(token):]
	if end := strings.IndexAny(value, " \t\r\n"); end >= 0 {
		value = value[:end]
	}
	return strings.Trim(strings.TrimSpace(value), "\"")
}

func latestImageDeleteAuditEvent(events []model.AuditEvent, appID, imageRef string) *model.AuditEvent {
	appID = strings.TrimSpace(appID)
	imageRef = strings.TrimSpace(imageRef)
	var latest *model.AuditEvent
	for i := range events {
		event := events[i]
		if !strings.EqualFold(strings.TrimSpace(event.Action), "app.image.delete") {
			continue
		}
		if appID != "" && !strings.EqualFold(strings.TrimSpace(event.TargetID), appID) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(event.Metadata["image_ref"]), imageRef) {
			continue
		}
		if latest == nil || event.CreatedAt.After(latest.CreatedAt) {
			eventCopy := event
			latest = &eventCopy
		}
	}
	return latest
}

func shouldIgnoreOptionalBuildEvidenceError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(message, "status=403") ||
		strings.Contains(message, "status=404") ||
		strings.Contains(message, "forbidden") ||
		strings.Contains(message, "not found")
}
