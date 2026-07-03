package controller

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	runtimepkg "fugue/internal/runtime"
)

const serviceAccountNamespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

var (
	errKubeNotFound = errors.New("kubernetes resource not found")
	errKubeConflict = errors.New("kubernetes resource conflict")
)

type kubeClient struct {
	client           *http.Client
	baseURL          string
	bearerToken      string
	namespace        string
	applyConcurrency int
	writeStats       kubeWriteStats
}

type kubeLease struct {
	APIVersion string `json:"apiVersion,omitempty"`
	Kind       string `json:"kind,omitempty"`
	Metadata   struct {
		Name            string            `json:"name,omitempty"`
		Namespace       string            `json:"namespace,omitempty"`
		ResourceVersion string            `json:"resourceVersion,omitempty"`
		Annotations     map[string]string `json:"annotations,omitempty"`
		Labels          map[string]string `json:"labels,omitempty"`
	} `json:"metadata"`
	Spec kubeLeaseSpec `json:"spec"`
}

type kubeLeaseSpec struct {
	HolderIdentity       string `json:"holderIdentity,omitempty"`
	LeaseDurationSeconds int    `json:"leaseDurationSeconds,omitempty"`
	AcquireTime          string `json:"acquireTime,omitempty"`
	RenewTime            string `json:"renewTime,omitempty"`
	LeaseTransitions     int    `json:"leaseTransitions,omitempty"`
}

type kubePodList struct {
	Items []kubePod `json:"items"`
}

type kubeEventList struct {
	Items []kubeEvent `json:"items"`
}

type kubeEvent struct {
	Metadata struct {
		Name              string    `json:"name,omitempty"`
		Namespace         string    `json:"namespace,omitempty"`
		CreationTimestamp time.Time `json:"creationTimestamp,omitempty"`
	} `json:"metadata"`
	InvolvedObject struct {
		Kind      string `json:"kind,omitempty"`
		Namespace string `json:"namespace,omitempty"`
		Name      string `json:"name,omitempty"`
		UID       string `json:"uid,omitempty"`
	} `json:"involvedObject,omitempty"`
	Type           string `json:"type,omitempty"`
	Reason         string `json:"reason,omitempty"`
	Message        string `json:"message,omitempty"`
	Count          int    `json:"count,omitempty"`
	FirstTimestamp string `json:"firstTimestamp,omitempty"`
	LastTimestamp  string `json:"lastTimestamp,omitempty"`
	EventTime      string `json:"eventTime,omitempty"`
}

type kubeNodeList struct {
	Items []kubeNode `json:"items"`
}

type kubeNode struct {
	Metadata struct {
		Name   string            `json:"name"`
		Labels map[string]string `json:"labels,omitempty"`
	} `json:"metadata"`
	Spec struct {
		Unschedulable bool        `json:"unschedulable,omitempty"`
		Taints        []kubeTaint `json:"taints,omitempty"`
	} `json:"spec"`
	Status struct {
		Conditions  []kubeNodeCondition `json:"conditions,omitempty"`
		Allocatable map[string]string   `json:"allocatable,omitempty"`
	} `json:"status"`
}

type kubeNodeCondition struct {
	Type   string `json:"type,omitempty"`
	Status string `json:"status,omitempty"`
}

type kubeTaint struct {
	Key    string `json:"key,omitempty"`
	Value  string `json:"value,omitempty"`
	Effect string `json:"effect,omitempty"`
}

type kubePersistentVolumeClaim struct {
	Metadata struct {
		Name        string            `json:"name"`
		Annotations map[string]string `json:"annotations,omitempty"`
		Labels      map[string]string `json:"labels,omitempty"`
	} `json:"metadata"`
	Spec struct {
		VolumeName       string `json:"volumeName,omitempty"`
		StorageClassName string `json:"storageClassName,omitempty"`
		Resources        struct {
			Requests map[string]string `json:"requests,omitempty"`
		} `json:"resources,omitempty"`
	} `json:"spec"`
	Status struct {
		Capacity map[string]string `json:"capacity,omitempty"`
	} `json:"status,omitempty"`
}

type kubePersistentVolume struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Spec struct {
		NodeAffinity struct {
			Required *struct {
				NodeSelectorTerms []struct {
					MatchExpressions []struct {
						Key      string   `json:"key,omitempty"`
						Operator string   `json:"operator,omitempty"`
						Values   []string `json:"values,omitempty"`
					} `json:"matchExpressions,omitempty"`
				} `json:"nodeSelectorTerms,omitempty"`
			} `json:"required,omitempty"`
		} `json:"nodeAffinity,omitempty"`
	} `json:"spec"`
}

type kubeStorageClass struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	AllowVolumeExpansion *bool `json:"allowVolumeExpansion,omitempty"`
}

type kubeJobList struct {
	Items []kubeJobInfo `json:"items"`
}

type kubeJobInfo struct {
	Metadata struct {
		Name   string            `json:"name"`
		Labels map[string]string `json:"labels,omitempty"`
	} `json:"metadata"`
	Status kubeJobStatus `json:"status"`
}

type kubeJobStatus struct {
	Active int `json:"active,omitempty"`
}

type kubeCronJob struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Spec struct {
		Suspend *bool `json:"suspend,omitempty"`
	} `json:"spec,omitempty"`
}

type kubeWorkloadList struct {
	Items []map[string]any `json:"items"`
}

type kubePod struct {
	Metadata struct {
		Name              string            `json:"name"`
		CreationTimestamp time.Time         `json:"creationTimestamp"`
		DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
		Labels            map[string]string `json:"labels,omitempty"`
		Annotations       map[string]string `json:"annotations,omitempty"`
	} `json:"metadata"`
	Spec   kubePodSpec `json:"spec"`
	Status struct {
		Phase                 string                `json:"phase"`
		Reason                string                `json:"reason,omitempty"`
		Message               string                `json:"message,omitempty"`
		Conditions            []kubePodCondition    `json:"conditions,omitempty"`
		InitContainerStatuses []kubeContainerStatus `json:"initContainerStatuses,omitempty"`
		ContainerStatuses     []kubeContainerStatus `json:"containerStatuses,omitempty"`
	} `json:"status"`
}

type kubePodSpec struct {
	NodeName                      string                  `json:"nodeName,omitempty"`
	TerminationGracePeriodSeconds *int64                  `json:"terminationGracePeriodSeconds,omitempty"`
	Tolerations                   []runtimepkg.Toleration `json:"tolerations,omitempty"`
	Volumes                       []kubePodVolume         `json:"volumes,omitempty"`

	InitContainers []kubeContainerSpec `json:"initContainers"`
	Containers     []kubeContainerSpec `json:"containers"`
}

type kubePodVolume struct {
	Name                  string                   `json:"name,omitempty"`
	PersistentVolumeClaim *kubePersistentVolumeRef `json:"persistentVolumeClaim,omitempty"`
}

type kubePersistentVolumeRef struct {
	ClaimName string `json:"claimName,omitempty"`
}

type kubeContainerSpec struct {
	Name      string                   `json:"name"`
	Image     string                   `json:"image,omitempty"`
	Resources kubeResourceRequirements `json:"resources,omitempty"`
}

type kubeResourceRequirements struct {
	Requests map[string]string `json:"requests,omitempty"`
	Limits   map[string]string `json:"limits,omitempty"`
}

type kubePodCondition struct {
	Type    string `json:"type,omitempty"`
	Status  string `json:"status,omitempty"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

type kubeContainerStatus struct {
	Name         string           `json:"name"`
	Ready        bool             `json:"ready,omitempty"`
	RestartCount int              `json:"restartCount,omitempty"`
	State        kubeRuntimeState `json:"state,omitempty"`
	LastState    kubeRuntimeState `json:"lastState,omitempty"`
}

type kubeRuntimeState struct {
	Waiting    *kubeStateDetail `json:"waiting,omitempty"`
	Terminated *kubeStateDetail `json:"terminated,omitempty"`
}

type kubeStateDetail struct {
	Reason     string `json:"reason,omitempty"`
	Message    string `json:"message,omitempty"`
	ExitCode   int    `json:"exitCode,omitempty"`
	StartedAt  string `json:"startedAt,omitempty"`
	FinishedAt string `json:"finishedAt,omitempty"`
}

func newKubeClient(namespace string) (*kubeClient, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, fmt.Errorf("kubernetes service host/port is not available in the environment")
	}

	token, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		return nil, fmt.Errorf("read service account token: %w", err)
	}
	caData, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("read service account CA: %w", err)
	}
	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(caData) {
		return nil, fmt.Errorf("load service account CA")
	}

	if strings.TrimSpace(namespace) == "" {
		if namespaceData, err := os.ReadFile(serviceAccountNamespacePath); err == nil {
			namespace = strings.TrimSpace(string(namespaceData))
		}
	}
	if strings.TrimSpace(namespace) == "" {
		return nil, fmt.Errorf("resolve kubernetes namespace")
	}

	return &kubeClient{
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig:     &tls.Config{RootCAs: rootCAs},
				ForceAttemptHTTP2:   true,
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 32,
				IdleConnTimeout:     90 * time.Second,
				TLSHandshakeTimeout: 5 * time.Second,
			},
		},
		baseURL:          "https://" + host + ":" + port,
		bearerToken:      strings.TrimSpace(string(token)),
		namespace:        strings.TrimSpace(namespace),
		applyConcurrency: 4,
	}, nil
}

func (c *kubeClient) effectiveNamespace(namespace string) string {
	namespace = strings.TrimSpace(namespace)
	if namespace != "" {
		return namespace
	}
	return c.namespace
}

func (c *kubeClient) getLease(ctx context.Context, namespace, name string) (kubeLease, bool, error) {
	var lease kubeLease
	status, err := c.doJSON(ctx, http.MethodGet, "/apis/coordination.k8s.io/v1/namespaces/"+c.effectiveNamespace(namespace)+"/leases/"+url.PathEscape(name), nil, &lease)
	if err != nil {
		if status == http.StatusNotFound {
			return kubeLease{}, false, nil
		}
		return kubeLease{}, false, err
	}
	return lease, true, nil
}

func (c *kubeClient) listJobsBySelector(ctx context.Context, namespace, selector string) ([]kubeJobInfo, error) {
	query := url.Values{}
	if strings.TrimSpace(selector) != "" {
		query.Set("labelSelector", selector)
	}

	var jobList kubeJobList
	apiPath := "/apis/batch/v1/namespaces/" + c.effectiveNamespace(namespace) + "/jobs"
	if encoded := query.Encode(); encoded != "" {
		apiPath += "?" + encoded
	}
	_, err := c.doJSON(ctx, http.MethodGet, apiPath, nil, &jobList)
	if err != nil {
		return nil, err
	}
	return jobList.Items, nil
}

func (c *kubeClient) getCronJob(ctx context.Context, namespace, name string) (kubeCronJob, bool, error) {
	var cronJob kubeCronJob
	status, err := c.doJSON(ctx, http.MethodGet, "/apis/batch/v1/namespaces/"+c.effectiveNamespace(namespace)+"/cronjobs/"+url.PathEscape(strings.TrimSpace(name)), nil, &cronJob)
	if err != nil {
		if status == http.StatusNotFound {
			return kubeCronJob{}, false, nil
		}
		return kubeCronJob{}, false, err
	}
	return cronJob, true, nil
}

func (c *kubeClient) listWorkloads(ctx context.Context, resource string) ([]map[string]any, error) {
	resource = strings.TrimSpace(resource)
	switch resource {
	case "deployments", "statefulsets", "daemonsets":
	default:
		return nil, fmt.Errorf("unsupported workload resource %q", resource)
	}
	var list kubeWorkloadList
	if _, err := c.doJSON(ctx, http.MethodGet, "/apis/apps/v1/"+resource, nil, &list); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (c *kubeClient) createJob(ctx context.Context, namespace string, job map[string]any) error {
	status, err := c.doJSON(ctx, http.MethodPost, "/apis/batch/v1/namespaces/"+c.effectiveNamespace(namespace)+"/jobs", job, nil)
	if status == http.StatusConflict {
		return nil
	}
	return err
}

func (c *kubeClient) deleteJob(ctx context.Context, namespace, name string) error {
	apiPath := "/apis/batch/v1/namespaces/" + c.effectiveNamespace(namespace) + "/jobs/" + url.PathEscape(strings.TrimSpace(name)) + "?propagationPolicy=Background"
	status, err := c.doJSON(ctx, http.MethodDelete, apiPath, nil, nil)
	if status == http.StatusNotFound {
		return nil
	}
	return err
}

func (c *kubeClient) listNodeNames(ctx context.Context) ([]string, error) {
	var nodeList kubeNodeList
	if _, err := c.doJSON(ctx, http.MethodGet, "/api/v1/nodes", nil, &nodeList); err != nil {
		return nil, err
	}

	names := make([]string, 0, len(nodeList.Items))
	for _, node := range nodeList.Items {
		name := strings.TrimSpace(node.Metadata.Name)
		if name == "" {
			continue
		}
		names = append(names, name)
	}
	return names, nil
}

func (c *kubeClient) listNodeReadyStates(ctx context.Context) (map[string]bool, error) {
	var nodeList kubeNodeList
	if _, err := c.doJSON(ctx, http.MethodGet, "/api/v1/nodes", nil, &nodeList); err != nil {
		return nil, err
	}

	readyByName := make(map[string]bool, len(nodeList.Items))
	for _, node := range nodeList.Items {
		name := strings.TrimSpace(node.Metadata.Name)
		if name == "" {
			continue
		}
		readyByName[name] = kubeNodeReady(node)
	}
	return readyByName, nil
}

func (c *kubeClient) getNode(ctx context.Context, name string) (kubeNode, bool, error) {
	var node kubeNode
	status, err := c.doJSON(ctx, http.MethodGet, "/api/v1/nodes/"+url.PathEscape(strings.TrimSpace(name)), nil, &node)
	if err != nil {
		if status == http.StatusNotFound {
			return kubeNode{}, false, nil
		}
		return kubeNode{}, false, err
	}
	return node, true, nil
}

func kubeNodeReady(node kubeNode) bool {
	for _, condition := range node.Status.Conditions {
		if !strings.EqualFold(strings.TrimSpace(condition.Type), "Ready") {
			continue
		}
		return strings.EqualFold(strings.TrimSpace(condition.Status), "True")
	}
	return false
}

func (c *kubeClient) listNodeRuntimeIDs(ctx context.Context) (map[string]string, error) {
	var nodeList kubeNodeList
	if _, err := c.doJSON(ctx, http.MethodGet, "/api/v1/nodes", nil, &nodeList); err != nil {
		return nil, err
	}

	runtimeIDs := make(map[string]string, len(nodeList.Items))
	for _, node := range nodeList.Items {
		name := strings.TrimSpace(node.Metadata.Name)
		if name == "" {
			continue
		}
		runtimeID := strings.TrimSpace(node.Metadata.Labels[runtimepkg.RuntimeIDLabelKey])
		if runtimeID == "" {
			continue
		}
		runtimeIDs[name] = runtimeID
	}
	return runtimeIDs, nil
}

func (c *kubeClient) createLease(ctx context.Context, namespace string, lease kubeLease) error {
	status, err := c.doJSON(ctx, http.MethodPost, "/apis/coordination.k8s.io/v1/namespaces/"+c.effectiveNamespace(namespace)+"/leases", lease, nil)
	if status == http.StatusConflict {
		return errKubeConflict
	}
	return err
}

func (c *kubeClient) updateLease(ctx context.Context, namespace string, lease kubeLease) error {
	status, err := c.doJSON(ctx, http.MethodPut, "/apis/coordination.k8s.io/v1/namespaces/"+c.effectiveNamespace(namespace)+"/leases/"+url.PathEscape(lease.Metadata.Name), lease, nil)
	if status == http.StatusConflict {
		return errKubeConflict
	}
	if status == http.StatusNotFound {
		return errKubeNotFound
	}
	return err
}

func (c *kubeClient) podWithContainerExists(ctx context.Context, namespace, labelSelector, containerName string) (bool, error) {
	pods, err := c.listPodsBySelector(ctx, namespace, labelSelector)
	if err != nil {
		return false, err
	}

	for _, pod := range pods {
		if pod.Status.Phase == "Succeeded" || pod.Status.Phase == "Failed" {
			continue
		}
		for _, container := range pod.Spec.Containers {
			if container.Name == containerName {
				return true, nil
			}
		}
	}
	return false, nil
}

func (c *kubeClient) getPod(ctx context.Context, namespace, name string) (kubePod, bool, error) {
	var pod kubePod
	status, err := c.doJSON(ctx, http.MethodGet, "/api/v1/namespaces/"+c.effectiveNamespace(namespace)+"/pods/"+url.PathEscape(strings.TrimSpace(name)), nil, &pod)
	if err != nil {
		if status == http.StatusNotFound {
			return kubePod{}, false, nil
		}
		return kubePod{}, false, err
	}
	return pod, true, nil
}

func (c *kubeClient) getPodIP(ctx context.Context, namespace, name string) (string, bool, error) {
	var pod struct {
		Status struct {
			PodIP string `json:"podIP,omitempty"`
		} `json:"status"`
	}
	status, err := c.doJSON(ctx, http.MethodGet, "/api/v1/namespaces/"+c.effectiveNamespace(namespace)+"/pods/"+url.PathEscape(strings.TrimSpace(name)), nil, &pod)
	if err != nil {
		if status == http.StatusNotFound {
			return "", false, nil
		}
		return "", false, err
	}
	return strings.TrimSpace(pod.Status.PodIP), true, nil
}

func (c *kubeClient) listPodsBySelector(ctx context.Context, namespace, labelSelector string) ([]kubePod, error) {
	query := url.Values{}
	if strings.TrimSpace(labelSelector) != "" {
		query.Set("labelSelector", labelSelector)
	}

	var podList kubePodList
	apiPath := "/api/v1/namespaces/" + c.effectiveNamespace(namespace) + "/pods"
	if encoded := query.Encode(); encoded != "" {
		apiPath += "?" + encoded
	}
	_, err := c.doJSON(ctx, http.MethodGet, apiPath, nil, &podList)
	if err != nil {
		return nil, err
	}

	sort.Slice(podList.Items, func(i, j int) bool {
		left := podList.Items[i]
		right := podList.Items[j]
		if !left.Metadata.CreationTimestamp.Equal(right.Metadata.CreationTimestamp) {
			return left.Metadata.CreationTimestamp.After(right.Metadata.CreationTimestamp)
		}
		return left.Metadata.Name < right.Metadata.Name
	})

	return podList.Items, nil
}

func (c *kubeClient) getPodLogs(ctx context.Context, namespace, podName, containerName string, previous bool, tailLines int) (string, bool, error) {
	podName = strings.TrimSpace(podName)
	if podName == "" {
		return "", false, nil
	}
	query := url.Values{}
	if strings.TrimSpace(containerName) != "" {
		query.Set("container", strings.TrimSpace(containerName))
	}
	if previous {
		query.Set("previous", "true")
	}
	if tailLines > 0 {
		query.Set("tailLines", fmt.Sprintf("%d", tailLines))
	}
	apiPath := "/api/v1/namespaces/" + c.effectiveNamespace(namespace) + "/pods/" + url.PathEscape(podName) + "/log"
	if encoded := query.Encode(); encoded != "" {
		apiPath += "?" + encoded
	}
	status, data, err := c.doRaw(ctx, http.MethodGet, apiPath, nil, "")
	if err != nil {
		if status == http.StatusNotFound || status == http.StatusBadRequest {
			return "", false, nil
		}
		return "", false, err
	}
	return string(data), true, nil
}

func (c *kubeClient) listEventsForObject(ctx context.Context, namespace, kind, name string) ([]kubeEvent, error) {
	namespace = c.effectiveNamespace(namespace)
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, nil
	}
	query := url.Values{}
	fields := []string{"involvedObject.name=" + name}
	if strings.TrimSpace(kind) != "" {
		fields = append(fields, "involvedObject.kind="+strings.TrimSpace(kind))
	}
	query.Set("fieldSelector", strings.Join(fields, ","))
	apiPath := "/api/v1/namespaces/" + namespace + "/events?" + query.Encode()
	var list kubeEventList
	status, err := c.doJSON(ctx, http.MethodGet, apiPath, nil, &list)
	if err != nil {
		if status == http.StatusForbidden || status == http.StatusNotFound {
			return nil, nil
		}
		return nil, err
	}
	sort.SliceStable(list.Items, func(i, j int) bool {
		left := kubeEventTime(list.Items[i])
		right := kubeEventTime(list.Items[j])
		if left.Equal(right) {
			return list.Items[i].Metadata.Name < list.Items[j].Metadata.Name
		}
		return left.Before(right)
	})
	return list.Items, nil
}

func (c *kubeClient) listAllPods(ctx context.Context) ([]kubePod, bool, error) {
	var podList kubePodList
	status, err := c.doJSON(ctx, http.MethodGet, "/api/v1/pods", nil, &podList)
	if err != nil {
		if status == http.StatusForbidden || status == http.StatusNotFound {
			return nil, false, nil
		}
		return nil, false, err
	}
	return podList.Items, true, nil
}

func (c *kubeClient) getPersistentVolumeClaim(ctx context.Context, namespace, name string) (kubePersistentVolumeClaim, bool, error) {
	var pvc kubePersistentVolumeClaim
	status, err := c.doJSON(ctx, http.MethodGet, "/api/v1/namespaces/"+c.effectiveNamespace(namespace)+"/persistentvolumeclaims/"+url.PathEscape(strings.TrimSpace(name)), nil, &pvc)
	if err != nil {
		if status == http.StatusNotFound {
			return kubePersistentVolumeClaim{}, false, nil
		}
		return kubePersistentVolumeClaim{}, false, err
	}
	return pvc, true, nil
}

func (c *kubeClient) getPersistentVolume(ctx context.Context, name string) (kubePersistentVolume, bool, error) {
	var pv kubePersistentVolume
	status, err := c.doJSON(ctx, http.MethodGet, "/api/v1/persistentvolumes/"+url.PathEscape(strings.TrimSpace(name)), nil, &pv)
	if err != nil {
		if status == http.StatusNotFound {
			return kubePersistentVolume{}, false, nil
		}
		return kubePersistentVolume{}, false, err
	}
	return pv, true, nil
}

func (c *kubeClient) getStorageClass(ctx context.Context, name string) (kubeStorageClass, bool, error) {
	var storageClass kubeStorageClass
	status, err := c.doJSON(ctx, http.MethodGet, "/apis/storage.k8s.io/v1/storageclasses/"+url.PathEscape(strings.TrimSpace(name)), nil, &storageClass)
	if err != nil {
		if status == http.StatusNotFound {
			return kubeStorageClass{}, false, nil
		}
		return kubeStorageClass{}, false, err
	}
	return storageClass, true, nil
}

func (c *kubeClient) doJSON(ctx context.Context, method, apiPath string, body any, out any) (int, error) {
	var payload io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, fmt.Errorf("marshal kubernetes request: %w", err)
		}
		payload = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+apiPath, payload)
	if err != nil {
		return 0, fmt.Errorf("create kubernetes request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("kubernetes request %s %s: %w", method, apiPath, err)
	}
	defer resp.Body.Close()

	responseBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("kubernetes request %s %s failed: status=%d body=%s", method, apiPath, resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	if out != nil && len(responseBody) > 0 {
		if err := json.Unmarshal(responseBody, out); err != nil {
			return resp.StatusCode, fmt.Errorf("decode kubernetes response: %w", err)
		}
	}
	return resp.StatusCode, nil
}

func (c *kubeClient) doRaw(ctx context.Context, method, apiPath string, body io.Reader, contentType string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+apiPath, body)
	if err != nil {
		return 0, nil, fmt.Errorf("create kubernetes request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	if strings.TrimSpace(contentType) != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("kubernetes request %s %s: %w", method, apiPath, err)
	}
	defer resp.Body.Close()
	responseBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return resp.StatusCode, responseBody, fmt.Errorf("kubernetes request %s %s failed: status=%d body=%s", method, apiPath, resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	return resp.StatusCode, responseBody, nil
}

func kubeEventTime(event kubeEvent) time.Time {
	for _, raw := range []string{event.EventTime, event.LastTimestamp, event.FirstTimestamp} {
		if parsed := parseKubeTimestamp(raw); !parsed.IsZero() {
			return parsed
		}
	}
	return event.Metadata.CreationTimestamp.UTC()
}

func parseKubeTimestamp(value string) time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return parsed.UTC()
}

func formatKubeTimestamp(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format("2006-01-02T15:04:05.000000Z07:00")
}
