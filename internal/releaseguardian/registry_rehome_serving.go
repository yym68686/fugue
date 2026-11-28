package releaseguardian

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/productioncontrol"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	kubejson "sigs.k8s.io/json"
)

// RegistryRehomeRoute is enrolled independently of the candidate and archive.
// It names the public HTTPS canary and the Service whose backend inventory
// must match the relocated Pods. Neither input is inferred from a response.
type RegistryRehomeRoute struct {
	Service     string `json:"service"`
	ServicePort string `json:"servicePort"`
	Address     string `json:"address"`
	Host        string `json:"host"`
	Path        string `json:"path"`
	Expected    string `json:"expected"`
}

func (r RegistryRehomeRoute) Validate() error {
	addr, err := netip.ParseAddrPort(r.Address)
	if !componentPattern.MatchString(r.Service) || len(validation.IsValidPortName(r.ServicePort)) != 0 ||
		err != nil || addr.Port() != 443 || !addr.Addr().IsGlobalUnicast() ||
		addr.Addr().Zone() != "" || !strings.Contains(r.Host, ".") || len(validation.IsDNS1123Subdomain(r.Host)) != 0 ||
		!validRehomeHTTPPath(r.Path) || !validRehomeHTTPMarker(r.Expected) {
		return errors.New("invalid independent registry rehome public route enrollment")
	}
	return nil
}

// RegistryRehomeServingObservation retains read-only health evidence against
// the exact frozen claim and proposed baseline. It is not a terminal result,
// does not attest provider drain, and cannot safely be used as a release seal.
type RegistryRehomeServingObservation struct {
	Cutover        RegistryRehomeCutoverReadback   `json:"cutover"`
	Route          RegistryRehomeRoute             `json:"route"`
	Service        RecoveryRecordRef               `json:"service"`
	EndpointSlices []RecoveryRecordRef             `json:"endpointSlices"`
	Dependency     LayerHealth                     `json:"dependency"`
	PodHTTP        []RegistryRehomeHTTPObservation `json:"podHTTP"`
	PublicHTTP     []RegistryRehomeHTTPObservation `json:"publicHTTP"`
	StartedAt      string                          `json:"startedAt"`
	FinishedAt     string                          `json:"finishedAt"`
	StateDigest    string                          `json:"stateDigest,omitempty"`
	EvidenceDigest string                          `json:"evidenceDigest,omitempty"`
}

// MaxRegistryRehomeServingReportBytes bounds retained report inputs. Unlike
// a signature, a report hash identifies bytes but does not prove their origin.
const MaxRegistryRehomeServingReportBytes = 128 << 10

// VerifyRetainedRegistryRehomeServing compares a fresh observation obtained
// from the enrolled provider with a separately pinned historical event.
// The caller must obtain fresh from ObserveRegistryRehomeServing, not a file.
// This comparison cannot establish provider write closure.
func VerifyRetainedRegistryRehomeServing(retained []byte, retainedPin string, fresh RegistryRehomeServingObservation) error {
	a, end, err := readRegistryRehomeServingReport(retained)
	if err != nil {
		return err
	}
	if a.EvidenceDigest != retainedPin {
		return errors.New("retained registry rehome serving event differs from its independent pin")
	}
	start, err := time.Parse(time.RFC3339Nano, fresh.StartedAt)
	if err != nil || !start.After(end) || fresh.EvidenceDigest == a.EvidenceDigest {
		return errors.New("registry rehome comparison requires a separate later observation")
	}
	if a.StateDigest != fresh.StateDigest {
		return errors.New("registry rehome serving state changed between observations")
	}
	return nil
}

func readRegistryRehomeServingReport(raw []byte) (RegistryRehomeServingObservation, time.Time, error) {
	var report RegistryRehomeServingObservation
	fail := func(reason string) (RegistryRehomeServingObservation, time.Time, error) {
		return RegistryRehomeServingObservation{}, time.Time{}, errors.New(reason)
	}
	if len(raw) == 0 || len(raw) > MaxRegistryRehomeServingReportBytes {
		return fail("invalid bounded registry rehome serving report")
	}
	if strict, err := kubejson.UnmarshalStrict(raw, &report); err != nil || len(strict) != 0 {
		return fail("invalid bounded registry rehome serving report")
	}
	start, startErr := time.Parse(time.RFC3339Nano, report.StartedAt)
	end, endErr := time.Parse(time.RFC3339Nano, report.FinishedAt)
	if startErr != nil || endErr != nil || end.Before(start) || end.Sub(start) > time.Minute ||
		report.Dependency.ObservedAt != report.StartedAt || report.Dependency.State != HealthHealthy {
		return fail("registry rehome serving report has an invalid observation window")
	}
	before := report
	if err := report.seal(); err != nil || before.StateDigest != report.StateDigest || before.EvidenceDigest != report.EvidenceDigest {
		return fail("registry rehome serving report differs from its content digests")
	}
	return report, end, nil
}

type RegistryRehomeHTTPObservation struct {
	Name       string `json:"name"`
	PodUID     string `json:"podUid,omitempty"`
	Address    string `json:"address"`
	Host       string `json:"host,omitempty"`
	Path       string `json:"path"`
	BodyDigest string `json:"bodyDigest"`
}

type registryRehomeHTTPReader func(ctx context.Context, address, host, path, expected string) (string, error)

// ObserveRegistryRehomeServing uses GET-only provider reads and explicit
// health targets. It does not proxy via, or require auth from, Fugue's API.
func ObserveRegistryRehomeServing(ctx context.Context, client kubernetes.Interface, journal *productioncontrol.Journal,
	cluster, intentDigest string, cutover KubeDeploymentCutover, provider productioncontrol.ParticipationKubernetesBinding,
	observer ed25519.PublicKey, recoveryRaw []byte, recoveryPin string, registry *IndependentRegistry, route RegistryRehomeRoute) (RegistryRehomeServingObservation, error) {
	return observeRegistryRehomeServing(ctx, client, journal, cluster, intentDigest, cutover, provider, observer, recoveryRaw, recoveryPin, registry,
		route, readRegistryRehomeHTTP)
}

func observeRegistryRehomeServing(ctx context.Context, client kubernetes.Interface, journal *productioncontrol.Journal,
	cluster, intentDigest string, cutover KubeDeploymentCutover, provider productioncontrol.ParticipationKubernetesBinding,
	observer ed25519.PublicKey, recoveryRaw []byte, recoveryPin string, registry *IndependentRegistry, route RegistryRehomeRoute,
	readHTTP registryRehomeHTTPReader) (RegistryRehomeServingObservation, error) {
	var none RegistryRehomeServingObservation
	if err := route.Validate(); err != nil {
		return none, err
	}
	if readHTTP == nil {
		return none, errors.New("registry rehome requires a health reader")
	}
	out := RegistryRehomeServingObservation{Route: route}
	combined, err := observeRegistryRehomeCutover(ctx, client, journal, cluster, intentDigest, cutover, provider, observer, recoveryRaw, recoveryPin, registry,
		func(verified verifiedRegistryRehomeRecoveryBundle, baseline RegistryRehomeBaseline, before RegistryRehomePendingObservation) error {
			// Scope health freshness independently of the potentially long OCI read.
			healthCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
			defer cancel()
			now := time.Now().UTC()
			out.StartedAt = now.Format(time.RFC3339Nano)
			return readRegistryRehomeServingHealth(healthCtx, client, verified.archive.forward.release, baseline, before, readHTTP, &out, now)
		})
	if err != nil {
		return none, err
	}
	finished := time.Now().UTC()
	started, err := time.Parse(time.RFC3339Nano, out.StartedAt)
	if err != nil || finished.Before(started) || finished.Sub(started) > time.Minute || ctx.Err() != nil {
		return none, errors.New("registry rehome serving observation exceeded its freshness window")
	}
	out.FinishedAt = finished.Format(time.RFC3339Nano)
	out.Cutover = combined
	if err := out.seal(); err != nil {
		return none, err
	}
	return out, nil
}

// StateDigest excludes only the observation's clock fields. It lets a fresh
// independent read compare the exact claim, baseline, provider versions and
// probe results without pretending to reproduce the original signed event.
// EvidenceDigest retains the full timestamps and binds the comparison digest.
func (observation *RegistryRehomeServingObservation) seal() error {
	state := *observation
	state.StartedAt, state.FinishedAt, state.Dependency.ObservedAt = "", "", ""
	state.StateDigest, state.EvidenceDigest = "", ""
	raw, err := declarativerelease.CanonicalJSON(state)
	if err != nil {
		return err
	}
	event := *observation
	event.StateDigest = digest(append([]byte("fugue.registry-rehome.serving-state.v1\x00"), raw...))
	event.EvidenceDigest = ""
	raw, err = declarativerelease.CanonicalJSON(event)
	if err != nil {
		return err
	}
	event.EvidenceDigest = digest(append([]byte("fugue.registry-rehome.serving-observation.v2\x00"), raw...))
	*observation = event
	return nil
}

func readRegistryRehomeServingHealth(ctx context.Context, client kubernetes.Interface, release declarativerelease.PlanRelease,
	baseline RegistryRehomeBaseline, before RegistryRehomePendingObservation, readHTTP registryRehomeHTTPReader,
	out *RegistryRehomeServingObservation, now time.Time) error {
	id := baseline.Workload
	if release.Delivery == nil || release.Delivery.Writer != "guardian" || release.Delivery.Group != baseline.Key.Group ||
		release.Delivery.DependencyService == "" || release.ComponentID != baseline.Key.Component ||
		recoveryWorkloadKey(release.Workload).resource != id {
		return errors.New("registry rehome serving health requires the sealed Guardian workload and dependency")
	}
	var podProbes, publicProbes []declarativerelease.HealthProbe
	primary := false
	for _, probe := range release.Health {
		switch probe.Type {
		case "deployment":
			if probe.Name != id.Name {
				return errors.New("registry rehome serving health cannot attest an auxiliary Deployment")
			}
			primary = true
		case "pod-http":
			if probe.Name != id.Name || !componentPattern.MatchString(probe.Port) || !validRehomeHTTPPath(probe.Path) ||
				!validRehomeHTTPMarker(probe.Expected) {
				return errors.New("registry rehome serving health requires a scoped Pod HTTP probe with an expected marker")
			}
			podProbes = append(podProbes, probe)
		case "public-route-http":
			route := RegistryRehomeRoute{Service: out.Route.Service, ServicePort: out.Route.ServicePort,
				Address: probe.Address, Host: probe.Host, Path: probe.Path, Expected: probe.Expected}
			if err := route.Validate(); err != nil {
				return err
			}
			publicProbes = append(publicProbes, probe)
		default:
			return fmt.Errorf("registry rehome serving health does not implement sealed probe %q", probe.Type)
		}
	}
	if !primary || len(podProbes) < 1 || len(podProbes)+len(publicProbes) > 8 {
		return errors.New("registry rehome serving health requires a bounded Deployment and Pod HTTP contract")
	}
	readPods := func() ([]corev1.Pod, error) {
		return readRehomeHealthPods(ctx, client, id, before.ProviderReadback)
	}
	pods, err := readPods()
	if err != nil {
		return err
	}
	// Link the selected port to a health contract port, not merely a Pod UID.
	probePorts := make(map[string]map[int32]bool, len(pods))
	for _, pod := range pods {
		ports := make(map[int32]bool, len(podProbes))
		for _, probe := range podProbes {
			_, port, err := registryRehomePodAddress(pod, release.Workload.Container, probe.Port)
			if err != nil {
				return err
			}
			ports[port] = true
		}
		probePorts[string(pod.UID)] = ports
	}
	readEndpoints := func() (RecoveryRecordRef, []RecoveryRecordRef, error) {
		return readRehomeServingEndpoints(ctx, client, id.Namespace, out.Route, pods, probePorts)
	}
	out.Service, out.EndpointSlices, err = readEndpoints()
	if err != nil {
		return err
	}
	target := TargetConfig{Key: baseline.Key, Namespace: id.Namespace, MonitorComponent: baseline.Key.Component, DependencyService: release.Delivery.DependencyService}
	if err := target.Validate(); err != nil {
		return err
	}
	out.Dependency, err = readRehomeDependency(ctx, client, target, now)
	if err != nil {
		return err
	}
	if len(pods)*len(podProbes)+len(publicProbes)+1 > 32 {
		return errors.New("registry rehome serving health exceeds its bounded HTTP probe count")
	}
	for _, probe := range podProbes {
		for _, pod := range pods {
			address, _, err := registryRehomePodAddress(pod, release.Workload.Container, probe.Port)
			if err != nil {
				return err
			}
			body, err := readHTTP(ctx, address, "", probe.Path, probe.Expected)
			if err != nil || !digestPattern.MatchString(body) {
				return errors.New("registry rehome exact serving Pod HTTP probe failed")
			}
			out.PodHTTP = append(out.PodHTTP, RegistryRehomeHTTPObservation{Name: probe.Name, PodUID: string(pod.UID), Address: address, Path: probe.Path, BodyDigest: body})
		}
	}
	// The enrolled route is always checked, even when historical packs only
	// require pod-http. Any sealed public-route probes are checked as well.
	publicProbes = append(publicProbes, declarativerelease.HealthProbe{Name: out.Route.Service,
		Address: out.Route.Address, Host: out.Route.Host, Path: out.Route.Path, Expected: out.Route.Expected})
	for _, probe := range publicProbes {
		body, err := readHTTP(ctx, probe.Address, probe.Host, probe.Path, probe.Expected)
		if err != nil || !digestPattern.MatchString(body) {
			return errors.New("registry rehome public HTTPS route probe failed")
		}
		out.PublicHTTP = append(out.PublicHTTP, RegistryRehomeHTTPObservation{Name: probe.Name, Address: probe.Address, Host: probe.Host, Path: probe.Path, BodyDigest: body})
	}
	service, endpoints, err := readEndpoints()
	if err != nil || service != out.Service || !slices.Equal(endpoints, out.EndpointSlices) {
		return errors.Join(errors.New("registry rehome Service or endpoint inventory changed during health probes"), err)
	}
	after, err := readRehomeDependency(ctx, client, target, now)
	if err != nil || after != out.Dependency {
		return errors.Join(errors.New("registry rehome dependency changed during health probes"), err)
	}
	return ctx.Err()
}

func readRehomeDependency(ctx context.Context, client kubernetes.Interface, target TargetConfig, now time.Time) (LayerHealth, error) {
	name := target.DependencyService
	svc, err := client.CoreV1().Services(target.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil || svc.UID == "" || svc.ResourceVersion == "" || svc.DeletionTimestamp != nil {
		return LayerHealth{}, errors.Join(errors.New("registry rehome dependency Service lacks a stable provider identity"), err)
	}
	list, err := client.DiscoveryV1().EndpointSlices(target.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: discoveryv1.LabelServiceName + "=" + name, Limit: 64,
	})
	if err != nil || list.Continue != "" || len(list.Items) > 64 {
		return LayerHealth{}, errors.Join(errors.New("registry rehome dependency endpoint inventory is unavailable or exceeds its bound"), err)
	}
	refs := make([]RecoveryRecordRef, 0, len(list.Items))
	ready := false
	for _, slice := range list.Items {
		if slice.UID == "" || slice.ResourceVersion == "" || slice.DeletionTimestamp != nil ||
			!metav1.IsControlledBy(&slice, svc) || slice.Labels[discoveryv1.LabelServiceName] != name {
			return LayerHealth{}, errors.New("registry rehome dependency EndpointSlice is not owned by the enrolled Service")
		}
		refs = append(refs, recoveryRef(&slice))
		for _, endpoint := range slice.Endpoints {
			c := endpoint.Conditions
			ready = ready || (c.Ready != nil && *c.Ready && (c.Terminating == nil || !*c.Terminating) && len(endpoint.Addresses) > 0)
		}
	}
	if !ready {
		return LayerHealth{}, errors.New("registry rehome dependency Service has no Ready endpoint")
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].Name < refs[j].Name })
	raw, err := declarativerelease.CanonicalJSON(struct {
		Service   RecoveryRecordRef
		Endpoints []RecoveryRecordRef
	}{recoveryRef(svc), refs})
	if err != nil {
		return LayerHealth{}, err
	}
	return LayerHealth{State: HealthHealthy, EvidenceDigest: digest(raw), ObservedAt: now.Format(time.RFC3339Nano)}, nil
}

func readRehomeHealthPods(ctx context.Context, client kubernetes.Interface, id declarativerelease.ResourceIdentity, before rehomePlacement) ([]corev1.Pod, error) {
	d, err := client.AppsV1().Deployments(id.Namespace).Get(ctx, id.Name, metav1.GetOptions{})
	if err != nil || d.DeletionTimestamp != nil || recoveryRef(d) != before.Deployment || d.Generation != before.Generation ||
		len(before.Pods) < 1 || len(before.Pods) > 32 {
		return nil, errors.Join(errors.New("registry rehome serving HTTP probe has no matching Deployment observation"), err)
	}
	selector, err := podTemplateLabelSelector(d.Spec.Template.Labels)
	if err != nil {
		return nil, err
	}
	list, err := client.CoreV1().Pods(id.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String(), Limit: 128})
	if err != nil || list.Continue != "" || len(list.Items) > 128 {
		return nil, errors.Join(errors.New("registry rehome Pod health inventory is unavailable or exceeds its bound"), err)
	}
	want := make(map[string]string, len(before.Pods))
	for _, pod := range before.Pods {
		want[pod.UID] = pod.ResourceVersion
	}
	pods := make([]corev1.Pod, 0, len(want))
	for _, pod := range list.Items {
		rv, ok := want[string(pod.UID)]
		if !ok {
			continue
		}
		if rv != pod.ResourceVersion || pod.DeletionTimestamp != nil || !podReady(pod) {
			return nil, errors.New("registry rehome exact serving Pod changed before its HTTP probe")
		}
		delete(want, string(pod.UID))
		pods = append(pods, pod)
	}
	if len(want) != 0 {
		return nil, errors.New("registry rehome serving Pods changed before their HTTP probes")
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i].UID < pods[j].UID })
	return pods, nil
}

func readRehomeServingEndpoints(ctx context.Context, client kubernetes.Interface, namespace string, route RegistryRehomeRoute,
	pods []corev1.Pod, probePorts map[string]map[int32]bool) (RecoveryRecordRef, []RecoveryRecordRef, error) {
	var none RecoveryRecordRef
	service := route.Service
	svc, err := client.CoreV1().Services(namespace).Get(ctx, service, metav1.GetOptions{})
	if err != nil || svc.UID == "" || svc.ResourceVersion == "" || svc.DeletionTimestamp != nil ||
		svc.Spec.Type == corev1.ServiceTypeExternalName || len(svc.Spec.Selector) == 0 {
		return none, nil, errors.Join(errors.New("registry rehome serving Service has no stable selector identity"), err)
	}
	var targetPort string
	for _, port := range svc.Spec.Ports {
		if port.Name != route.ServicePort {
			continue
		}
		if targetPort != "" || port.Port < 1 || (port.Protocol != "" && port.Protocol != corev1.ProtocolTCP) {
			return none, nil, errors.New("registry rehome enrolled Service port is invalid or ambiguous")
		}
		targetPort = port.TargetPort.String()
	}
	if targetPort == "" || targetPort == "0" {
		return none, nil, errors.New("registry rehome enrolled Service port has no explicit target")
	}
	selector := discoveryv1.LabelServiceName + "=" + service
	list, err := client.DiscoveryV1().EndpointSlices(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 64})
	if err != nil || list.Continue != "" || len(list.Items) > 64 {
		return none, nil, errors.Join(errors.New("registry rehome Service EndpointSlice inventory is unavailable or exceeds its bound"), err)
	}
	want := make(map[string]corev1.Pod, len(pods))
	targets := make(map[string]int32, len(pods))
	for _, pod := range pods {
		for key, value := range svc.Spec.Selector {
			if actual, ok := pod.Labels[key]; !ok || actual != value {
				return none, nil, errors.New("registry rehome Service does not select an exact serving Pod")
			}
		}
		uid := string(pod.UID)
		port, err := registryRehomeServiceTargetPort(pod, targetPort)
		if err != nil || !probePorts[uid][port] {
			return none, nil, errors.New("registry rehome enrolled Service port does not target the sealed Pod HTTP port")
		}
		want[uid], targets[uid] = pod, port
	}
	seen := make(map[string]bool, len(pods))
	refs := make([]RecoveryRecordRef, 0, len(list.Items))
	for _, slice := range list.Items {
		if slice.UID == "" || slice.ResourceVersion == "" || slice.DeletionTimestamp != nil ||
			slice.Labels[discoveryv1.LabelServiceName] != service || !metav1.IsControlledBy(&slice, svc) ||
			(slice.AddressType != discoveryv1.AddressTypeIPv4 && slice.AddressType != discoveryv1.AddressTypeIPv6) {
			return none, nil, errors.New("registry rehome EndpointSlice is not owned by the exact serving Service")
		}
		refs = append(refs, recoveryRef(&slice))
		var endpointPort int32
		for _, port := range slice.Ports {
			if port.Name == nil || *port.Name != route.ServicePort {
				continue
			}
			if endpointPort != 0 || port.Port == nil || *port.Port < 1 || *port.Port > 65535 ||
				(port.Protocol != nil && *port.Protocol != corev1.ProtocolTCP) {
				return none, nil, errors.New("registry rehome EndpointSlice serving port is invalid or ambiguous")
			}
			endpointPort = *port.Port
		}
		for _, endpoint := range slice.Endpoints {
			conditions := endpoint.Conditions
			// A terminating endpoint with Serving=true can still be used
			// when healthy local endpoints are unavailable. Fail closed.
			if conditions.Ready != nil && !*conditions.Ready && (conditions.Serving == nil || !*conditions.Serving) {
				continue
			}
			ref := endpoint.TargetRef
			if ref == nil || ref.UID == "" || ref.Kind != "Pod" || ref.Namespace != namespace || len(endpoint.Addresses) != 1 ||
				(endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating) {
				return none, nil, errors.New("registry rehome Service has an unverified serving endpoint")
			}
			pod, ok := want[string(ref.UID)]
			if !ok || pod.Name != ref.Name || !registryRehomePodHasIP(pod, endpoint.Addresses[0]) || endpointPort != targets[string(ref.UID)] {
				return none, nil, errors.New("registry rehome Service routes to a different Pod, address or port")
			}
			if conditions.Ready != nil && *conditions.Ready {
				seen[string(ref.UID)] = true
			}
		}
	}
	if len(seen) != len(want) {
		return none, nil, errors.New("registry rehome Service is missing exact serving endpoints")
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].Name < refs[j].Name })
	return recoveryRef(svc), refs, nil
}

func registryRehomePodHasIP(pod corev1.Pod, address string) bool {
	return address == pod.Status.PodIP || slices.ContainsFunc(pod.Status.PodIPs, func(ip corev1.PodIP) bool { return ip.IP == address })
}

func registryRehomeServiceTargetPort(pod corev1.Pod, target string) (int32, error) {
	if numeric, err := strconv.Atoi(target); err == nil {
		if numeric > 0 && numeric <= 65535 {
			return int32(numeric), nil
		}
		return 0, errors.New("invalid numeric Kubernetes Service target")
	}
	return registryRehomeNamedPort(pod, "", target)
}

func registryRehomePodAddress(pod corev1.Pod, container, portName string) (string, int32, error) {
	ip, err := netip.ParseAddr(pod.Status.PodIP)
	if err != nil || !ip.IsGlobalUnicast() || ip.Zone() != "" || pod.Spec.HostNetwork {
		return "", 0, errors.New("registry rehome Pod has no scoped probe IP")
	}
	port, err := registryRehomeNamedPort(pod, container, portName)
	if err != nil {
		return "", 0, err
	}
	return net.JoinHostPort(ip.String(), strconv.Itoa(int(port))), port, nil
}

func registryRehomeNamedPort(pod corev1.Pod, container, portName string) (int32, error) {
	var port int32
	for _, c := range pod.Spec.Containers {
		if container != "" && c.Name != container {
			continue
		}
		for _, p := range c.Ports {
			if p.Name == portName {
				if port != 0 || p.ContainerPort < 1 || p.ContainerPort > 65535 || (p.Protocol != "" && p.Protocol != corev1.ProtocolTCP) {
					return 0, errors.New("registry rehome Pod HTTP port is invalid or ambiguous")
				}
				port = p.ContainerPort
			}
		}
	}
	if port == 0 {
		return 0, errors.New("registry rehome Pod does not expose its sealed HTTP port")
	}
	return port, nil
}

func validRehomeHTTPPath(path string) bool {
	return len(path) <= 512 && strings.HasPrefix(path, "/") && !strings.HasPrefix(path, "//") &&
		!strings.ContainsAny(path, "?#\\\r\n\x00")
}

func validRehomeHTTPMarker(marker string) bool {
	return len(marker) > 0 && len(marker) <= 256 && !strings.ContainsAny(marker, "\r\n\x00")
}

func readRegistryRehomeHTTP(ctx context.Context, address, host, path, expected string) (string, error) {
	addr, err := netip.ParseAddrPort(address)
	if err != nil || addr.Port() == 0 || !validRehomeHTTPPath(path) || !validRehomeHTTPMarker(expected) {
		return "", errors.New("invalid registry rehome HTTP probe")
	}
	scheme, authority := "http", address
	if host != "" {
		scheme, authority = "https", host
	}
	dialer := net.Dialer{Timeout: 3 * time.Second}
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, ServerName: host},
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp", address)
		},
		DisableKeepAlives: true,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 6 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	endpoint := url.URL{Scheme: scheme, Host: authority, Path: path}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Cache-Control", "no-cache, no-store")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	const maxBody = 64 << 10
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil || resp.StatusCode != http.StatusOK || len(raw) > maxBody || !bytes.Contains(raw, []byte(expected)) {
		return "", errors.New("registry rehome HTTP response lacks fresh positive health evidence")
	}
	return digest(raw), nil
}
