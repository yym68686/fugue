package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"syscall"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/productioncontrol"
	"fugue/internal/releaseguardian"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

const workerRetirementSchema = productioncontrol.FrontWorkerRetirementSchema

type workerRetirementPlan struct {
	Schema        string    `json:"schema"`
	JournalDigest string    `json:"journal_digest"`
	DaemonSet     string    `json:"daemonset"`
	DaemonSetUID  types.UID `json:"daemonset_uid"`
	Generation    int64     `json:"generation"`
}

type workerRetirementDecision struct {
	Plan      workerRetirementPlan `json:"plan"`
	Node      string               `json:"node"`
	WorkerUID string               `json:"worker_uid"`
	Initiated time.Time            `json:"initiated_at"`
	Deadline  time.Time            `json:"deadline"`
	Outcome   string               `json:"outcome"`
	Sockets   int                  `json:"old_sockets"`
}

func retirementName(phase string, j edgegroupfront.NodeWorkerJournal) string {
	return "fugue-node-worker-" + phase + "-" + j.Digest[7:39]
}

func (a *frontAuthorityActivator) loadWorkerRetirement(ctx context.Context, j edgegroupfront.NodeWorkerJournal) (*workerRetirementPlan, error) {
	name := retirementName("retirement", j)
	cm, err := a.client.CoreV1().ConfigMaps(a.config.Namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var p workerRetirementPlan
	if !j.FixedTransport || j.ProbeServiceUID == "" || cm.Name != name || cm.UID == "" || cm.ResourceVersion == "" ||
		cm.DeletionTimestamp != nil || cm.Immutable == nil || !*cm.Immutable || len(cm.BinaryData) != 0 || len(cm.Data) != 1 ||
		cm.Labels["fugue.pro/group"] != j.Recipient.Group || cm.Labels["fugue.pro/authority-kind"] != "node-worker-retirement" ||
		decodeStrictJSON([]byte(cm.Data["retirement.json"]), &p) != nil || p.Schema != workerRetirementSchema ||
		p.JournalDigest != j.Digest || len(validation.IsDNS1123Subdomain(p.DaemonSet)) != 0 ||
		p.DaemonSetUID == "" || p.Generation < 1 || p.Generation == 1<<63-1 {
		return nil, errors.New("invalid immutable node Worker retirement declaration")
	}
	return &p, nil
}

func workerRetirementKey(node string) string {
	sum := sha256.Sum256([]byte(node))
	return fmt.Sprintf("fugue.pro/worker-retired-%x", sum[:10])
}

// MatchFields is independent of labels. A node cannot regain this allocation
// by relabeling itself. Every OR term gets the same single-node restriction.
func excludeWorkerRetirementNode(spec *corev1.PodSpec, node string) error {
	if spec.Affinity == nil {
		spec.Affinity = &corev1.Affinity{}
	}
	if spec.Affinity.NodeAffinity == nil {
		spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	exclusion := corev1.NodeSelectorRequirement{Key: "metadata.name", Operator: corev1.NodeSelectorOpNotIn, Values: []string{node}}
	affinity := spec.Affinity.NodeAffinity
	if affinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		affinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchFields: []corev1.NodeSelectorRequirement{exclusion}}}}
		return nil
	}
	terms := affinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) == 0 {
		return errors.New("Worker has no schedulable node-affinity term")
	}
	for i := range terms {
		if len(terms[i].MatchFields)+len(terms[i].MatchExpressions) == 0 {
			return errors.New("refusing to widen an empty Worker node-affinity term")
		}
		terms[i].MatchFields = append(terms[i].MatchFields, exclusion)
	}
	return nil
}

func workerNodeExcluded(spec corev1.PodSpec, node string) bool {
	if spec.Affinity == nil || spec.Affinity.NodeAffinity == nil || spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return false
	}
	terms := spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	for _, term := range terms {
		match := false
		for _, field := range term.MatchFields {
			match = match || field.Key == "metadata.name" && field.Operator == corev1.NodeSelectorOpNotIn && len(field.Values) == 1 && field.Values[0] == node
		}
		if !match {
			return false
		}
	}
	return len(terms) > 0
}

func (p workerRetirementPlan) checkDS(ds *appsv1.DaemonSet, j edgegroupfront.NodeWorkerJournal, deadline time.Time) (*workerRetirementDecision, error) {
	if ds.UID != p.DaemonSetUID || ds.ResourceVersion == "" || ds.DeletionTimestamp != nil ||
		ds.Spec.UpdateStrategy.Type != appsv1.OnDeleteDaemonSetStrategyType || ds.Spec.Template.Spec.HostNetwork ||
		ds.Spec.Template.Labels["fugue.io/edge-slot"] != j.Expected.ActiveSlot ||
		ds.Spec.Template.Labels["fugue.io/edge-group-id"] != j.Recipient.Group {
		return nil, errors.New("old slot is not the declared OnDelete Worker DaemonSet")
	}
	raw, marked := ds.Annotations[workerRetirementKey(j.Recipient.Node)]
	if !marked && ds.Generation == p.Generation && !workerNodeExcluded(ds.Spec.Template.Spec, j.Recipient.Node) {
		return nil, nil
	}
	var d workerRetirementDecision
	if !marked || deadline.IsZero() || ds.Generation != p.Generation+1 || !workerNodeExcluded(ds.Spec.Template.Spec, j.Recipient.Node) ||
		decodeStrictJSON([]byte(raw), &d) != nil || d.Plan != p || d.Node != j.Recipient.Node ||
		d.WorkerUID != j.PredecessorWorkerUID || !d.Deadline.Equal(deadline) || d.Initiated.IsZero() ||
		d.Sockets < -1 || d.Outcome == "" || d.Outcome != retirementOutcome(d.Initiated, deadline, d.Sockets, true) {
		return nil, errors.New("old Worker DaemonSet diverged from its exact node retirement")
	}
	return &d, nil
}

func retirementOutcome(now, deadline time.Time, sockets int, frontDrained bool) string {
	if !now.Before(deadline) {
		return "deadline"
	}
	if sockets == 0 && frontDrained {
		return "empty"
	}
	return ""
}

func workerSlotPorts(slot string) (uint16, uint16) {
	if slot == "a" {
		return 18080, 18443
	}
	return 28080, 28443
}

// Inspect the Worker namespace, not one Front: a retired Front may still
// hold an upstream socket. Unknown or incomplete inventories fail closed.
func workerSlotSockets(raw []byte, httpPort, httpsPort uint16) (int, error) {
	headers, open := 0, 0
	listening := map[uint16]bool{}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if fields[0] == "sl" && len(fields) > 3 && fields[1] == "local_address" {
			headers++
			continue
		}
		if headers < 1 || len(fields) < 4 {
			return 0, errors.New("malformed Worker kernel TCP inventory")
		}
		_, rawPort, ok := strings.Cut(fields[1], ":")
		port, pErr := strconv.ParseUint(rawPort, 16, 16)
		state, sErr := strconv.ParseUint(fields[3], 16, 8)
		if !ok || pErr != nil || sErr != nil || state == 0 || state > 12 {
			return 0, errors.New("invalid Worker kernel TCP entry")
		}
		if uint16(port) != httpPort && uint16(port) != httpsPort {
			continue
		}
		switch state {
		case 10:
			listening[uint16(port)] = true
		case 6, 7: // TIME_WAIT and CLOSE do not retain a live exchange.
		default:
			open++
		}
	}
	if headers != 2 || !listening[httpPort] || !listening[httpsPort] {
		return 0, errors.New("could not prove both Worker listeners and TCP families")
	}
	return open, nil
}

func (a *frontAuthorityActivator) requireWorkerRetirementPreflight(ctx context.Context, j edgegroupfront.NodeWorkerJournal, r nodeWorkerRuntime) error {
	p, err := a.loadWorkerRetirement(ctx, j)
	if err != nil {
		return err
	}
	return a.checkWorkerRetirementPreflight(ctx, j, r, p)
}

func (a *frontAuthorityActivator) checkWorkerRetirementPreflight(ctx context.Context, j edgegroupfront.NodeWorkerJournal, r nodeWorkerRuntime, p *workerRetirementPlan) error {
	if p == nil {
		return nil
	}
	ds, err := a.client.AppsV1().DaemonSets(a.config.Namespace).Get(ctx, p.DaemonSet, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if _, err := p.checkDS(ds, j, time.Time{}); err != nil {
		return err
	}
	owner := metav1.GetControllerOf(&r.prior.pod)
	if owner == nil || owner.Kind != "DaemonSet" || owner.UID != p.DaemonSetUID || owner.Name != p.DaemonSet ||
		r.prior.pod.Spec.HostNetwork || r.prior.pod.UID == "" {
		return errors.New("retirement does not own the exact old Worker Pod")
	}
	services, err := a.client.CoreV1().Services(a.config.Namespace).List(ctx, metav1.ListOptions{Limit: nodeWorkerListLimit})
	if err != nil || services.Continue != "" || len(services.Items) > nodeWorkerListLimit {
		return errors.Join(errors.New("old Worker service inventory is incomplete"), err)
	}
	for _, service := range services.Items {
		if len(service.Spec.Selector) != 0 && labels.SelectorFromSet(labels.Set(service.Spec.Selector)).Matches(labels.Set(r.prior.pod.Labels)) {
			return fmt.Errorf("Service %s still targets the old Worker directly", service.Name)
		}
	}
	endpoints, err := a.client.DiscoveryV1().EndpointSlices(a.config.Namespace).List(ctx, metav1.ListOptions{Limit: nodeWorkerListLimit})
	if err != nil || endpoints.Continue != "" || len(endpoints.Items) > nodeWorkerListLimit {
		return errors.Join(errors.New("old Worker EndpointSlice inventory is incomplete"), err)
	}
	for _, slice := range endpoints.Items {
		for _, ep := range slice.Endpoints {
			if ep.TargetRef != nil && ep.TargetRef.UID == r.prior.pod.UID {
				return fmt.Errorf("EndpointSlice %s still targets the old Worker directly", slice.Name)
			}
			for _, ip := range ep.Addresses {
				if ip == r.prior.pod.Status.PodIP {
					return fmt.Errorf("EndpointSlice %s still names the old Worker address", slice.Name)
				}
			}
		}
	}
	spec := ds.Spec.Template.Spec.DeepCopy()
	return excludeWorkerRetirementNode(spec, j.Recipient.Node)
}

func (a *frontAuthorityActivator) reconcileNodeWorkerDrain(ctx context.Context, store *releaseguardian.AuthorityStore,
	fence releaseguardian.NodeWorkerFence, result releaseguardian.NodeWorkerResult) (bool, error) {
	j := fence.Journal
	p, err := a.loadWorkerRetirement(ctx, j)
	if err != nil {
		return false, err
	}
	if p == nil {
		return a.refreshNodeWorkerDrain(ctx, store, fence, result.Activation)
	}
	ns := a.config.Namespace
	dsClient := a.client.AppsV1().DaemonSets(ns)
	ds, err := dsClient.Get(ctx, p.DaemonSet, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	deadline := result.DrainStart.Add(edgegroupfront.WorkerDrainLimit).UTC()
	decision, err := p.checkDS(ds, j, deadline)
	if err != nil {
		return false, err
	}
	excluded := decision != nil
	r, err := a.observeNodeWorkerRuntimeWithRetirement(ctx, j, true, true, excluded)
	if err != nil {
		return false, err
	}
	if err := a.proveNodeWorkerDrainServing(ctx, j, result.Activation, r, excluded); err != nil {
		return false, err
	}
	httpPort, httpsPort := workerSlotPorts(j.Expected.ActiveSlot)
	if excluded {
		return a.confirmWorkerRetirement(ctx, j, ds, *decision, r.front.Status.HostIP, httpPort, httpsPort)
	}
	grant, err := store.ProjectNodeWorkerDrain(ctx, fence, a.now())
	if err != nil {
		return false, err
	}
	if err := a.requireWorkerRetirementPreflight(ctx, j, r); err != nil {
		return false, err
	}
	count, frontDrained := -1, false
	if a.now().Before(deadline) {
		frontDrained, err = nodeWorkerDrainComplete(ctx, repairHealthEndpoint(r.front.Status.PodIP, frontHealthPort, "/metrics"), grant)
		if err == nil && frontDrained && a.now().Before(deadline) {
			read, cancel := context.WithTimeout(ctx, 4*time.Second)
			var raw []byte
			raw, err = a.executor.Exec(read, ns, r.prior.pod.Name, "caddy", "cat", "/proc/net/tcp", "/proc/net/tcp6")
			cancel()
			if err == nil {
				count, err = workerSlotSockets(raw, httpPort, httpsPort)
			}
		}
		if err != nil {
			if a.now().Before(deadline) {
				return false, err
			}
			count = -1
		}
	}
	now := a.now().UTC()
	outcome := retirementOutcome(now, deadline, count, frontDrained)
	if outcome == "" {
		return false, nil
	}
	// Recheck both Pods and the routing pointer before changing scheduling.
	fresh, err := a.observeNodeWorkerRuntime(ctx, j, true, true)
	if err != nil || !r.sameBinding(fresh, true) {
		return false, errors.Join(errors.New("Worker retirement binding changed before exclusion"), err)
	}
	state, err := a.readNodeWorkerSelection(ctx, j, fresh)
	if err != nil || state != result.Activation {
		return false, errors.Join(errors.New("Worker retirement no longer has the verified serving activation"), err)
	}
	d := workerRetirementDecision{Plan: *p, Node: j.Recipient.Node, WorkerUID: j.PredecessorWorkerUID,
		Initiated: now, Deadline: deadline, Outcome: outcome, Sockets: count}
	data, err := json.Marshal(d)
	if err != nil {
		return false, err
	}
	next := ds.DeepCopy()
	if err := excludeWorkerRetirementNode(&next.Spec.Template.Spec, j.Recipient.Node); err != nil {
		return false, err
	}
	if next.Annotations == nil {
		next.Annotations = map[string]string{}
	}
	next.Annotations[workerRetirementKey(j.Recipient.Node)] = string(data)
	live, err := dsClient.Update(ctx, next, metav1.UpdateOptions{})
	if err != nil {
		// The next pass uses the durable annotation to resolve a lost reply.
		return false, err
	}
	got, err := p.checkDS(live, j, deadline)
	if err != nil || got == nil || *got != d {
		return false, errors.Join(errors.New("Worker node exclusion was not stored with its decision"), err)
	}
	return a.confirmWorkerRetirement(ctx, j, live, d, r.front.Status.HostIP, httpPort, httpsPort)
}

func (a *frontAuthorityActivator) confirmWorkerRetirement(ctx context.Context, j edgegroupfront.NodeWorkerJournal, ds *appsv1.DaemonSet,
	d workerRetirementDecision, host string, ports ...uint16) (bool, error) {
	ns := a.config.Namespace
	api := a.client.CoreV1().Pods(ns)
	pods, err := api.List(ctx, metav1.ListOptions{FieldSelector: fields.OneTermEqualSelector("spec.nodeName", d.Node).String(), Limit: nodeWorkerListLimit})
	if err != nil || pods.Continue != "" || len(pods.Items) > nodeWorkerListLimit {
		return false, errors.Join(errors.New("old Worker retirement inventory is incomplete"), err)
	}
	for _, pod := range pods.Items {
		if pod.Spec.NodeName != d.Node {
			continue
		}
		if string(pod.UID) == d.WorkerUID {
			owner := metav1.GetControllerOf(&pod)
			if owner == nil || owner.Kind != "DaemonSet" || owner.UID != ds.UID || owner.Name != ds.Name || pod.ResourceVersion == "" {
				return false, errors.New("old Worker no longer belongs to the excluded controller")
			}
			if pod.DeletionTimestamp == nil || pod.DeletionGracePeriodSeconds != nil && *pod.DeletionGracePeriodSeconds > 0 {
				// The node has already been durably excluded. Exact preconditions
				// prevent a delayed deletion from removing any replacement.
				uid, rv, zero := pod.UID, pod.ResourceVersion, int64(0)
				err := api.Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero,
					Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}})
				if err != nil && !apierrors.IsNotFound(err) {
					return false, err
				}
			}
			return false, nil
		}
		if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			continue
		}
		owner := metav1.GetControllerOf(&pod)
		if owner != nil && owner.UID == ds.UID {
			return false, errors.New("retired Worker was recreated on the excluded node")
		}
		for _, c := range pod.Spec.Containers {
			for _, mapping := range c.Ports {
				for _, port := range ports {
					if mapping.HostPort == int32(port) {
						return false, errors.New("a different Pod still owns the retired Worker hostPort")
					}
				}
			}
		}
	}
	if ds.Status.ObservedGeneration != ds.Generation {
		return false, nil
	}
	for _, port := range ports {
		address := net.JoinHostPort(host, strconv.Itoa(int(port)))
		conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", address)
		if err == nil {
			_ = conn.Close()
			return false, nil
		}
		if !errors.Is(err, syscall.ECONNREFUSED) {
			return false, fmt.Errorf("retired listener %s is not proven closed: %w", address, err)
		}
	}
	return a.recordWorkerRetirement(ctx, j, d)
}

func (a *frontAuthorityActivator) recordWorkerRetirement(ctx context.Context, j edgegroupfront.NodeWorkerJournal, d workerRetirementDecision) (bool, error) {
	raw, err := json.Marshal(d)
	if err != nil {
		return false, err
	}
	name, ns, immutable := retirementName("retired", j), a.config.Namespace, true
	records := a.client.CoreV1().ConfigMaps(ns)
	record, err := records.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: ns, Labels: map[string]string{
			"fugue.pro/group": j.Recipient.Group, "fugue.pro/authority-kind": "node-worker-retired",
			"app.kubernetes.io/managed-by": "fugue-release-guardian",
		}}, Immutable: &immutable, Data: map[string]string{"retired.json": string(raw)}}, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		record, err = records.Get(ctx, name, metav1.GetOptions{})
	}
	if err != nil || record.UID == "" || record.ResourceVersion == "" || record.DeletionTimestamp != nil || record.Immutable == nil || !*record.Immutable ||
		record.Labels["app.kubernetes.io/managed-by"] != "fugue-release-guardian" ||
		record.Labels["fugue.pro/group"] != j.Recipient.Group || record.Labels["fugue.pro/authority-kind"] != "node-worker-retired" ||
		len(record.Data) != 1 || len(record.BinaryData) != 0 || record.Data["retired.json"] != string(raw) {
		return false, errors.Join(errors.New("old Worker retirement receipt does not match the exact decision"), err)
	}
	return true, nil
}
