package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"time"

	"fugue/internal/volumeretire"

	"github.com/spf13/cobra"
)

const volumeRetirementSchema = "fugue.local-volume-retirement/v1"

type volumeRetirementOptions struct {
	namespace, ssh, nodeSSH, kubeconfig, kubeContext, storageRoot string
	allowTerminalJobs                                             bool
}

type retirementTree struct {
	Device         int64  `json:"device"`
	Inode          int64  `json:"inode"`
	AllocatedBytes int64  `json:"allocated_bytes"`
	EntryCount     int64  `json:"entry_count"`
	SHA256         string `json:"tree_sha256"`
}

type retirementPlan struct {
	Schema            string            `json:"schema"`
	ObservedAt        time.Time         `json:"observed_at"`
	Namespace         string            `json:"namespace"`
	PVC               string            `json:"pvc"`
	PVCUID            string            `json:"pvc_uid"`
	PV                string            `json:"pv"`
	PVUID             string            `json:"pv_uid"`
	Node              string            `json:"node"`
	NodeUID           string            `json:"node_uid"`
	KubeSystemUID     string            `json:"kube_system_uid"`
	Path              string            `json:"path"`
	StorageRoot       string            `json:"storage_root"`
	Host              map[string]string `json:"host"`
	Tree              retirementTree    `json:"tree"`
	DatabaseCluster   string            `json:"database_cluster,omitempty"`
	DatabaseUID       string            `json:"database_uid,omitempty"`
	CurrentPrimary    string            `json:"current_primary,omitempty"`
	BackupObserved    string            `json:"last_successful_backup_observed,omitempty"`
	AllowTerminalJobs bool              `json:"allow_terminal_job_references,omitempty"`
}

type retirementPreview struct {
	Plan       retirementPlan `json:"plan"`
	SHA256     string         `json:"plan_sha256"`
	CanExecute bool           `json:"can_execute"`
	Blockers   []string       `json:"blockers"`
}

type volumeHostFunc func(context.Context, map[string]any) (map[string]any, error)

type retirementInventoryKey struct{}

func retirementDigest(p retirementPlan) string {
	raw, _ := json.Marshal(p)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func retirementCommand(ctx context.Context, input []byte, command ...string) (map[string]any, error) {
	cmd := exec.CommandContext(ctx, command[0], command[1:]...)
	cmd.Stdin = bytes.NewReader(input)
	var output bytes.Buffer
	// Bound memory while still consuming a failed command's output. Never print
	// raw Kubernetes manifests, stderr or configuration contents in an error.
	cmd.Stdout = &retirementBoundedWriter{buffer: &output, limit: 32 << 20}
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		var failure struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(output.Bytes(), &failure) == nil && failure.Error != "" {
			return nil, fmt.Errorf("node retirement check failed: %.1024s", failure.Error)
		}
		return nil, fmt.Errorf("retirement command failed (%v); inspect access and the saved receipt", err)
	}
	if output.Len() > 32<<20 {
		return nil, fmt.Errorf("retirement response exceeds safety bound")
	}
	if len(bytes.TrimSpace(output.Bytes())) == 0 {
		return map[string]any{}, nil // kubectl get --ignore-not-found
	}
	var result map[string]any
	decoder := json.NewDecoder(&output)
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil || result == nil {
		return nil, fmt.Errorf("invalid retirement command response")
	}
	return result, nil
}

type retirementBoundedWriter struct {
	buffer *bytes.Buffer
	limit  int
}

func (w *retirementBoundedWriter) Write(data []byte) (int, error) {
	n := len(data)
	remaining := w.limit + 1 - w.buffer.Len()
	if remaining > 0 {
		_, _ = w.buffer.Write(data[:min(remaining, n)])
	}
	return n, nil
}

func volumeRetirementRunners(opts volumeRetirementOptions, nodeOptional ...bool) (physicalKubeFunc, volumeHostFunc, error) {
	name := regexp.MustCompile(`^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$`)
	alias := regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.@-]*$`)
	requireNode := len(nodeOptional) == 0 || !nodeOptional[0]
	if !name.MatchString(opts.namespace) || len(opts.namespace) > 63 || requireNode && opts.nodeSSH == "" || opts.nodeSSH != "" && !alias.MatchString(opts.nodeSSH) || opts.ssh != "" && !alias.MatchString(opts.ssh) {
		return nil, nil, fmt.Errorf("valid --namespace and --node-ssh are required; SSH targets must be host aliases")
	}
	sshCommand := func(host string, command []string) []string {
		quoted := make([]string, len(command))
		for i, arg := range command {
			quoted[i] = shellSingleQuote(arg)
		}
		return []string{"ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=yes", "-o", "ForwardAgent=no", "-o", "ClearAllForwardings=yes", host, strings.Join(quoted, " ")}
	}
	kube := func(ctx context.Context, body any, args ...string) (map[string]any, error) {
		ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
		defer cancel()
		command := []string{"kubectl", "--request-timeout=30s", "-n", opts.namespace}
		if opts.kubeconfig != "" {
			command = append(command, "--kubeconfig", opts.kubeconfig)
		}
		if opts.kubeContext != "" {
			command = append(command, "--context", opts.kubeContext)
		}
		command = append(command, args...)
		if p, ok := ctx.Value(retirementInventoryKey{}).(retirementPlan); ok && len(args) >= 3 && args[0] == "get" && (args[2] == "-o" || args[2] == "--all-namespaces") {
			for i := range command {
				if command[i] == "-o" && i+1 < len(command) && command[i+1] == "json" {
					command[i+1] = `jsonpath={range .items[*]}{@}{"\n"}{end}`
				}
			}
			quote := func(parts []string) string {
				out := make([]string, len(parts))
				for i, part := range parts {
					out[i] = shellSingleQuote(part)
				}
				return strings.Join(out, " ")
			}
			filter := []string{"python3", "-c", volumeretire.InventoryScript, p.PVC, p.Path, p.PVUID}
			command = []string{"bash", "-o", "pipefail", "-c", quote(command) + " | " + quote(filter)}
		}
		if opts.ssh != "" {
			command = sshCommand(opts.ssh, command)
		}
		var input []byte
		if body != nil {
			var err error
			input, err = json.Marshal(body)
			if err != nil {
				return nil, err
			}
		}
		result, err := retirementCommand(ctx, input, command...)
		if err != nil && len(args) >= 2 {
			return nil, fmt.Errorf("kubectl %s %s: %w", args[0], args[1], err)
		}
		return result, err
	}
	host := func(ctx context.Context, request map[string]any) (map[string]any, error) {
		if opts.nodeSSH == "" {
			return nil, fmt.Errorf("node access was not selected")
		}
		ctx, cancel := context.WithTimeout(ctx, 11*time.Minute)
		defer cancel()
		input, err := json.Marshal(request)
		if err != nil {
			return nil, err
		}
		return retirementCommand(ctx, input, sshCommand(opts.nodeSSH, []string{"python3", "-c", volumeretire.HostScript})...)
	}
	return kube, host, nil
}

func retirementItems(value map[string]any) []map[string]any {
	items, _ := value["items"].([]any)
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

func retirementHasReference(value any, claim, localPath string) bool {
	switch value := value.(type) {
	case map[string]any:
		if physicalString(physicalMap(value, "persistentVolumeClaim"), "claimName") == claim {
			return true
		}
		if source := physicalString(physicalMap(value, "hostPath"), "path"); source != "" {
			source = path.Clean(source)
			// A host administration agent can mount / or a storage parent without
			// owning every child volume. Exact/descendant mounts are references;
			// actual mounts and process handles are independently checked on-node.
			if source == localPath || strings.HasPrefix(source, localPath+"/") {
				return true
			}
		}
		for _, item := range value {
			if retirementHasReference(item, claim, localPath) {
				return true
			}
		}
	case []any:
		for _, item := range value {
			if retirementHasReference(item, claim, localPath) {
				return true
			}
		}
	case string:
		// Recovery Jobs and custom CNPG bootstrap specs can name a claim or
		// source directory in arguments/env rather than a PVC volume source.
		return value == claim || strings.Contains(value, localPath)
	}
	return false
}

func retirementPathsOverlap(a, b string) bool {
	a, b = path.Clean(a), path.Clean(b)
	return a == b || strings.HasPrefix(a, strings.TrimSuffix(b, "/")+"/") || strings.HasPrefix(b, strings.TrimSuffix(a, "/")+"/")
}

func retirementHostPathReference(value any, localPath string) bool {
	switch value := value.(type) {
	case map[string]any:
		if source := physicalString(physicalMap(value, "hostPath"), "path"); source != "" {
			source = path.Clean(source)
			if source == localPath || strings.HasPrefix(source, localPath+"/") {
				return true
			}
		}
		for _, child := range value {
			if retirementHostPathReference(child, localPath) {
				return true
			}
		}
	case []any:
		for _, child := range value {
			if retirementHostPathReference(child, localPath) {
				return true
			}
		}
	}
	return false
}

func retirementVolumeNode(pv map[string]any) string {
	terms, _ := physicalMap(physicalMap(physicalMap(pv, "spec"), "nodeAffinity"), "required")["nodeSelectorTerms"].([]any)
	if len(terms) != 1 {
		return ""
	}
	term, _ := terms[0].(map[string]any)
	expressions, _ := term["matchExpressions"].([]any)
	fields, _ := term["matchFields"].([]any)
	if len(expressions) != 1 || len(fields) != 0 {
		return ""
	}
	expression, _ := expressions[0].(map[string]any)
	values := anyStrings(expression["values"])
	if physicalString(expression, "key") != "kubernetes.io/hostname" || physicalString(expression, "operator") != "In" || len(values) != 1 {
		return ""
	}
	return values[0]
}

func retirementPVMatches(pv map[string]any, p retirementPlan) bool {
	spec, meta := physicalMap(pv, "spec"), physicalMap(pv, "metadata")
	claim := physicalMap(spec, "claimRef")
	protection := physicalString(physicalMap(meta, "annotations"), "fugue.pro/retirement-protected")
	return physicalString(meta, "uid") == p.PVUID && physicalString(physicalMap(spec, "local"), "path") == p.Path && physicalString(spec, "persistentVolumeReclaimPolicy") == "Retain" && physicalString(claim, "uid") == p.PVCUID && physicalString(claim, "name") == p.PVC && physicalString(claim, "namespace") == p.Namespace && physicalString(meta, "deletionTimestamp") == "" && retirementVolumeNode(pv) == p.Node && (protection == "" || protection == "false")
}

func retirementTerminalJob(object map[string]any) bool {
	if physicalString(object, "kind") != "Job" {
		return false
	}
	status := physicalMap(object, "status")
	for _, key := range []string{"active", "terminating"} {
		if value, exists := status[key]; exists && fmt.Sprint(value) != "0" {
			return false
		}
	}
	for _, value := range physicalMap(status, "uncountedTerminatedPods") {
		items, ok := value.([]any)
		if !ok || len(items) != 0 {
			return false
		}
	}
	conditions, _ := status["conditions"].([]any)
	for _, value := range conditions {
		condition, _ := value.(map[string]any)
		if physicalString(condition, "status") == "True" && (physicalString(condition, "type") == "Complete" || physicalString(condition, "type") == "Failed") {
			return true
		}
	}
	return false
}

// reviewRetirement reads references even after the PVC is deleted. The PV is
// deliberately kept Retain/Released until physical removal is verified.
func reviewRetirement(ctx context.Context, kube physicalKubeFunc, p retirementPlan, released bool, allowAbsentPV ...bool) ([]string, error) {
	ctx = context.WithValue(ctx, retirementInventoryKey{}, p)
	blockers := []string{}
	node, err := kube(ctx, nil, "get", "node", p.Node, "-o", "json")
	if err != nil {
		return nil, err
	}
	system, err := kube(ctx, nil, "get", "namespace", "kube-system", "-o", "json")
	if err != nil {
		return nil, err
	}
	if physicalString(physicalMap(node, "metadata"), "uid") != p.NodeUID || physicalString(physicalMap(system, "metadata"), "uid") != p.KubeSystemUID || physicalString(physicalMap(physicalMap(node, "metadata"), "labels"), "kubernetes.io/hostname") != p.Node {
		blockers = append(blockers, "Kubernetes cluster or node identity changed")
	}
	if p.Host["machine_id"] != "" && physicalString(physicalMap(physicalMap(node, "status"), "nodeInfo"), "machineID") != p.Host["machine_id"] {
		blockers = append(blockers, "node machine identity changed")
	}
	ready := false
	conditions, _ := physicalMap(node, "status")["conditions"].([]any)
	for _, item := range conditions {
		condition, _ := item.(map[string]any)
		if physicalString(condition, "type") == "Ready" && physicalString(condition, "status") == "True" {
			ready = true
		}
	}
	if !ready || physicalString(physicalMap(node, "metadata"), "deletionTimestamp") != "" {
		blockers = append(blockers, "node is not Ready")
	}
	pv, err := kube(ctx, nil, "get", "pv", p.PV, "-o", "json", "--ignore-not-found")
	if err != nil {
		return nil, err
	}
	if len(pv) == 0 {
		if len(allowAbsentPV) == 0 || !allowAbsentPV[0] {
			blockers = append(blockers, "pinned PV is absent")
		}
	} else {
		meta := physicalMap(pv, "metadata")
		if !retirementPVMatches(pv, p) {
			blockers = append(blockers, "PV identity, binding, path or Retain policy changed")
		}
		if released && physicalString(physicalMap(pv, "status"), "phase") != "Released" {
			blockers = append(blockers, "PV is not Released")
		}
		if !released && physicalString(physicalMap(pv, "status"), "phase") != "Bound" {
			blockers = append(blockers, "PV is not Bound")
		}
		if protection := physicalString(physicalMap(meta, "annotations"), "fugue.pro/retirement-protected"); protection != "" && protection != "false" {
			blockers = append(blockers, "PV has an explicit retirement hold")
		}
	}
	pvc, err := kube(ctx, nil, "get", "pvc", p.PVC, "-o", "json", "--ignore-not-found")
	if err != nil {
		return nil, err
	}
	if released && len(pvc) != 0 {
		blockers = append(blockers, "PVC still exists or its name was reused")
	} else if !released {
		meta, spec := physicalMap(pvc, "metadata"), physicalMap(pvc, "spec")
		if physicalString(meta, "uid") != p.PVCUID || physicalString(spec, "volumeName") != p.PV || physicalString(meta, "deletionTimestamp") != "" {
			blockers = append(blockers, "PVC identity or binding changed")
		}
		if protection := physicalString(physicalMap(meta, "annotations"), "fugue.pro/retirement-protected"); protection != "" && protection != "false" {
			blockers = append(blockers, "PVC has an explicit retirement hold")
		}
		owners, _ := meta["ownerReferences"].([]any)
		for _, item := range owners {
			owner, _ := item.(map[string]any)
			if p.DatabaseUID == "" || physicalString(owner, "uid") != p.DatabaseUID || physicalString(owner, "kind") != "Cluster" {
				blockers = append(blockers, "PVC has an unsupported controller owner")
			}
		}
		if p.DatabaseUID != "" && physicalString(physicalMap(meta, "annotations"), "cnpg.io/pvcStatus") != "detached" {
			blockers = append(blockers, "CNPG PVC is not explicitly detached")
		}
	}
	objects, err := kube(ctx, nil, "get", "pods,deployments.apps,daemonsets.apps,statefulsets.apps,replicasets.apps,replicationcontrollers,jobs.batch,cronjobs.batch", "-o", "json")
	if err != nil {
		return nil, err
	}
	for _, object := range retirementItems(objects) {
		if retirementHasReference(object["spec"], p.PVC, p.Path) {
			if !p.AllowTerminalJobs || !retirementTerminalJob(object) {
				blockers = append(blockers, "workload reference: "+physicalString(object, "kind")+"/"+physicalString(physicalMap(object, "metadata"), "name"))
			}
		}
		if physicalString(object, "kind") == "StatefulSet" {
			for _, template := range retirementItems(map[string]any{"items": physicalMap(object, "spec")["volumeClaimTemplates"]}) {
				prefix := physicalString(physicalMap(template, "metadata"), "name") + "-" + physicalString(physicalMap(object, "metadata"), "name") + "-"
				if strings.HasPrefix(p.PVC, prefix) && regexp.MustCompile(`^[0-9]+$`).MatchString(strings.TrimPrefix(p.PVC, prefix)) {
					blockers = append(blockers, "StatefulSet claim template owns this PVC name")
				}
			}
		}
	}
	nodePods, err := kube(ctx, nil, "get", "pods", "--all-namespaces", "--field-selector=spec.nodeName="+p.Node, "-o", "json")
	if err != nil {
		return nil, err
	}
	for _, pod := range retirementItems(nodePods) {
		if retirementHostPathReference(pod["spec"], p.Path) {
			blockers = append(blockers, "node Pod has a direct hostPath reference: "+physicalString(physicalMap(pod, "metadata"), "name"))
		}
	}
	// All PVs: overlapping local sources or hostPath aliases can own the same
	// bytes even if the old claim has no Pod. Do not restrict this to a namespace.
	volumes, err := kube(ctx, nil, "get", "pv", "-o", "json")
	if err != nil {
		return nil, err
	}
	for _, volume := range retirementItems(volumes) {
		if physicalString(physicalMap(volume, "metadata"), "uid") == p.PVUID {
			continue
		}
		for _, kind := range []string{"local", "hostPath"} {
			if source := physicalString(physicalMap(physicalMap(volume, "spec"), kind), "path"); source != "" && retirementPathsOverlap(source, p.Path) {
				blockers = append(blockers, "overlapping PV: "+physicalString(physicalMap(volume, "metadata"), "name"))
			}
		}
	}
	if p.DatabaseCluster != "" {
		clusters, err := kube(ctx, nil, "get", "clusters.postgresql.cnpg.io", "-o", "json")
		if err != nil {
			return nil, err
		}
		for _, other := range retirementItems(clusters) {
			if retirementHasReference(other["spec"], p.PVC, p.Path) {
				blockers = append(blockers, "database recovery configuration references the old volume")
			}
		}
		cluster, err := kube(ctx, nil, "get", "clusters.postgresql.cnpg.io", p.DatabaseCluster, "-o", "json")
		if err != nil {
			return nil, err
		}
		status := physicalMap(cluster, "status")
		if physicalString(physicalMap(cluster, "metadata"), "uid") != p.DatabaseUID || !retirementDatabaseHealthy(cluster) || physicalString(status, "currentPrimary") != p.CurrentPrimary || p.CurrentPrimary == p.PVC {
			blockers = append(blockers, "database identity, primary or health changed")
		}
		if retirementHasReference(cluster["spec"], p.PVC, p.Path) {
			blockers = append(blockers, "database recovery configuration references the old volume")
		}
		for _, key := range []string{"healthyPVC", "initializingPVC"} {
			for _, claim := range anyStrings(status[key]) {
				if claim == p.PVC {
					blockers = append(blockers, "database still considers the volume active")
				}
			}
		}
	}
	return blockers, nil
}

func planVolumeRetirement(ctx context.Context, kube physicalKubeFunc, host volumeHostFunc, opts volumeRetirementOptions, claim string) (retirementPreview, error) {
	result := retirementPreview{Blockers: []string{}}
	pvc, err := kube(ctx, nil, "get", "pvc", claim, "-o", "json")
	if err != nil {
		return result, err
	}
	meta := physicalMap(pvc, "metadata")
	p := retirementPlan{Schema: volumeRetirementSchema, ObservedAt: time.Now().UTC(), Namespace: opts.namespace, PVC: claim, PVCUID: physicalString(meta, "uid"), PV: physicalString(physicalMap(pvc, "spec"), "volumeName"), StorageRoot: opts.storageRoot, AllowTerminalJobs: opts.allowTerminalJobs}
	if p.PVCUID == "" || p.PV == "" || physicalString(meta, "namespace") != opts.namespace {
		return result, fmt.Errorf("a bound, identified PVC is required")
	}
	pv, err := kube(ctx, nil, "get", "pv", p.PV, "-o", "json")
	if err != nil {
		return result, err
	}
	p.PVUID = physicalString(physicalMap(pv, "metadata"), "uid")
	p.Path = physicalString(physicalMap(physicalMap(pv, "spec"), "local"), "path")
	if p.PVUID == "" || p.Path == "" || physicalString(physicalMap(pv, "spec"), "persistentVolumeReclaimPolicy") != "Retain" || !path.IsAbs(p.Path) || path.Clean(p.Path) != p.Path || path.Dir(p.Path) != p.StorageRoot || len(strings.Split(p.StorageRoot, "/")) < 4 {
		return result, fmt.Errorf("only one local Retain PV directly below --storage-root is supported; CSI, LVM and hostPath volumes are not retired by this command")
	}
	// An unambiguous, single-node local PV. OR expressions and alternate
	// affinity keys cannot be inferred as authorization for a physical host.
	p.Node = retirementVolumeNode(pv)
	if p.Node == "" {
		return result, fmt.Errorf("local PV has no single hostname affinity")
	}
	node, err := kube(ctx, nil, "get", "node", p.Node, "-o", "json")
	if err != nil {
		return result, err
	}
	p.NodeUID = physicalString(physicalMap(node, "metadata"), "uid")
	if p.NodeUID == "" || physicalString(physicalMap(physicalMap(node, "metadata"), "labels"), "kubernetes.io/hostname") != p.Node {
		return result, fmt.Errorf("node hostname label is ambiguous")
	}
	system, err := kube(ctx, nil, "get", "namespace", "kube-system", "-o", "json")
	if err != nil {
		return result, err
	}
	p.KubeSystemUID = physicalString(physicalMap(system, "metadata"), "uid")
	p.DatabaseCluster = physicalString(physicalMap(meta, "labels"), "cnpg.io/cluster")
	if p.DatabaseCluster != "" {
		cluster, err := kube(ctx, nil, "get", "clusters.postgresql.cnpg.io", p.DatabaseCluster, "-o", "json")
		if err != nil {
			return result, err
		}
		p.DatabaseUID = physicalString(physicalMap(cluster, "metadata"), "uid")
		p.CurrentPrimary = physicalString(physicalMap(cluster, "status"), "currentPrimary")
		p.BackupObserved = physicalString(physicalMap(cluster, "status"), "lastSuccessfulBackup")
	}
	blockers, err := reviewRetirement(ctx, kube, p, false)
	if err != nil {
		return result, err
	}
	observation, err := host(ctx, map[string]any{"mode": "inspect", "path": p.Path, "storage_root": p.StorageRoot, "hostname": p.Node, "machine_id": physicalString(physicalMap(physicalMap(node, "status"), "nodeInfo"), "machineID")})
	if err != nil {
		return result, err
	}
	raw, _ := json.Marshal(observation["host"])
	if err := json.Unmarshal(raw, &p.Host); err != nil {
		return result, fmt.Errorf("invalid host identity response")
	}
	raw, _ = json.Marshal(observation["tree"])
	if err := json.Unmarshal(raw, &p.Tree); err != nil || p.Tree.Inode <= 0 || p.Tree.EntryCount <= 0 || p.Tree.SHA256 == "" || p.Host["boot_id"] == "" {
		return result, fmt.Errorf("invalid node volume metadata")
	}
	blockers = append(blockers, anyStrings(observation["blockers"])...)
	result.Plan, result.Blockers, result.CanExecute = p, blockers, len(blockers) == 0
	result.SHA256 = retirementDigest(p)
	return result, nil
}

func anyStrings(value any) []string {
	items, _ := value.([]any)
	out := []string{}
	for _, item := range items {
		if text, ok := item.(string); ok {
			out = append(out, text)
		}
	}
	return out
}

func loadRetirementPlan(filename, expectedSHA string) (retirementPreview, error) {
	var result retirementPreview
	file, err := os.Open(filename)
	if err != nil {
		return result, err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return result, fmt.Errorf("invalid retirement plan: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return result, fmt.Errorf("unexpected trailing retirement plan data")
	}
	if result.Plan.Schema != volumeRetirementSchema || !result.CanExecute || len(result.Blockers) != 0 || expectedSHA == "" || result.SHA256 != expectedSHA || retirementDigest(result.Plan) != expectedSHA {
		return result, fmt.Errorf("an executable preview and its exact --plan-sha256 are required")
	}
	return result, nil
}

func (c *CLI) newVolumeRetirementCommand() *cobra.Command {
	opts := volumeRetirementOptions{namespace: "default"}
	root := &cobra.Command{Use: "volume-retirement", Short: "Preview and retire one unused local Retain volume with an identity-bound receipt", Long: "Use explicit Kubernetes credentials (or --ssh) and --node-ssh. Preview reads metadata only. Execution irreversibly removes one pinned local PV's data; it never deletes CSI/LVM pools, forces finalizers, changes replicas or restarts workloads. Kubernetes and node audit receipts preserve partial progress for retry."}
	root.Example = "fugue admin storage volume-retirement plan archive-db-2 --namespace test-system --ssh control-node --node-ssh storage-node --storage-root /var/lib/storage --output-file plan.json\nfugue admin storage volume-retirement status --namespace test-system --ssh control-node --pv-uid volume-uid"
	f := root.PersistentFlags()
	f.StringVar(&opts.namespace, "namespace", "default", "Namespace owning the exact PVC")
	f.StringVar(&opts.ssh, "ssh", "", "Run kubectl on this explicitly selected SSH host")
	f.StringVar(&opts.nodeSSH, "node-ssh", "", "Explicit root SSH alias for the storage node (identity is checked)")
	f.StringVar(&opts.kubeconfig, "kubeconfig", "", "Kubeconfig path, on --ssh host when selected")
	f.StringVar(&opts.kubeContext, "kube-context", "", "Kubernetes context")
	f.StringVar(&opts.storageRoot, "storage-root", "", "Exact parent storage directory, required for preview")
	f.BoolVar(&opts.allowTerminalJobs, "allow-terminal-job-references", false, "Explicitly allow terminal Job history with no active, terminating or uncounted Pods; Pod references still block")
	root.AddCommand(&cobra.Command{Use: "plan PVC", Short: "Read references and node metadata without writing remote state", Example: strings.Split(root.Example, "\n")[0], Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		kube, host, err := volumeRetirementRunners(opts)
		if err != nil {
			return err
		}
		preview, err := planVolumeRetirement(cmd.Context(), kube, host, opts, args[0])
		if err != nil {
			return err
		}
		return c.writeJSON(preview)
	}})
	var filename, expectedSHA, reason string
	var acknowledge bool
	execute := &cobra.Command{Use: "execute", Short: "Recheck a saved preview, retire its claim and data, and verify the result", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if !acknowledge || strings.TrimSpace(reason) == "" || len(reason) > 1024 {
			return fmt.Errorf("execution requires --ack-delete-data and a --reason (1–1024 bytes)")
		}
		preview, err := loadRetirementPlan(filename, expectedSHA)
		if err != nil {
			return err
		}
		if cmd.Flags().Changed("namespace") && opts.namespace != preview.Plan.Namespace {
			return fmt.Errorf("namespace does not match plan")
		}
		opts.namespace = preview.Plan.Namespace
		kube, host, err := volumeRetirementRunners(opts)
		if err != nil {
			return err
		}
		result, err := executeVolumeRetirement(cmd.Context(), kube, host, preview, reason)
		if result != nil {
			if outputErr := c.writeJSON(result); outputErr != nil {
				return outputErr
			}
		}
		return err
	}}
	f = execute.Flags()
	f.StringVar(&filename, "plan-file", "", "Saved JSON from a successful plan")
	f.StringVar(&expectedSHA, "plan-sha256", "", "Exact preview digest")
	f.StringVar(&reason, "reason", "", "Audited reason authorizing retirement of this data")
	f.BoolVar(&acknowledge, "ack-delete-data", false, "Authorize irreversible deletion of the exact previewed data")
	root.AddCommand(execute)
	var statusPVUID string
	status := &cobra.Command{Use: "status", Short: "Read a durable retirement receipt without making changes", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if !regexp.MustCompile(`^[a-zA-Z0-9-]{1,64}$`).MatchString(statusPVUID) {
			return fmt.Errorf("--pv-uid is required")
		}
		kube, _, err := volumeRetirementRunners(opts, true)
		if err != nil {
			return err
		}
		cm, err := kube(cmd.Context(), nil, "get", "configmap", retirementReceiptName(statusPVUID), "-o", "json")
		if err != nil {
			return err
		}
		var receipt map[string]any
		if err := json.Unmarshal([]byte(physicalString(physicalMap(cm, "data"), "receipt.json")), &receipt); err != nil {
			return fmt.Errorf("invalid retirement receipt")
		}
		return c.writeJSON(receipt)
	}}
	status.Flags().StringVar(&statusPVUID, "pv-uid", "", "Exact original PV UID")
	root.AddCommand(status)
	return root
}
