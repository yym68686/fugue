package releaseguardian

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"fugue/internal/declarativerelease"
)

type ProcessExecutor struct {
	Binary string
	PodUID string
}

var (
	serviceAccountCAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	serviceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
)

func NewProcessExecutor(binary, podUID string) (*ProcessExecutor, error) {
	binary, podUID = strings.TrimSpace(binary), strings.TrimSpace(podUID)
	if binary == "" || !filepath.IsAbs(binary) || podUID == "" || len(podUID) > 80 || strings.ContainsAny(podUID, "\r\n:\x00") {
		return nil, errors.New("Guardian process executor identity is invalid")
	}
	return &ProcessExecutor{Binary: binary, PodUID: podUID}, nil
}

func (executor *ProcessExecutor) Rollout(ctx context.Context, snapshot Snapshot) (ExecutionReceipt, error) {
	return executor.execute(ctx, snapshot, "execute", snapshot.Bundle.Files)
}

func (executor *ProcessExecutor) Rollback(ctx context.Context, snapshot Snapshot) (ExecutionReceipt, error) {
	files := make(map[string][]byte, len(snapshot.CurrentMonitorData))
	for name, value := range snapshot.CurrentMonitorData {
		files[name] = []byte(value)
	}
	return executor.execute(ctx, snapshot, "restore-monitor", files)
}

func (executor *ProcessExecutor) execute(ctx context.Context, snapshot Snapshot, operation string, files map[string][]byte) (ExecutionReceipt, error) {
	if executor == nil || snapshot.Record.Validate() != nil || (operation != "execute" && operation != "restore-monitor") {
		return ExecutionReceipt{}, errors.New("Guardian executor request is invalid")
	}
	directory, err := os.MkdirTemp("", "fugue-release-guardian-")
	if err != nil {
		return ExecutionReceipt{}, err
	}
	defer os.RemoveAll(directory)
	if err := os.Chmod(directory, 0o700); err != nil {
		return ExecutionReceipt{}, err
	}
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value := bytes.TrimSpace(files[name])
		if len(value) == 0 || len(value) > 4<<20 || filepath.Base(name) != name {
			return ExecutionReceipt{}, fmt.Errorf("Guardian executor file %q is invalid", name)
		}
		if err := os.WriteFile(filepath.Join(directory, name), append(value, '\n'), 0o600); err != nil {
			return ExecutionReceipt{}, err
		}
	}
	arguments := []string{operation, directory}
	if operation == "restore-monitor" {
		arguments = append(arguments, snapshot.LKGMonitorRecordDigest)
	}
	command := exec.CommandContext(ctx, executor.Binary, arguments...)
	environment := os.Environ()
	if strings.TrimSpace(os.Getenv("KUBECONFIG")) == "" {
		kubeconfig, configErr := writeInClusterKubeconfig(directory, serviceAccountCAPath, serviceAccountTokenPath)
		if configErr != nil {
			return ExecutionReceipt{}, configErr
		}
		environment = setEnvironment(environment, "KUBECONFIG", kubeconfig)
	}
	command.Env = append(environment,
		"FUGUE_COMPONENT_LEASE_OWNER=guardian",
		"FUGUE_RELEASE_GUARDIAN_POD_UID="+executor.PodUID,
		"FUGUE_RELEASE_GUARDIAN_RECORD_DIGEST="+snapshot.Record.RecordDigest,
	)
	var stdout, stderr boundedBuffer
	stdout.limit, stderr.limit = 1<<20, 1<<20
	command.Stdout, command.Stderr = &stdout, &stderr
	runErr := command.Run()
	result, decodeErr := declarativerelease.DecodeExecutionResult(bytes.NewReader(bytes.TrimSpace(stdout.Bytes())))
	if decodeErr != nil {
		if runErr != nil {
			return ExecutionReceipt{}, fmt.Errorf("Guardian %s result is unknown: %w: %s", operation, runErr, strings.TrimSpace(stderr.String()))
		}
		return ExecutionReceipt{}, fmt.Errorf("decode Guardian %s receipt: %w", operation, decodeErr)
	}
	if result.Component != snapshot.Key.Component || result.ConfigSHA != snapshot.Bundle.Prepared.ConfigSHA || result.ReceiptDigest == "" {
		return ExecutionReceipt{}, errors.New("Guardian executor receipt binding is invalid")
	}
	recordDigest := snapshot.Record.RecordDigest
	if operation == "restore-monitor" && result.Status == "compensated" {
		recordDigest = snapshot.Record.LKGRecordDigest
	}
	return ExecutionReceipt{Status: result.Status, Reason: result.Reason, RecordDigest: recordDigest, ReceiptDigest: result.ReceiptDigest}, nil
}

func writeInClusterKubeconfig(directory, caPath, tokenPath string) (string, error) {
	host := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST"))
	port := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS"))
	if net.ParseIP(host) == nil {
		return "", errors.New("Guardian in-cluster Kubernetes host is invalid")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return "", errors.New("Guardian in-cluster Kubernetes port is invalid")
	}
	for _, path := range []string{caPath, tokenPath} {
		info, statErr := os.Stat(path)
		if statErr != nil || !info.Mode().IsRegular() || info.Size() == 0 {
			return "", errors.New("Guardian in-cluster service account material is unavailable")
		}
	}
	path := filepath.Join(directory, "kubeconfig")
	content := fmt.Sprintf("apiVersion: v1\nkind: Config\nclusters:\n- name: in-cluster\n  cluster:\n    certificate-authority: %s\n    server: https://%s\ncontexts:\n- name: in-cluster\n  context:\n    cluster: in-cluster\n    user: service-account\ncurrent-context: in-cluster\nusers:\n- name: service-account\n  user:\n    tokenFile: %s\n", caPath, net.JoinHostPort(host, port), tokenPath)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		return "", err
	}
	return path, nil
}

func setEnvironment(environment []string, name, value string) []string {
	prefix := name + "="
	filtered := environment[:0]
	for _, entry := range environment {
		if !strings.HasPrefix(entry, prefix) {
			filtered = append(filtered, entry)
		}
	}
	return append(filtered, prefix+value)
}

type boundedBuffer struct {
	bytes.Buffer
	limit int
}

func (buffer *boundedBuffer) Write(value []byte) (int, error) {
	if buffer.Buffer.Len()+len(value) > buffer.limit {
		return 0, errors.New("Guardian child output exceeded its bound")
	}
	return buffer.Buffer.Write(value)
}
