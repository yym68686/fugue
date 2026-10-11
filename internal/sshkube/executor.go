package sshkube

import (
	"bytes"
	"context"
	"crypto/sha256"
	"debug/elf"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"fugue/internal/managedssh"
	"fugue/internal/sshexecutor"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

const helperReady = "FUGUE_SSH_HELPER_READY_v1\n"

const helperBootstrap = `set -eu
umask 077
directory=$(mktemp -d /tmp/fugue-ssh.XXXXXXXX)
cleanup() { rm -f "$directory/helper"; rmdir "$directory" 2>/dev/null || true; }
trap cleanup EXIT
head -c "$1" > "$directory/helper"
actual=$(sha256sum "$directory/helper")
actual=${actual%% *}
test "$actual" = "$2"
chmod 700 "$directory/helper"
printf 'FUGUE_SSH_HELPER_READY_v1\n'
"$directory/helper"
`

const helperCapabilityProbe = `set -eu
for executable in head sha256sum mktemp chmod rm rmdir; do command -v "$executable" >/dev/null; done
test -d "$1"
test -w /tmp
uname -m
id -un
id -u
`

type Kubernetes struct {
	configuration *rest.Config
	client        kubernetes.Interface
	helper        []byte
	digest        string
	architecture  string
	shell         string
}

func NewKubernetes(configuration *rest.Config, helper []byte, expectedDigest, shell string) (*Kubernetes, error) {
	if configuration == nil || configuration.TLSClientConfig.Insecure || !strings.HasPrefix(configuration.Host, "https://") || len(helper) == 0 || len(helper) > 32*1024*1024 || shell == "" || !strings.HasPrefix(shell, "/") {
		return nil, managedssh.ErrConfiguration
	}
	digest := sha256.Sum256(helper)
	if "sha256:"+hex.EncodeToString(digest[:]) != expectedDigest {
		return nil, managedssh.ErrConfiguration
	}
	image, err := elf.NewFile(bytes.NewReader(helper))
	if err != nil {
		return nil, managedssh.ErrConfiguration
	}
	defer image.Close()
	architecture := ""
	switch image.Machine {
	case elf.EM_X86_64:
		architecture = "x86_64"
	case elf.EM_AARCH64:
		architecture = "aarch64"
	default:
		return nil, managedssh.ErrCapabilities
	}
	for _, program := range image.Progs {
		if program.Type == elf.PT_INTERP {
			return nil, managedssh.ErrCapabilities
		}
	}
	client, err := kubernetes.NewForConfig(configuration)
	if err != nil {
		return nil, err
	}
	return &Kubernetes{configuration: rest.CopyConfig(configuration), client: client, helper: append([]byte(nil), helper...), digest: hex.EncodeToString(digest[:]), architecture: architecture, shell: shell}, nil
}

func (executor *Kubernetes) boundPod(ctx context.Context, target managedssh.Target) (*corev1.Pod, error) {
	pod, err := executor.client.CoreV1().Pods(target.Namespace).Get(ctx, target.PodName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if err := validatePodTarget(pod, target); err != nil {
		return nil, err
	}
	return pod, nil
}

func validatePodTarget(pod *corev1.Pod, target managedssh.Target) error {
	if pod == nil || pod.Namespace != target.Namespace || pod.Name != target.PodName || string(pod.UID) != target.PodUID || pod.DeletionTimestamp != nil || pod.Labels["fugue.pro/app-id"] != target.AppID || pod.Labels["fugue.pro/tenant-id"] != target.TenantID || pod.Spec.HostNetwork || pod.Spec.HostPID || pod.Spec.HostIPC {
		return managedssh.ErrUnauthorized
	}
	for _, container := range pod.Spec.Containers {
		if container.Name != target.Container {
			continue
		}
		if container.SecurityContext != nil && container.SecurityContext.Privileged != nil && *container.SecurityContext.Privileged {
			return managedssh.ErrUnauthorized
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name == target.Container && status.ContainerID == target.ContainerID && status.State.Running != nil {
				return nil
			}
		}
	}
	return managedssh.ErrUnavailable
}

func (executor *Kubernetes) Validate(ctx context.Context, target managedssh.Target) (managedssh.Capabilities, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := executor.boundPod(ctx, target); err != nil {
		return managedssh.Capabilities{}, err
	}
	output := &limitedBuffer{limit: 2048}
	err := executor.stream(ctx, target, []string{executor.shell, "-c", helperCapabilityProbe, "fugue-ssh", target.WorkingDirectory}, nil, output, io.Discard)
	if err != nil {
		return managedssh.Capabilities{}, managedssh.ErrCapabilities
	}
	fields := strings.Fields(output.String())
	if len(fields) != 3 || fields[0] != executor.architecture || (fields[1] != target.User && fields[2] != target.User) {
		return managedssh.Capabilities{}, managedssh.ErrCapabilities
	}
	if _, err := executor.boundPod(ctx, target); err != nil {
		return managedssh.Capabilities{}, err
	}
	return managedssh.Capabilities{Exec: true, Shell: true, PTY: true, SFTP: true, Signals: true, Forward: true}, nil
}

func (executor *Kubernetes) Run(ctx context.Context, target managedssh.Target, request managedssh.SessionRequest, streams managedssh.SessionIO) (managedssh.Exit, error) {
	stream, err := executor.openHelper(ctx, target)
	if err != nil {
		return managedssh.Exit{}, err
	}
	invocation := sshexecutor.Invocation{Schema: "fugue.ssh-execution/v1", Target: target, Shell: executor.shell, Kind: request.Kind, Command: request.Command, Terminal: request.Terminal, Environment: request.Environment}
	return sshexecutor.ExecuteHelper(ctx, stream, invocation, request, streams)
}

func (executor *Kubernetes) Forward(ctx context.Context, target managedssh.Target, host string, port uint32) (net.Conn, error) {
	address := net.ParseIP(host)
	if address == nil || !address.IsLoopback() || port == 0 || port > 65535 {
		return nil, managedssh.ErrUnauthorized
	}
	stream, err := executor.openHelper(ctx, target)
	if err != nil {
		return nil, err
	}
	client, backend := net.Pipe()
	result := &forwardConnection{Conn: client}
	go func() {
		defer backend.Close()
		invocation := sshexecutor.Invocation{Schema: "fugue.ssh-execution/v1", Target: target, Shell: executor.shell, Kind: "forward", ForwardHost: host, ForwardPort: port}
		_, err := sshexecutor.ExecuteHelper(ctx, stream, invocation, managedssh.SessionRequest{Kind: "forward"}, managedssh.SessionIO{Stdin: backend, Stdout: backend, Stderr: io.Discard})
		result.mutex.Lock()
		result.failure = err
		result.mutex.Unlock()
	}()
	return result, nil
}

type forwardConnection struct {
	net.Conn
	mutex   sync.Mutex
	failure error
}

func (connection *forwardConnection) Read(value []byte) (int, error) {
	count, err := connection.Conn.Read(value)
	if err != nil {
		connection.mutex.Lock()
		failure := connection.failure
		connection.mutex.Unlock()
		if failure != nil {
			return count, failure
		}
	}
	return count, err
}

type helperStream struct {
	reader   *io.PipeReader
	writer   *io.PipeWriter
	cancel   context.CancelFunc
	once     sync.Once
	finished chan struct{}
}

func (stream *helperStream) Read(value []byte) (int, error)  { return stream.reader.Read(value) }
func (stream *helperStream) Write(value []byte) (int, error) { return stream.writer.Write(value) }
func (stream *helperStream) Close() error {
	stream.once.Do(func() {
		_ = stream.writer.Close()
		select {
		case <-stream.finished:
		case <-time.After(time.Second):
		}
		stream.cancel()
		_ = stream.reader.Close()
	})
	return nil
}

func (executor *Kubernetes) openHelper(ctx context.Context, target managedssh.Target) (*helperStream, error) {
	if _, err := executor.boundPod(ctx, target); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	input, writer := io.Pipe()
	reader, output := io.Pipe()
	stream := &helperStream{reader: reader, writer: writer, cancel: cancel, finished: make(chan struct{})}
	go func() {
		defer close(stream.finished)
		defer input.Close()
		defer output.Close()
		err := executor.stream(ctx, target, []string{executor.shell, "-c", helperBootstrap, "fugue-ssh", strconv.Itoa(len(executor.helper)), executor.digest}, input, output, io.Discard)
		if err != nil {
			_ = output.CloseWithError(managedssh.ErrUnavailable)
		}
	}()
	deadline := time.AfterFunc(15*time.Second, func() { _ = stream.Close() })
	defer deadline.Stop()
	if _, err := stream.Write(executor.helper); err != nil {
		_ = stream.Close()
		return nil, err
	}
	marker := make([]byte, len(helperReady))
	if _, err := io.ReadFull(stream, marker); err != nil || string(marker) != helperReady {
		_ = stream.Close()
		return nil, managedssh.ErrCapabilities
	}
	if _, err := executor.boundPod(ctx, target); err != nil {
		_ = stream.Close()
		return nil, err
	}
	return stream, nil
}

func (executor *Kubernetes) stream(ctx context.Context, target managedssh.Target, command []string, input io.Reader, output, diagnostic io.Writer) error {
	request := executor.client.CoreV1().RESTClient().Post().Resource("pods").Namespace(target.Namespace).Name(target.PodName).SubResource("exec").VersionedParams(&corev1.PodExecOptions{Container: target.Container, Command: command, Stdin: input != nil, Stdout: true, Stderr: true, TTY: false}, scheme.ParameterCodec)
	websocket, err := remotecommand.NewWebSocketExecutor(executor.configuration, http.MethodGet, request.URL().String())
	if err != nil {
		return err
	}
	spdy, err := remotecommand.NewSPDYExecutor(executor.configuration, http.MethodPost, request.URL())
	if err != nil {
		return err
	}
	transport, err := remotecommand.NewFallbackExecutor(websocket, spdy, func(err error) bool { return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err) })
	if err != nil {
		return err
	}
	return transport.StreamWithContext(ctx, remotecommand.StreamOptions{Stdin: input, Stdout: output, Stderr: diagnostic})
}

type limitedBuffer struct {
	bytes.Buffer
	limit int
}

func (buffer *limitedBuffer) Write(value []byte) (int, error) {
	if len(value)+buffer.Len() > buffer.limit {
		return 0, errors.New("runtime response exceeds limit")
	}
	return buffer.Buffer.Write(value)
}
