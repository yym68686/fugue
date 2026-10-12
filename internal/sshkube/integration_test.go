package sshkube

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/managedssh"
	"fugue/internal/sshdeployment"
	"fugue/internal/sshidentity"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type testAudit struct{}

func (testAudit) Record(context.Context, managedssh.AuditEvent) error { return nil }

func TestManagedSSHIsolatedKubernetes(test *testing.T) {
	kubeconfig, helperPath := os.Getenv("FUGUE_SSH_TEST_KUBECONFIG"), os.Getenv("FUGUE_SSH_TEST_HELPER")
	if kubeconfig == "" || helperPath == "" {
		test.Skip("set explicit isolated kind kubeconfig and static Linux helper")
	}
	loaded, err := clientcmd.LoadFromFile(kubeconfig)
	if err != nil {
		test.Fatal(err)
	}
	if !strings.HasPrefix(loaded.CurrentContext, "kind-fugue-ssh-") {
		test.Fatal("requires task-owned isolated kind context")
	}
	fixturePath := filepath.Join(test.TempDir(), "background.test")
	buildContext, stopBuild := context.WithTimeout(context.Background(), time.Minute)
	defer stopBuild()
	build := exec.CommandContext(buildContext, "go", "test", "-c", "-o", fixturePath, "../sshexecutor")
	build.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux")
	if output, err := build.CombinedOutput(); err != nil {
		test.Fatal("background fixture build failed", string(output), err)
	}
	configuration, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		test.Fatal(err)
	}
	address, err := url.Parse(configuration.Host)
	if err != nil || address.Hostname() != "127.0.0.1" {
		test.Fatal("requires loopback API server")
	}
	client, err := kubernetes.NewForConfig(configuration)
	if err != nil {
		test.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	namespace, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "fugue-ssh-test-"}}, metav1.CreateOptions{})
	if err != nil {
		test.Fatal(err)
	}
	defer client.CoreV1().Namespaces().Delete(context.Background(), namespace.Name, metav1.DeleteOptions{})
	pod, err := client.CoreV1().Pods(namespace.Name).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "workspace", Labels: map[string]string{"fugue.pro/tenant-id": "tenant_fixture", "fugue.pro/app-id": "app_fixture"}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "workspace", Image: "alpine:3.22", ImagePullPolicy: corev1.PullNever,
			Env:     []corev1.EnvVar{{Name: "HOME", Value: "/bootstrap-home"}, {Name: "SHELL", Value: "/bin/sh"}},
			Command: []string{"/bin/sh", "-c", "mkdir -p /workspace; sed -i 's@^root:.*@root:x:0:0:root:/workspace:/bin/ash@' /etc/passwd; printf 'export FUGUE_LOGIN_PROFILE=account-profile-loaded\\n' > /workspace/.profile; while true; do printf 'HTTP/1.0 200 OK\\r\\nContent-Length: 14\\r\\n\\r\\nbusiness-ready' | nc -l -p 9090; done"}}}},
	}, metav1.CreateOptions{})
	if err != nil {
		test.Fatal(err)
	}
	for {
		pod, err = client.CoreV1().Pods(namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			test.Fatal(err)
		}
		if len(pod.Status.ContainerStatuses) > 0 && pod.Status.ContainerStatuses[0].State.Running != nil {
			break
		}
		select {
		case <-ctx.Done():
			test.Fatal("fixture did not start", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	target := managedssh.Target{TenantID: "tenant_fixture", AppID: "app_fixture", Namespace: namespace.Name, PodName: pod.Name, PodUID: string(pod.UID), Container: "workspace", ContainerID: pod.Status.ContainerStatuses[0].ContainerID, User: "root", WorkingDirectory: "/workspace"}
	plan := sshdeployment.DeploymentPlan{Name: "managed-ssh", Namespace: namespace.Name, Image: "registry.example/ssh@sha256:" + strings.Repeat("a", 64), HelperDigest: "sha256:" + strings.Repeat("b", 64), ArtifactSecret: "artifact", TrustSecret: "trust", HostKeySecret: "identity", StateClaim: "state", APIServerCIDR: "192.0.2.10/32", APIServerPort: 6443, IngressNamespace: namespace.Name, IngressPodLabels: map[string]string{"component": "front"}, Target: target}
	account, role, binding, err := sshdeployment.RuntimePermissions(plan)
	if err != nil {
		test.Fatal(err)
	}
	if _, err := client.CoreV1().ServiceAccounts(namespace.Name).Create(ctx, &account, metav1.CreateOptions{}); err != nil {
		test.Fatal(err)
	}
	if _, err := client.RbacV1().Roles(namespace.Name).Create(ctx, &role, metav1.CreateOptions{}); err != nil {
		test.Fatal(err)
	}
	if _, err := client.RbacV1().RoleBindings(namespace.Name).Create(ctx, &binding, metav1.CreateOptions{}); err != nil {
		test.Fatal(err)
	}
	credential, err := client.CoreV1().ServiceAccounts(namespace.Name).CreateToken(ctx, account.Name, &authenticationv1.TokenRequest{}, metav1.CreateOptions{})
	if err != nil {
		test.Fatal(err)
	}
	scoped := rest.CopyConfig(configuration)
	scoped.TLSClientConfig.CertData = nil
	scoped.TLSClientConfig.KeyData = nil
	scoped.TLSClientConfig.CertFile = ""
	scoped.TLSClientConfig.KeyFile = ""
	scoped.BearerToken = credential.Status.Token
	scoped.BearerTokenFile = ""
	scopedClient, err := kubernetes.NewForConfig(scoped)
	if err != nil {
		test.Fatal(err)
	}
	if _, err := scopedClient.CoreV1().Pods(namespace.Name).Get(ctx, "another-workspace", metav1.GetOptions{}); err == nil {
		test.Fatal("scoped account can access another pod")
	}
	if _, err := scopedClient.CoreV1().Secrets(namespace.Name).List(ctx, metav1.ListOptions{}); err == nil {
		test.Fatal("SSH execution account can list secrets")
	}
	helper, err := os.ReadFile(helperPath)
	if err != nil {
		test.Fatal(err)
	}
	digest := sha256.Sum256(helper)
	executor, err := NewKubernetes(scoped, helper, "sha256:"+hex.EncodeToString(digest[:]), "/bin/sh")
	if err != nil {
		test.Fatal(err)
	}
	if _, err := executor.Validate(ctx, target); err != nil {
		test.Fatal("runtime preflight failed", err)
	}
	keyring, err := sshidentity.NewKeyring("test", map[string][]byte{"test": bytes.Repeat([]byte{15}, 32)})
	if err != nil {
		test.Fatal(err)
	}
	identity, err := keyring.Create(target.TenantID, target.AppID, time.Now())
	if err != nil {
		test.Fatal(err)
	}
	hostSigner, err := keyring.Signer(identity)
	if err != nil {
		test.Fatal(err)
	}
	userIdentity, err := keyring.Create(target.TenantID, "user_fixture", time.Now())
	if err != nil {
		test.Fatal(err)
	}
	userSigner, err := keyring.Signer(userIdentity)
	if err != nil {
		test.Fatal(err)
	}
	policy := managedssh.Configuration{Schema: "fugue.managed-ssh/v1", Generation: 1, Identity: identity.Identity, Target: target, Keys: []managedssh.KeyGrant{{ID: "grant", PublicKey: userIdentity.Identity.PublicKey, ExpiresAt: time.Now().Add(time.Minute)}}, ValidUntil: time.Now().Add(time.Hour), AllowPTY: true, AllowSFTP: true, ForwardRules: []managedssh.ForwardRule{{Host: "127.0.0.1", Port: 9090}}, MaxConnections: 4, MaxConnectionsPerIP: 4, MaxChannels: 8}
	server, err := managedssh.NewServer(executor, testAudit{})
	if err != nil {
		test.Fatal(err)
	}
	if err := server.Activate(ctx, policy, hostSigner); err != nil {
		test.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		test.Fatal(err)
	}
	serveContext, stopServer := context.WithCancel(ctx)
	defer stopServer()
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(serveContext, listener) }()
	defer func() {
		stopServer()
		_ = listener.Close()
		select {
		case <-serverDone:
		case <-time.After(5 * time.Second):
			test.Error("SSH server did not stop")
		}
	}()
	connection, err := ssh.Dial("tcp", listener.Addr().String(), &ssh.ClientConfig{User: "root", Auth: []ssh.AuthMethod{ssh.PublicKeys(userSigner)}, HostKeyCallback: ssh.FixedHostKey(hostSigner.PublicKey()), Timeout: 5 * time.Second})
	if err != nil {
		test.Fatal(err)
	}
	defer connection.Close()
	session, err := connection.NewSession()
	if err != nil {
		test.Fatal(err)
	}
	output, err := session.CombinedOutput("pwd; id -un; printf no-app-sshd")
	_ = session.Close()
	if err != nil || !strings.Contains(string(output), "/workspace\nroot\nno-app-sshd") {
		test.Fatal("managed execution failed", string(output), err)
	}
	terminal, err := connection.NewSession()
	if err != nil {
		test.Fatal(err)
	}
	if err := terminal.RequestPty("xterm", 33, 101, ssh.TerminalModes{ssh.ECHO: 0}); err != nil {
		test.Fatal(err)
	}
	output, err = terminal.CombinedOutput("stty size")
	_ = terminal.Close()
	if err != nil || !strings.Contains(string(output), "33 101") {
		test.Fatal("target PTY failed", string(output), err)
	}
	transfer, err := sftp.NewClient(connection)
	if err != nil {
		test.Fatal(err)
	}
	file, err := transfer.Create("transfer.txt")
	if err != nil {
		test.Fatal(err)
	}
	if _, err := file.Write([]byte("container-file")); err != nil {
		test.Fatal(err)
	}
	_ = file.Close()
	fixture, err := os.ReadFile(fixturePath)
	if err != nil {
		test.Fatal(err)
	}
	file, err = transfer.Create("background.test")
	if err != nil {
		test.Fatal(err)
	}
	if _, err := file.Write(fixture); err != nil {
		test.Fatal(err)
	}
	if err := file.Chmod(0o700); err != nil {
		test.Fatal(err)
	}
	if err := file.Close(); err != nil {
		test.Fatal(err)
	}
	_ = transfer.Close()
	fileCheck, err := connection.NewSession()
	if err != nil {
		test.Fatal(err)
	}
	output, err = fileCheck.Output("cat /workspace/transfer.txt")
	_ = fileCheck.Close()
	if err != nil || string(output) != "container-file" {
		test.Fatal("transfer did not reach target filesystem", string(output), err)
	}
	forward, err := connection.Dial("tcp", "127.0.0.1:9090")
	if err != nil {
		test.Fatal(err)
	}
	_, _ = fmt.Fprint(forward, "GET / HTTP/1.0\r\nHost: localhost\r\n\r\n")
	output, err = io.ReadAll(forward)
	_ = forward.Close()
	if err != nil || !strings.Contains(string(output), "business-ready") {
		direct, directErr := executor.Forward(ctx, target, "127.0.0.1", 9090)
		if directErr == nil {
			_, _ = fmt.Fprint(direct, "GET / HTTP/1.0\r\nHost: localhost\r\n\r\n")
			directOutput, readErr := io.ReadAll(direct)
			_ = direct.Close()
			test.Log("direct forward", string(directOutput), readErr)
		} else {
			test.Log("direct open", directErr)
		}
		test.Fatal("forward did not reach target network", string(output), err)
	}
	identityDirectory := test.TempDir()
	privateKey, err := keyring.PrivateKeyPEM(userIdentity)
	if err != nil {
		test.Fatal(err)
	}
	privatePath := filepath.Join(identityDirectory, "user-key")
	knownPath := filepath.Join(identityDirectory, "known-hosts")
	if err := os.WriteFile(privatePath, privateKey, 0o600); err != nil {
		test.Fatal(err)
	}
	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		test.Fatal(err)
	}
	if err := os.WriteFile(knownPath, []byte("["+host+"]:"+port+" "+identity.Identity.PublicKey+"\n"), 0o600); err != nil {
		test.Fatal(err)
	}
	sshOptions := []string{"-F", "/dev/null", "-i", privatePath, "-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes", "-o", "UserKnownHostsFile=" + knownPath}
	native := exec.CommandContext(ctx, "ssh", append(append([]string{}, sshOptions...), "-p", port, "root@"+host, "printf native-client")...)
	output, err = native.CombinedOutput()
	if err != nil || string(output) != "native-client" {
		test.Fatal("native OpenSSH failed", string(output), err)
	}
	bootstrap := "FUGUE_TEST_BACKGROUND_SOCKET=/workspace/background.sock nohup /workspace/background.test -test.run=^TestBackgroundServiceFixture$ </dev/null >/workspace/background.log 2>&1 &"
	native = exec.CommandContext(ctx, "ssh", append(append([]string{}, sshOptions...), "-T", "-p", port, "root@"+host, bootstrap)...)
	output, err = native.CombinedOutput()
	if err != nil || len(output) != 0 {
		test.Fatal("native detached bootstrap failed", string(output), err)
	}
	proxy := "attempt=0; until test -S /workspace/background.sock; do attempt=$((attempt+1)); test $attempt -lt 50 || exit 1; sleep 0.1; done; FUGUE_TEST_BACKGROUND_SOCKET=/workspace/background.sock FUGUE_TEST_BACKGROUND_CLIENT=true exec /workspace/background.test -test.run=^TestBackgroundServiceFixture$"
	native = exec.CommandContext(ctx, "ssh", append(append([]string{}, sshOptions...), "-T", "-p", port, "root@"+host, proxy)...)
	output, err = native.CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("PASS")) {
		test.Fatal("native cross-session Unix socket proxy failed", string(output), err)
	}
	test.Log("native detached bootstrap and cross-session Unix socket verified")
	native = exec.CommandContext(ctx, "ssh", append(append([]string{}, sshOptions...), "-tt", "-p", port, "root@"+host, "test -t 0 && stty size")...)
	output, err = native.CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("24 80")) {
		test.Fatal("native forced PTY without local dimensions failed", string(output), err)
	}
	native = exec.CommandContext(ctx, "ssh", append(append([]string{}, sshOptions...), "-tt", "-p", port, "root@"+host)...)
	native.Stdin = strings.NewReader("printf 'ACCOUNT:%s:%s:%s:%s:%s:%s\\n' \"$SHELL\" \"$HOME\" \"$USER\" \"$LOGNAME\" \"$0\" \"$FUGUE_LOGIN_PROFILE\"\nexit 0\n")
	output, err = native.CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("ACCOUNT:/bin/ash:/workspace:root:root:-ash:account-profile-loaded")) {
		test.Fatal("native interactive login ignored the target account shell, home, or profile", string(output), err)
	}
	localFile := filepath.Join(identityDirectory, "upload.txt")
	if err := os.WriteFile(localFile, []byte("native-scp-payload"), 0o600); err != nil {
		test.Fatal(err)
	}
	native = exec.CommandContext(ctx, "scp", append(append([]string{}, sshOptions...), "-P", port, localFile, "root@"+host+":/workspace/native.txt")...)
	output, err = native.CombinedOutput()
	if err != nil {
		test.Fatal("native SCP upload failed", string(output), err)
	}
	downloadPath := filepath.Join(identityDirectory, "download.txt")
	native = exec.CommandContext(ctx, "scp", append(append([]string{}, sshOptions...), "-P", port, "root@"+host+":/workspace/native.txt", downloadPath)...)
	output, err = native.CombinedOutput()
	if err != nil {
		test.Fatal("native SCP download failed", string(output), err)
	}
	content, err := os.ReadFile(downloadPath)
	if err != nil || string(content) != "native-scp-payload" {
		test.Fatal("SCP content mismatch", err)
	}
	cleanup, err := connection.NewSession()
	if err != nil {
		test.Fatal(err)
	}
	output, err = cleanup.Output("test -z \"$(command -v sshd)\"; find /tmp -maxdepth 2 -name helper | wc -l")
	_ = cleanup.Close()
	if err != nil || strings.TrimSpace(string(output)) != "1" {
		test.Fatal("application unexpectedly has sshd or abandoned runtime helpers", string(output), err)
	}
	after, err := client.CoreV1().Pods(namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		test.Fatal(err)
	}
	if after.UID != pod.UID || after.Status.ContainerStatuses[0].ContainerID != target.ContainerID || after.Status.ContainerStatuses[0].RestartCount != 0 || after.Spec.Containers[0].Image != pod.Spec.Containers[0].Image {
		test.Fatal("managed SSH restarted or changed application image")
	}
}
