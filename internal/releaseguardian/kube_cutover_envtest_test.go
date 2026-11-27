package releaseguardian

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// startAdmissionTestServer never uses kubeconfig, an existing cluster, a
// Docker daemon, or the network. Both child servers bind only to loopback
// with private temporary state and a randomly generated client token.
func startAdmissionTestServer(t *testing.T) *rest.Config {
	t.Helper()
	assets := os.Getenv("FUGUE_TEST_ENVTEST_ASSETS")
	if assets == "" {
		t.Skip("set FUGUE_TEST_ENVTEST_ASSETS to a directory with kube-apiserver and etcd")
	}
	for _, name := range []string{"kube-apiserver", "etcd"} {
		if info, err := os.Stat(filepath.Join(assets, name)); err != nil || !info.Mode().IsRegular() {
			t.Fatalf("envtest asset %s is missing: %v", name, err)
		}
	}
	data := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	saKey := filepath.Join(data, "serviceaccount.pem")
	if err := os.WriteFile(saKey, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	public, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	saPub := filepath.Join(data, "serviceaccount.pub")
	if err := os.WriteFile(saPub, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: public}), 0o600); err != nil {
		t.Fatal(err)
	}
	token := rand.Text()
	tokenFile := filepath.Join(data, "token.csv")
	if err := os.WriteFile(tokenFile, []byte(token+",fixture-owner,fixture-owner\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	etcdAddress := "http://" + unusedLoopbackAddress(t)
	etcdDone := startTestBinary(t, data, assets, "etcd",
		"--data-dir="+filepath.Join(data, "etcd"), "--listen-client-urls="+etcdAddress, "--advertise-client-urls="+etcdAddress,
		"--listen-peer-urls=http://127.0.0.1:0", "--unsafe-no-fsync=true")
	apiAddress := unusedLoopbackAddress(t)
	_, apiPort, err := net.SplitHostPort(apiAddress)
	if err != nil {
		t.Fatal(err)
	}
	const dnsName = "127.0.0.1"
	apiDone := startTestBinary(t, data, assets, "kube-apiserver",
		"--bind-address="+dnsName, "--advertise-address="+dnsName, "--secure-port="+apiPort, "--cert-dir="+data,
		"--etcd-servers="+etcdAddress, "--service-cluster-ip-range=198.19.0.0/16", "--disable-admission-plugins=ServiceAccount",
		"--authorization-mode=AlwaysAllow", "--anonymous-auth=false", "--token-auth-file="+tokenFile,
		"--service-account-issuer=https://identity.invalid", "--service-account-key-file="+saPub, "--service-account-signing-key-file="+saKey)
	cfg := &rest.Config{Host: "https://" + apiAddress, BearerToken: token, Timeout: 3 * time.Second,
		QPS: 100, Burst: 200, TLSClientConfig: rest.TLSClientConfig{Insecure: true}}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 45*time.Second, true, func(ctx context.Context) (bool, error) {
		select {
		case <-apiDone:
			return false, errors.New("kube-apiserver exited before readiness")
		case <-etcdDone:
			return false, errors.New("etcd exited before readiness")
		default:
		}
		_, err := client.CoreV1().RESTClient().Get().AbsPath("/readyz").DoRaw(ctx)
		return err == nil, nil
	}); err != nil {
		t.Fatalf("isolated Kubernetes API server did not start: %v", err)
	}
	return cfg
}

func startTestBinary(t *testing.T, data, assets, name string, args ...string) <-chan struct{} {
	t.Helper()
	logFile, err := os.Create(filepath.Join(data, name+".log"))
	if err != nil {
		t.Fatal(err)
	}
	binary, err := filepath.Abs(filepath.Join(assets, name))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, args...)
	cmd.Dir, cmd.Stdout, cmd.Stderr = data, logFile, logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-done
		_ = logFile.Close()
		if t.Failed() {
			if data, err := os.ReadFile(logFile.Name()); err == nil {
				if len(data) > 12<<10 {
					data = data[len(data)-(12<<10):]
				}
				t.Logf("%s log:\n%s", name, data)
			}
		}
	})
	return done
}

func unusedLoopbackAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func admissionClient(t *testing.T, cfg *rest.Config, identity string) kubernetes.Interface {
	t.Helper()
	copy := rest.CopyConfig(cfg)
	if identity != "" {
		copy.Impersonate.UserName = identity
	}
	client, err := kubernetes.NewForConfig(copy)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func applyCutoverPhase(ctx context.Context, client kubernetes.Interface, config KubeDeploymentCutover) error {
	pairs, err := DeploymentCutoverPolicies(config)
	if err != nil {
		return err
	}
	for _, pair := range pairs {
		policies := client.AdmissionregistrationV1().ValidatingAdmissionPolicies()
		old, err := policies.Get(ctx, pair.Policy.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			_, err = policies.Create(ctx, pair.Policy, metav1.CreateOptions{})
		} else if err == nil {
			pair.Policy.ResourceVersion = old.ResourceVersion
			_, err = policies.Update(ctx, pair.Policy, metav1.UpdateOptions{})
		}
		if err != nil {
			return err
		}
		bindings := client.AdmissionregistrationV1().ValidatingAdmissionPolicyBindings()
		if _, err := bindings.Create(ctx, pair.Binding, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}

func requireCutoverDenial(t *testing.T, err error) {
	t.Helper()
	if !isCutoverDenial(err) {
		t.Fatalf("expected the Fugue admission policy to deny mutation, got: %v", err)
	}
}

func isCutoverDenial(err error) bool {
	return apierrors.IsForbidden(err) && strings.Contains(err.Error(), "Fugue cutover denies mutation")
}

func waitForCutover(t *testing.T, ctx context.Context, client kubernetes.Interface, config KubeDeploymentCutover, ready func(context.Context) (bool, error)) {
	t.Helper()
	if err := applyCutoverPhase(ctx, client, config); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 12*time.Second, true, ready); err != nil {
		t.Fatalf("provider never enforced phase %s: %v", config.Phase, err)
	}
}

func createAdmissionTestNamespace(t *testing.T, ctx context.Context, admin kubernetes.Interface, namespace string) {
	t.Helper()
	if _, err := admin.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
}
