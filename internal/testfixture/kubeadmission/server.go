// Package kubeadmission starts disposable loopback Kubernetes admission fixtures.
package kubeadmission

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Start never uses kubeconfig, an existing cluster, a
// Docker daemon, or a remote network. Both child servers bind only to loopback
// with private temporary state and a randomly generated client token.
func Start(t *testing.T, identities ...string) (*rest.Config, map[string]*rest.Config) {
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
	tokens := make(map[string]string, len(identities))
	entries := token + ",fixture-owner,fixture-owner\n"
	for _, identity := range identities {
		if identity == "" || strings.ContainsAny(identity, ",\r\n") || tokens[identity] != "" {
			t.Fatalf("invalid or duplicate isolated API test identity")
		}
		credential := rand.Text()
		tokens[identity] = credential
		entries += credential + "," + identity + "," + identity + "\n"
	}
	if err := os.WriteFile(tokenFile, []byte(entries), 0o600); err != nil {
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
	ca, tlsCert, tlsKey := servingTLS(t, data, net.ParseIP(dnsName))
	apiDone := startTestBinary(t, data, assets, "kube-apiserver",
		"--bind-address="+dnsName, "--advertise-address="+dnsName, "--secure-port="+apiPort,
		"--tls-cert-file="+tlsCert, "--tls-private-key-file="+tlsKey,
		"--etcd-servers="+etcdAddress, "--service-cluster-ip-range=198.19.0.0/16", "--disable-admission-plugins=ServiceAccount",
		"--authorization-mode=AlwaysAllow", "--anonymous-auth=false", "--token-auth-file="+tokenFile,
		"--service-account-issuer=https://identity.invalid", "--service-account-key-file="+saPub, "--service-account-signing-key-file="+saKey)
	cfg := &rest.Config{Host: "https://" + apiAddress, BearerToken: token, Timeout: 3 * time.Second,
		QPS: 100, Burst: 200, TLSClientConfig: rest.TLSClientConfig{CAData: ca}}
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
	clients := make(map[string]*rest.Config, len(tokens))
	for identity, credential := range tokens {
		client := rest.CopyConfig(cfg)
		client.BearerToken = credential
		clients[identity] = client
	}
	return cfg, clients
}

func servingTLS(t *testing.T, dir string, ip net.IP) ([]byte, string, string) {
	t.Helper()
	key := func() *ecdsa.PrivateKey {
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	caKey, serverKey := key(), key()
	now := time.Now()
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "isolated Kubernetes CA"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	server := &x509.Certificate{
		SerialNumber: big.NewInt(2), IPAddresses: []net.IP{ip}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter,
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, server, ca, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	rawKey, err := x509.MarshalPKCS8PrivateKey(serverKey)
	if err != nil {
		t.Fatal(err)
	}
	write := func(name, kind string, der []byte) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		write("apiserver.crt", "CERTIFICATE", serverDER), write("apiserver.key", "PRIVATE KEY", rawKey)
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
