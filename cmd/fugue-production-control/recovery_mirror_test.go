package main

import (
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func TestSealedRecoveryClientVerifiesTLSAndDoesNotFollowRedirects(t *testing.T) {
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("recovery observation followed a redirect")
		w.WriteHeader(http.StatusForbidden)
	}))
	defer target.Close()
	origin := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/namespaces/release/configmaps/fugue-release-monitor-api" ||
			r.Header.Get("Authorization") != "Bearer recovery-read-fixture" {
			t.Errorf("unexpected recovery request: %s %s", r.Method, r.URL.Path)
		}
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer origin.Close()
	cfg := clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{"fixture": {
			Server: origin.URL, CertificateAuthorityData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: origin.Certificate().Raw}),
		}},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{"reader": {Token: "recovery-read-fixture"}},
		Contexts:  map[string]*clientcmdapi.Context{"recovery": {Cluster: "fixture", AuthInfo: "reader"}},
	}
	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := clientcmd.WriteToFile(cfg, path); err != nil {
		t.Fatal(err)
	}
	client, err := sealedRecoveryClient(path, "recovery")
	if err != nil {
		t.Fatal(err)
	}
	maps := client.ConfigMaps("release")
	if _, err := maps.Get(context.Background(), "fugue-release-monitor-api", metav1.GetOptions{}); err == nil {
		t.Fatal("Kubernetes redirect was accepted as evidence")
	}
	if err := maps.Delete(context.Background(), "fugue-release-monitor-api", metav1.DeleteOptions{}); err == nil {
		t.Fatal("read-only Kubernetes client accepted DELETE")
	}
	cfg.Clusters["fixture"].CertificateAuthorityData = nil
	cfg.Clusters["fixture"].InsecureSkipTLSVerify = true
	if err := clientcmd.WriteToFile(cfg, path); err != nil {
		t.Fatal(err)
	}
	if _, err := sealedRecoveryClient(path, "recovery"); err == nil {
		t.Fatal("insecure Kubernetes context was accepted")
	}
}
