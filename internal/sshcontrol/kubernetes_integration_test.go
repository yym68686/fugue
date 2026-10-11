package sshcontrol

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/managedssh"
	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/sshdeployment"
	"fugue/internal/sshidentity"

	"golang.org/x/crypto/ssh"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func TestCandidatePublicationIsolatedKubernetes(test *testing.T) {
	kubeconfig := os.Getenv("FUGUE_SSH_TEST_KUBECONFIG")
	image := os.Getenv("FUGUE_SSH_TEST_IMAGE")
	helperDigest := os.Getenv("FUGUE_SSH_TEST_HELPER_DIGEST")
	if kubeconfig == "" || image == "" || helperDigest == "" {
		test.Skip("requires task-owned isolated Kubernetes, managed SSH image and helper digest")
	}
	loaded, err := clientcmd.LoadFromFile(kubeconfig)
	if err != nil || !strings.HasPrefix(loaded.CurrentContext, "kind-fugue-ssh-") {
		test.Fatal("requires an explicit isolated kind context", err)
	}
	configuration, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		test.Fatal(err)
	}
	address, err := url.Parse(configuration.Host)
	if err != nil || address.Hostname() != "127.0.0.1" {
		test.Fatal("requires loopback test API")
	}
	client, err := kubernetes.NewForConfig(configuration)
	if err != nil {
		test.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 210*time.Second)
	defer cancel()
	platform, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "fugue-ssh-candidate-", Labels: map[string]string{"fugue.pro/managed-ssh-control": "true"}}}, metav1.CreateOptions{})
	if err != nil {
		test.Fatal(err)
	}
	defer client.CoreV1().Namespaces().Delete(context.Background(), platform.Name, metav1.DeleteOptions{})
	app := model.App{ID: model.NewID("app-fixture"), TenantID: model.NewID("tenant-fixture"), Name: "workspace", Spec: model.AppSpec{RuntimeID: "runtime-fixture", Image: "alpine:3.22", Replicas: 1}}
	namespace := runtimepkg.NamespaceForTenant(app.TenantID)
	if _, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{}); err != nil {
		test.Fatal(err)
	}
	defer client.CoreV1().Namespaces().Delete(context.Background(), namespace, metav1.DeleteOptions{})
	defer func() {
		if !test.Failed() {
			return
		}
		diagnosticsContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, selected := range []string{platform.Name, namespace} {
			pods, err := client.CoreV1().Pods(selected).List(diagnosticsContext, metav1.ListOptions{})
			if err != nil {
				test.Log("failed to collect candidate pods", err)
				continue
			}
			for _, pod := range pods.Items {
				test.Logf("candidate diagnostic namespace=%s pod=%s phase=%s node=%s", selected, pod.Name, pod.Status.Phase, pod.Spec.NodeName)
				for _, status := range pod.Status.ContainerStatuses {
					state, _ := json.Marshal(status.State)
					test.Logf("container=%s image=%s image_id=%s ready=%t restarts=%d state=%s", status.Name, status.Image, status.ImageID, status.Ready, status.RestartCount, state)
				}
				if pod.Labels["app.kubernetes.io/component"] == "managed-ssh" {
					lines := int64(20)
					stream, err := client.CoreV1().Pods(selected).GetLogs(pod.Name, &corev1.PodLogOptions{Container: "ssh", TailLines: &lines}).Stream(diagnosticsContext)
					if err == nil {
						output, _ := io.ReadAll(io.LimitReader(stream, 8192))
						stream.Close()
						test.Logf("managed SSH startup diagnostics: %s", output)
					}
				}
			}
			events, err := client.CoreV1().Events(selected).List(diagnosticsContext, metav1.ListOptions{})
			if err != nil {
				continue
			}
			for _, event := range events.Items {
				test.Logf("candidate event object=%s reason=%s message=%s", event.InvolvedObject.Name, event.Reason, event.Message)
			}
		}
	}()
	labels := map[string]string{"fugue.pro/app-id": app.ID, "fugue.pro/tenant-id": app.TenantID, "release": "active"}
	pod, err := client.CoreV1().Pods(namespace).Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "workspace", Labels: labels}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "workspace", Image: "alpine:3.22", ImagePullPolicy: corev1.PullNever, Command: []string{"/bin/sh", "-c", "mkdir -p /workspace; exec sleep 3600"}}}}}, metav1.CreateOptions{})
	if err != nil {
		test.Fatal(err)
	}
	if _, err := client.CoreV1().Services(namespace).Create(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: runtimepkg.RuntimeAppServiceName(app), Labels: labels}, Spec: corev1.ServiceSpec{Selector: labels, Ports: []corev1.ServicePort{{Port: 8080}}}}, metav1.CreateOptions{}); err != nil {
		test.Fatal(err)
	}
	for {
		pod, err = client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			test.Fatal(err)
		}
		if len(pod.Status.ContainerStatuses) == 1 && pod.Status.ContainerStatuses[0].Ready {
			break
		}
		select {
		case <-ctx.Done():
			test.Fatal("fixture not ready", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	originalUID, originalContainer, originalSpec := pod.UID, pod.Status.ContainerStatuses[0].ContainerID, pod.Spec.DeepCopy()
	node, err := client.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{})
	if err != nil {
		test.Fatal(err)
	}
	release := managedssh.RuntimeRelease{Image: image, HelperDigest: helperDigest, Architecture: node.Status.NodeInfo.Architecture}
	keyring, err := sshidentity.NewKeyring("storage", map[string][]byte{"storage": bytes.Repeat([]byte{91}, 32)})
	if err != nil {
		test.Fatal(err)
	}
	identity, err := keyring.Create(app.TenantID, app.ID, time.Now())
	if err != nil {
		test.Fatal(err)
	}
	privateKey, err := keyring.PrivateKeyPEM(identity)
	if err != nil {
		test.Fatal(err)
	}
	user, err := keyring.Create(app.TenantID, "test-user", time.Now())
	if err != nil {
		test.Fatal(err)
	}
	userSigner, err := keyring.Signer(user)
	if err != nil {
		test.Fatal(err)
	}
	intent := model.ManagedSSHIntent{TenantID: app.TenantID, AppID: app.ID, Generation: 1, Spec: model.ManagedSSHIntentSpec{Enabled: true, IdentityID: identity.Identity.ID, Container: "workspace", User: "root", WorkingDirectory: "/workspace", AuthorizedKeyIDs: []string{"test-user"}, AllowPTY: true, AllowSFTP: true, MaxConnections: 8, MaxConnectionsPerIP: 4, MaxChannels: 4}}
	runtimeFile := filepath.Join(test.TempDir(), "runtime.json")
	options := KubernetesOptions{Schema: "fugue.managed-ssh-runtime/v1", Namespace: platform.Name, RuntimeIDs: []string{app.Spec.RuntimeID}, Releases: map[string]managedssh.RuntimeRelease{release.Architecture: release}, StateStorageClass: "standard", APIServers: []sshdeployment.APIServerPeer{{CIDR: "127.0.0.1/32", Port: 6443}}, IngressNamespace: platform.Name, IngressPodLabels: map[string]string{"component": "front"}, ControllerNamespace: platform.Name, ControllerPodLabels: map[string]string{"component": "controller"}}
	apiService, err := client.CoreV1().Services("default").Get(ctx, "kubernetes", metav1.GetOptions{})
	if err != nil {
		test.Fatal(err)
	}
	options.APIServers = []sshdeployment.APIServerPeer{{CIDR: apiService.Spec.ClusterIP + "/32", Port: 443}}
	apiEndpoints, err := client.DiscoveryV1().EndpointSlices("default").List(ctx, metav1.ListOptions{LabelSelector: "kubernetes.io/service-name=kubernetes"})
	if err != nil {
		test.Fatal(err)
	}
	for _, slice := range apiEndpoints.Items {
		for _, endpoint := range slice.Endpoints {
			for _, address := range endpoint.Addresses {
				options.APIServers = append(options.APIServers, sshdeployment.APIServerPeer{CIDR: address + "/32", Port: 6443})
			}
		}
	}
	raw, _ := json.Marshal(options)
	if err := os.WriteFile(runtimeFile, raw, 0o600); err != nil {
		test.Fatal(err)
	}
	runtime, err := NewKubernetesRuntime(configuration, runtimeFile)
	if err != nil {
		test.Fatal(err)
	}
	var target managedssh.Target
	for {
		target, _, err = runtime.Resolve(ctx, app, intent, managedssh.Target{})
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			test.Fatal("target resolution failed", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	keys := []model.SSHKey{{ID: "test-user", TenantID: app.TenantID, Status: model.SSHKeyStatusActive, PublicKey: user.Identity.PublicKey, Fingerprint: user.Identity.Fingerprint}}
	serving, err := managedssh.CompileConfiguration(intent, identity.Identity, target, keys, 1, time.Now().UTC())
	if err != nil {
		test.Fatal(err)
	}
	signingKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{93}, 32))
	trust := map[string]ed25519.PublicKey{"config-key": signingKey.Public().(ed25519.PublicKey)}
	artifact, err := managedssh.SignArtifact(serving, "config-key", signingKey)
	if err != nil {
		test.Fatal(err)
	}
	slot := managedssh.ServingSlot{ID: managedssh.SlotID(identity.Identity.ID, target, release), Namespace: platform.Name, Release: release, Artifact: artifact, CreatedAt: time.Now().UTC()}
	for attempt := 0; attempt < 2; attempt++ {
		privateKey, err = keyring.PrivateKeyPEM(identity)
		if err != nil {
			test.Fatal(err)
		}
		if err := runtime.Apply(ctx, slot, privateKey, trust); err != nil {
			test.Fatal("candidate publication failed", err)
		}
	}
	for {
		evidence, err := runtime.Observe(ctx, slot)
		if err == nil {
			slot.Evidence = &evidence
			break
		}
		select {
		case <-ctx.Done():
			test.Fatal("candidate verification failed", err)
		case <-time.After(200 * time.Millisecond):
		}
	}
	if !slot.Verified(time.Now().UTC()) {
		test.Fatal("candidate evidence not bound")
	}
	if os.Getenv("FUGUE_SSH_TEST_NETWORK_POLICY") == "required" {
		verifyCandidateNetworkIsolation(test, ctx, configuration, client, slot, namespace)
	}
	hostSigner, err := keyring.Signer(identity)
	if err != nil {
		test.Fatal(err)
	}
	service, err := client.CoreV1().Services(platform.Name).Get(ctx, slot.ID, metav1.GetOptions{})
	if err != nil {
		test.Fatal(err)
	}
	connection, err := ssh.Dial("tcp", net.JoinHostPort(service.Spec.ClusterIP, "2222"), &ssh.ClientConfig{User: "root", Auth: []ssh.AuthMethod{ssh.PublicKeys(userSigner)}, HostKeyCallback: ssh.FixedHostKey(hostSigner.PublicKey()), Timeout: 5 * time.Second})
	if err != nil {
		test.Fatal(err)
	}
	defer connection.Close()
	session, err := connection.NewSession()
	if err != nil {
		test.Fatal(err)
	}
	output, err := session.CombinedOutput("pwd")
	if err != nil || strings.TrimSpace(string(output)) != "/workspace" {
		test.Fatal("candidate executed in wrong filesystem", string(output), err)
	}
	serving.Generation++
	serving.Keys = []managedssh.KeyGrant{}
	slot.Artifact, err = managedssh.SignArtifact(serving, "config-key", signingKey)
	if err != nil {
		test.Fatal(err)
	}
	if err := runtime.PublishAuthorization(ctx, slot, trust); err != nil {
		test.Fatal("revocation publication failed", err)
	}
	for {
		if _, err := runtime.Observe(ctx, slot); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			test.Fatal("revocation did not reach candidate")
		case <-time.After(time.Second):
		}
	}
	if unauthorized, err := connection.NewSession(); err == nil {
		unauthorized.Close()
		test.Fatal("revoked connection opened another session")
	}
	serving.Generation++
	serving.Draining = true
	slot.Artifact, err = managedssh.SignArtifact(serving, "config-key", signingKey)
	if err != nil {
		test.Fatal(err)
	}
	retiringAt := time.Now().UTC()
	slot.RetiringAt = &retiringAt
	if err := runtime.Apply(ctx, slot, privateKey, trust); err != nil {
		test.Fatal("drain configuration failed", err)
	}
	for {
		stopped, err := runtime.StopDrainedSlot(ctx, slot)
		if err == nil && stopped {
			break
		}
		if err := runtime.Apply(ctx, slot, privateKey, trust); err != nil {
			test.Fatal("retirement reconciliation failed", err)
		}
		select {
		case <-ctx.Done():
			test.Fatal("candidate did not drain and stop", err)
		case <-time.After(time.Second):
		}
	}
	if err := runtime.Apply(ctx, slot, privateKey, trust); err != nil {
		test.Fatal(err)
	}
	retiredDeployment, err := client.AppsV1().Deployments(slot.Namespace).Get(ctx, slot.ID, metav1.GetOptions{})
	if err != nil || *retiredDeployment.Spec.Replicas != 0 {
		test.Fatal("reconciliation restarted a retired slot", err)
	}
	after, err := client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil || after.UID != originalUID || after.Status.ContainerStatuses[0].ContainerID != originalContainer || after.Status.ContainerStatuses[0].RestartCount != 0 || !reflect.DeepEqual(originalSpec, &after.Spec) {
		test.Fatal("platform SSH publication modified or restarted application", err)
	}
}
