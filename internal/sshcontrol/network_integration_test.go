package sshcontrol

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"fugue/internal/managedssh"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/util/exec"
)

func verifyCandidateNetworkIsolation(test *testing.T, ctx context.Context, configuration *rest.Config, client kubernetes.Interface, slot managedssh.ServingSlot, untrustedNamespace string) {
	test.Helper()
	service, err := client.CoreV1().Services(slot.Namespace).Get(ctx, slot.ID, metav1.GetOptions{})
	if err != nil {
		test.Fatal(err)
	}
	pods, err := client.CoreV1().Pods(slot.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "fugue.pro/ssh-instance=" + slot.ID})
	if err != nil || len(pods.Items) != 1 {
		test.Fatal("candidate pod missing", err)
	}
	candidate := pods.Items[0]
	createProbe := func(namespace, component string) corev1.Pod {
		test.Helper()
		pod, err := client.CoreV1().Pods(namespace).Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{GenerateName: "ssh-net-probe-", Labels: map[string]string{"component": component}}, Spec: corev1.PodSpec{AutomountServiceAccountToken: booleanPointer(false), Containers: []corev1.Container{{Name: "probe", Image: "alpine:3.22", ImagePullPolicy: corev1.PullNever, Command: []string{"/bin/sh", "-c", "while true; do printf 'fixture-ready' | nc -l -p 8090; done"}}}}}, metav1.CreateOptions{})
		if err != nil {
			test.Fatal(err)
		}
		for {
			pod, err = client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			if err != nil {
				test.Fatal(err)
			}
			if len(pod.Status.ContainerStatuses) == 1 && pod.Status.ContainerStatuses[0].RestartCount > 0 {
				test.Fatal("network fixture unexpectedly restarted")
			}
			if len(pod.Status.ContainerStatuses) == 1 && pod.Status.ContainerStatuses[0].Ready {
				return *pod
			}
			select {
			case <-ctx.Done():
				test.Fatal(ctx.Err())
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	front := createProbe(slot.Namespace, "front")
	controller := createProbe(slot.Namespace, "controller")
	untrusted := createProbe(untrustedNamespace, "controller")
	probe := func(pod corev1.Pod, container, address string, port int) bool {
		test.Helper()
		probeContext, cancel := context.WithTimeout(ctx, 6*time.Second)
		defer cancel()
		request := client.CoreV1().RESTClient().Post().Resource("pods").Namespace(pod.Namespace).Name(pod.Name).SubResource("exec").VersionedParams(&corev1.PodExecOptions{Container: container, Command: []string{"/bin/sh", "-c", "nc -z -w 2 \"$1\" \"$2\"", "network-probe", address, strconv.Itoa(port)}, Stdout: true, Stderr: true}, scheme.ParameterCodec)
		executor, err := remotecommand.NewSPDYExecutor(configuration, "POST", request.URL())
		if err != nil {
			test.Fatal(err)
		}
		var output bytes.Buffer
		err = executor.StreamWithContext(probeContext, remotecommand.StreamOptions{Stdout: &output, Stderr: &output})
		if err == nil {
			return true
		}
		var exitError exec.ExitError
		if !errors.As(err, &exitError) || exitError.ExitStatus() != 1 {
			test.Fatal("network probe failed without a TCP deny result", err, output.String())
		}
		return false
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		if probe(front, "probe", service.Spec.ClusterIP, 2222) && probe(controller, "probe", service.Spec.ClusterIP, 7837) && probe(front, "probe", untrusted.Status.PodIP, 8090) {
			break
		}
		if time.Now().After(deadline) {
			test.Fatal("positive network policy controls failed")
		}
		time.Sleep(200 * time.Millisecond)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if probe(untrusted, "probe", service.Spec.ClusterIP, 2222) {
			test.Fatal("other namespace reached managed SSH")
		}
		if probe(untrusted, "probe", service.Spec.ClusterIP, 7837) {
			test.Fatal("other namespace impersonated the controller")
		}
		if probe(front, "probe", service.Spec.ClusterIP, 7837) {
			test.Fatal("front accessed controller-only health surface")
		}
		if probe(candidate, "ssh", untrusted.Status.PodIP, 8090) {
			test.Fatal("SSH server egress reached a tenant workload")
		}
	}
	test.Log("enforcing network policy verified: approved SSH/controller ingress; cross-namespace ingress, front health, and arbitrary tenant egress denied")
}

func booleanPointer(value bool) *bool { return &value }
