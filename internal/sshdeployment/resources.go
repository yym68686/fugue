package sshdeployment

import (
	"fmt"
	"fugue/internal/managedssh"
	"net"
	"regexp"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

type DeploymentPlan struct {
	Name                 string
	Namespace            string
	Image                string
	HelperDigest         string
	ArtifactSecret       string
	TrustSecret          string
	HostKeySecret        string
	StateClaim           string
	APIServerCIDR        string
	APIServerPort        int
	IngressNamespace     string
	IngressPodLabels     map[string]string
	Target               managedssh.Target
	Architecture         string
	ControllerNamespace  string
	ControllerPodLabels  map[string]string
	IngressCIDRs         []string
	AdditionalAPIServers []APIServerPeer
}

type APIServerPeer struct {
	CIDR string `json:"cidr"`
	Port int    `json:"port"`
}

var immutableImagePattern = regexp.MustCompile(`^[^\s]+@sha256:[a-f0-9]{64}$`)
var helperDigestPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

func RuntimePermissions(plan DeploymentPlan) (corev1.ServiceAccount, rbacv1.Role, rbacv1.RoleBinding, error) {
	if err := validateDeploymentPlan(plan); err != nil {
		return corev1.ServiceAccount{}, rbacv1.Role{}, rbacv1.RoleBinding{}, err
	}
	account := corev1.ServiceAccount{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"}, ObjectMeta: metav1.ObjectMeta{Name: plan.Name, Namespace: plan.Namespace}}
	role := rbacv1.Role{TypeMeta: metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"}, ObjectMeta: metav1.ObjectMeta{Name: plan.Name, Namespace: plan.Target.Namespace}, Rules: []rbacv1.PolicyRule{
		{APIGroups: []string{""}, Resources: []string{"pods"}, ResourceNames: []string{plan.Target.PodName}, Verbs: []string{"get"}},
		{APIGroups: []string{""}, Resources: []string{"pods/exec"}, ResourceNames: []string{plan.Target.PodName}, Verbs: []string{"get", "create"}},
	}}
	binding := rbacv1.RoleBinding{TypeMeta: metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"}, ObjectMeta: metav1.ObjectMeta{Name: plan.Name, Namespace: plan.Target.Namespace}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: plan.Name, Namespace: plan.Namespace}}, RoleRef: rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: plan.Name}}
	return account, role, binding, nil
}

func DeploymentObjects(plan DeploymentPlan) ([]any, error) {
	account, role, binding, err := RuntimePermissions(plan)
	if err != nil {
		return nil, err
	}
	labels := map[string]string{"app.kubernetes.io/component": "managed-ssh", "fugue.pro/ssh-instance": plan.Name, "fugue.pro/ssh-app-id": plan.Target.AppID, "fugue.pro/ssh-tenant-id": plan.Target.TenantID}
	selector := map[string]string{"fugue.pro/ssh-instance": plan.Name}
	metadata := func(name string) map[string]any {
		return map[string]any{"name": name, "namespace": plan.Namespace, "labels": labels}
	}
	secretVolume := func(name, secret string) map[string]any {
		return map[string]any{"name": name, "secret": map[string]any{"secretName": secret, "defaultMode": 0o400}}
	}
	deployment := map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment", "metadata": metadata(plan.Name),
		"spec": map[string]any{
			"replicas": 1, "selector": map[string]any{"matchLabels": selector}, "strategy": map[string]any{"type": "Recreate"},
			"template": map[string]any{"metadata": map[string]any{"labels": labels}, "spec": map[string]any{
				"serviceAccountName": plan.Name, "terminationGracePeriodSeconds": 60,
				"securityContext": map[string]any{"seccompProfile": map[string]any{"type": "RuntimeDefault"}},
				"containers": []any{map[string]any{
					"name": "ssh", "image": plan.Image, "imagePullPolicy": "IfNotPresent",
					"env":             []any{map[string]any{"name": "FUGUE_SSH_SERVER_POD_UID", "valueFrom": map[string]any{"fieldRef": map[string]any{"fieldPath": "metadata.uid"}}}},
					"args":            []string{"--listen=:2222", "--health-listen=:7837", "--artifact=/run/fugue/artifact/artifact.json", "--trust=/run/fugue/trust/trust.json", "--host-key=/run/fugue/identity/host-key", "--lkg=/var/lib/fugue-ssh/lkg.json", "--audit=/var/lib/fugue-ssh/audit.jsonl", "--helper=/usr/local/bin/fugue-ssh-helper", "--helper-digest=" + plan.HelperDigest},
					"ports":           []any{map[string]any{"name": "ssh", "containerPort": 2222}, map[string]any{"name": "health", "containerPort": 7837}},
					"securityContext": map[string]any{"runAsUser": 0, "allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true, "capabilities": map[string]any{"drop": []string{"ALL"}}},
					"resources":       map[string]any{"requests": map[string]string{"cpu": "25m", "memory": "64Mi"}, "limits": map[string]string{"cpu": "1", "memory": "256Mi"}},
					"readinessProbe":  map[string]any{"httpGet": map[string]any{"path": "/healthz", "port": "health"}, "periodSeconds": 5, "timeoutSeconds": 2},
					"volumeMounts":    []any{map[string]any{"name": "artifact", "mountPath": "/run/fugue/artifact", "readOnly": true}, map[string]any{"name": "trust", "mountPath": "/run/fugue/trust", "readOnly": true}, map[string]any{"name": "identity", "mountPath": "/run/fugue/identity", "readOnly": true}, map[string]any{"name": "state", "mountPath": "/var/lib/fugue-ssh"}},
				}},
				"volumes": []any{secretVolume("artifact", plan.ArtifactSecret), secretVolume("trust", plan.TrustSecret), secretVolume("identity", plan.HostKeySecret), map[string]any{"name": "state", "persistentVolumeClaim": map[string]any{"claimName": plan.StateClaim}}},
			}},
		},
	}
	if plan.Architecture != "" {
		deployment["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["nodeSelector"] = map[string]string{"kubernetes.io/arch": plan.Architecture}
	}
	service := map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": metadata(plan.Name), "spec": map[string]any{"type": "ClusterIP", "selector": selector, "ports": []any{map[string]any{"name": "ssh", "port": 2222, "targetPort": "ssh"}, map[string]any{"name": "health", "port": 7837, "targetPort": "health"}}}}
	policy := map[string]any{"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy", "metadata": metadata(plan.Name), "spec": map[string]any{
		"podSelector": map[string]any{"matchLabels": selector}, "policyTypes": []string{"Ingress", "Egress"},
		"ingress": []any{map[string]any{"from": []any{map[string]any{"namespaceSelector": map[string]any{"matchLabels": map[string]string{"kubernetes.io/metadata.name": plan.IngressNamespace}}, "podSelector": map[string]any{"matchLabels": plan.IngressPodLabels}}}, "ports": []any{map[string]any{"protocol": "TCP", "port": 2222}}}},
		"egress":  []any{map[string]any{"to": []any{map[string]any{"ipBlock": map[string]any{"cidr": plan.APIServerCIDR}}}, "ports": []any{map[string]any{"protocol": "TCP", "port": plan.APIServerPort}}}},
	}}
	policySpec := policy["spec"].(map[string]any)
	ingress := policySpec["ingress"].([]any)
	for _, cidr := range plan.IngressCIDRs {
		ingress = append(ingress, map[string]any{"from": []any{map[string]any{"ipBlock": map[string]any{"cidr": cidr}}}, "ports": []any{map[string]any{"protocol": "TCP", "port": 2222}}})
	}
	if plan.ControllerNamespace != "" {
		ingress = append(ingress, map[string]any{"from": []any{map[string]any{"namespaceSelector": map[string]any{"matchLabels": map[string]string{"kubernetes.io/metadata.name": plan.ControllerNamespace}}, "podSelector": map[string]any{"matchLabels": plan.ControllerPodLabels}}}, "ports": []any{map[string]any{"protocol": "TCP", "port": 2222}, map[string]any{"protocol": "TCP", "port": 7837}}})
	}
	policySpec["ingress"] = ingress
	egress := policySpec["egress"].([]any)
	for _, peer := range plan.AdditionalAPIServers {
		egress = append(egress, map[string]any{"to": []any{map[string]any{"ipBlock": map[string]any{"cidr": peer.CIDR}}}, "ports": []any{map[string]any{"protocol": "TCP", "port": peer.Port}}})
	}
	policySpec["egress"] = egress
	return []any{policy, account, role, binding, service, deployment}, nil
}

func validateDeploymentPlan(plan DeploymentPlan) error {
	for _, name := range []string{plan.Name, plan.Namespace, plan.ArtifactSecret, plan.TrustSecret, plan.HostKeySecret, plan.StateClaim, plan.Target.Namespace, plan.Target.PodName, plan.IngressNamespace} {
		if len(validation.IsDNS1123Subdomain(name)) != 0 {
			return managedssh.ErrConfiguration
		}
	}
	if !immutableImagePattern.MatchString(plan.Image) || !helperDigestPattern.MatchString(plan.HelperDigest) || plan.APIServerPort < 1 || plan.APIServerPort > 65535 || len(plan.IngressPodLabels) == 0 {
		return managedssh.ErrConfiguration
	}
	_, network, err := net.ParseCIDR(plan.APIServerCIDR)
	if err != nil {
		return managedssh.ErrConfiguration
	}
	ones, bits := network.Mask.Size()
	if ones != bits {
		return fmt.Errorf("managed SSH requires an exact API server address")
	}
	if plan.Target.TenantID == "" || plan.Target.AppID == "" || plan.Target.PodUID == "" || plan.Target.ContainerID == "" {
		return managedssh.ErrConfiguration
	}
	if plan.Architecture != "" && plan.Architecture != "amd64" && plan.Architecture != "arm64" {
		return managedssh.ErrConfiguration
	}
	if plan.ControllerNamespace != "" && (len(validation.IsDNS1123Subdomain(plan.ControllerNamespace)) != 0 || len(plan.ControllerPodLabels) == 0) {
		return managedssh.ErrConfiguration
	}
	for _, peer := range append([]APIServerPeer{{CIDR: plan.APIServerCIDR, Port: plan.APIServerPort}}, plan.AdditionalAPIServers...) {
		if !exactAddressCIDR(peer.CIDR) || peer.Port < 1 || peer.Port > 65535 {
			return managedssh.ErrConfiguration
		}
	}
	for _, cidr := range plan.IngressCIDRs {
		if !exactAddressCIDR(cidr) {
			return managedssh.ErrConfiguration
		}
	}
	return nil
}

func exactAddressCIDR(cidr string) bool {
	address, network, err := net.ParseCIDR(cidr)
	if err != nil || address.IsUnspecified() {
		return false
	}
	ones, bits := network.Mask.Size()
	return ones == bits
}
