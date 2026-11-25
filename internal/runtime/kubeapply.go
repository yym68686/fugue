package runtime

import (
	"fmt"
)

const (
	serviceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	serviceAccountCAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

func ObjectAPIPath(defaultNamespace string, obj map[string]any) (string, error) {
	apiVersion, _ := obj["apiVersion"].(string)
	kind, _ := obj["kind"].(string)
	metadata, _ := obj["metadata"].(map[string]any)
	name, _ := metadata["name"].(string)
	namespace, _ := metadata["namespace"].(string)
	if namespace == "" {
		namespace = defaultNamespace
	}

	switch {
	case apiVersion == "v1" && kind == "Namespace":
		return "/api/v1/namespaces/" + name, nil
	case apiVersion == "v1" && kind == "PersistentVolumeClaim":
		return "/api/v1/namespaces/" + namespace + "/persistentvolumeclaims/" + name, nil
	case apiVersion == "v1" && kind == "Secret":
		return "/api/v1/namespaces/" + namespace + "/secrets/" + name, nil
	case apiVersion == "v1" && kind == "Service":
		return "/api/v1/namespaces/" + namespace + "/services/" + name, nil
	case apiVersion == "v1" && kind == "Pod":
		return "/api/v1/namespaces/" + namespace + "/pods/" + name, nil
	case apiVersion == "apps/v1" && kind == "Deployment":
		return "/apis/apps/v1/namespaces/" + namespace + "/deployments/" + name, nil
	case apiVersion == KubernetesRBACAPIVersion && kind == "Role":
		return "/apis/rbac.authorization.k8s.io/v1/namespaces/" + namespace + "/roles/" + name, nil
	case apiVersion == KubernetesRBACAPIVersion && kind == "RoleBinding":
		return "/apis/rbac.authorization.k8s.io/v1/namespaces/" + namespace + "/rolebindings/" + name, nil
	case apiVersion == KubernetesNetworkPolicyAPIVersion && kind == "NetworkPolicy":
		return "/apis/networking.k8s.io/v1/namespaces/" + namespace + "/networkpolicies/" + name, nil
	case apiVersion == CloudNativePGAPIVersion && kind == CloudNativePGClusterKind:
		return "/apis/postgresql.cnpg.io/v1/namespaces/" + namespace + "/clusters/" + name, nil
	case apiVersion == VolSyncAPIVersion && kind == VolSyncReplicationDestinationKind:
		return "/apis/volsync.backube/v1alpha1/namespaces/" + namespace + "/replicationdestinations/" + name, nil
	case apiVersion == VolSyncAPIVersion && kind == VolSyncReplicationSourceKind:
		return "/apis/volsync.backube/v1alpha1/namespaces/" + namespace + "/replicationsources/" + name, nil
	case apiVersion == ManagedAppAPIVersion && kind == ManagedAppKind:
		return "/apis/" + ManagedAppAPIGroup + "/v1alpha1/namespaces/" + namespace + "/" + ManagedAppPlural + "/" + name, nil
	default:
		return "", fmt.Errorf("unsupported object %s %s", apiVersion, kind)
	}
}
