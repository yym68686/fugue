package releaseguardian

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"fugue/internal/declarativerelease"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const registryRehomeRBACVersion = "rbac.authorization.k8s.io/v1"

type RegistryRehomeObservedCompanion struct {
	Live            RegistryRehomeObservedObject `json:"live"`
	ResourceVersion string                       `json:"resourceVersion"`
	Declaration     RegistryRehomeCompanion      `json:"declaration"`
}

func (review RegistryRehomeReview) requireExecutableCompanions() error {
	if review.Draft.Schema == registryRehomeDraftV2 || len(review.Draft.RBAC) != 0 || len(review.Source.RBAC) != 0 {
		return errors.New("registry rehome with RBAC companions is shadow-only until their provider write boundary is enrolled")
	}
	return nil
}

func registryRehomeServiceIdentity(id declarativerelease.ResourceIdentity, namespace string) bool {
	return id.APIVersion == "v1" && id.Kind == "Service" && id.Namespace == namespace && componentPattern.MatchString(id.Name)
}

func registryRehomeRBACIdentity(id declarativerelease.ResourceIdentity) bool {
	return id.APIVersion == registryRehomeRBACVersion && id.Namespace == "" && componentPattern.MatchString(id.Name) &&
		(id.Kind == "ClusterRole" || id.Kind == "ClusterRoleBinding")
}

func registryRehomeRBACOrder(id declarativerelease.ResourceIdentity) string {
	return id.Kind + "\x00" + id.Name
}

// The resource set's subset comparison alone is not sufficient for RBAC:
// an extra verb, rule, or subject can change the effective authorization.
// An aggregated role cannot be verified as static by these object reads.
func registryRehomeRBACAccess(id declarativerelease.ResourceIdentity, object map[string]any) ([]byte, error) {
	switch id.Kind {
	case "ClusterRole":
		rules, ok := object["rules"].([]any)
		if _, aggregated := object["aggregationRule"]; aggregated || !ok || len(rules) == 0 {
			return nil, errors.New("registry rehome requires explicit static ClusterRole rules")
		}
		return declarativerelease.CanonicalJSON(rules)
	case "ClusterRoleBinding":
		ref, refOK := object["roleRef"].(map[string]any)
		subjects, subjectsOK := object["subjects"].([]any)
		if !refOK || !subjectsOK || len(subjects) == 0 {
			return nil, errors.New("registry rehome requires explicit ClusterRoleBinding roleRef and subjects")
		}
		return declarativerelease.CanonicalJSON(map[string]any{"roleRef": ref, "subjects": subjects})
	default:
		return nil, errors.New("unsupported registry rehome RBAC companion")
	}
}

// readRegistryRehomeRBAC only gets the named sealed object. It is not an
// RBAC writer or proof that every authorization input in the cluster is held.
func readRegistryRehomeRBAC(ctx context.Context, client kubernetes.Interface, id declarativerelease.ResourceIdentity,
	wanted map[string]any, declaration RegistryRehomeCompanion) (RegistryRehomeObservedCompanion, error) {
	var none RegistryRehomeObservedCompanion
	if !registryRehomeRBACIdentity(id) {
		return none, errors.New("registry rehome RBAC companion is outside its approved scope")
	}
	rbac := client.RbacV1()
	var meta metav1.Object
	var value any
	var err error
	if id.Kind == "ClusterRole" {
		obj, getErr := rbac.ClusterRoles().Get(ctx, id.Name, metav1.GetOptions{})
		meta, value, err = obj, obj, getErr
	} else {
		obj, getErr := rbac.ClusterRoleBindings().Get(ctx, id.Name, metav1.GetOptions{})
		meta, value, err = obj, obj, getErr
	}
	if err != nil {
		return none, fmt.Errorf("read companion %s %s: %w", id.Kind, id.Name, err)
	}
	if meta.GetDeletionTimestamp() != nil || meta.GetName() != id.Name || meta.GetNamespace() != "" || meta.GetUID() == "" ||
		meta.GetResourceVersion() == "" || meta.GetGeneration() < 0 {
		return none, errors.New("registry rehome RBAC companion has no stable provider identity")
	}
	observed, err := rehomeJSONMap(value)
	if err != nil {
		return none, err
	}
	observed["apiVersion"], observed["kind"] = id.APIVersion, id.Kind
	access, err := registryRehomeRBACAccess(id, observed)
	if err != nil {
		return none, err
	}
	sealedAccess, err := registryRehomeRBACAccess(id, wanted)
	if err != nil || !bytes.Equal(sealedAccess, access) {
		return none, errors.New("companion RBAC access differs from its sealed declaration")
	}
	if mismatch := declarativerelease.ResourceDesiredMismatch(wanted, observed); mismatch != "" {
		return none, fmt.Errorf("companion RBAC differs from its sealed declaration at %s", mismatch)
	}
	// Count changes to access even if this API does not advance generation.
	return RegistryRehomeObservedCompanion{
		Live:            RegistryRehomeObservedObject{id, string(meta.GetUID()), meta.GetGeneration(), digest(access)},
		ResourceVersion: meta.GetResourceVersion(), Declaration: declaration,
	}, nil
}
