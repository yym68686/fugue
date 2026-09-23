package controller

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

// Reconcile durable database configuration before executable rollout gates.
// Recovering a database credential reference must not depend on a new image
// being deployable. No database/PVC/Deployment is created or replaced here.
func (s *Service) reconcileManagedPostgresCredentials(ctx context.Context, client *kubeClient, namespace string, app model.App) (model.App, error) {
	for index, service := range app.BackingServices {
		if service.Spec.Postgres == nil || service.Type != model.BackingServiceTypePostgres || (service.OwnerAppID != "" && service.OwnerAppID != app.ID) {
			continue
		}
		if service.Provisioner != model.BackingServiceProvisionerManaged {
			continue
		}
		postgres := *model.CloneAppPostgresSpec(service.Spec.Postgres)
		// Derive the exact normalized Cluster identity from the renderer.
		objects := s.Renderer.BuildManagedAppChildObjects(app, runtime.SchedulingConstraints{}, nil)
		var desired map[string]any
		for _, object := range objects {
			if cloudNativePGObject(object) && objectLabels(object)[runtime.FugueLabelBackingServiceID] == service.ID {
				desired = object
				break
			}
		}
		if desired == nil {
			continue
		}
		clusterName, _ := objectNameAndNamespace(namespace, desired)
		current, found, err := client.getRawObject(ctx, cloudNativePGClusterAPIPath(namespace, clusterName))
		if err != nil {
			return app, err
		}
		if found && !postgresCredentialOwnerMatches(current, desired) {
			return app, fmt.Errorf("resource_ownership_conflict: managed postgres Cluster %s/%s belongs to another backing service", namespace, clusterName)
		}
		name := postgres.CredentialSecretName
		if name == "" {
			name = runtime.ManagedPostgresCredentialSecretName(service.ID, app.ID, postgres)
			if found {
				legacy := ""
				for _, role := range cloudNativePGManagedRolesFromObject(current) {
					if managedRoleName(role) == postgres.User {
						legacy = credentialReferenceName(role)
						break
					}
				}
				if legacy == "" {
					bootstrap := normalizeKubeMap(normalizeKubeMap(current["spec"])["bootstrap"])
					legacy, _ = normalizeKubeMap(normalizeKubeMap(bootstrap["initdb"])["secret"])["name"].(string)
				}
				if legacy != "" {
					secret, exists, readErr := client.getRawObject(ctx, "/api/v1/namespaces/"+namespace+"/secrets/"+url.PathEscape(legacy))
					if readErr != nil {
						return app, readErr
					}
					if exists && postgresCredentialSecret(secret) {
						if err := client.protectLegacyPostgresCredential(ctx, namespace, legacy, secret); err != nil {
							return app, err
						}
					}
					refs, readErr := client.postgresCredentialReferences(ctx, namespace, legacy)
					if readErr != nil {
						return app, readErr
					}
					if exists && postgresCredentialOwnerMatches(secret, desired) && len(refs) == 1 && refs[0] == clusterName {
						name = legacy
					}
				}
			}
			updated, err := s.Store.AssignManagedPostgresCredentialSecret(service, name)
			if err != nil {
				return app, fmt.Errorf("persist postgres credential identity for service %s: %w", service.ID, err)
			}
			app.BackingServices[index] = updated
			postgres = *model.CloneAppPostgresSpec(updated.Spec.Postgres)
			if s.Logger != nil {
				s.Logger.Printf("managed postgres credential identity assigned service=%s cluster=%s secret=%s", service.ID, clusterName, name)
			}
		}
		if !found {
			continue
		}
		// Re-render using persisted intent. Only the credential Secret and role
		// references are reconciled; database/bootstrap settings and storage stay
		// unchanged apart from the application credential Secret reference.
		objects = s.Renderer.BuildManagedAppChildObjects(app, runtime.SchedulingConstraints{}, nil)
		var roleObjects []map[string]any
		for _, object := range objects {
			if objectLabels(object)[runtime.FugueLabelBackingServiceID] != service.ID {
				continue
			}
			if postgresCredentialSecret(object) {
				if err := client.applyPostgresCredentialSecret(ctx, object, nil); err != nil {
					return app, err
				}
			}
			if cloudNativePGObject(object) {
				roleObjects = append(roleObjects, object)
			}
		}
		if err := reconcileCloudNativePGManagedRoles(ctx, client, namespace, roleObjects); err != nil {
			return app, err
		}
	}
	return app, nil
}

// Keep credentials until their final runtime or recovery reference is removed.
func (c *kubeClient) deletePostgresCredentialSecret(ctx context.Context, namespace, name string, current map[string]any) error {
	refs, err := c.postgresCredentialReferences(ctx, namespace, name)
	if err != nil {
		return err
	}
	if len(refs) > 0 {
		c.writeStats.record("credential_delete_retained_referenced", current)
		return nil
	}
	metadata := normalizeKubeMap(current["metadata"])
	uid, _ := metadata["uid"].(string)
	rv, _ := metadata["resourceVersion"].(string)
	if strings.TrimSpace(uid) == "" || strings.TrimSpace(rv) == "" {
		return fmt.Errorf("refusing credential Secret deletion without UID/resourceVersion")
	}
	finalizers := credentialFinalizers(metadata, false)
	if !reflect.DeepEqual(normalizeKubeValue(metadata["finalizers"]), normalizeKubeValue(finalizers)) {
		body := map[string]any{"metadata": map[string]any{"uid": uid, "resourceVersion": rv, "finalizers": finalizers}}
		var updated map[string]any
		_, err := c.doRequest(ctx, "PATCH", "/api/v1/namespaces/"+c.effectiveNamespace(namespace)+"/secrets/"+url.PathEscape(name), "application/merge-patch+json", body, &updated)
		if err != nil {
			return fmt.Errorf("release credential Secret protection failed")
		}
		if next, _ := normalizeKubeMap(updated["metadata"])["resourceVersion"].(string); next != "" {
			rv = next
		}
	}
	body := map[string]any{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": map[string]any{"uid": uid, "resourceVersion": rv}}
	_, err = c.doRequest(ctx, "DELETE", "/api/v1/namespaces/"+c.effectiveNamespace(namespace)+"/secrets/"+url.PathEscape(name), "application/json", body, nil)
	return normalizeDeleteNotFound(err)
}

// A legacy collision may have moved its ownerReference to another app. Pin
// deletion before moving the role reference so Kubernetes GC cannot remove a
// Secret still used by a surviving Cluster.
func (c *kubeClient) protectLegacyPostgresCredential(ctx context.Context, namespace, name string, current map[string]any) error {
	metadata := normalizeKubeMap(current["metadata"])
	finalizers := credentialFinalizers(metadata, true)
	if reflect.DeepEqual(normalizeKubeValue(metadata["finalizers"]), finalizers) {
		return nil
	}
	if metadata["uid"] == nil || metadata["resourceVersion"] == nil {
		return fmt.Errorf("legacy credential Secret has no concurrency identity")
	}
	body := map[string]any{"metadata": map[string]any{"uid": metadata["uid"], "resourceVersion": metadata["resourceVersion"], "finalizers": finalizers}}
	status, err := c.doRequest(ctx, "PATCH", "/api/v1/namespaces/"+namespace+"/secrets/"+url.PathEscape(name), "application/merge-patch+json", body, nil)
	if err != nil {
		return fmt.Errorf("protect legacy credential Secret %s/%s failed (status=%d)", namespace, name, status)
	}
	return nil
}

func managedPostgresCredentialAcknowledged(deployment runtime.ManagedBackingServiceDeployment, cluster kubeCloudNativePGCluster) bool {
	if deployment.CredentialSecretName == "" {
		return true
	}
	for _, role := range cluster.Spec.Managed.Roles {
		if role.Name != deployment.CredentialUser {
			continue
		}
		if role.PasswordSecret.Name != deployment.CredentialSecretName {
			return false
		}
		version := cluster.Status.SecretsResourceVersion.ManagedRoleSecretVersion[deployment.CredentialSecretName]
		return version != "" && cluster.Status.ManagedRolesStatus.PasswordStatus[deployment.CredentialUser].ResourceVersion == version
	}
	return false
}

// A deleted ManagedApp can no longer retry cleanup. Finish GC-requested Secret
// deletion only after every Cluster reference has disappeared. Healthy Secret
// objects are never selected by this recovery pass.
func (c *kubeClient) reconcileTerminatingPostgresCredentialSecrets(ctx context.Context) error {
	var list struct {
		Items []map[string]any `json:"items"`
	}
	path := "/api/v1/secrets?labelSelector=" + url.QueryEscape(runtime.FugueLabelBackingServiceType+"=postgres")
	if _, err := c.doJSON(ctx, "GET", path, nil, &list); err != nil {
		return err
	}
	for _, secret := range list.Items {
		metadata := normalizeKubeMap(secret["metadata"])
		if metadata["deletionTimestamp"] == nil {
			continue
		}
		protected := false
		items, _ := normalizeKubeValue(metadata["finalizers"]).([]any)
		for _, item := range items {
			if item == postgresCredentialFinalizer {
				protected = true
			}
		}
		if !protected {
			continue
		}
		name, namespace := objectNameAndNamespace(c.namespace, secret)
		if err := c.deletePostgresCredentialSecret(ctx, namespace, name, secret); err != nil {
			return err
		}
	}
	return nil
}
