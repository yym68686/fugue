package controller

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"

	"fugue/internal/runtime"
)

const postgresCredentialFinalizer = "fugue.pro/postgres-credential-protection"

func credentialFinalizers(metadata map[string]any, add bool) []any {
	var result []any
	items, _ := normalizeKubeValue(metadata["finalizers"]).([]any)
	for _, item := range items {
		if item != postgresCredentialFinalizer {
			result = append(result, item)
		}
	}
	if add {
		result = append(result, postgresCredentialFinalizer)
	}
	return result
}

func postgresCredentialSecret(obj map[string]any) bool {
	return objectStringField(obj, "kind") == "Secret" && objectLabels(obj)[runtime.FugueLabelBackingServiceType] == "postgres"
}

func objectLabels(obj map[string]any) map[string]string {
	metadata := normalizeKubeMap(obj["metadata"])
	labels := normalizeKubeMap(metadata["labels"])
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		if value, ok := v.(string); ok {
			out[k] = value
		}
	}
	return out
}

func postgresCredentialOwnerMatches(current, desired map[string]any) bool {
	a, b := objectLabels(current), objectLabels(desired)
	identity := runtime.FugueLabelBackingServiceID
	if b[identity] == "" {
		identity = runtime.FugueLabelOwnerAppID
	}
	if b[identity] == "" || a[identity] != b[identity] {
		return false
	}
	for _, key := range []string{runtime.FugueLabelTenantID, runtime.FugueLabelProjectID, runtime.FugueLabelOwnerAppID} {
		if a[key] != b[key] {
			return false
		}
	}
	return true
}

func credentialSecretData(obj map[string]any) map[string]any {
	data := normalizeKubeMap(obj["data"])
	if data == nil {
		data = map[string]any{}
	}
	for key, raw := range normalizeKubeMap(obj["stringData"]) {
		if value, ok := raw.(string); ok {
			data[key] = base64.StdEncoding.EncodeToString([]byte(value))
		}
	}
	return data
}

func postgresCredentialConflict(namespace, name string) error {
	return fmt.Errorf("resource_ownership_conflict: refusing to overwrite managed postgres credential Secret %s/%s owned by another backing service", namespace, name)
}

// Secret writes use Create or an optimistic-concurrency Update. A common SSA
// field manager (even without force) is not a business ownership boundary.
func (c *kubeClient) applyPostgresCredentialSecret(ctx context.Context, desired map[string]any, out any) error {
	name, namespace := objectNameAndNamespace(c.namespace, desired)
	namespace = c.effectiveNamespace(namespace)
	collection := "/api/v1/namespaces/" + namespace + "/secrets"
	path := collection + "/" + url.PathEscape(name)
	for attempt := 0; attempt < 3; attempt++ {
		current, found, err := c.getRawObject(ctx, path)
		if err != nil {
			return err
		}
		body := cloneKubeMap(desired)
		metadata := normalizeKubeMap(body["metadata"])
		metadata["finalizers"] = credentialFinalizers(metadata, true)
		body["metadata"] = metadata
		body["data"] = credentialSecretData(desired)
		delete(body, "stringData")
		method, target := http.MethodPost, collection
		if found {
			if !postgresCredentialOwnerMatches(current, desired) {
				return postgresCredentialConflict(namespace, name)
			}
			cm, dm := normalizeKubeMap(current["metadata"]), normalizeKubeMap(desired["metadata"])
			if cm["deletionTimestamp"] != nil {
				return fmt.Errorf("managed postgres credential Secret %s/%s is terminating", namespace, name)
			}
			if cm["uid"] == nil || cm["resourceVersion"] == nil {
				return fmt.Errorf("credential Secret %s/%s has no concurrency identity", namespace, name)
			}
			metadata := cloneKubeMap(cm)
			metadata["finalizers"] = credentialFinalizers(metadata, true)
			for _, key := range []string{"labels", "annotations"} {
				merged := normalizeKubeMap(metadata[key])
				if merged == nil {
					merged = map[string]any{}
				}
				for k, v := range normalizeKubeMap(dm[key]) {
					merged[k] = v
				}
				if len(merged) > 0 {
					metadata[key] = merged
				}
			}
			if refs, present := dm["ownerReferences"]; present {
				metadata["ownerReferences"] = normalizeKubeValue(refs)
			}
			if reflect.DeepEqual(credentialSecretData(current), body["data"]) && reflect.DeepEqual(metadata, cm) && current["type"] == body["type"] {
				c.writeStats.record("credential_unchanged", desired)
				if out != nil {
					encoded, _ := json.Marshal(current)
					return json.Unmarshal(encoded, out)
				}
				return nil
			}
			body = cloneKubeMap(current)
			body["metadata"], body["data"], body["type"] = metadata, credentialSecretData(desired), desired["type"]
			delete(body, "stringData")
			method, target = http.MethodPut, path
		}
		status, err := c.doRequest(ctx, method, target, "application/json", body, out)
		if status == http.StatusConflict {
			continue
		}
		if err != nil {
			return fmt.Errorf("write managed postgres credential Secret %s/%s failed (status=%d)", namespace, name, status)
		}
		c.writeStats.record("credential_written", desired)
		return nil
	}
	return fmt.Errorf("managed postgres credential Secret %s/%s changed concurrently; retry reconciliation", namespace, name)
}

func (c *kubeClient) postgresCredentialReferences(ctx context.Context, namespace, secretName string) ([]string, error) {
	var list struct {
		Items []map[string]any `json:"items"`
	}
	_, err := c.doJSON(ctx, http.MethodGet, "/apis/postgresql.cnpg.io/v1/namespaces/"+c.effectiveNamespace(namespace)+"/clusters", nil, &list)
	if err != nil {
		return nil, err
	}
	var refs []string
	for _, cluster := range list.Items {
		found := false
		spec := normalizeKubeMap(cluster["spec"])
		bootstrap := normalizeKubeMap(spec["bootstrap"])
		for _, mode := range []string{"initdb", "recovery", "pg_basebackup"} {
			secret := normalizeKubeMap(normalizeKubeMap(bootstrap[mode])["secret"])
			if secret["name"] == secretName {
				found = true
			}
		}
		for _, role := range cloudNativePGManagedRolesFromObject(cluster) {
			if normalizeKubeMap(role["passwordSecret"])["name"] == secretName {
				found = true
			}
		}
		// A changed spec is not an acknowledgement. Keep the previous
		// credential until CNPG no longer reports its managed Secret version.
		status := normalizeKubeMap(cluster["status"])
		versions := normalizeKubeMap(normalizeKubeMap(status["secretsResourceVersion"])["managedRoleSecretVersion"])
		if _, reported := versions[secretName]; reported {
			found = true
		}
		if found {
			name, _ := objectNameAndNamespace(namespace, cluster)
			refs = append(refs, name)
		}
	}
	return refs, nil
}

func preservePostgresBootstrap(current, desired map[string]any) {
	if !cloudNativePGObject(desired) {
		return
	}
	ds := normalizeKubeMap(desired["spec"])
	if bootstrap, _ := postgresBootstrapWithCredential(current, cloudNativePGManagedRolesFromObject(desired)); bootstrap != nil {
		ds["bootstrap"] = bootstrap
		desired["spec"] = ds
	}
}

// CNPG continuously reconciles the application owner from bootstrap.secret,
// as well as managed.roles. Both references must move together. Preserve the
// bootstrap method, database, owner, recovery source and every other setting.
func postgresBootstrapWithCredential(current map[string]any, roles []map[string]any) (map[string]any, bool) {
	bootstrap := cloneKubeMap(normalizeKubeMap(normalizeKubeMap(current["spec"])["bootstrap"]))
	if bootstrap == nil {
		return nil, false
	}
	for _, mode := range []string{"initdb", "recovery", "pg_basebackup"} {
		settings := normalizeKubeMap(bootstrap[mode])
		if settings == nil {
			continue
		}
		owner, _ := settings["owner"].(string)
		if owner == "" {
			owner, _ = settings["database"].(string)
		}
		for _, role := range roles {
			name := credentialReferenceName(role)
			if owner == "" || managedRoleName(role) != owner || name == "" {
				continue
			}
			secret := normalizeKubeMap(settings["secret"])
			if secret["name"] == name {
				return bootstrap, false
			}
			settings["secret"] = map[string]any{"name": name}
			bootstrap[mode] = settings
			return bootstrap, true
		}
	}
	return bootstrap, false
}

func (c *kubeClient) preflightPostgresCredentialSecrets(ctx context.Context, objects []map[string]any) error {
	for _, obj := range objects {
		if !postgresCredentialSecret(obj) {
			continue
		}
		name, namespace := objectNameAndNamespace(c.namespace, obj)
		current, found, err := c.getRawObject(ctx, "/api/v1/namespaces/"+c.effectiveNamespace(namespace)+"/secrets/"+url.PathEscape(name))
		if err != nil {
			return err
		}
		if found && !postgresCredentialOwnerMatches(current, obj) {
			return postgresCredentialConflict(namespace, name)
		}
	}
	return nil
}

// Avoid leaking raw Secret API responses into operator logs.
func credentialReferenceName(role map[string]any) string {
	value, _ := normalizeKubeMap(role["passwordSecret"])["name"].(string)
	return strings.TrimSpace(value)
}
