package edgecontrol

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"fugue/internal/declarativerelease"
)

const inventoryReceiverReceiptPath = "component/inventory-receiver-receipt.json"

type inventoryReceiverCandidateReceipt struct {
	Schema                               string                           `json:"schema"`
	BaseCommit                           string                           `json:"base_commit"`
	ProductionMutationAllowed            bool                             `json:"production_mutation_allowed"`
	AuthorityCutoverAllowedWithoutWorker bool                             `json:"authority_cutover_allowed_without_worker_candidate"`
	Artifact                             inventoryReceiverArtifactReceipt `json:"artifact"`
	GroupRegistry                        string                           `json:"group_registry"`
	InventoryContract                    inventoryReceiverContractReceipt `json:"inventory_contract"`
	FaultDomain                          inventoryReceiverFaultReceipt    `json:"fault_domain"`
	Rollout                              inventoryReceiverRolloutReceipt  `json:"rollout"`
	Rollback                             inventoryReceiverRollbackReceipt `json:"rollback"`
}

type inventoryReceiverArtifactReceipt struct {
	Role         string `json:"role"`
	Repository   string `json:"repository"`
	Dockerfile   string `json:"dockerfile"`
	BuildPackage string `json:"build_package"`
}

type inventoryReceiverContractReceipt struct {
	Path                       string `json:"path"`
	Method                     string `json:"method"`
	Authentication             string `json:"authentication"`
	QueryCredentialsAllowed    bool   `json:"query_credentials_allowed"`
	Component                  string `json:"component"`
	ArtifactKind               string `json:"artifact_kind"`
	Scope                      string `json:"scope"`
	Node                       string `json:"node"`
	Cursor                     string `json:"cursor"`
	LegacyWorkerAbsenceAllowed bool   `json:"legacy_worker_absence_allowed"`
}

type inventoryReceiverFaultReceipt struct {
	State                 string `json:"state"`
	Signature             string `json:"signature"`
	Publication           string `json:"publication"`
	CrossGroupTransaction bool   `json:"cross_group_transaction"`
	StaleGroupBehavior    string `json:"stale_group_behavior"`
}

type inventoryReceiverRolloutReceipt struct {
	ArtifactBatches     int    `json:"artifact_batches"`
	GroupSource         string `json:"group_source"`
	GroupsAreSequential bool   `json:"groups_are_sequential"`
}

type inventoryReceiverRollbackReceipt struct {
	Artifact                  string `json:"artifact"`
	State                     string `json:"state"`
	AutomaticCrossGroupAction bool   `json:"automatic_cross_group_action"`
}

type inventoryReceiverResourceSet struct {
	APIVersion string                            `json:"apiVersion"`
	Kind       string                            `json:"kind"`
	Items      []inventoryReceiverResourceObject `json:"items"`
}

type inventoryReceiverResourceObject struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name      string            `json:"name"`
		Namespace string            `json:"namespace"`
		Labels    map[string]string `json:"labels"`
	} `json:"metadata"`
	Spec json.RawMessage `json:"spec"`
}

type inventoryReceiverDeploymentSpec struct {
	Replicas int `json:"replicas"`
	Strategy struct {
		Type string `json:"type"`
	} `json:"strategy"`
	Template struct {
		Metadata struct {
			Labels      map[string]string `json:"labels"`
			Annotations map[string]string `json:"annotations"`
		} `json:"metadata"`
		Spec struct {
			AutomountServiceAccountToken bool              `json:"automountServiceAccountToken"`
			ServiceAccountName           string            `json:"serviceAccountName"`
			NodeSelector                 map[string]string `json:"nodeSelector"`
			Tolerations                  []struct {
				Effect   string `json:"effect"`
				Key      string `json:"key"`
				Operator string `json:"operator"`
				Value    string `json:"value,omitempty"`
			} `json:"tolerations"`
			InitContainers []struct {
				Name            string   `json:"name"`
				Image           string   `json:"image"`
				Command         []string `json:"command"`
				SecurityContext struct {
					AllowPrivilegeEscalation bool `json:"allowPrivilegeEscalation"`
					ReadOnlyRootFilesystem   bool `json:"readOnlyRootFilesystem"`
					RunAsNonRoot             bool `json:"runAsNonRoot"`
					RunAsUser                int  `json:"runAsUser"`
					RunAsGroup               int  `json:"runAsGroup"`
					Capabilities             struct {
						Add  []string `json:"add"`
						Drop []string `json:"drop"`
					} `json:"capabilities"`
				} `json:"securityContext"`
				VolumeMounts []struct {
					Name      string `json:"name"`
					MountPath string `json:"mountPath"`
				} `json:"volumeMounts"`
			} `json:"initContainers"`
			Containers []struct {
				Name  string `json:"name"`
				Image string `json:"image"`
				Env   []struct {
					Name  string `json:"name"`
					Value string `json:"value"`
				} `json:"env"`
				VolumeMounts []struct {
					Name      string `json:"name"`
					MountPath string `json:"mountPath"`
					ReadOnly  bool   `json:"readOnly"`
				} `json:"volumeMounts"`
			} `json:"containers"`
			Volumes []struct {
				Name                  string `json:"name"`
				PersistentVolumeClaim *struct {
					ClaimName string `json:"claimName"`
				} `json:"persistentVolumeClaim,omitempty"`
				Secret *struct {
					SecretName  string `json:"secretName"`
					DefaultMode int    `json:"defaultMode"`
				} `json:"secret,omitempty"`
			} `json:"volumes"`
		} `json:"spec"`
	} `json:"template"`
}

func TestInventoryReceiverCandidateSelectsOneControlArtifactAndPhysicallyIsolatesGroups(t *testing.T) {
	t.Parallel()

	receiptRaw, err := os.ReadFile(inventoryReceiverReceiptPath)
	if err != nil {
		t.Fatal(err)
	}
	var receipt inventoryReceiverCandidateReceipt
	if err := decodeStrictInventoryReceiverJSON(receiptRaw, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Schema != "edge-control-inventory-receiver-candidate/v1" || receipt.BaseCommit != "b9b75596bb84ba701c843b1bafa961cadc74d223" ||
		!receipt.ProductionMutationAllowed || receipt.AuthorityCutoverAllowedWithoutWorker ||
		receipt.Artifact.Role != "fugue-artifact://edge-control" || receipt.Artifact.Repository != "ghcr.io/yym68686/fugue-edge-control" ||
		receipt.Artifact.Dockerfile != "Dockerfile.edge-control" || receipt.Artifact.BuildPackage != "./cmd/fugue-edge-control" ||
		receipt.InventoryContract.Path != GroupAuthorityInventoryHeartbeatPathV1 || receipt.InventoryContract.Method != "POST" ||
		receipt.InventoryContract.Authentication != "platform component Bearer identity" || receipt.InventoryContract.QueryCredentialsAllowed ||
		receipt.InventoryContract.Component != "edge-worker" || receipt.InventoryContract.ArtifactKind != "edge_route_bundle" ||
		!receipt.InventoryContract.LegacyWorkerAbsenceAllowed || receipt.FaultDomain.CrossGroupTransaction ||
		receipt.FaultDomain.StaleGroupBehavior != "preserve only that group last-known-good publication" ||
		receipt.GroupRegistry != "deploy/releases/edge-groups.json" || receipt.Rollout.ArtifactBatches != 1 ||
		receipt.Rollout.GroupSource != "edge_group_registry" || !receipt.Rollout.GroupsAreSequential ||
		receipt.Rollback.AutomaticCrossGroupAction {
		t.Fatalf("unsafe inventory receiver receipt: %+v", receipt)
	}

	registryFile, err := os.Open(filepath.Join("../..", receipt.GroupRegistry))
	if err != nil {
		t.Fatal(err)
	}
	registry, err := declarativerelease.DecodeEdgeGroupRegistry(registryFile)
	_ = registryFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	for _, group := range registry.Groups {
		raw, err := os.ReadFile(filepath.Join("../..", group.Control.ManifestPath))
		if err != nil {
			if os.IsNotExist(err) && group.Control.MigrationState == "pending" {
				continue
			}
			t.Fatal(err)
		}
		raw, err = declarativerelease.MaterializeManifestTemplate(raw, group.Control.ManifestVariables)
		if err != nil {
			t.Fatalf("materialize %s resources: %v", group.Control.ID, err)
		}
		for _, other := range registry.Groups {
			if other.ID == group.ID {
				continue
			}
			if bytes.Contains(raw, []byte(other.GroupID)) || bytes.Contains(raw, []byte(other.Control.Workload.Name)) {
				t.Fatalf("%s resource set crosses into %s", group.Control.ID, other.Control.ID)
			}
		}
		validateInventoryReceiverResourceSet(t, raw, group.GroupID, group.ID)
	}
}

func validateInventoryReceiverResourceSet(t *testing.T, raw []byte, groupID, country string) {
	t.Helper()
	if bytes.Contains(raw, []byte("fugue-artifact://edge-worker")) || bytes.Contains(raw, []byte("registry.invalid")) ||
		bytes.Contains(raw, []byte("@@")) {
		t.Fatalf("%s resource set crosses its control artifact/group boundary", groupID)
	}
	var set inventoryReceiverResourceSet
	if err := json.Unmarshal(raw, &set); err != nil {
		t.Fatal(err)
	}
	if set.APIVersion != "release.fugue.dev/v2" || set.Kind != "ComponentResourceSet" || len(set.Items) != 6 {
		t.Fatalf("%s resource set header/items=%+v", groupID, set)
	}
	expectedNames := map[string]bool{
		"Deployment/edge-control-" + country:                       false,
		"NetworkPolicy/edge-control-" + country:                    false,
		"PodDisruptionBudget/edge-control-" + country:              false,
		"PersistentVolumeClaim/edge-control-" + country + "-state": false,
		"Service/edge-control-" + country:                          false,
		"ServiceAccount/edge-control-" + country:                   false,
	}
	for _, object := range set.Items {
		identity := object.Kind + "/" + object.Metadata.Name
		if _, exists := expectedNames[identity]; !exists || expectedNames[identity] || object.Metadata.Namespace != "fugue-system" {
			t.Fatalf("%s unexpected/duplicate object %s", groupID, identity)
		}
		expectedNames[identity] = true
		if object.Kind != "Deployment" {
			continue
		}
		var deployment inventoryReceiverDeploymentSpec
		if err := json.Unmarshal(object.Spec, &deployment); err != nil {
			t.Fatal(err)
		}
		if deployment.Replicas != 1 || deployment.Strategy.Type != "Recreate" || deployment.Template.Spec.AutomountServiceAccountToken ||
			deployment.Template.Spec.ServiceAccountName != "edge-control-"+country ||
			deployment.Template.Spec.NodeSelector["fugue.io/location-country-code"] != country ||
			deployment.Template.Metadata.Labels["fugue.io/edge-group-id"] != groupID ||
			deployment.Template.Metadata.Annotations["fugue.pro/edge-control-authority"] != "group-scoped" ||
			deployment.Template.Metadata.Annotations["fugue.pro/edge-control-publication"] != "group-cas" || len(deployment.Template.Spec.Containers) != 1 ||
			len(deployment.Template.Spec.Tolerations) != 1 || deployment.Template.Spec.Tolerations[0].Key != "fugue.io/tenant" ||
			deployment.Template.Spec.Tolerations[0].Operator != "Exists" || deployment.Template.Spec.Tolerations[0].Effect != "NoSchedule" ||
			deployment.Template.Spec.Tolerations[0].Value != "" {
			t.Fatalf("%s deployment is not a group-local authority: %+v", groupID, deployment)
		}
		if len(deployment.Template.Spec.InitContainers) != 1 {
			t.Fatalf("%s state permission initializer is missing", groupID)
		}
		initializer := deployment.Template.Spec.InitContainers[0]
		if initializer.Name != "state-permissions" || initializer.Image != "fugue-artifact://edge-control" ||
			len(initializer.Command) != 3 || initializer.Command[0] != "/bin/sh" || initializer.Command[1] != "-ceu" ||
			initializer.Command[2] != "chmod 0770 /var/lib/fugue-edge-control && chown 65532:65532 /var/lib/fugue-edge-control" ||
			initializer.SecurityContext.AllowPrivilegeEscalation || !initializer.SecurityContext.ReadOnlyRootFilesystem ||
			initializer.SecurityContext.RunAsNonRoot || initializer.SecurityContext.RunAsUser != 0 || initializer.SecurityContext.RunAsGroup != 0 ||
			len(initializer.SecurityContext.Capabilities.Drop) != 1 || initializer.SecurityContext.Capabilities.Drop[0] != "ALL" ||
			len(initializer.SecurityContext.Capabilities.Add) != 2 || initializer.SecurityContext.Capabilities.Add[0] != "CHOWN" || initializer.SecurityContext.Capabilities.Add[1] != "FOWNER" ||
			len(initializer.VolumeMounts) != 1 || initializer.VolumeMounts[0].Name != "authority-state" || initializer.VolumeMounts[0].MountPath != "/var/lib/fugue-edge-control" {
			t.Fatalf("%s state permission initializer drifted: %+v", groupID, initializer)
		}
		container := deployment.Template.Spec.Containers[0]
		if container.Name != "edge-control" || container.Image != "fugue-artifact://edge-control" {
			t.Fatalf("%s deployment artifact drift: %+v", groupID, container)
		}
		environment := make(map[string]string, len(container.Env))
		for _, variable := range container.Env {
			environment[variable.Name] = variable.Value
		}
		if environment["FUGUE_EDGE_CONTROL_AUTHORITY_RUNTIME_ENABLED"] != "true" || environment["FUGUE_EDGE_CONTROL_AUTHORITY_GROUP_IDS"] != groupID ||
			environment["FUGUE_EDGE_CONTROL_AUTHORITY_STATE_DIR"] != "/var/lib/fugue-edge-control" ||
			environment["FUGUE_EDGE_CONTROL_INVENTORY_WRITER_KEYRING_DIR"] != "/var/run/secrets/fugue-edge-control/inventory-writer" {
			t.Fatalf("%s authority environment drift: %+v", groupID, environment)
		}
		stateBound := false
		writerBound := false
		for _, volume := range deployment.Template.Spec.Volumes {
			if volume.Name == "authority-state" && volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == "edge-control-"+country+"-state" {
				stateBound = true
			}
			if volume.Name == "inventory-writer-keyring" && volume.Secret != nil &&
				volume.Secret.SecretName == "fugue-edge-control-inventory-writer-"+country && volume.Secret.DefaultMode == 384 {
				writerBound = true
			}
		}
		if !stateBound || !writerBound {
			t.Fatalf("%s authority state/identity projection is not group-local", groupID)
		}
	}
	for identity, found := range expectedNames {
		if !found {
			t.Fatalf("%s resource set is missing %s", groupID, identity)
		}
	}
}

func decodeStrictInventoryReceiverJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("inventory receiver receipt contains trailing data")
	}
	return nil
}
