package releasedomain

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
)

const (
	PublicDataPlaneSecretLookupWitnessKind = "PublicDataPlaneSecretLookupWitness"
	PublicDataPlaneSecretRenderWitnessKind = "PublicDataPlaneSecretRenderWitness"
	secretRedactedDataValue                = "UkVEQUNURUQ="
	secretRedactedStringDataValue          = "[secret-redacted]"
)

type PublicDataPlaneSecretWitnessMember struct {
	Name                  string   `json:"name"`
	Type                  string   `json:"type"`
	UIDDigest             string   `json:"uidDigest,omitempty"`
	ResourceVersionDigest string   `json:"resourceVersionDigest,omitempty"`
	PayloadKeys           []string `json:"payloadKeys"`
}

type PublicDataPlaneSecretLookupWitness struct {
	APIVersion       string                               `json:"apiVersion"`
	Kind             string                               `json:"kind"`
	ReleaseName      string                               `json:"releaseName"`
	ReleaseNamespace string                               `json:"releaseNamespace"`
	Members          []PublicDataPlaneSecretWitnessMember `json:"members"`
	Digest           string                               `json:"digest"`
}

type PublicDataPlaneSecretRenderWitness struct {
	APIVersion       string                               `json:"apiVersion"`
	Kind             string                               `json:"kind"`
	ReleaseNamespace string                               `json:"releaseNamespace"`
	Members          []PublicDataPlaneSecretWitnessMember `json:"members"`
	PayloadHMAC      string                               `json:"payloadHmac"`
	Digest           string                               `json:"digest"`
}

type PublicDataPlaneSecretLookupNames struct {
	Config           string
	ControlPlaneDB   string
	PlatformIdentity string
}

func BuildPublicDataPlaneSecretLookupWitness(
	raw []byte, releaseName, namespace string, names PublicDataPlaneSecretLookupNames,
) (PublicDataPlaneSecretLookupWitness, error) {
	if releaseName == "" || namespace == "" || names.Config == "" || names.ControlPlaneDB == "" || names.PlatformIdentity == "" {
		return PublicDataPlaneSecretLookupWitness{}, fmt.Errorf("secret lookup identity is incomplete")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var document map[string]any
	if err := decoder.Decode(&document); err != nil {
		return PublicDataPlaneSecretLookupWitness{}, fmt.Errorf("secret lookup snapshot is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return PublicDataPlaneSecretLookupWitness{}, fmt.Errorf("secret lookup snapshot is invalid")
	}
	if document["apiVersion"] != "v1" || document["kind"] != "List" {
		return PublicDataPlaneSecretLookupWitness{}, fmt.Errorf("secret lookup snapshot is not a Kubernetes List")
	}
	items, ok := document["items"].([]any)
	if !ok || len(items) != 3 {
		return PublicDataPlaneSecretLookupWitness{}, fmt.Errorf("secret lookup snapshot must contain exactly three Secrets")
	}
	expected := map[string]struct {
		typeName string
		required []string
	}{
		names.Config: {
			typeName: "Opaque",
			required: []string{"FUGUE_WORKLOAD_IDENTITY_SIGNING_KEY", "FUGUE_BUNDLE_SIGNING_KEY", "FUGUE_EDGE_TLS_ASK_TOKEN"},
		},
		names.ControlPlaneDB: {typeName: "kubernetes.io/basic-auth", required: []string{"password"}},
		names.PlatformIdentity: {
			typeName: "Opaque",
			required: []string{"FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY", "FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY_ID"},
		},
	}
	members := make([]PublicDataPlaneSecretWitnessMember, 0, len(items))
	seen := map[string]struct{}{}
	for _, rawItem := range items {
		item, ok := rawItem.(map[string]any)
		if !ok || item["apiVersion"] != "v1" || item["kind"] != "Secret" {
			return PublicDataPlaneSecretLookupWitness{}, fmt.Errorf("secret lookup snapshot contains a non-Secret object")
		}
		metadata, ok := item["metadata"].(map[string]any)
		if !ok {
			return PublicDataPlaneSecretLookupWitness{}, fmt.Errorf("secret lookup metadata is invalid")
		}
		name, _ := metadata["name"].(string)
		requirement, expectedName := expected[name]
		if !expectedName {
			return PublicDataPlaneSecretLookupWitness{}, fmt.Errorf("secret lookup snapshot contains an unexpected Secret")
		}
		if _, duplicate := seen[name]; duplicate {
			return PublicDataPlaneSecretLookupWitness{}, fmt.Errorf("secret lookup snapshot contains a duplicate Secret")
		}
		seen[name] = struct{}{}
		objectNamespace, _ := metadata["namespace"].(string)
		uid, _ := metadata["uid"].(string)
		resourceVersion, _ := metadata["resourceVersion"].(string)
		if objectNamespace != namespace || uid == "" || resourceVersion == "" || metadata["deletionTimestamp"] != nil {
			return PublicDataPlaneSecretLookupWitness{}, fmt.Errorf("secret lookup metadata is not live and exact")
		}
		labels, _ := metadata["labels"].(map[string]any)
		annotations, _ := metadata["annotations"].(map[string]any)
		if labels["app.kubernetes.io/instance"] != releaseName || labels["app.kubernetes.io/managed-by"] != "Helm" ||
			annotations["meta.helm.sh/release-name"] != releaseName || annotations["meta.helm.sh/release-namespace"] != namespace {
			return PublicDataPlaneSecretLookupWitness{}, fmt.Errorf("secret lookup object is foreign to the Helm release")
		}
		typeName, _ := item["type"].(string)
		if typeName != requirement.typeName {
			return PublicDataPlaneSecretLookupWitness{}, fmt.Errorf("secret lookup object type is invalid")
		}
		data, ok := item["data"].(map[string]any)
		if !ok || len(data) == 0 {
			return PublicDataPlaneSecretLookupWitness{}, fmt.Errorf("secret lookup payload is unavailable")
		}
		keys := make([]string, 0, len(data))
		for key, encoded := range data {
			value, ok := encoded.(string)
			if !ok {
				return PublicDataPlaneSecretLookupWitness{}, fmt.Errorf("secret lookup payload encoding is invalid")
			}
			if _, err := base64.StdEncoding.DecodeString(value); err != nil {
				return PublicDataPlaneSecretLookupWitness{}, fmt.Errorf("secret lookup payload encoding is invalid")
			}
			keys = append(keys, key)
		}
		for _, required := range requirement.required {
			value, ok := data[required].(string)
			decoded, err := base64.StdEncoding.DecodeString(value)
			if !ok || err != nil || len(decoded) == 0 {
				return PublicDataPlaneSecretLookupWitness{}, fmt.Errorf("secret lookup required payload is unavailable")
			}
		}
		sort.Strings(keys)
		members = append(members, PublicDataPlaneSecretWitnessMember{
			Name: name, Type: typeName, UIDDigest: digestPublicDataPlaneText(uid),
			ResourceVersionDigest: digestPublicDataPlaneText(resourceVersion), PayloadKeys: keys,
		})
	}
	sort.Slice(members, func(i, j int) bool { return members[i].Name < members[j].Name })
	witness := PublicDataPlaneSecretLookupWitness{
		APIVersion: PublicDataPlaneAdoptionAPIVersion, Kind: PublicDataPlaneSecretLookupWitnessKind,
		ReleaseName: releaseName, ReleaseNamespace: namespace, Members: members,
	}
	witness.Digest = publicDataPlaneSecretLookupWitnessDigest(witness)
	if err := VerifyPublicDataPlaneSecretLookupWitness(witness); err != nil {
		return PublicDataPlaneSecretLookupWitness{}, err
	}
	return witness, nil
}

func VerifyPublicDataPlaneSecretLookupWitness(witness PublicDataPlaneSecretLookupWitness) error {
	if witness.APIVersion != PublicDataPlaneAdoptionAPIVersion || witness.Kind != PublicDataPlaneSecretLookupWitnessKind ||
		witness.ReleaseName == "" || witness.ReleaseNamespace == "" || len(witness.Members) != 3 {
		return fmt.Errorf("secret lookup witness identity is invalid")
	}
	if err := verifyPublicDataPlaneSecretMembers(witness.Members, true); err != nil {
		return err
	}
	if witness.Digest != publicDataPlaneSecretLookupWitnessDigest(witness) {
		return fmt.Errorf("secret lookup witness digest mismatch")
	}
	return nil
}

func DecodePublicDataPlaneSecretLookupWitness(raw []byte) (PublicDataPlaneSecretLookupWitness, error) {
	var witness PublicDataPlaneSecretLookupWitness
	if err := decodeCanonicalPublicDataPlaneSecretWitness(raw, &witness); err != nil {
		return PublicDataPlaneSecretLookupWitness{}, err
	}
	if err := VerifyPublicDataPlaneSecretLookupWitness(witness); err != nil {
		return PublicDataPlaneSecretLookupWitness{}, err
	}
	return witness, nil
}

func CanonicalizePublicDataPlaneSecretFreeManifest(
	rendered []byte, spec *OwnershipSpec, namespace string, hmacKey []byte,
) ([]byte, PublicDataPlaneSecretRenderWitness, error) {
	objects, unknown := decodeManifest(rendered, spec, namespace, "secret-free-render")
	if len(unknown) != 0 {
		return nil, PublicDataPlaneSecretRenderWitness{}, manifestEvidenceError(unknown)
	}
	indexed, duplicates := indexManifestObjects(objects, "secret-free-render")
	if len(duplicates) != 0 {
		return nil, PublicDataPlaneSecretRenderWitness{}, manifestEvidenceError(duplicates)
	}
	type secretPayload struct {
		Name       string            `json:"name"`
		Namespace  string            `json:"namespace"`
		Type       string            `json:"type"`
		Data       map[string]string `json:"data,omitempty"`
		StringData map[string]string `json:"stringData,omitempty"`
	}
	payloads := make([]secretPayload, 0)
	members := make([]PublicDataPlaneSecretWitnessMember, 0)
	for key, object := range indexed {
		if object.Identity.APIGroup != "" || object.Identity.Version != "v1" || object.Identity.Kind != "Secret" {
			continue
		}
		clone := cloneManifestMap(object.Object)
		typeName, _ := clone["type"].(string)
		if typeName == "" {
			typeName = "Opaque"
		}
		payload := secretPayload{Name: object.Identity.Name, Namespace: object.Identity.Namespace, Type: typeName}
		keys := make([]string, 0)
		for _, field := range []string{"data", "stringData"} {
			values, exists := clone[field]
			if !exists {
				continue
			}
			mapping, ok := values.(map[string]any)
			if !ok {
				return nil, PublicDataPlaneSecretRenderWitness{}, fmt.Errorf("rendered Secret payload shape is invalid")
			}
			secretValues := make(map[string]string, len(mapping))
			for payloadKey, rawValue := range mapping {
				value, ok := rawValue.(string)
				if !ok {
					return nil, PublicDataPlaneSecretRenderWitness{}, fmt.Errorf("rendered Secret payload value is invalid")
				}
				secretValues[payloadKey] = value
				keys = append(keys, payloadKey)
				if field == "data" {
					mapping[payloadKey] = secretRedactedDataValue
				} else {
					mapping[payloadKey] = secretRedactedStringDataValue
				}
			}
			if field == "data" {
				payload.Data = secretValues
			} else {
				payload.StringData = secretValues
			}
		}
		if len(keys) == 0 {
			return nil, PublicDataPlaneSecretRenderWitness{}, fmt.Errorf("rendered Secret payload is empty")
		}
		sort.Strings(keys)
		for index := 1; index < len(keys); index++ {
			if keys[index] == keys[index-1] {
				return nil, PublicDataPlaneSecretRenderWitness{}, fmt.Errorf("rendered Secret payload key is duplicated")
			}
		}
		object.Object = clone
		indexed[key] = object
		payloads = append(payloads, payload)
		members = append(members, PublicDataPlaneSecretWitnessMember{Name: object.Identity.Name, Type: typeName, PayloadKeys: keys})
	}
	sort.Slice(payloads, func(i, j int) bool {
		return payloads[i].Namespace+"\x00"+payloads[i].Name < payloads[j].Namespace+"\x00"+payloads[j].Name
	})
	sort.Slice(members, func(i, j int) bool { return members[i].Name < members[j].Name })
	witness := PublicDataPlaneSecretRenderWitness{
		APIVersion: PublicDataPlaneAdoptionAPIVersion, Kind: PublicDataPlaneSecretRenderWitnessKind,
		ReleaseNamespace: namespace, Members: members,
	}
	if len(hmacKey) != 0 {
		if len(hmacKey) != 32 {
			return nil, PublicDataPlaneSecretRenderWitness{}, fmt.Errorf("secret render HMAC key is invalid")
		}
		encoded, err := json.Marshal(payloads)
		if err != nil {
			return nil, PublicDataPlaneSecretRenderWitness{}, fmt.Errorf("secret render payload cannot be sealed")
		}
		mac := hmac.New(sha256.New, hmacKey)
		_, _ = mac.Write(encoded)
		witness.PayloadHMAC = "hmac-sha256:" + hex.EncodeToString(mac.Sum(nil))
	}
	witness.Digest = publicDataPlaneSecretRenderWitnessDigest(witness)
	if err := VerifyPublicDataPlaneSecretRenderWitness(witness, len(hmacKey) != 0); err != nil {
		return nil, PublicDataPlaneSecretRenderWitness{}, err
	}
	canonical, err := encodePublicDataPlaneManifestObjects(indexed)
	if err != nil {
		return nil, PublicDataPlaneSecretRenderWitness{}, err
	}
	return canonical, witness, nil
}

func VerifyPublicDataPlaneSecretRenderWitness(witness PublicDataPlaneSecretRenderWitness, requireHMAC bool) error {
	if witness.APIVersion != PublicDataPlaneAdoptionAPIVersion || witness.Kind != PublicDataPlaneSecretRenderWitnessKind ||
		witness.ReleaseNamespace == "" || len(witness.Members) == 0 {
		return fmt.Errorf("secret render witness identity is invalid")
	}
	if err := verifyPublicDataPlaneSecretMembers(witness.Members, false); err != nil {
		return err
	}
	if requireHMAC {
		if len(witness.PayloadHMAC) != len("hmac-sha256:")+64 || witness.PayloadHMAC[:len("hmac-sha256:")] != "hmac-sha256:" {
			return fmt.Errorf("secret render payload HMAC is invalid")
		}
		hexValue := witness.PayloadHMAC[len("hmac-sha256:"):]
		if hexValue != strings.ToLower(hexValue) {
			return fmt.Errorf("secret render payload HMAC is invalid")
		}
		if _, err := hex.DecodeString(hexValue); err != nil {
			return fmt.Errorf("secret render payload HMAC is invalid")
		}
	} else if witness.PayloadHMAC != "" {
		return fmt.Errorf("unkeyed secret render witness contains a payload HMAC")
	}
	if witness.Digest != publicDataPlaneSecretRenderWitnessDigest(witness) {
		return fmt.Errorf("secret render witness digest mismatch")
	}
	return nil
}

func DecodePublicDataPlaneSecretRenderWitness(raw []byte, requireHMAC bool) (PublicDataPlaneSecretRenderWitness, error) {
	var witness PublicDataPlaneSecretRenderWitness
	if err := decodeCanonicalPublicDataPlaneSecretWitness(raw, &witness); err != nil {
		return PublicDataPlaneSecretRenderWitness{}, err
	}
	if err := VerifyPublicDataPlaneSecretRenderWitness(witness, requireHMAC); err != nil {
		return PublicDataPlaneSecretRenderWitness{}, err
	}
	return witness, nil
}

func decodeCanonicalPublicDataPlaneSecretWitness(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("secret witness is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("secret witness is invalid")
	}
	return nil
}

func verifyPublicDataPlaneSecretMembers(members []PublicDataPlaneSecretWitnessMember, requireMetadata bool) error {
	previous := ""
	for _, member := range members {
		if member.Name == "" || member.Type == "" || member.Name <= previous || len(member.PayloadKeys) == 0 {
			return fmt.Errorf("secret witness members are not canonical")
		}
		previous = member.Name
		if requireMetadata {
			if err := validateCanonicalSHA256Digest(member.UIDDigest, "secret lookup UID digest"); err != nil {
				return err
			}
			if err := validateCanonicalSHA256Digest(member.ResourceVersionDigest, "secret lookup resourceVersion digest"); err != nil {
				return err
			}
		} else if member.UIDDigest != "" || member.ResourceVersionDigest != "" {
			return fmt.Errorf("secret render witness contains live metadata")
		}
		for index, key := range member.PayloadKeys {
			if key == "" || (index > 0 && key <= member.PayloadKeys[index-1]) {
				return fmt.Errorf("secret witness payload keys are not canonical")
			}
		}
	}
	return nil
}

func VerifyPublicDataPlaneSecretWitnessBinding(
	lookup PublicDataPlaneSecretLookupWitness, renders ...PublicDataPlaneSecretRenderWitness,
) error {
	if err := VerifyPublicDataPlaneSecretLookupWitness(lookup); err != nil {
		return err
	}
	if len(renders) == 0 {
		return fmt.Errorf("secret render witness is missing")
	}
	for _, render := range renders {
		if err := VerifyPublicDataPlaneSecretRenderWitness(render, true); err != nil {
			return err
		}
		if render.ReleaseNamespace != lookup.ReleaseNamespace || !equalPublicDataPlaneSecretMemberShapes(lookup.Members, render.Members) {
			return fmt.Errorf("secret lookup and render witnesses do not describe the same generated objects")
		}
		if render.PayloadHMAC != renders[0].PayloadHMAC || render.Digest != renders[0].Digest {
			return fmt.Errorf("secret render payload or object set drifted")
		}
	}
	return nil
}

func VerifyPublicDataPlaneSecretRenderWitnessForPlan(
	plan PublicDataPlaneAdoptionPlan, witness PublicDataPlaneSecretRenderWitness,
) error {
	if err := VerifyPublicDataPlaneAdoptionPlan(plan); err != nil {
		return err
	}
	if err := VerifyPublicDataPlaneSecretRenderWitness(witness, true); err != nil {
		return err
	}
	if witness.ReleaseNamespace != plan.ReleaseNamespace || witness.Digest != plan.SecretRenderWitnessDigest ||
		witness.PayloadHMAC != plan.SecretPayloadHMAC {
		return fmt.Errorf("secret render witness does not match the Stage1 plan")
	}
	return nil
}

func equalPublicDataPlaneSecretMemberShapes(left, right []PublicDataPlaneSecretWitnessMember) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Name != right[index].Name || left[index].Type != right[index].Type ||
			!slices.Equal(left[index].PayloadKeys, right[index].PayloadKeys) {
			return false
		}
	}
	return true
}

func publicDataPlaneSecretLookupWitnessDigest(witness PublicDataPlaneSecretLookupWitness) string {
	clone := witness
	clone.Digest = ""
	encoded, _ := json.Marshal(clone)
	return digestBytesSHA256(encoded)
}

func publicDataPlaneSecretRenderWitnessDigest(witness PublicDataPlaneSecretRenderWitness) string {
	clone := witness
	clone.Digest = ""
	encoded, _ := json.Marshal(clone)
	return digestBytesSHA256(encoded)
}
