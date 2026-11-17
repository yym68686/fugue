package pgimage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	cnpgcontract "fugue/deploy/environments/production/cnpg-controller"
)

const OperationType = "database-image-migrate"
const BaselineEvidence = "postgres_image_migration_baseline"
const IntentEvidence = "postgres_image_migration_intent"
const VerifiedEvidence = "postgres_image_migration_verified"

var immutable = regexp.MustCompile(`^.+@sha256:[0-9a-f]{64}$`)

type Catalog struct {
	DefaultImage  string
	Images        []string
	PostgresBase  string
	OperatorImage string
}

func ApprovedCatalog() (Catalog, error) {
	var release struct {
		Candidate struct {
			Image string `json:"image"`
		} `json:"candidate"`
		Receipt struct {
			Contract struct {
				DefaultImage string   `json:"defaultImage"`
				Images       []string `json:"images"`
			} `json:"baked_postgres_contract"`
			Verification struct {
				PostgresBase     string `json:"postgres_base"`
				Verified         bool   `json:"upgrade_path_verified"`
				ReadOnlyVerified bool   `json:"readonly_startup_verified"`
				Image            string `json:"image"`
			} `json:"baked_postgres_verification"`
			Rehearsal struct {
				Checks         []string `json:"checks"`
				BakedImage     string   `json:"baked_image"`
				CandidateImage string   `json:"candidate_image"`
			} `json:"isolated_cluster_verification"`
		} `json:"candidateReceipt"`
	}
	if err := json.Unmarshal(cnpgcontract.Release, &release); err != nil {
		return Catalog{}, err
	}
	c := Catalog{release.Receipt.Contract.DefaultImage, release.Receipt.Contract.Images, release.Receipt.Verification.PostgresBase, release.Candidate.Image}
	if !immutable.MatchString(c.DefaultImage) || !immutable.MatchString(c.PostgresBase) || !immutable.MatchString(c.OperatorImage) || !release.Receipt.Verification.Verified || !release.Receipt.Verification.ReadOnlyVerified || release.Receipt.Verification.Image != c.DefaultImage || release.Receipt.Rehearsal.BakedImage != c.DefaultImage || release.Receipt.Rehearsal.CandidateImage != c.OperatorImage {
		return c, fmt.Errorf("verified baked-manager release receipt is unavailable")
	}
	for _, required := range []string{"default_initdb_without_bootstrap", "sql_read_write", "container_restart", "pod_recreation", "legacy_unchanged"} {
		found := false
		for _, check := range release.Receipt.Rehearsal.Checks {
			if check == required {
				found = true
			}
		}
		if !found {
			return c, fmt.Errorf("required baked-manager rehearsal check %s is missing", required)
		}
	}
	return c, nil
}

func Target(image string) (string, error) {
	c, err := ApprovedCatalog()
	if err != nil {
		return "", err
	}
	image = strings.TrimSpace(image)
	if image == "" {
		image = c.DefaultImage
	}
	for _, approved := range c.Images {
		if image == approved && image == c.DefaultImage && immutable.MatchString(image) {
			return image, nil
		}
	}
	return "", fmt.Errorf("image is not in the verified baked-manager release catalog")
}

func Hash(value any) string {
	b, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

type Plan struct {
	ServiceID            string   `json:"service_id"`
	AppID                string   `json:"app_id"`
	TargetImage          string   `json:"target_image"`
	ConfigHash           string   `json:"config_hash"`
	RequiresDowntime     bool     `json:"requires_downtime"`
	LivePreflightPending bool     `json:"live_preflight_pending"`
	Stages               []string `json:"stages"`
}
