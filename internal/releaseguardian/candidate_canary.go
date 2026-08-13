package releaseguardian

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
)

const CandidateCanaryRequiredSamples = 3

type CandidateRouteSample struct {
	GroupID               string        `json:"groupId"`
	AuthorityRecordDigest string        `json:"authorityRecordDigest"`
	WorkerSlot            AuthoritySlot `json:"workerSlot"`
	ReleaseRecordDigest   string        `json:"releaseRecordDigest"`
	Attested              bool          `json:"attested"`
	ObservedRecordDigest  string        `json:"observedRecordDigest,omitempty"`
	ObservedReleaseDigest string        `json:"observedReleaseDigest,omitempty"`
	ObservedWorkerSlot    AuthoritySlot `json:"observedWorkerSlot,omitempty"`
	StatusCode            int           `json:"statusCode"`
	BodyDigest            string        `json:"bodyDigest"`
	ExpectedBodyDigest    string        `json:"expectedBodyDigest"`
	OriginEvidenceDigest  string        `json:"originEvidenceDigest"`
	TransportErrorClass   string        `json:"transportErrorClass,omitempty"`
	ObservedAt            string        `json:"observedAt"`
}

func (sample CandidateRouteSample) validate(candidate CandidateAuthority, requireAttestation bool) error {
	if candidate.Validate() != nil || sample.GroupID != candidate.GroupID ||
		sample.AuthorityRecordDigest != candidate.RecordDigest || sample.WorkerSlot != candidate.WorkerSlot ||
		sample.ReleaseRecordDigest != candidate.ReleaseRecordDigest || sample.WorkerSlot.Validate() != nil ||
		!digestPattern.MatchString(sample.BodyDigest) || !digestPattern.MatchString(sample.ExpectedBodyDigest) ||
		!digestPattern.MatchString(sample.OriginEvidenceDigest) || sample.StatusCode < 0 || sample.StatusCode > 599 ||
		len(sample.TransportErrorClass) > 128 || strings.ContainsAny(sample.TransportErrorClass, "\r\n\x00") {
		return errors.New("candidate route sample identity is invalid")
	}
	if requireAttestation {
		if !sample.Attested || sample.ObservedRecordDigest != candidate.RecordDigest ||
			sample.ObservedReleaseDigest != candidate.ReleaseRecordDigest || sample.ObservedWorkerSlot != candidate.WorkerSlot {
			return errors.New("candidate route sample attestation is invalid")
		}
	} else if sample.Attested {
		if sample.ObservedRecordDigest != candidate.RecordDigest || sample.ObservedWorkerSlot != candidate.WorkerSlot ||
			!digestPattern.MatchString(sample.ObservedReleaseDigest) {
			return errors.New("previous authority control attestation is invalid")
		}
	} else if sample.ObservedRecordDigest != "" || sample.ObservedReleaseDigest != "" || sample.ObservedWorkerSlot != "" {
		return errors.New("previous authority control has a partial attestation")
	}
	observed, err := time.Parse(time.RFC3339Nano, sample.ObservedAt)
	if err != nil || !observed.Equal(observed.UTC()) {
		return errors.New("candidate route sample time is invalid")
	}
	return nil
}

type CandidateWorkerInstance struct {
	NodeName string `json:"nodeName"`
	PodUID   string `json:"podUid"`
}

type CandidateWorkerCohort struct {
	GroupID           string                    `json:"groupId"`
	WorkerSlot        AuthoritySlot             `json:"workerSlot"`
	BundleGeneration  string                    `json:"bundleGeneration"`
	WorkerSourceSHA   string                    `json:"workerSourceSha"`
	WorkerImageDigest string                    `json:"workerImageDigest"`
	Instances         []CandidateWorkerInstance `json:"instances"`
	CohortDigest      string                    `json:"cohortDigest"`
}

func (cohort CandidateWorkerCohort) Seal() (CandidateWorkerCohort, error) {
	cohort.CohortDigest = ""
	cohort.Instances = append([]CandidateWorkerInstance(nil), cohort.Instances...)
	if err := cohort.validateUnsigned(); err != nil {
		return CandidateWorkerCohort{}, err
	}
	sort.Slice(cohort.Instances, func(i, j int) bool {
		if cohort.Instances[i].NodeName != cohort.Instances[j].NodeName {
			return cohort.Instances[i].NodeName < cohort.Instances[j].NodeName
		}
		return cohort.Instances[i].PodUID < cohort.Instances[j].PodUID
	})
	raw, err := declarativerelease.CanonicalJSON(cohort)
	if err != nil {
		return CandidateWorkerCohort{}, err
	}
	cohort.CohortDigest = digest(raw)
	return cohort, nil
}

func (cohort CandidateWorkerCohort) Validate() error {
	if !digestPattern.MatchString(cohort.CohortDigest) || cohort.validateUnsigned() != nil {
		return errors.New("candidate worker cohort is invalid")
	}
	copy := cohort
	copy.CohortDigest = ""
	copy.Instances = append([]CandidateWorkerInstance(nil), cohort.Instances...)
	sort.Slice(copy.Instances, func(i, j int) bool {
		if copy.Instances[i].NodeName != copy.Instances[j].NodeName {
			return copy.Instances[i].NodeName < copy.Instances[j].NodeName
		}
		return copy.Instances[i].PodUID < copy.Instances[j].PodUID
	})
	raw, err := declarativerelease.CanonicalJSON(copy)
	if err != nil || digest(raw) != cohort.CohortDigest {
		return errors.New("candidate worker cohort digest is invalid")
	}
	return nil
}

func (cohort CandidateWorkerCohort) validateUnsigned() error {
	if !groupPattern.MatchString(cohort.GroupID) || cohort.WorkerSlot.Validate() != nil ||
		!authorityGenerationPattern.MatchString(cohort.BundleGeneration) || !shaPattern.MatchString(cohort.WorkerSourceSHA) ||
		!digestPattern.MatchString(cohort.WorkerImageDigest) || len(cohort.Instances) < 1 || len(cohort.Instances) > 100 {
		return errors.New("candidate worker cohort identity is invalid")
	}
	seenNodes, seenPods := map[string]bool{}, map[string]bool{}
	for _, instance := range cohort.Instances {
		if !componentPattern.MatchString(instance.NodeName) || len(instance.PodUID) < 8 || len(instance.PodUID) > 64 ||
			strings.ContainsAny(instance.PodUID, "\r\n\t ,") || seenNodes[instance.NodeName] || seenPods[instance.PodUID] {
			return errors.New("candidate worker cohort member is invalid")
		}
		seenNodes[instance.NodeName], seenPods[instance.PodUID] = true, true
	}
	return nil
}

func (sample CandidateRouteSample) healthy() bool {
	return sample.TransportErrorClass == "" && sample.StatusCode >= 200 && sample.StatusCode < 300 &&
		sample.BodyDigest == sample.ExpectedBodyDigest
}

// EvaluateCandidateCanary classifies three candidate route samples against
// three previous-authority controls. It never changes CandidateAuthority or
// CurrentAuthority. A candidate failure with a healthy previous route is
// candidate-local; simultaneous failure is dependency degradation and cannot
// authorize a switch or an unrelated rollback.
func EvaluateCandidateCanary(candidate CandidateAuthority, current CurrentAuthority, cohort CandidateWorkerCohort, candidateSamples, previousSamples []CandidateRouteSample, observedAt time.Time, ttl time.Duration, keyID string, signingKey []byte) (CandidateCanaryResult, error) {
	if candidate.Validate() != nil || !candidate.HasPromotionWitness() || !authorityGenerationPattern.MatchString(candidate.BundleGeneration) || candidate.State != CandidateAuthorityLoaded || current.Validate() != nil ||
		cohort.Validate() != nil || cohort.GroupID != candidate.GroupID || cohort.WorkerSlot != candidate.WorkerSlot || cohort.BundleGeneration != candidate.BundleGeneration ||
		candidate.GroupID != current.GroupID || candidate.RecordDigest == current.CurrentRecordDigest ||
		candidate.WorkerSlot == current.CurrentWorkerSlot || len(candidateSamples) != CandidateCanaryRequiredSamples ||
		len(previousSamples) != CandidateCanaryRequiredSamples || ttl <= 0 || ttl > time.Minute ||
		!componentPattern.MatchString(keyID) || observedAt.IsZero() || !observedAt.Equal(observedAt.UTC()) {
		return CandidateCanaryResult{}, errors.New("candidate canary evaluation input is invalid")
	}
	windowStart := observedAt.Add(-10 * time.Second)
	candidateHealthy, previousHealthy := true, true
	all := make([]CandidateRouteSample, 0, len(candidateSamples)+len(previousSamples))
	for _, sample := range candidateSamples {
		if err := sample.validate(candidate, true); err != nil {
			return CandidateCanaryResult{}, err
		}
		timestamp, _ := time.Parse(time.RFC3339Nano, sample.ObservedAt)
		if timestamp.Before(windowStart) || timestamp.After(observedAt) {
			return CandidateCanaryResult{}, errors.New("candidate route sample is outside the bounded observation window")
		}
		candidateHealthy = candidateHealthy && sample.healthy()
		all = append(all, sample)
	}
	previousBinding := CandidateAuthority{
		APIVersion: APIVersion, Kind: CandidateAuthorityKind, GroupID: current.GroupID,
		RecordDigest: current.CurrentRecordDigest, BundleGeneration: candidate.BundleGeneration, WorkerSlot: current.CurrentWorkerSlot,
		ReleaseRecordDigest: candidate.ReleaseRecordDigest, State: CandidateAuthorityLoaded, Generation: candidate.Generation,
	}
	for _, sample := range previousSamples {
		if err := sample.validate(previousBinding, false); err != nil {
			return CandidateCanaryResult{}, fmt.Errorf("previous authority control: %w", err)
		}
		timestamp, _ := time.Parse(time.RFC3339Nano, sample.ObservedAt)
		if timestamp.Before(windowStart) || timestamp.After(observedAt) {
			return CandidateCanaryResult{}, errors.New("previous route sample is outside the bounded observation window")
		}
		previousHealthy = previousHealthy && sample.healthy()
		all = append(all, sample)
	}
	sort.Slice(all, func(left, right int) bool {
		if all[left].ObservedAt != all[right].ObservedAt {
			return all[left].ObservedAt < all[right].ObservedAt
		}
		if all[left].AuthorityRecordDigest != all[right].AuthorityRecordDigest {
			return all[left].AuthorityRecordDigest < all[right].AuthorityRecordDigest
		}
		return all[left].WorkerSlot < all[right].WorkerSlot
	})
	evidence, err := declarativerelease.CanonicalJSON(struct {
		CandidateGeneration int64                  `json:"candidateGeneration"`
		CurrentEpoch        int64                  `json:"currentAuthorityEpoch"`
		Samples             []CandidateRouteSample `json:"samples"`
	}{CandidateGeneration: candidate.Generation, CurrentEpoch: current.AuthorityEpoch, Samples: all})
	if err != nil {
		return CandidateCanaryResult{}, err
	}
	routeState, dependencyState := HealthDegraded, HealthHealthy
	if candidateHealthy && previousHealthy {
		routeState = HealthHealthy
	}
	if !previousHealthy {
		dependencyState = HealthDegraded
	}
	return SignCandidateCanaryResult(CandidateCanaryResult{
		GroupID: candidate.GroupID, CandidateRecordDigest: candidate.RecordDigest, WorkerSlot: candidate.WorkerSlot,
		AuthoritySequence: candidate.AuthoritySequence, CandidateSequence: candidate.CandidateSequence,
		CurrentPublicationSequence: candidate.CurrentPublicationSequence, CurrentRecoveryEpoch: candidate.CurrentRecoveryEpoch,
		CurrentBundleDigest: candidate.CurrentBundleDigest, CandidateEpoch: candidate.CandidateEpoch,
		BundleGeneration: candidate.BundleGeneration, ServingGeneration: candidate.ServingGeneration, WorkerSourceSHA: cohort.WorkerSourceSHA,
		WorkerImageDigest: cohort.WorkerImageDigest, WorkerCohortDigest: cohort.CohortDigest,
		ReleaseRecordDigest: candidate.ReleaseRecordDigest, RouteState: routeState, DependencyState: dependencyState,
		EvidenceDigest: digest(evidence), ObservedAt: observedAt.Format(time.RFC3339Nano),
		ExpiresAt: observedAt.Add(ttl).Format(time.RFC3339Nano), KeyID: keyID,
	}, signingKey)
}
