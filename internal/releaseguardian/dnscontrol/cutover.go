package dnscontrol

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"fugue/internal/platformproducer"
	"fugue/internal/productioncontrol"
)

// DNSRevocation identifies independently retained provider-side revocation and
// drainage records, attested after the exact legacy selection was frozen.
type DNSRevocation struct {
	FreezeIntent         string `json:"freezeIntent"`
	CredentialRevocation string `json:"credentialRevocationDigest"`
	RequestDrainage      string `json:"requestDrainageDigest"`
}

func verifyDNSRevocation(ctx context.Context, j *productioncontrol.Journal, source productioncontrol.WitnessReader, c ServingCheckpoint, w productioncontrol.ExecutionWitness) error {
	raw, err := source.ReadExecutionWitness(ctx, w.AuthorityDigest)
	var a DNSRevocation
	if err != nil || json.Unmarshal(raw, &a) != nil {
		return errors.New("DNS baseline lacks a revocation record")
	}
	canonical, err := json.Marshal(a)
	if err != nil || !bytes.Equal(raw, canonical) || a.FreezeIntent == a.CredentialRevocation || a.FreezeIntent == a.RequestDrainage ||
		a.CredentialRevocation == a.RequestDrainage {
		return errors.New("DNS baseline lacks distinct freeze, credential revocation and request drainage records")
	}
	for _, ref := range []string{a.CredentialRevocation, a.RequestDrainage} {
		if !platformproducer.ValidDigest(ref) || ref == w.AuthorityDigest || ref == w.ObservationDigest {
			return errors.New("invalid DNS revocation or drainage reference")
		}
		record, err := source.ReadExecutionWitness(ctx, ref)
		if err != nil || len(record) == 0 || len(record) > productioncontrol.MaxWitnessBytes ||
			fmt.Sprintf("sha256:%x", sha256.Sum256(record)) != ref {
			return errors.New("DNS credential revocation or request drainage record is unavailable at its signed digest")
		}
	}
	return j.CheckGlobalProducerFreeze(ctx, c.Scope, c.Producer.ReleaseID, c.Version(), a.FreezeIntent, w.ObservedAt)
}

func ApplyCutover(ctx context.Context, j *productioncontrol.Journal, r *Reader, cluster, actor, intent, approval string,
	phase productioncontrol.DNSCutoverPhase, source productioncontrol.WitnessReader, key ed25519.PublicKey) (productioncontrol.Recorded, error) {
	return j.ApplyDNSCutover(ctx, cluster, actor, intent, approval, phase, func(ctx context.Context, p productioncontrol.DNSCutoverPlan) (time.Time, error) {
		if r == nil || r.origin != p.Origin {
			return time.Time{}, errors.New("DNS cutover reader differs from approved plan")
		}
		match := func(ref string, c ServingCheckpoint) error {
			if ref != p.CheckpointDigest || c.Version() != p.Version || c.Producer.ReleaseID != p.ReleaseID {
				return errors.New("DNS cutover serving predecessor differs from approved plan")
			}
			return nil
		}
		if phase == productioncontrol.DNSActivate {
			var deadline time.Time
			e, err := newSettlement(j, r, cluster, source, key, func(ref string, c ServingCheckpoint, w productioncontrol.ExecutionWitness) error {
				deadline = time.Unix(w.ExpiresAt, 0)
				return match(ref, c)
			})
			if err != nil {
				return time.Time{}, err
			}
			err = e.VerifyBaselineEvidence(ctx, productioncontrol.Baseline{Scope: Scope(cluster), Verified: p.Version, EvidenceDigest: p.ActivationWitness})
			return deadline, err
		}
		change, err := validateFreezeShadow(cluster, p)
		if err != nil {
			return time.Time{}, err
		}
		if _, err := r.Revalidate(ctx, change, time.Now); err != nil {
			return time.Time{}, err
		}
		c, _, err := r.ObserveListenerCoverage(ctx, j, cluster, "", time.Now)
		if err != nil {
			return time.Time{}, err
		}
		ref, err := c.Digest()
		if err != nil {
			return time.Time{}, err
		}
		return time.Now().Add(15 * time.Second), match(ref, c)
	})
}

func validateFreezeShadow(cluster string, p productioncontrol.DNSCutoverPlan) (productioncontrol.Submission, error) {
	var change Payload
	if json.Unmarshal(p.ShadowPayload, &change) != nil {
		return productioncontrol.Submission{}, errors.New("invalid cutover shadow payload")
	}
	i, err := productioncontrol.NewIntent(cluster, Resource, "preview", productioncontrol.DNSMembership,
		p.Version.Generation, p.Version.Digest, change.NextProducer.Digest, p.Version.Digest, p.ShadowPayload)
	s := productioncontrol.Submission{Intent: i, Payload: p.ShadowPayload}
	if err != nil {
		return productioncontrol.Submission{}, err
	}
	change, err = Inspect(s)
	if err != nil || change.Origin != p.Origin || change.Precondition.PreviousPolicy.ReleaseID != p.ReleaseID || change.Reverses != nil {
		return productioncontrol.Submission{}, errors.New("cutover shadow is for a different predecessor")
	}
	return s, nil
}
