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

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/productioncontrol"
)

// ServingEvidence is the observation record signed by the independent
// witness. The DNS probe report is distinct from the control-plane checkpoint
// and must be retained by the signer at its immutable digest. An API readback
// alone is not proof that DNS consumers actually served the intended answers.
type ServingEvidence struct {
	CheckpointDigest       string `json:"checkpointDigest"`
	AuthoritativeDNSDigest string `json:"authoritativeDnsDigest"`
}

func EncodeServingEvidence(e ServingEvidence) ([]byte, string, error) {
	if !platformproducer.ValidDigest(e.CheckpointDigest) || !platformproducer.ValidDigest(e.AuthoritativeDNSDigest) ||
		e.CheckpointDigest == e.AuthoritativeDNSDigest {
		return nil, "", errors.New("DNS serving evidence must bind separate checkpoint and authoritative DNS observations")
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return nil, "", err
	}
	d, err := platformconfig.Digest(e)
	return raw, d, err
}

// ServingCheckpoint links the exact selected producer, verified Full/LKG,
// and the published DNS member. This control-plane read is one input to an
// independent witness; it does not sample authoritative DNS responses.
type ServingCheckpoint struct {
	Scope    productioncontrol.Scope               `json:"scope"`
	Origin   string                                `json:"origin"`
	Producer model.PlatformPublicationPrecondition `json:"producer"`
	Full     model.PlatformPublicationPrecondition `json:"full"`
	LKGID    string                                `json:"lkgId"`
	LKGProof string                                `json:"lkgProof"`
	DNS      ArtifactRef                           `json:"dns"`
	Claim    *productioncontrol.Claim              `json:"claim,omitempty"`
	Outcome  productioncontrol.Outcome             `json:"outcome,omitempty"`
}

func (c ServingCheckpoint) Version() productioncontrol.Version {
	return productioncontrol.Version{Generation: c.Producer.FencingToken, Digest: c.Producer.ContentHash}
}

func (c ServingCheckpoint) Digest() (string, error) {
	return platformconfig.Digest(struct {
		Schema string `json:"schema"`
		ServingCheckpoint
	}{"fugue.dns-serving-checkpoint/v1", c})
}

type servingSelection struct {
	producer, full Publication
	lkg            model.PlatformLKGSnapshot
}

func verifyServingCheckpoint(sel servingSelection, now time.Time) (ServingCheckpoint, string, error) {
	var c ServingCheckpoint
	var err error
	c.Producer, err = publicationRef(sel.producer, model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow")
	if err != nil {
		return c, "", err
	}
	p, err := platformproducer.Decode(sel.producer.Artifact)
	if err != nil || p.TargetScope != "global" || p.Mode != "serving" || !p.RequireDNSQueryPolicy || p.DNSPolicyArtifactID == "" || p.StaticIntentArtifactID == "" {
		return c, "", errors.New("DNS checkpoint lacks an exact global serving producer")
	}
	c.Full, err = verifiedServingFull(sel.producer, p, sel.full, sel.lkg, now)
	if err != nil {
		return c, "", err
	}
	c.LKGID, c.LKGProof = sel.lkg.ID, sel.lkg.VerificationEvidenceHash
	var set platformconfig.ReleaseSet
	raw, err := json.Marshal(sel.full.Artifact.Content)
	if err != nil || json.Unmarshal(raw, &set) != nil || set.Scope != "global" || set.PublicationRole != "" || set.Generation != sel.full.Artifact.Generation {
		return c, "", errors.New("DNS checkpoint Full does not identify its release-set members")
	}
	for index, kind := range set.ArtifactKinds {
		if kind == model.PlatformArtifactKindDNSAnswerBundle {
			return c, set.ArtifactIDs[index], nil
		}
	}
	return c, "", errors.New("DNS checkpoint Full has no exact DNS member")
}

func checkpointOutcome(c ServingCheckpoint, sel servingSelection, p Payload) (productioncontrol.Outcome, error) {
	policy, err := platformproducer.Decode(sel.producer.Artifact)
	if err != nil {
		return "", err
	}
	if artifactRef(sel.producer.Artifact) == p.PreviousProducer && c.Producer == p.Precondition.PreviousPolicy &&
		c.Full == p.Precondition.ServingFull && c.LKGID == p.LKGID && c.LKGProof == p.Precondition.VerificationEvidenceHash {
		return productioncontrol.NotApplied, nil
	}
	if artifactRef(sel.producer.Artifact) == p.NextProducer && c.Producer.FencingToken == p.Precondition.PreviousPolicy.FencingToken+1 &&
		c.Full.FencingToken > p.Precondition.ServingFull.FencingToken && c.Full.ReleaseID != p.Precondition.ServingFull.ReleaseID &&
		policy.StaticIntentArtifactID == p.Static.ID && policy.StaticIntentDigest == p.Static.Digest &&
		policy.DNSPolicyArtifactID == p.NextDNS.ID && policy.DNSPolicyDigest == p.NextDNS.Digest {
		return productioncontrol.Applied, nil
	}
	return "", errors.New("DNS serving publication is not the exact approved result or unchanged predecessor")
}

// ObserveServingCheckpoint performs only GETs and journal reads. It rereads
// both selected publications and LKG after fetching the immutable DNS member.
func (r *Reader) ObserveServingCheckpoint(ctx context.Context, j *productioncontrol.Journal, cluster, intentDigest string, now func() time.Time) (ServingCheckpoint, error) {
	if r == nil || j == nil || now == nil || cluster == "" {
		return ServingCheckpoint{}, errors.New("DNS checkpoint requires an explicit journal and reader")
	}
	scope := Scope(cluster)
	var claim *productioncontrol.Claim
	var approved Payload
	if intentDigest != "" {
		i, raw, err := j.ReadIntent(ctx, intentDigest)
		if err != nil || i.ClusterID != cluster {
			return ServingCheckpoint{}, productioncontrol.ErrStaleFence
		}
		approved, err = Inspect(productioncontrol.Submission{Intent: i, Payload: raw})
		if err != nil || approved.Origin != r.origin {
			return ServingCheckpoint{}, errors.New("DNS checkpoint origin differs from the approved change")
		}
		stored, result, err := j.ReadExecution(ctx, scope, intentDigest)
		if err != nil || result != nil {
			return ServingCheckpoint{}, productioncontrol.ErrStaleFence
		}
		claim = &stored
	}
	read := func() (servingSelection, error) {
		p, f, l, err := r.current(ctx)
		return servingSelection{p, f, l}, err
	}
	initial, err := read()
	if err != nil {
		return ServingCheckpoint{}, err
	}
	c, dnsID, err := verifyServingCheckpoint(initial, now())
	if err != nil {
		return ServingCheckpoint{}, err
	}
	dns, err := r.artifact(ctx, dnsID, model.PlatformArtifactKindDNSAnswerBundle, "global")
	if err != nil {
		return ServingCheckpoint{}, err
	}
	decoded, err := platformconfig.DecodeDNSArtifact(dns)
	if err != nil {
		return ServingCheckpoint{}, err
	}
	var set platformconfig.ReleaseSet
	raw, err := json.Marshal(initial.full.Artifact.Content)
	if err != nil || json.Unmarshal(raw, &set) != nil || decoded.Lineage != set.Lineage {
		return ServingCheckpoint{}, errors.New("DNS member lineage differs from its verified release set")
	}
	latest, err := read()
	if err != nil {
		return ServingCheckpoint{}, err
	}
	again, latestDNS, err := verifyServingCheckpoint(latest, now())
	if err != nil || latestDNS != dnsID || again != c || latest.lkg != initial.lkg {
		return ServingCheckpoint{}, errors.New("producer, Full or verified LKG changed during DNS serving readback")
	}
	c.Scope, c.Origin, c.DNS, c.Claim = scope, r.origin, artifactRef(dns), claim
	if claim != nil {
		c.Outcome, err = checkpointOutcome(c, latest, approved)
		if err != nil {
			return ServingCheckpoint{}, err
		}
		head, err := j.ReadResource(ctx, scope)
		if err != nil || head.Pending != claim.IntentDigest || head.LastFence != claim.Fence {
			return ServingCheckpoint{}, productioncontrol.ErrStaleFence
		}
	}
	return c, nil
}

// NewSettlement authenticates a separate observer's witness, requires a new
// matching control-plane observation, then closes execution in the same SQL
// lock domain as the provider. The signer is responsible for retaining direct
// DNS answers along with the exact checkpoint as its observation evidence.
func NewSettlement(j *productioncontrol.Journal, r *Reader, cluster string, source productioncontrol.WitnessReader, key ed25519.PublicKey) (*productioncontrol.ExecutionJournal, error) {
	return productioncontrol.NewGlobalProducerExecutionJournal(j, source, map[string]productioncontrol.TrustedWitness{
		"provider-observer": {Scope: Scope(cluster), PublicKey: key},
	}, func(ctx context.Context, w productioncontrol.ExecutionWitness) error {
		c, err := r.ObserveServingCheckpoint(ctx, j, cluster, w.IntentDigest, time.Now)
		if err != nil {
			return err
		}
		d, err := c.Digest()
		if err != nil || w.Generation != c.Version().Generation || w.VerifiedDigest != c.Version().Digest ||
			(w.Kind == productioncontrol.TerminalWitness && (c.Claim == nil || c.Claim.IntentDigest != w.IntentDigest || c.Claim.ApprovalDigest != w.ApprovalDigest || c.Claim.Fence != w.Fence || c.Outcome != w.Outcome)) {
			return errors.New("DNS witness differs from the independently read serving checkpoint")
		}
		raw, err := source.ReadExecutionWitness(ctx, w.ObservationDigest)
		var e ServingEvidence
		if err != nil || len(raw) > productioncontrol.MaxWitnessBytes || json.Unmarshal(raw, &e) != nil {
			return errors.New("DNS witness observation record is unavailable or invalid")
		}
		canonical, digest, err := EncodeServingEvidence(e)
		if err != nil || digest != w.ObservationDigest || !bytes.Equal(canonical, raw) || e.CheckpointDigest != d ||
			e.AuthoritativeDNSDigest == w.AuthorityDigest {
			return errors.New("DNS witness does not bind this checkpoint and a separate DNS answer report")
		}
		for _, ref := range []string{e.AuthoritativeDNSDigest, w.AuthorityDigest} {
			record, err := source.ReadExecutionWitness(ctx, ref)
			if err != nil || len(record) == 0 || len(record) > productioncontrol.MaxWitnessBytes || fmt.Sprintf("sha256:%x", sha256.Sum256(record)) != ref {
				return errors.New("DNS answer or authority evidence is unavailable at its signed digest")
			}
		}
		if w.Kind == productioncontrol.BaselineWitness {
			return j.CheckGlobalProducerFreeze(ctx, c.Producer.ReleaseID, c.Version(), w.AuthorityDigest)
		}
		return nil
	})
}
