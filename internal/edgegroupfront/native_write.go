package edgegroupfront

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
)

// Ordinary Worker recipients are the declared Front process incarnation, not
// an executable, filename, node address, or container supplied by a caller.
type NativeRecipient struct {
	PodUID      string `json:"pod_uid"`
	Node        string `json:"node"`
	Group       string `json:"group"`
	Incarnation string `json:"incarnation"`
}
type NativeWrite struct {
	Schema    string                `json:"schema"`
	Recipient NativeRecipient       `json:"recipient"`
	Operation string                `json:"operation"`
	Write     ActivationRepairWrite `json:"write"`
}

const NativeWriteSchema = "ordinary-worker-convergence/v1"
const NativeStatePath = "/var/lib/fugue-edge-front/activation.json"
const NativeJournalPath = "/var/run/fugue-worker-transaction/journal.json"

// A projection acknowledgement is not an activation receipt. Matching the
// digest of the whole canonical document also detects lagging projections of
// the same ConfigMap name after an earlier transaction has been archived.
type NativeProjection struct {
	Recipient NativeRecipient `json:"recipient"`
	Digest    string          `json:"content_digest"`
}

func ProjectedJournalDigest(raw []byte) (string, error) {
	if len(raw) == 0 || len(raw) > 256<<10 {
		return "", errors.New("ordinary Worker projection is empty or oversized")
	}
	return nativeDigest(raw), nil
}

type NativePermit struct {
	Recipient     NativeRecipient `json:"recipient"`
	RequestDigest string          `json:"request_digest"`
}
type NativeJournalScope struct {
	Schema        string                  `json:"schema"`
	Authorization string                  `json:"authorization"`
	Targets       map[string]NativePermit `json:"targets"`
}

const NativeJournalSchema = "ordinary-worker-native-journal/v1"

func nativeDigest(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// Exclude only the self-reference: the containing journal digest already
// commits to this permit, recipient, and exact per-node activation request.
func NativeRequestDigest(r ActivationRepairCASRequest) (string, error) {
	r.JournalDigest = ""
	b, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	return nativeDigest(append([]byte("ordinary-worker-cas/v1\n"), b...)), nil
}

// Only the kubelet projection of the actual group's immutable transaction
// can supply this input. A CLI envelope, its hash, and a matching Pod UID
// by themselves are NOT authority. Reject ordinary promotion journals,
// special repair journals, foreign recipients and non-canonical input.
func CheckNativeJournal(raw []byte, w NativeWrite) error {
	fail := errors.New("ordinary Worker write lacks its exact projected journal")
	if len(raw) == 0 || len(raw) > 256<<10 || w.Schema != NativeWriteSchema || w.Write.Schema != ActivationRepairWriteSchemaV1 ||
		!activationDigestPattern.MatchString(w.Write.Request.JournalDigest) || w.Write.Request.validate() != nil {
		return fail
	}
	var fields map[string]any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(&fields); err != nil || fields == nil {
		return fail
	}
	normalized, err := json.Marshal(fields)
	if err != nil || !bytes.Equal(raw, normalized) || fields["Schema"] != "edge-front-authority-repair/v1" || fields["Digest"] != w.Write.Request.JournalDigest {
		return fail
	}
	fields["Digest"] = ""
	body, err := json.Marshal(fields)
	if err != nil || nativeDigest(body) != w.Write.Request.JournalDigest {
		return fail
	}
	var entry struct {
		Ordinary            *NativeJournalScope
		AuthorizationDigest string
		CreatedAt           time.Time
		Plan                struct {
			CurrentUID, CurrentResourceVersion, Digest string
			TargetGeneration                           uint64
			Before                                     struct {
				GroupID, CurrentWorkerSlot, CurrentBundleGeneration, CurrentWorkerSourceSHA, CurrentWorkerImageDigest string
				PreviousWorkerSlot, PreviousBundleGeneration, PreviousWorkerSourceSHA, PreviousWorkerImageDigest      string
				CurrentFrontGeneration, PreviousFrontGeneration                                                       uint64
			}
			Members []struct {
				Node                            struct{ Name, UID string }
				Activation                      ActivationState
				FrontPodUID, TargetWorkerPodUID string
			}
		}
	}
	if err := json.Unmarshal(raw, &entry); err != nil || entry.Ordinary == nil || entry.Ordinary.Schema != NativeJournalSchema ||
		!activationDigestPattern.MatchString(entry.AuthorizationDigest) || entry.Ordinary.Authorization != entry.AuthorizationDigest ||
		len(entry.Ordinary.Targets) < 2 || len(entry.Ordinary.Targets) > 100 {
		return fail
	}
	id := w.Recipient
	plan := entry.Plan
	pf, ok := fields["Plan"].(map[string]any)
	if !ok || !activationDigestPattern.MatchString(plan.Digest) || plan.CurrentUID == "" || plan.CurrentResourceVersion == "" {
		return fail
	}
	pf["Digest"] = ""
	pb, err := json.Marshal(pf)
	if err != nil || nativeDigest(pb) != plan.Digest {
		return fail
	}
	b := plan.Before
	r := w.Write.Request
	if entry.CreatedAt.IsZero() || r.Expected.UpdatedAt.After(entry.CreatedAt) || !w.Write.ExpiresAt.After(entry.CreatedAt) ||
		len(plan.Members) != len(entry.Ordinary.Targets) || b.GroupID != id.Group ||
		r.CommittedGeneration != b.CurrentFrontGeneration || b.PreviousFrontGeneration == 0 || b.PreviousFrontGeneration >= b.CurrentFrontGeneration ||
		plan.TargetGeneration != b.CurrentFrontGeneration+2 ||
		r.Committed != (ActivationRepairRelease{b.CurrentWorkerSlot, b.CurrentBundleGeneration, b.CurrentWorkerSourceSHA, b.CurrentWorkerImageDigest}) ||
		r.Previous != (ActivationRepairRelease{b.PreviousWorkerSlot, b.PreviousBundleGeneration, b.PreviousWorkerSourceSHA, b.PreviousWorkerImageDigest}) {
		return fail
	}
	seenNodes, seenUID := map[string]bool{}, map[string]bool{}
	for _, m := range plan.Members {
		permit, ok := entry.Ordinary.Targets[m.Node.Name]
		if !ok || m.Node.Name == "" || m.Node.UID == "" || seenNodes[m.Node.Name] || seenUID[m.Node.UID] || permit.Recipient.Node != m.Node.Name || permit.Recipient.Group != b.GroupID ||
			permit.Recipient.PodUID == m.FrontPodUID || permit.Recipient.PodUID == m.TargetWorkerPodUID {
			return fail
		}
		seenNodes[m.Node.Name], seenUID[m.Node.UID] = true, true
		if m.Node.Name == id.Node && !sameRepairActivation(m.Activation, r.Expected) {
			return fail
		}
	}
	if !seenNodes[id.Node] {
		return fail
	}
	p, ok := entry.Ordinary.Targets[id.Node]
	digest, err := NativeRequestDigest(w.Write.Request)
	if err != nil || !ok || id.Node == "" || id.PodUID == "" || id.Group != w.Write.Request.Expected.GroupID ||
		!activationDigestPattern.MatchString(id.Incarnation) || p.Recipient != id || p.RequestDigest != digest ||
		(w.Operation != "commit" && (w.Operation != "prepare" || w.Write.Request.Expected.ActiveSlot == w.Write.Request.Committed.Slot)) {
		return fail
	}
	return nil
}
