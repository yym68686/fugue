package productioncontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/netip"
	"net/url"
	"regexp"
	"strings"
	"time"

	"fugue/internal/edgegroupfront"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	FrontWorkerResourcePrefix   = "front-worker/"
	FrontWorkerShadowSchema     = "fugue.release-guardian.node-worker-shadow.v1"
	FrontWorkerRetirementSchema = "edge-node-worker-retirement/v1"
)

var frontWorkerLabel = regexp.MustCompile(`^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$`)

// FrontWorkerResource identifies the shared prepared-journal fence. A Node
// activation generation is local to that Node and cannot be the group head.
func FrontWorkerResource(namespace, group string) string {
	if !frontWorkerName(namespace) || !frontWorkerName(group) {
		return ""
	}
	return FrontWorkerResourcePrefix + namespace + "/" + group
}

func FrontWorkerGroupFenceName(group string) string {
	return "fugue-authority-transition-prepared-" + group
}

type FrontWorkerShadow struct {
	Schema     string                                 `json:"schema"`
	Namespace  string                                 `json:"namespace"`
	GroupFence string                                 `json:"group_fence"`
	Component  string                                 `json:"component"`
	LeaseName  string                                 `json:"component_lease"`
	Journal    edgegroupfront.NodeWorkerJournal       `json:"journal"`
	Selection  edgegroupfront.NodeWorkerSelectionPlan `json:"selection"`
	Rollback   FrontWorkerRollbackObservation         `json:"rollback_observation"`
	Retirement *FrontWorkerRetirement                 `json:"retirement,omitempty"`
	ObservedAt time.Time                              `json:"observed_at"`
}

type FrontWorkerRollbackObservation struct {
	RetainedWorkerUID string `json:"retained_worker_uid"`
	RetainedTarget    string `json:"retained_target"`
	CanaryHost        string `json:"canary_host"`
	CanaryPath        string `json:"canary_path"`
	CanaryBodyDigest  string `json:"canary_body_digest"`
}

type FrontWorkerRetirement struct {
	Schema        string `json:"schema"`
	JournalDigest string `json:"journal_digest"`
	DaemonSet     string `json:"daemonset"`
	DaemonSetUID  string `json:"daemonset_uid"`
	Generation    int64  `json:"generation"`
}

func frontWorkerName(name string) bool {
	return len(name) <= 63 && frontWorkerLabel.MatchString(name)
}

func (s FrontWorkerShadow) validate() error {
	group := s.Journal.Recipient.Group
	selection, err := s.Journal.SelectionPlan()
	if err != nil || selection != s.Selection || s.Schema != FrontWorkerShadowSchema ||
		FrontWorkerResource(s.Namespace, group) == "" || s.GroupFence != FrontWorkerGroupFenceName(group) ||
		!frontWorkerName(s.Component) || !frontWorkerName(s.LeaseName) ||
		len(validation.IsDNS1123Subdomain(s.Journal.Recipient.Node)) != 0 ||
		s.ObservedAt.Before(s.Journal.CreatedAt) || !s.ObservedAt.Before(s.Journal.CreatedAt.Add(2*time.Minute)) {
		return errors.New("Front/Worker shadow does not bind the exact group, journal and successor states")
	}
	target, err := netip.ParseAddrPort(s.Rollback.RetainedTarget)
	path, pathErr := url.ParseRequestURI(s.Rollback.CanaryPath)
	wantPort := uint16(18443)
	if s.Journal.Expected.ActiveSlot == "b" {
		wantPort = 28443
	}
	if err != nil || target.Addr().Zone() != "" || target.Port() != wantPort || target.Addr().IsUnspecified() ||
		target.Addr().IsMulticast() || target.Addr().IsLoopback() || s.Rollback.RetainedWorkerUID != s.Journal.PredecessorWorkerUID ||
		len(validation.IsDNS1123Subdomain(s.Rollback.CanaryHost)) != 0 || pathErr != nil || path.Host != "" || !strings.HasPrefix(path.Path, "/") ||
		strings.ContainsAny(s.Rollback.CanaryPath, "\r\n?#") || !digestPattern.MatchString(s.Rollback.CanaryBodyDigest) {
		return errors.New("Front/Worker shadow does not bind the retained listener and route canary")
	}
	if p := s.Retirement; p != nil && (!s.Journal.FixedTransport || s.Journal.ProbeServiceUID == "" ||
		p.Schema != FrontWorkerRetirementSchema || p.JournalDigest != s.Journal.Digest ||
		len(validation.IsDNS1123Subdomain(p.DaemonSet)) != 0 || p.DaemonSetUID == "" || p.Generation < 1 || p.Generation == math.MaxInt64) {
		return errors.New("Front/Worker shadow does not bind the exact retirement plan")
	}
	return nil
}

func DecodeFrontWorkerShadow(raw []byte) (FrontWorkerShadow, error) {
	var shadow FrontWorkerShadow
	if len(raw) > MaxPayloadBytes || decodeControlJSON(raw, &shadow) != nil {
		return FrontWorkerShadow{}, errors.New("invalid Front/Worker shadow document")
	}
	return shadow, shadow.validate()
}

// DraftFrontWorker binds an observed physical change to a group ledger head.
// Applied versions identify the exact approved plan. Restoration keeps the
// previous plan digest and advances the ledger's independent sequence.
func DraftFrontWorker(cluster, request string, before Version, shadow FrontWorkerShadow) (Submission, error) {
	if err := shadow.validate(); err != nil {
		return Submission{}, err
	}
	if !before.valid() || before.Generation == math.MaxInt64 {
		return Submission{}, errors.New("invalid Front/Worker group predecessor version")
	}
	payload, err := json.Marshal(shadow)
	if err != nil {
		return Submission{}, err
	}
	resource := FrontWorkerResource(shadow.Namespace, shadow.Journal.Recipient.Group)
	intent, err := NewIntent(cluster, resource, request, FrontWorker, before.Generation, before.Digest,
		digest(payload), before.Digest, payload)
	return Submission{intent, payload}, err
}

func InspectFrontWorker(s Submission) (FrontWorkerShadow, error) {
	plan, err := DecodeFrontWorkerShadow(s.Payload)
	if err != nil {
		return FrontWorkerShadow{}, err
	}
	i := s.Intent
	want, err := DraftFrontWorker(i.ClusterID, i.RequestID, Version{i.Generation, i.CurrentDigest}, plan)
	if err != nil || want.Intent != i || !bytes.Equal(want.Payload, s.Payload) {
		return FrontWorkerShadow{}, errors.New("production intent does not bind this exact Front/Worker shadow")
	}
	return plan, nil
}

// MatchFreshFrontWorkerShadow compares the renewed runtime observation with
// the approved fields. The original sealed journal and timestamps stay in
// the approval history. observed_at is supplied by the live Guardian.
func MatchFreshFrontWorkerShadow(approved, fresh FrontWorkerShadow) error {
	if err := approved.validate(); err != nil {
		return err
	}
	if fresh.ObservedAt.Before(approved.ObservedAt) {
		return errors.New("Front/Worker shadow observation predates approval input")
	}
	fresh.ObservedAt = approved.ObservedAt
	got, err := json.Marshal(fresh)
	want, encodeErr := json.Marshal(approved)
	if err != nil || encodeErr != nil || !bytes.Equal(got, want) {
		return errors.New("Front/Worker live shadow differs from the approved plan")
	}
	return nil
}

// ReadApprovedFrontWorker is a read-only guard for Guardian dry runs. A
// provider writer still needs an exclusive durable claim and provider fence.
func (j *Journal) ReadApprovedFrontWorker(ctx context.Context, cluster, intentDigest, approvalDigest string) (FrontWorkerShadow, error) {
	if !digestPattern.MatchString(approvalDigest) {
		return FrontWorkerShadow{}, errors.New("invalid Front/Worker approval digest")
	}
	intent, raw, err := j.ReadIntent(ctx, intentDigest)
	if err != nil {
		return FrontWorkerShadow{}, err
	}
	plan, err := InspectFrontWorker(Submission{intent, raw})
	if err != nil || intent.ClusterID != cluster {
		return FrontWorkerShadow{}, errors.Join(errors.New("Front/Worker intent is outside the configured journal scope"), err)
	}
	if err := verifyRecordedApproval(ctx, j.db, j, intent, intentDigest, approvalDigest); err != nil {
		return FrontWorkerShadow{}, err
	}
	state, err := j.ReadResource(ctx, intent.scope())
	if err != nil {
		return FrontWorkerShadow{}, err
	}
	if state.Pending != "" {
		return FrontWorkerShadow{}, ErrResourceBusy
	}
	if state.LastVerified != (Version{intent.Generation, intent.CurrentDigest}) {
		return FrontWorkerShadow{}, ErrResourceDrift
	}
	return plan, nil
}
