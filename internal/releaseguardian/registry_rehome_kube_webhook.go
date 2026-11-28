package releaseguardian

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"time"

	"fugue/internal/productioncontrol"
	admission "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubejson "sigs.k8s.io/json"
)

// rehomePendingWrite must check the read-only durable journal for this
// pre-enrolled claim. It is never selected using data from AdmissionReview.
type rehomePendingWrite func(ctx context.Context, policyDigest, writeDigest string) error

type registryRehomeAdmissionHandler struct {
	gate   registryRehomeKubeFence
	review RegistryRehomeReview
	verify rehomePendingWrite
}

// newJournaledRegistryRehomeAdmission binds the webhook to an exact intent
// and expected claim before it serves any AdmissionReview. The reader's
// database identity does not need to grant reservations or modify the ledger.
func newJournaledRegistryRehomeAdmission(gate registryRehomeKubeFence, s productioncontrol.Submission,
	claim productioncontrol.Claim, reader *productioncontrol.RegistryRehomeAdmissionReader) (*registryRehomeAdmissionHandler, error) {
	review, err := InspectRegistryRehomeSubmission(s)
	if err != nil {
		return nil, err
	}
	identity, err := s.Intent.Digest()
	if err != nil || reader == nil || claim.IntentDigest != identity || claim.ResourceID != s.Intent.ResourceID ||
		claim.ClusterID != s.Intent.ClusterID || claim.Fence < 1 || !digestPattern.MatchString(claim.ApprovalDigest) ||
		gate.cutover.Key != review.Source.Key || gate.cutover.MonitorComponent != review.Draft.Component {
		return nil, errors.New("registry rehome webhook requires the exact pre-enrolled claim and Guardian scope")
	}
	return registryRehomeAdmissionServer(gate, review, func(ctx context.Context, policy, write string) error {
		return reader.VerifyPendingWrite(ctx, claim, policy, write)
	})
}

func registryRehomeAdmissionServer(gate registryRehomeKubeFence, review RegistryRehomeReview, verify rehomePendingWrite) (*registryRehomeAdmissionHandler, error) {
	id := review.Draft.Workload
	source := review.Source.Deployment
	if gate.hook == nil || verify == nil || id.APIVersion != "apps/v1" || id.Kind != "Deployment" || id.Namespace != gate.cutover.Namespace ||
		id.Name != gate.cutover.Deployment || source.Identity != id || source.UID != gate.scope.UID || source.Generation != gate.scope.Generation {
		return nil, errors.New("registry rehome webhook requires the exact reviewed Deployment and read-only claim verifier")
	}
	return &registryRehomeAdmissionHandler{gate, review, verify}, nil
}

func (h *registryRehomeAdmissionHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != registryRehomeAdmissionPath || r.URL.RawPath != "" || !rehomeWebhookQuery(r.URL.RawQuery) {
		http.NotFound(w, r)
		return
	}
	kind, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || kind != "application/json" {
		http.Error(w, "expected Kubernetes AdmissionReview", http.StatusUnsupportedMediaType)
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 8<<20))
	var input admission.AdmissionReview
	if err != nil || !strictRehomeKubeJSON(raw, &input) || input.APIVersion != "admission.k8s.io/v1" ||
		input.Kind != "AdmissionReview" || input.Request == nil || input.Response != nil || input.Request.UID == "" {
		http.Error(w, "invalid Kubernetes AdmissionReview", http.StatusBadRequest)
		return
	}
	response := &admission.AdmissionResponse{UID: input.Request.UID, Allowed: true}
	if reason := h.admit(r.Context(), input.Request); reason != "" {
		response.Allowed = false
		response.Result = &metav1.Status{Status: metav1.StatusFailure, Code: http.StatusForbidden,
			Reason: metav1.StatusReasonForbidden, Message: "Fugue registry rehome denied: " + reason}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(admission.AdmissionReview{TypeMeta: input.TypeMeta, Response: response})
}

func rehomeWebhookQuery(raw string) bool {
	if raw == "" {
		return true
	}
	// kube-apiserver supplies its remaining admission deadline as a query
	// parameter. It is not an input to the permission check.
	args, err := url.ParseQuery(raw)
	if err != nil || len(args) != 1 || len(args["timeout"]) != 1 {
		return false
	}
	timeout, err := time.ParseDuration(args["timeout"][0])
	return err == nil && timeout > 0 && timeout <= 4*time.Second
}

func strictRehomeKubeJSON(raw []byte, out any) bool {
	strict, err := kubejson.UnmarshalStrict(raw, out)
	return err == nil && len(strict) == 0
}

func (h *registryRehomeAdmissionHandler) admit(ctx context.Context, r *admission.AdmissionRequest) string {
	const denied = "not the exact enrolled Deployment update"
	id := h.review.Draft.Workload
	wantKind := metav1.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	wantResource := metav1.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	if r.Operation != admission.Update || r.UserInfo.Username != h.gate.scope.Writer || r.Namespace != id.Namespace || r.Name != id.Name ||
		r.Kind != wantKind || r.Resource != wantResource || r.SubResource != "" || r.RequestSubResource != "" ||
		(r.RequestKind != nil && *r.RequestKind != wantKind) || (r.RequestResource != nil && *r.RequestResource != wantResource) {
		return denied
	}
	var before, proposed appsv1.Deployment
	if !strictRehomeKubeJSON(r.OldObject.Raw, &before) || !strictRehomeKubeJSON(r.Object.Raw, &proposed) ||
		before.APIVersion != id.APIVersion || proposed.APIVersion != id.APIVersion || before.Kind != id.Kind || proposed.Kind != id.Kind {
		return denied
	}
	cas, err := prepareRegistryRehomeDeploymentCAS(h.review, &before)
	if err != nil {
		return denied
	}
	policy, write, err := h.gate.digests(cas)
	if err != nil {
		return denied
	}
	expected := cas.expected
	expected.Generation = before.Generation + 1
	// Kubernetes owns managedFields and computes it during admission. All
	// other metadata, the whole workload spec and status must match.
	expected.ManagedFields, proposed.ManagedFields = nil, nil
	if !apiequality.Semantic.DeepEqual(expected, &proposed) {
		return "object differs from the reviewed image-only change"
	}
	// A provider-side dry-run can witness the active rule without a claim.
	// Only the Kubernetes API's AdmissionReview dry-run bit is accepted.
	if r.DryRun != nil && *r.DryRun {
		return ""
	}
	if err := h.verify(ctx, policy, write); err != nil {
		return "no matching live journal claim and approval"
	}
	return ""
}
