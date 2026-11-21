package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/edgegroupfront"
	"fugue/internal/releaseguardian"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const frontRepairJournalSchema = "edge-front-authority-repair/v1"

type frontRepairJournal struct {
	Schema              string
	Plan                frontRepairPlan
	AuthorizationDigest string
	CreatedAt           time.Time
	Digest              string
}

func sealFrontRepairJournal(plan frontRepairPlan, authorizationDigest string, now time.Time) (frontRepairJournal, error) {
	journal := frontRepairJournal{Schema: frontRepairJournalSchema, Plan: plan, AuthorizationDigest: authorizationDigest, CreatedAt: now.UTC()}
	value, err := journal.validatedDigest()
	if err != nil {
		return frontRepairJournal{}, err
	}
	journal.Digest = value
	return journal, nil
}

func (journal frontRepairJournal) validatedDigest() (string, error) {
	plan := journal.Plan
	before := plan.Before
	planDigest, err := frontRepairPlanDigest(plan)
	if err != nil || plan.Digest != planDigest || before.Validate() != nil || before.CurrentFrontGeneration <= before.PreviousFrontGeneration ||
		before.PreviousFrontGeneration == 0 || before.CurrentFrontGeneration > math.MaxUint64-3 || before.AuthorityEpoch == math.MaxInt64 ||
		plan.TargetGeneration != before.CurrentFrontGeneration+2 || !frontRepairIdentity(plan.CurrentUID) || !frontRepairIdentity(plan.CurrentResourceVersion) ||
		len(plan.Members) < 2 || len(plan.Members) > 100 || journal.Schema != frontRepairJournalSchema ||
		!exactSHA256Digest(journal.AuthorizationDigest) || journal.CreatedAt.IsZero() {
		return "", errors.New("Front repair journal identity is invalid")
	}
	// Revalidate the sealed roster and per-node predecessor classifications;
	// these are journal facts, not live readiness observations.
	var expected []frontRepairNode
	for _, member := range plan.Members {
		expected = append(expected, member.Node)
		if member.Activation.UpdatedAt.After(journal.CreatedAt) {
			return "", errors.New("Front repair journal predates its node witness")
		}
	}
	rederived, err := bindFrontRepairPredecessors(before, plan.CurrentUID, plan.CurrentResourceVersion, expected, plan.Members)
	if err != nil || rederived.Digest != plan.Digest {
		return "", errors.New("Front repair journal roster or predecessor binding is invalid")
	}
	journal.Digest = ""
	raw, err := declarativerelease.CanonicalJSON(journal)
	if err != nil || len(raw) > 256<<10 {
		return "", errors.New("Front repair journal encoding is invalid")
	}
	return shaDigest(raw), nil
}

func (journal frontRepairJournal) validate() error {
	value, err := journal.validatedDigest()
	if err != nil || value != journal.Digest {
		return errors.Join(errors.New("Front repair journal digest is invalid"), err)
	}
	return nil
}

func (journal frontRepairJournal) activationRequest(member frontRepairMember) edgegroupfront.ActivationRepairCASRequest {
	before := journal.Plan.Before
	return edgegroupfront.ActivationRepairCASRequest{JournalDigest: journal.Digest, Expected: member.Activation, CommittedGeneration: before.CurrentFrontGeneration,
		Committed: edgegroupfront.ActivationRepairRelease{Slot: string(before.CurrentWorkerSlot), BundleGeneration: before.CurrentBundleGeneration, WorkerSourceCommit: before.CurrentWorkerSourceSHA, WorkerImageDigest: before.CurrentWorkerImageDigest},
		Previous:  edgegroupfront.ActivationRepairRelease{Slot: string(before.PreviousWorkerSlot), BundleGeneration: before.PreviousBundleGeneration, WorkerSourceCommit: before.PreviousWorkerSourceSHA, WorkerImageDigest: before.PreviousWorkerImageDigest}}
}

type frontRepairProgress struct {
	Remaining    map[string]edgegroupfront.ActivationRepairCASRequest
	WorkersReady bool
	Converged    bool
}

// resume recognizes only the journal's exact predecessor or exact new state
// on each original Node/Pod. Lost replies become observations, never rewrites.
// Every live Front and target Worker must be healthy before Converged is true.
func (journal frontRepairJournal) resume(refreshed []frontRepairWitness, now time.Time) (frontRepairProgress, error) {
	if journal.validate() != nil || len(refreshed) != len(journal.Plan.Members) {
		return frontRepairProgress{}, errors.New("Front repair evidence does not cover the durable journal")
	}
	want := make(map[string]frontRepairMember, len(journal.Plan.Members))
	for _, member := range journal.Plan.Members {
		want[member.Node.Name] = member
	}
	pending := make(map[string]edgegroupfront.ActivationRepairCASRequest)
	healthy := true
	var publication string
	before := journal.Plan.Before
	target := releaseguardian.FrontAuthorityTarget{GroupID: before.GroupID, FrontBundleGeneration: before.CurrentBundleGeneration}
	for _, seen := range refreshed {
		name := seen.Node.Name
		member, exists := want[name]
		identity := seen.frontRepairMember
		identity.Activation = member.Activation
		if !exists || identity != member || !frontRepairLiveMatches(seen.LiveFront, seen.Activation) {
			return frontRepairProgress{}, fmt.Errorf("Front repair member %s no longer proves its Node/Pod and live activation", name)
		}
		delete(want, name)
		request := journal.activationRequest(member)
		observed := seen.Activation
		observed.UpdatedAt = observed.UpdatedAt.UTC()
		original := member.Activation
		original.UpdatedAt = original.UpdatedAt.UTC()
		if observed == original {
			pending[name] = request
		} else if !request.MatchesActivation(observed) || observed.UpdatedAt.Before(journal.CreatedAt) {
			return frontRepairProgress{}, fmt.Errorf("Front repair member %s is outside the journal's exact transition", name)
		}
		worker := seen.TargetWorkerHealth
		if !frontRepairWorkerReady(seen, target, now) || (publication != "" && publication != worker.BundleVersion) {
			healthy = false
		}
		publication = worker.BundleVersion
	}
	return frontRepairProgress{Remaining: pending, WorkersReady: healthy, Converged: healthy && len(pending) == 0}, nil
}

// frontRepairStore is used only from the reviewed repair path (not normal
// promotion). Both Lease claims must be live on the Kubernetes API server.
// No production command is wired to this store until the repair lane exists.
type frontRepairStore struct {
	client    kubernetes.Interface
	namespace string
	authority *releaseguardian.AuthorityStore
	locks     *frontRepairLeasePair
}

func newFrontRepairStore(locks *frontRepairLeasePair) (frontRepairStore, error) {
	if locks == nil {
		return frontRepairStore{}, errors.New("Front repair requires the exact Kubernetes Lease pair")
	}
	authority, err := releaseguardian.NewAuthorityStore(locks.client, locks.namespace)
	if err != nil {
		return frontRepairStore{}, err
	}
	return frontRepairStore{client: locks.client, namespace: locks.namespace, authority: authority, locks: locks}, nil
}

func (store frontRepairStore) requireHeld(ctx context.Context, group string) error {
	if store.client == nil || store.authority == nil || store.locks == nil || store.namespace != store.locks.namespace {
		return errors.New("Front repair store must be bound to both authority locks")
	}
	return store.locks.requireHeld(ctx, group)
}

func (store frontRepairStore) load(ctx context.Context, group string) (frontRepairJournal, error) {
	cms := store.client.CoreV1().ConfigMaps(store.namespace)
	object, err := cms.Get(ctx, releaseguardian.FrontRepairFenceName(group), metav1.GetOptions{})
	if err != nil {
		return frontRepairJournal{}, err
	}
	return decodeFrontRepairJournal(object, group, releaseguardian.FrontRepairJournalKind)
}

func decodeFrontRepairJournal(object *corev1.ConfigMap, group, kind string) (frontRepairJournal, error) {
	var journal frontRepairJournal
	if object.DeletionTimestamp != nil || object.Immutable == nil || !*object.Immutable || len(object.Data) != 1 || len(object.BinaryData) != 0 ||
		object.Labels["fugue.pro/group"] != group || object.Labels["fugue.pro/authority-kind"] != kind ||
		len(object.Data["journal.json"]) > 256<<10 || decodeStrictJSON([]byte(object.Data["journal.json"]), &journal) != nil ||
		journal.validate() != nil || journal.Plan.Before.GroupID != group {
		return frontRepairJournal{}, errors.New("persisted Front repair journal is invalid")
	}
	return journal, nil
}

func (store frontRepairStore) create(ctx context.Context, journal frontRepairJournal) error {
	group := journal.Plan.Before.GroupID
	if err := store.requireHeld(ctx, group); err != nil {
		return err
	}
	if err := journal.validate(); err != nil {
		return err
	}
	if _, err := store.loadCompletion(ctx, group, journal.AuthorizationDigest); !apierrors.IsNotFound(err) {
		return errors.Join(errors.New("Front repair approval already has a completion archive or cannot be checked"), err)
	}
	plan := journal.Plan
	if err := store.authority.RequireNoActivatedTransition(ctx, group); err != nil {
		return err
	}
	current, uid, rv, err := store.authority.LoadCurrent(ctx, group)
	if err != nil || current != plan.Before || string(uid) != plan.CurrentUID || rv != plan.CurrentResourceVersion {
		return errors.Join(errors.New("Front repair journal predecessor changed before creation"), err)
	}
	raw, err := declarativerelease.CanonicalJSON(journal)
	if err != nil {
		return err
	}
	immutable := true
	object := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: releaseguardian.FrontRepairFenceName(group), Namespace: store.namespace, Labels: map[string]string{
		"fugue.pro/group": group, "fugue.pro/authority-kind": releaseguardian.FrontRepairJournalKind,
	}}, Immutable: &immutable, Data: map[string]string{"journal.json": string(raw)}}
	writeCtx, cancel, err := store.locks.mutationContext(ctx, group)
	if err != nil {
		return err
	}
	defer cancel()
	if _, err := store.client.CoreV1().ConfigMaps(store.namespace).Create(writeCtx, object, metav1.CreateOptions{}); err != nil {
		existing, loadErr := store.load(ctx, group)
		if loadErr != nil || existing.Digest != journal.Digest {
			return errors.Join(err, loadErr)
		}
	}
	return nil
}

// writePlan reauthorizes only the outstanding CAS requests from the durable
// journal. It must be repeated after every observation or failed reply; the
// node CAS independently rechecks the predecessor and preparation at write.
func (store frontRepairStore) writePlan(ctx context.Context, group, journalDigest string, refreshed []frontRepairWitness,
	preparations map[string]edgegroupfront.PublicationPreparation) (frontRepairProgress, error) {
	if err := store.requireHeld(ctx, group); err != nil {
		return frontRepairProgress{}, err
	}
	journal, err := store.load(ctx, group)
	if err != nil || journal.Digest != journalDigest {
		return frontRepairProgress{}, errors.Join(errors.New("Front repair must resume the exact durable journal"), err)
	}
	plan := journal.Plan
	if err := store.authority.RequireNoActivatedTransition(ctx, group); err != nil {
		return frontRepairProgress{}, err
	}
	current, uid, rv, err := store.authority.LoadCurrent(ctx, group)
	if err != nil || current != plan.Before || string(uid) != plan.CurrentUID || rv != plan.CurrentResourceVersion {
		return frontRepairProgress{}, errors.Join(errors.New("Front repair CurrentAuthority changed before node writes"), err)
	}
	now := store.locks.now().UTC()
	progress, err := journal.resume(refreshed, now)
	if err != nil || !progress.WorkersReady {
		return frontRepairProgress{}, errors.Join(errors.New("Front repair target Workers do not prove one healthy applied publication"), err)
	}
	for node, request := range progress.Remaining {
		if request.Expected.ActiveSlot == request.Committed.Slot {
			continue
		}
		p, found := preparations[node]
		if !found || !frontRepairPreparationReady(p, request.Expected, plan.Before, now) {
			return frontRepairProgress{}, fmt.Errorf("Front repair member %s has no fresh publication preparation", node)
		}
	}
	if err := store.requireHeld(ctx, group); err != nil {
		return frontRepairProgress{}, err
	}
	return progress, nil
}

// commit advances only the group generation and its epoch after exact runtime
// convergence. Refresh the witnesses under both locks directly before calling.
func (store frontRepairStore) commit(ctx context.Context, group, journalDigest string, refreshed []frontRepairWitness) error {
	if err := store.requireHeld(ctx, group); err != nil {
		return err
	}
	journal, err := store.load(ctx, group)
	if err != nil || journal.Digest != journalDigest {
		return errors.Join(errors.New("Front repair cannot commit without the exact durable journal"), err)
	}
	progress, err := journal.resume(refreshed, store.locks.now().UTC())
	if err != nil || !progress.Converged {
		return errors.Join(errors.New("Front repair cohort has not converged"), err)
	}
	if err := store.authority.RequireNoActivatedTransition(ctx, group); err != nil {
		return err
	}
	plan := journal.Plan
	after := journal.committedAuthority()
	current, uid, rv, err := store.authority.LoadCurrent(ctx, group)
	if err != nil || string(uid) != plan.CurrentUID {
		return errors.Join(errors.New("Front repair CurrentAuthority was replaced"), err)
	}
	if current == after {
		return nil // recover an unknown Update outcome; no repeated write
	}
	if current != plan.Before || rv != plan.CurrentResourceVersion {
		return errors.New("Front repair CurrentAuthority changed before commit")
	}
	cms := store.client.CoreV1().ConfigMaps(store.namespace)
	object, err := cms.Get(ctx, "fugue-current-authority-"+group, metav1.GetOptions{})
	if err != nil {
		return err
	}
	var check releaseguardian.CurrentAuthority
	if object.UID != uid || object.ResourceVersion != rv || decodeStrictJSON([]byte(object.Data["authority.json"]), &check) != nil || check != current {
		return errors.New("Front repair CurrentAuthority CAS changed")
	}
	raw, err := declarativerelease.CanonicalJSON(after)
	if err != nil {
		return err
	}
	object.Data["authority.json"] = string(raw)
	writeCtx, cancel, err := store.locks.mutationContext(ctx, group)
	if err != nil {
		return err
	}
	defer cancel()
	if _, err := cms.Update(writeCtx, object, metav1.UpdateOptions{}); err != nil {
		actual, observedUID, _, readErr := store.authority.LoadCurrent(ctx, group)
		if apierrors.IsConflict(err) || readErr != nil || observedUID != uid || actual != after {
			return errors.Join(err, readErr)
		}
	}
	return nil
}

func (journal frontRepairJournal) committedAuthority() releaseguardian.CurrentAuthority {
	after := journal.Plan.Before
	after.CurrentFrontGeneration, after.AuthorityEpoch = journal.Plan.TargetGeneration, after.AuthorityEpoch+1
	return after
}

const frontRepairArchiveKind = "front-repair-completed"

func frontRepairArchiveName(group, authorization string) string {
	return "fugue-front-repair-completed-" + group + "-" + authorization[7:31]
}

// Key completion by the approved intent, which is stable across retries.
// The journal's own digest also includes creation time and is only known
// after preparation. An absent fence must not allow a completed intent to
// silently start another transaction.
func (store frontRepairStore) loadCompletion(ctx context.Context, group, authorization string) (frontRepairJournal, error) {
	if !exactSHA256Digest(authorization) {
		return frontRepairJournal{}, errors.New("Front repair completion authorization is invalid")
	}
	object, err := store.client.CoreV1().ConfigMaps(store.namespace).Get(ctx, frontRepairArchiveName(group, authorization), metav1.GetOptions{})
	if err != nil {
		return frontRepairJournal{}, err
	}
	stored, err := decodeFrontRepairJournal(object, group, frontRepairArchiveKind)
	if err != nil || stored.AuthorizationDigest != authorization {
		return frontRepairJournal{}, errors.Join(errors.New("Front repair completion archive conflicts"), err)
	}
	return stored, nil
}

// finish archives exact completion before releasing the shared fence. It
// never deletes a pending, replaced, or ordinary promotion journal.
func (store frontRepairStore) finish(ctx context.Context, wanted frontRepairJournal, refreshed []frontRepairWitness) error {
	group, journalDigest := wanted.Plan.Before.GroupID, wanted.Digest
	if err := store.requireHeld(ctx, group); err != nil {
		return err
	}
	if wanted.validate() != nil {
		return errors.New("Front repair completion digest is invalid")
	}
	cms := store.client.CoreV1().ConfigMaps(store.namespace)
	verifyArchive := func() error {
		stored, err := store.loadCompletion(ctx, group, wanted.AuthorizationDigest)
		if err != nil || stored.Digest != journalDigest {
			return errors.Join(errors.New("Front repair completion archive conflicts"), err)
		}
		return nil
	}
	fenceName := releaseguardian.FrontRepairFenceName(group)
	fence, err := cms.Get(ctx, fenceName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return verifyArchive()
	}
	if err != nil {
		return err
	}
	journal, err := decodeFrontRepairJournal(fence, group, releaseguardian.FrontRepairJournalKind)
	if err != nil || journal.Digest != journalDigest || fence.UID == "" || fence.ResourceVersion == "" {
		return errors.Join(errors.New("Front repair completion fence changed"), err)
	}
	progress, err := journal.resume(refreshed, store.locks.now().UTC())
	if err != nil || !progress.Converged {
		return errors.Join(errors.New("Front repair cannot release the fence without live convergence"), err)
	}
	current, uid, _, err := store.authority.LoadCurrent(ctx, group)
	if err != nil || string(uid) != journal.Plan.CurrentUID || current != journal.committedAuthority() {
		return errors.Join(errors.New("Front repair authority is not committed"), err)
	}
	if err := store.authority.RequireNoActivatedTransition(ctx, group); err != nil {
		return err
	}
	archive := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: frontRepairArchiveName(group, journal.AuthorizationDigest), Namespace: store.namespace, Labels: map[string]string{
		"fugue.pro/group": group, "fugue.pro/authority-kind": frontRepairArchiveKind,
	}}, Immutable: fence.Immutable, Data: map[string]string{"journal.json": fence.Data["journal.json"]}}
	writeCtx, cancel, err := store.locks.mutationContext(ctx, group)
	if err != nil {
		return err
	}
	_, createErr := cms.Create(writeCtx, archive, metav1.CreateOptions{})
	cancel()
	if err := verifyArchive(); err != nil {
		return errors.Join(createErr, err)
	}
	writeCtx, cancel, err = store.locks.mutationContext(ctx, group)
	if err != nil {
		return err
	}
	defer cancel()
	err = cms.Delete(writeCtx, fenceName, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &fence.UID, ResourceVersion: &fence.ResourceVersion}})
	if apierrors.IsNotFound(err) {
		return verifyArchive()
	}
	if err != nil && !apierrors.IsConflict(err) {
		// A timed-out Delete may have already released the exact fence. Read
		// back rather than deleting a potential replacement.
		readCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer stop()
		if _, readErr := cms.Get(readCtx, fenceName, metav1.GetOptions{}); apierrors.IsNotFound(readErr) {
			stored, verifyErr := store.loadCompletion(readCtx, group, journal.AuthorizationDigest)
			if verifyErr == nil && stored.Digest == journalDigest {
				return nil
			}
		}
	}
	return err
}
