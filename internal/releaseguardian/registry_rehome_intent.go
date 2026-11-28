package releaseguardian

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/productioncontrol"
	"github.com/google/go-containerregistry/pkg/name"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type RegistryRehomeObservedObject struct {
	Identity   declarativerelease.ResourceIdentity `json:"identity"`
	UID        string                              `json:"uid"`
	Generation int64                               `json:"generation"`
	SpecDigest string                              `json:"specDigest"`
}

type RegistryRehomeObservedService struct {
	Live        RegistryRehomeObservedObject `json:"live"`
	Declaration RegistryRehomeService        `json:"declaration"`
}

type RegistryRehomeSource struct {
	Key           Key                             `json:"key"`
	ArchiveDigest string                          `json:"archiveDigest"`
	MonitorDigest string                          `json:"monitorDigest"`
	MonitorUID    string                          `json:"monitorUid"`
	Desired       RecoveryRecordRef               `json:"desired"`
	Version       productioncontrol.Version       `json:"version"`
	Deployment    RegistryRehomeObservedObject    `json:"deployment"`
	Services      []RegistryRehomeObservedService `json:"services"`
}

type RegistryRehomeReview struct {
	Source RegistryRehomeSource `json:"source"`
	Draft  RegistryRehomeDraft  `json:"draft"`
}

type rehomeJournalReader interface {
	ReadResource(context.Context, productioncontrol.Scope) (productioncontrol.ResourceState, error)
}

// ObserveRegistryRehomeSource checks the archived source against live
// provider reads. It is not proof of writer revocation or permission to
// anchor the journal. It works before an independent mirror is available.
func ObserveRegistryRehomeSource(ctx context.Context, client kubernetes.Interface, raw []byte, pin, group string) (RegistryRehomeSource, error) {
	archive, err := readReleaseRecoveryArchive(raw, pin)
	if err != nil {
		return RegistryRehomeSource{}, err
	}
	return observeRegistryRehomeSource(ctx, client, archive, group)
}

func observeRegistryRehomeSource(ctx context.Context, client kubernetes.Interface, archive verifiedReleaseRecoveryArchive, group string) (RegistryRehomeSource, error) {
	pair := archive.report.Pair
	key := Key{Component: pair.Component, Group: group}
	current := archive.forward
	workload := recoveryWorkloadKey(current.workload).resource
	if client == nil || key.Validate() != nil || workload.APIVersion != "apps/v1" || workload.Kind != "Deployment" {
		return RegistryRehomeSource{}, errors.New("registry rehome source requires one scoped Kubernetes Deployment")
	}
	if current.release.Delivery != nil && (current.release.Delivery.Writer != "guardian" || current.release.Delivery.Group != group) {
		return RegistryRehomeSource{}, errors.New("registry rehome group differs from the sealed release")
	}
	if current.release.Transition != nil || len(current.release.RuntimeResourcesFromForward) != 0 || current.workload.NodeParticipation != "" {
		return RegistryRehomeSource{}, errors.New("registry rehome cannot delegate other runtime transitions")
	}
	latest, err := ObserveSealedRecoveryPair(ctx, client.CoreV1(), pair.Namespace, pair.Component)
	if err != nil || latest.Pointer.UID != pair.Pointer.UID || latest.Forward != pair.Forward || latest.Rollback != pair.Rollback {
		return RegistryRehomeSource{}, errors.Join(errors.New("verified recovery archive no longer matches the production monitor"), err)
	}
	stable, _, err := canonicalStableReleaseRecord(key, current.files)
	if err != nil {
		return RegistryRehomeSource{}, err
	}
	source := RegistryRehomeSource{Key: key, ArchiveDigest: archive.report.ArchiveDigest,
		MonitorDigest: current.version.Monitor.RecordDigest, MonitorUID: pair.Pointer.UID,
		Version: productioncontrol.Version{Generation: 1, Digest: stable.RecordDigest}}
	maps := client.CoreV1().ConfigMaps(pair.Namespace)
	want, err := maps.Get(ctx, desiredName(key), metav1.GetOptions{})
	switch {
	case err == nil:
		var desired DesiredRelease
		if want.DeletionTimestamp != nil || want.UID == "" || want.ResourceVersion == "" ||
			!stringMapEqual(want.Labels, guardianLabels(key)) || hasNativePublicationMarker(want.Annotations) ||
			decodeStrict([]byte(want.Data["desired.json"]), &desired) != nil || desired.Validate() != nil ||
			desired.Key() != key {
			return RegistryRehomeSource{}, errors.New("registry rehome cannot adopt a pending or already native release")
		}
		if desired.RecordDigest != stable.RecordDigest {
			// Guardian keeps the original desired digest after a successful
			// rollout, even though the monitor derives a distinct envelope.
			// An exact immutable bundle and settled status must prove the alias.
			recordMap, err := maps.Get(ctx, releaseRecordName(key, desired.RecordDigest), metav1.GetOptions{})
			if err != nil {
				return RegistryRehomeSource{}, fmt.Errorf("read retained Guardian release alias: %w", err)
			}
			record, _, err := validateGuardianRecordArtifact(recordMap)
			files, filesErr := executionFilesFromStrings(recordMap.Data)
			bundle, bundleErr := DecodeExecutionBundle(files, key)
			derived, deriveErr := bundle.ReleaseRecord(key, record.LKGRecordDigest)
			if err != nil || recordMap.DeletionTimestamp != nil || recordMap.UID == "" || recordMap.ResourceVersion == "" ||
				filesErr != nil || bundleErr != nil || deriveErr != nil || record != derived ||
				record.RecordDigest != desired.RecordDigest || !SameCommittedReleaseTarget(record, stable) ||
				bundle.Artifact != current.version.Receipt || !bytes.Equal(bundle.Forward, current.forward) {
				return RegistryRehomeSource{}, errors.New("registry rehome desired pointer is not the exact verified release alias")
			}
			statusMap, err := maps.Get(ctx, statusName(key), metav1.GetOptions{})
			if err != nil {
				return RegistryRehomeSource{}, fmt.Errorf("read retained Guardian release status: %w", err)
			}
			var status ReleaseStatus
			statusErr := decodeStrict([]byte(statusMap.Data["status.json"]), &status)
			observedAt, timeErr := time.Parse(time.RFC3339Nano, status.ObservedAt)
			if statusMap.DeletionTimestamp != nil || !stringMapEqual(statusMap.Labels, guardianLabels(key)) ||
				statusErr != nil || timeErr != nil || status.Validate(observedAt) != nil || status.Key() != key ||
				(status.State != StateStable && status.State != StateLKGStable) || status.CurrentRecordDigest != desired.RecordDigest ||
				status.TargetRecordDigest != desired.RecordDigest || status.LastSuccessfulLKG != desired.RecordDigest {
				return RegistryRehomeSource{}, errors.New("registry rehome desired alias is not settled")
			}
		}
		source.Desired = recoveryRef(want)
		source.Version = productioncontrol.Version{Generation: desired.Generation, Digest: desired.RecordDigest}
	case !apierrors.IsNotFound(err):
		return RegistryRehomeSource{}, fmt.Errorf("read existing release pointer: %w", err)
	}
	deployment, err := client.AppsV1().Deployments(workload.Namespace).Get(ctx, workload.Name, metav1.GetOptions{})
	if err != nil {
		return RegistryRehomeSource{}, fmt.Errorf("read registry rehome Deployment: %w", err)
	}
	replicas := derefReplicas(deployment.Spec.Replicas)
	if deployment.DeletionTimestamp != nil || deployment.UID == "" || deployment.ResourceVersion == "" ||
		deployment.Spec.Paused || replicas < 1 || deployment.Generation < 1 || deployment.Status.ObservedGeneration != deployment.Generation ||
		deployment.Status.UpdatedReplicas != replicas || deployment.Status.ReadyReplicas != replicas || deployment.Status.AvailableReplicas != replicas {
		return RegistryRehomeSource{}, errors.New("registry rehome source Deployment is not settled and Ready")
	}
	if err := nativeDeploymentConverged(deployment, workload, current.prepared.Forward, current.forward, current.release); err != nil {
		return RegistryRehomeSource{}, err
	}
	pod, err := rehomeJSONMap(deployment.Spec.Template.Spec)
	if err != nil {
		return RegistryRehomeSource{}, err
	}
	if _, err := singleArtifactDeploymentContainers(pod, current.prepared.Forward.ImageRef); err != nil {
		return RegistryRehomeSource{}, err
	}
	spec, err := declarativerelease.CanonicalJSON(deployment.Spec)
	if err != nil {
		return RegistryRehomeSource{}, err
	}
	source.Deployment = RegistryRehomeObservedObject{workload, string(deployment.UID), deployment.Generation, digest(spec)}

	ids, err := declarativerelease.ResourceSetIdentities(current.forward)
	if err != nil {
		return RegistryRehomeSource{}, err
	}
	source.Services = make([]RegistryRehomeObservedService, 0, len(ids)-1)
	for _, id := range ids {
		if id == workload {
			continue
		}
		if id.Kind != "Service" || id.APIVersion != "v1" || id.Namespace != workload.Namespace {
			return RegistryRehomeSource{}, errors.New("registry rehome can only observe Service companions")
		}
		wanted, err := declarativerelease.ResourceSetItem(current.forward, id)
		if err != nil {
			return RegistryRehomeSource{}, err
		}
		decl, err := registryRehomeServiceShape(wanted)
		if err != nil {
			return RegistryRehomeSource{}, err
		}
		live, err := client.CoreV1().Services(id.Namespace).Get(ctx, id.Name, metav1.GetOptions{})
		if err != nil {
			return RegistryRehomeSource{}, fmt.Errorf("read companion Service: %w", err)
		}
		if live.DeletionTimestamp != nil || live.UID == "" || live.ResourceVersion == "" {
			return RegistryRehomeSource{}, errors.New("companion Service has no stable provider identity")
		}
		live.APIVersion, live.Kind = id.APIVersion, id.Kind
		observed, err := rehomeJSONMap(live)
		if err != nil {
			return RegistryRehomeSource{}, err
		}
		if mismatch := declarativerelease.ResourceDesiredMismatch(wanted, observed); mismatch != "" {
			return RegistryRehomeSource{}, fmt.Errorf("companion Service differs from its sealed declaration at %s", mismatch)
		}
		serviceSpec, err := declarativerelease.CanonicalJSON(live.Spec)
		if err != nil {
			return RegistryRehomeSource{}, err
		}
		source.Services = append(source.Services, RegistryRehomeObservedService{
			Live:        RegistryRehomeObservedObject{id, string(live.UID), live.Generation, digest(serviceSpec)},
			Declaration: RegistryRehomeService{id, digest(decl)},
		})
	}
	return source, nil
}

func rehomeJSONMap(value any) (map[string]any, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// DraftRegistryRehomeSubmission binds the independently verified mirror to
// a live precondition and the ordinary Deployment journal head. It only
// reads providers; ordinary execution explicitly rejects this operation.
func DraftRegistryRehomeSubmission(ctx context.Context, client kubernetes.Interface, journal rehomeJournalReader, registry *IndependentRegistry, raw []byte, pin, group, cluster, request, repository string) (productioncontrol.Submission, error) {
	archive, err := readReleaseRecoveryArchive(raw, pin)
	if err != nil {
		return productioncontrol.Submission{}, err
	}
	source, err := observeRegistryRehomeSource(ctx, client, archive, group)
	if err != nil {
		return productioncontrol.Submission{}, err
	}
	scope := productioncontrol.Scope{ClusterID: cluster, ResourceID: nativeKubernetesResourceID(source.Deployment.Identity)}
	if journal == nil || registry == nil {
		return productioncontrol.Submission{}, errors.New("registry rehome review requires the verified journal and independent registry")
	}
	state, err := journal.ReadResource(ctx, scope)
	if err != nil || state.Scope != scope || state.LastVerified != source.Version || state.Pending != "" {
		return productioncontrol.Submission{}, errors.Join(errors.New("registry rehome needs a settled journal head matching the production Deployment"), err)
	}
	draft, err := registry.DraftRegistryRehome(ctx, raw, pin, repository, repository)
	if err != nil {
		return productioncontrol.Submission{}, err
	}
	after, err := observeRegistryRehomeSource(ctx, client, archive, group)
	if err != nil || !reflect.DeepEqual(source, after) {
		return productioncontrol.Submission{}, errors.Join(errors.New("registry rehome provider changed during mirror verification"), err)
	}
	review := RegistryRehomeReview{source, draft}
	id := source.Deployment.Identity
	live, err := client.AppsV1().Deployments(id.Namespace).Get(ctx, id.Name, metav1.GetOptions{})
	if err != nil {
		return productioncontrol.Submission{}, fmt.Errorf("read registry rehome Deployment for shadow projection: %w", err)
	}
	if _, err := prepareRegistryRehomeDeploymentCAS(review, live); err != nil {
		return productioncontrol.Submission{}, fmt.Errorf("registry rehome cannot build a reviewed image-only patch: %w", err)
	}
	fresh, err := journal.ReadResource(ctx, scope)
	if err != nil || fresh != state {
		return productioncontrol.Submission{}, errors.Join(errors.New("registry rehome journal head changed during verification"), err)
	}
	rawPayload, err := declarativerelease.CanonicalJSON(review)
	if err != nil {
		return productioncontrol.Submission{}, err
	}
	intent, err := productioncontrol.NewIntent(cluster, scope.ResourceID, request, productioncontrol.ComponentRegistryMove,
		source.Version.Generation, source.Version.Digest, draft.DraftDigest, source.Version.Digest, rawPayload)
	if err != nil {
		return productioncontrol.Submission{}, err
	}
	submission := productioncontrol.Submission{Intent: intent, Payload: rawPayload}
	if _, err := InspectRegistryRehomeSubmission(submission); err != nil {
		return productioncontrol.Submission{}, err
	}
	return submission, nil
}

func InspectRegistryRehomeSubmission(s productioncontrol.Submission) (RegistryRehomeReview, error) {
	var p RegistryRehomeReview
	if s.Intent.Operation != productioncontrol.ComponentRegistryMove || s.Intent.VerifyPayload(s.Payload) != nil ||
		decodeStrict(s.Payload, &p) != nil {
		return RegistryRehomeReview{}, errors.New("invalid registry rehome intent")
	}
	canonical, err := declarativerelease.CanonicalJSON(p)
	before, d := p.Source, p.Draft
	want, digestErr := registryRehomeDraftDigest(d)
	if err != nil || !bytes.Equal(canonical, s.Payload) || digestErr != nil || d.DraftDigest != want ||
		d.Schema != "fugue.production.registry-rehome-draft.v1" || !digestPattern.MatchString(d.ArchiveDigest) ||
		d.ArchiveDigest != before.ArchiveDigest || d.Current.RecordDigest != before.MonitorDigest || before.MonitorUID == "" ||
		d.Workload != before.Deployment.Identity || d.Workload.APIVersion != "apps/v1" || d.Workload.Kind != "Deployment" ||
		before.Deployment.UID == "" || before.Deployment.Generation < 1 || !digestPattern.MatchString(before.Deployment.SpecDigest) ||
		before.Key.Validate() != nil || before.Key.Component != d.Component ||
		s.Intent.ResourceID != nativeKubernetesResourceID(d.Workload) || s.Intent.Generation != before.Version.Generation ||
		s.Intent.CurrentDigest != before.Version.Digest || s.Intent.RollbackDigest != before.Version.Digest || s.Intent.TargetDigest != d.DraftDigest ||
		len(d.Services) != len(before.Services) || !validRegistryRehomeProjection(d.Current, d.Mirror.Forward) ||
		!validRegistryRehomeProjection(d.Rollback, d.Mirror.Rollback) || d.Current.RecordDigest == d.Rollback.RecordDigest {
		return RegistryRehomeReview{}, errors.New("registry rehome payload does not bind the verified scope, mirror and predecessor")
	}
	a, _ := name.NewDigest(d.Current.IndependentImage, name.StrictValidation)
	b, _ := name.NewDigest(d.Rollback.IndependentImage, name.StrictValidation)
	if before.Desired == (RecoveryRecordRef{}) {
		if before.Version.Generation != 1 {
			return RegistryRehomeReview{}, errors.New("unmanaged registry rehome source must begin at generation one")
		}
	} else if before.Desired.Name != desiredName(before.Key) || before.Desired.UID == "" || before.Desired.ResourceVersion == "" {
		return RegistryRehomeReview{}, errors.New("registry rehome does not identify the exact existing desired pointer")
	}
	if a.Context() != b.Context() {
		return RegistryRehomeReview{}, errors.New("registry rehome versions do not share an independent repository")
	}
	for i, service := range d.Services {
		live := before.Services[i]
		if live.Declaration != service || live.Live.Identity != service.Identity ||
			service.Identity.APIVersion != "v1" || service.Identity.Kind != "Service" || service.Identity.Namespace != d.Workload.Namespace ||
			live.Live.UID == "" || !digestPattern.MatchString(live.Live.SpecDigest) || !digestPattern.MatchString(service.ShapeDigest) ||
			(i > 0 && d.Services[i-1].Identity.Name >= service.Identity.Name) {
			return RegistryRehomeReview{}, errors.New("registry rehome companion Service does not match live readback")
		}
	}
	return p, nil
}

func validRegistryRehomeProjection(p RegistryRehomeProjection, mirror RegistryRecoveryReadback) bool {
	src, sErr := name.NewDigest(p.SourceImage, name.StrictValidation)
	dst, dErr := name.NewDigest(p.IndependentImage, name.StrictValidation)
	if sErr != nil || dErr != nil || src.Name() != p.SourceImage || dst.Name() != p.IndependentImage ||
		src.DigestStr() != dst.DigestStr() || src.RegistryStr() == dst.RegistryStr() ||
		!digestPattern.MatchString(src.DigestStr()) || githubRegistryHost(dst.RegistryStr()) ||
		mirror.SourceImage != p.SourceImage || mirror.RecoveryImage.Image != p.IndependentImage ||
		mirror.RecoveryImage.IndexDigest != src.DigestStr() ||
		mirror.RecoveryPin != dst.Context().Tag("fugue-recovery-sha256-"+dst.DigestStr()[len("sha256:"):]).Name() ||
		mirror.RecoveryImage.Verification != "independent_registry_full_readback" || mirror.RecoveryImage.Platform != "linux/amd64" ||
		!shaPattern.MatchString(mirror.RecoveryImage.OCIRevision) || !digestPattern.MatchString(mirror.SourceReceiptDigest) ||
		!digestPattern.MatchString(mirror.RecoveryImage.ManifestDigest) || !digestPattern.MatchString(mirror.RecoveryImage.ConfigDigest) ||
		!digestPattern.MatchString(p.RecordDigest) || !digestPattern.MatchString(p.SourceManifestDigest) ||
		!digestPattern.MatchString(p.SourcePodSpecDigest) || !digestPattern.MatchString(p.TargetPodSpecDigest) ||
		p.SourcePodSpecDigest == p.TargetPodSpecDigest {
		return false
	}
	seen := map[string]bool{}
	regular := false
	for _, c := range p.Containers {
		if (c.Field != "containers" && c.Field != "initContainers") || !componentPattern.MatchString(c.Name) || seen[c.Name] {
			return false
		}
		regular = regular || c.Field == "containers"
		seen[c.Name] = true
	}
	return regular
}
