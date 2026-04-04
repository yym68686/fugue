package appimages

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	"fugue/internal/model"
	"fugue/internal/sourceimport"
)

const bytesPerGiB int64 = 1 << 30

type InspectFunc func(ctx context.Context, imageRef string) (bool, map[string]int64, error)

func ManagedImageRefs(
	app model.App,
	ops []model.Operation,
	registryPushBase string,
	registryPullBase string,
) []string {
	refs := collectAppImageRefs(app, ops, registryPushBase, registryPullBase)
	if len(refs) == 0 {
		return nil
	}

	sorted := make([]string, 0, len(refs))
	for imageRef := range refs {
		sorted = append(sorted, imageRef)
	}
	sort.Strings(sorted)
	return sorted
}

func ManagedImageRefSet(
	apps []model.App,
	ops []model.Operation,
	registryPushBase string,
	registryPullBase string,
) map[string]struct{} {
	opsByAppID := make(map[string][]model.Operation)
	for _, op := range ops {
		appID := strings.TrimSpace(op.AppID)
		if appID == "" {
			continue
		}
		opsByAppID[appID] = append(opsByAppID[appID], op)
	}

	refs := make(map[string]struct{})
	for _, app := range apps {
		for imageRef := range collectAppImageRefs(app, opsByAppID[app.ID], registryPushBase, registryPullBase) {
			refs[imageRef] = struct{}{}
		}
	}
	return refs
}

func DeletableManagedImageRefs(
	deletedApp model.App,
	deletedAppOps []model.Operation,
	remainingApps []model.App,
	remainingOps []model.Operation,
	registryPushBase string,
	registryPullBase string,
) []string {
	targetRefs := collectAppImageRefs(deletedApp, deletedAppOps, registryPushBase, registryPullBase)
	if len(targetRefs) == 0 {
		return nil
	}

	remainingRefs := ManagedImageRefSet(remainingApps, remainingOps, registryPushBase, registryPullBase)
	deletable := make([]string, 0, len(targetRefs))
	for imageRef := range targetRefs {
		if _, inUse := remainingRefs[imageRef]; inUse {
			continue
		}
		deletable = append(deletable, imageRef)
	}
	sort.Strings(deletable)
	return deletable
}

func MeasureTenantStorageBytes(
	ctx context.Context,
	inspect InspectFunc,
	apps []model.App,
	ops []model.Operation,
	registryPushBase string,
	registryPullBase string,
) (int64, error) {
	if inspect == nil || strings.TrimSpace(registryPushBase) == "" {
		return 0, nil
	}

	opsByAppID := make(map[string][]model.Operation)
	for _, op := range ops {
		appID := strings.TrimSpace(op.AppID)
		if appID == "" {
			continue
		}
		opsByAppID[appID] = append(opsByAppID[appID], op)
	}

	imageRefs := make(map[string]struct{})
	for _, app := range apps {
		if strings.EqualFold(strings.TrimSpace(app.Status.Phase), "deleting") {
			continue
		}
		for imageRef := range collectAppImageRefs(app, opsByAppID[app.ID], registryPushBase, registryPullBase) {
			imageRefs[imageRef] = struct{}{}
		}
	}

	sortedImageRefs := make([]string, 0, len(imageRefs))
	for imageRef := range imageRefs {
		sortedImageRefs = append(sortedImageRefs, imageRef)
	}
	sort.Strings(sortedImageRefs)

	totalBlobSizes := make(map[string]int64)
	for _, imageRef := range sortedImageRefs {
		exists, blobSizes, err := inspect(ctx, imageRef)
		if err != nil {
			return 0, fmt.Errorf("inspect image %s: %w", imageRef, err)
		}
		if !exists {
			continue
		}
		unionBlobSizes(totalBlobSizes, blobSizes)
	}

	return sumBlobSizes(totalBlobSizes), nil
}

func StorageBytesToGibibytes(bytes int64) int64 {
	if bytes <= 0 {
		return 0
	}
	return int64(math.Ceil(float64(bytes) / float64(bytesPerGiB)))
}

func collectAppImageRefs(
	app model.App,
	ops []model.Operation,
	registryPushBase string,
	registryPullBase string,
) map[string]struct{} {
	refs := make(map[string]struct{})
	if imageRef := managedImageRefForSource(app, app.Source, app.Spec.Image, registryPushBase, registryPullBase); imageRef != "" {
		refs[imageRef] = struct{}{}
	}
	for _, op := range ops {
		if op.DesiredSource == nil {
			continue
		}
		runtimeImageRef := ""
		if op.DesiredSpec != nil {
			runtimeImageRef = strings.TrimSpace(op.DesiredSpec.Image)
		}
		if imageRef := managedImageRefForSource(app, op.DesiredSource, runtimeImageRef, registryPushBase, registryPullBase); imageRef != "" {
			refs[imageRef] = struct{}{}
		}
	}
	return refs
}

func managedImageRefForSource(
	app model.App,
	source *model.AppSource,
	runtimeImageRef string,
	registryPushBase string,
	registryPullBase string,
) string {
	if source == nil {
		return ""
	}
	if resolved := strings.TrimSpace(source.ResolvedImageRef); resolved != "" {
		return resolved
	}
	if managedRuntimeImageRef := registryRefFromRuntimeImageRef(runtimeImageRef, registryPushBase, registryPullBase); managedRuntimeImageRef != "" {
		return managedRuntimeImageRef
	}

	switch strings.TrimSpace(source.Type) {
	case model.AppSourceTypeGitHubPublic, model.AppSourceTypeGitHubPrivate:
		return inferManagedGitHubImageRef(registryPushBase, source)
	case model.AppSourceTypeUpload:
		return inferManagedUploadImageRef(registryPushBase, app.Name, source)
	case model.AppSourceTypeDockerImage:
		if imageRef := strings.TrimSpace(source.ImageRef); isManagedRegistryRef(imageRef, registryPushBase) {
			return imageRef
		}
	default:
		if imageRef := strings.TrimSpace(source.ImageRef); isManagedRegistryRef(imageRef, registryPushBase) {
			return imageRef
		}
	}
	return ""
}

func inferManagedGitHubImageRef(registryPushBase string, source *model.AppSource) string {
	if source == nil {
		return ""
	}
	repoOwner, repoName, err := sourceimport.ParseGitHubRepoURL(strings.TrimSpace(source.RepoURL))
	if err != nil {
		return ""
	}
	commitSHA := shortImageCommit(strings.TrimSpace(source.CommitSHA))
	if commitSHA == "" {
		return ""
	}

	repoPath := repoOwner + "-" + repoName
	if suffix := model.Slugify(strings.TrimSpace(source.ImageNameSuffix)); suffix != "" {
		repoPath += "-" + suffix
	}
	return fmt.Sprintf("%s/fugue-apps/%s:git-%s", strings.Trim(strings.TrimSpace(registryPushBase), "/"), repoPath, commitSHA)
}

func inferManagedUploadImageRef(registryPushBase, appName string, source *model.AppSource) string {
	if source == nil {
		return ""
	}
	tagSeed := strings.TrimSpace(source.ArchiveSHA256)
	if tagSeed == "" {
		tagSeed = strings.TrimSpace(source.CommitSHA)
	}
	shortTag := shortImageCommit(tagSeed)
	if shortTag == "" {
		return ""
	}

	repoPath := model.Slugify(strings.TrimSpace(appName))
	if repoPath == "" {
		repoPath = "app"
	}
	if suffix := model.Slugify(strings.TrimSpace(source.ImageNameSuffix)); suffix != "" {
		repoPath += "-" + suffix
	}
	return fmt.Sprintf("%s/fugue-apps/%s:upload-%s", strings.Trim(strings.TrimSpace(registryPushBase), "/"), repoPath, shortTag)
}

func shortImageCommit(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 12 {
		return value[:12]
	}
	return value
}

func registryRefFromRuntimeImageRef(runtimeImageRef, registryPushBase, registryPullBase string) string {
	runtimeImageRef = strings.TrimSpace(runtimeImageRef)
	if runtimeImageRef == "" {
		return ""
	}
	pushBase := strings.Trim(strings.TrimSpace(registryPushBase), "/")
	pullBase := strings.Trim(strings.TrimSpace(registryPullBase), "/")
	if pushBase == "" {
		return ""
	}
	if strings.HasPrefix(runtimeImageRef, pushBase+"/") {
		return runtimeImageRef
	}
	if pullBase == "" || pullBase == pushBase {
		return ""
	}
	prefix := pullBase + "/"
	if !strings.HasPrefix(runtimeImageRef, prefix) {
		return ""
	}
	return pushBase + "/" + strings.TrimPrefix(runtimeImageRef, prefix)
}

func isManagedRegistryRef(imageRef, registryPushBase string) bool {
	imageRef = strings.TrimSpace(imageRef)
	pushBase := strings.Trim(strings.TrimSpace(registryPushBase), "/")
	if imageRef == "" || pushBase == "" {
		return false
	}
	return strings.HasPrefix(imageRef, pushBase+"/")
}

func unionBlobSizes(target, source map[string]int64) {
	for digest, sizeBytes := range source {
		if sizeBytes <= 0 {
			continue
		}
		if existing, ok := target[digest]; ok && existing >= sizeBytes {
			continue
		}
		target[digest] = sizeBytes
	}
}

func sumBlobSizes(blobSizes map[string]int64) int64 {
	var total int64
	for _, sizeBytes := range blobSizes {
		if sizeBytes > 0 {
			total += sizeBytes
		}
	}
	return total
}
