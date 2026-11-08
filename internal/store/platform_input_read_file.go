package store

import (
	"context"
	"encoding/json"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/inputarchive"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
)

// File mode has no PG pin ledger. Verify the actual original input, signatures
// and complete typed closure in ONE locked state, rather than treating the
// absence of a database as an admission exemption. No network or writes occur.
func (s *Store) checkFilePlatformInputRead(ctx context.Context, reads []PlatformInputReadSelection) error {
	s.platformArtifactKeyringMu.RLock()
	defer s.platformArtifactKeyringMu.RUnlock()
	return s.withLockedState(false, func(state *model.State) error {
		return checkFileCompilerInputClosure(ctx, state, compilerInputReadRoots(reads), reads, s.platformArtifactKeyring)
	})
}

// The original file store keeps LKG history in this slice and selects its
// latest row. Historical rows are not additional activation roots. An invalid
// latest row never permits fallback, and an ambiguous selected ID is refused.
func fileInputReadLKG(state *model.State, kind, scope string) (*model.PlatformLKGSnapshot, error) {
	current := platformLKGSnapshotForScope(state.PlatformLKGSnapshots, kind, scope)
	if current != nil {
		count := 0
		for _, row := range state.PlatformLKGSnapshots {
			if row.ArtifactKind == kind && row.ScopeKey == scope && row.ID == current.ID {
				count++
			}
		}
		if count != 1 {
			return nil, ErrConflict
		}
	}
	return current, nil
}

// The caller holds keyring RLock before the state lock, through admission and
// any original task state transition. Never reacquire either lock here.
func checkFileCompilerInputClosure(ctx context.Context, state *model.State, roots []inputarchive.HotRoot, reads []PlatformInputReadSelection, keyring bundleauth.Keyring) error {
	artifacts := map[string]model.PlatformArtifact{}
	generations := map[inputarchive.HotReference]model.PlatformArtifact{}
	releases := map[string]model.PlatformArtifactRelease{}
	sets := map[string]model.PlatformExpectedConsumerSet{}
	contents := map[string]model.PlatformArtifactContent{}
	duplicates := map[string]bool{}
	for _, a := range state.PlatformArtifacts {
		if _, ok := artifacts[a.ID]; ok {
			duplicates["artifact/"+a.ID] = true
		}
		artifacts[a.ID] = a
		ref := inputarchive.HotReference{Kind: "generation", ID: a.Generation, ArtifactKind: a.ArtifactKind, ScopeKey: a.ScopeKey}
		if _, ok := generations[ref]; ok {
			duplicates["generation/"+compilerInputLKGSelectorKey(a.ArtifactKind, a.ScopeKey)+"/"+a.Generation] = true
		}
		generations[ref] = a
	}
	for _, r := range state.PlatformArtifactReleases {
		if _, ok := releases[r.ID]; ok {
			duplicates["release/"+r.ID] = true
		}
		releases[r.ID] = r
	}
	for _, set := range state.ExpectedConsumerSets {
		if _, ok := sets[set.ID]; ok {
			duplicates["expectation/"+set.ID] = true
		}
		sets[set.ID] = set
	}
	for _, c := range state.PlatformArtifactContents {
		if _, ok := contents[c.ContentHash]; ok {
			duplicates["input/"+c.ContentHash] = true
		}
		contents[c.ContentHash] = c
	}
	expires := time.Now().Add(time.Minute)
	for _, read := range reads {
		if read.LKGKind == "" {
			continue
		}
		current, err := fileInputReadLKG(state, read.LKGKind, read.LKGScope)
		if err != nil {
			return err
		}
		if !sameCompilerInputReadLKG(read.LKG, current) {
			return ErrConflict
		}
		if current != nil {
			a, ok := artifacts[current.ArtifactID]
			if !ok || !platformLKGSnapshotMatchesArtifact(*current, a, time.Now(), keyring) {
				return ErrConflict
			}
			if current.ExpiresAt.Before(expires) {
				expires = current.ExpiresAt
			}
		}
	}
	request, err := json.Marshal(struct {
		Roots []inputarchive.HotRoot
		Reads []PlatformInputReadSelection
	}{roots, reads})
	if err != nil || len(request) > 32<<20 {
		return ErrConflict
	}
	requestBytes, inputBytes := len(request), int64(0)
	closure, err := inputarchive.PlanHotClosureBatched(ctx, roots,
		inputarchive.HotClosureBudget{Roots: compilerInputHotRootLimit, Nodes: 32768, Edges: 262144, Bindings: 524288},
		func(ctx context.Context, refs []inputarchive.HotReference) (map[inputarchive.HotReference]inputarchive.HotNode, error) {
			out, bytes := map[inputarchive.HotReference]inputarchive.HotNode{}, requestBytes
			requestBytes = 0
			for _, ref := range refs {
				if ctx.Err() != nil || duplicates[ref.Kind+"/"+ref.ID] {
					return nil, ErrConflict
				}
				node := inputarchive.HotNode{Identity: ref, References: []inputarchive.HotReference{}}
				node.Identity.ExpectedHash = ""
				var metadata any
				switch ref.Kind {
				case "artifact":
					a, ok := artifacts[ref.ID]
					if !ok {
						return nil, ErrConflict
					}
					body, e := json.Marshal(a.Content)
					if e != nil || len(body) > compilerInputHotArtifactBytes || !platformsafety.EvaluateArtifactIntegrity(a, keyring).Pass {
						return nil, ErrConflict
					}
					if compilerInputReadRetainsOriginal(reads) {
						node, err = compilerInputArtifactHotNode(a)
					} else {
						node, err = compilerInputArtifactRuntimeNode(a)
					}
					metadata = a
				case "generation":
					key := ref
					key.ExpectedHash = ""
					a, ok := generations[key]
					if !ok || duplicates["generation/"+compilerInputLKGSelectorKey(ref.ArtifactKind, ref.ScopeKey)+"/"+ref.ID] {
						return nil, ErrConflict
					}
					node = compilerInputHotGenerationNode(ref, a.ID, a.ContentHash)
					metadata = node
				case "release":
					r, ok := releases[ref.ID]
					if !ok {
						return nil, ErrConflict
					}
					node = compilerInputHotReleaseNode(ref, r.ArtifactID, r.ArtifactKind, r.ScopeKey,
						[]string{r.Generation, r.CandidateGeneration, r.ServingUnverifiedGeneration, r.VerifiedLKGGeneration, r.PinnedRollbackGeneration, r.RollbackTargetGeneration})
					metadata = node
				case "expectation":
					set, ok := sets[ref.ID]
					if !ok {
						return nil, ErrConflict
					}
					node.References = []inputarchive.HotReference{{Kind: "artifact", ID: set.ReleaseSetID}, {Kind: "release", ID: set.ArtifactReleaseID},
						{Kind: "generation", ID: set.ExpectedGeneration, ArtifactKind: set.ArtifactKind, ScopeKey: set.ScopeKey}}
					metadata = node
				case "input":
					c, ok := contents[ref.ID]
					if !ok || !inputDigestPattern.MatchString(ref.ID) || c.SizeBytes < 1 || c.SizeBytes > inputMaxBytes || inputBytes > inputMaxBytes-c.SizeBytes {
						return nil, ErrConflict
					}
					original, e := json.Marshal(c.Content)
					if e != nil || int64(len(original)) != c.SizeBytes {
						return nil, ErrConflict
					}
					// Match the file store's original canonical RuntimeSnapshot
					// contract, not the recorded digest/length alone.
					snapshot, e := platformconfig.DecodeRuntimeSnapshotContent(c.Content)
					if e != nil {
						return nil, ErrConflict
					}
					actual, e := platformconfig.RuntimeSnapshotDigest(snapshot)
					if e != nil || actual != ref.ID {
						return nil, ErrConflict
					}
					inputBytes += c.SizeBytes
					node.ContentHash, node.LogicalBytes, node.Format, node.Location = actual, c.SizeBytes, "file-runtime-snapshot", "file"
					metadata = node
				default:
					return nil, ErrConflict
				}
				if err != nil {
					return nil, err
				}
				raw, e := json.Marshal(metadata)
				if e != nil || len(raw) > (32<<20)-bytes {
					return nil, ErrConflict
				}
				bytes += len(raw)
				out[ref] = node
			}
			return out, nil
		})
	if err != nil || !closure.MetadataComplete || len(closure.Blockers) != 0 || ctx.Err() != nil || !time.Now().Before(expires) {
		return ErrConflict
	}
	return nil
}
