package store

import (
	"context"
	"strconv"

	"fugue/internal/bundleauth"
	"fugue/internal/inputarchive"
	"fugue/internal/model"
)

// File task admission uses the actual original input, not a simulated PG pin.
// The business caller already owns keyring RLock then the single state lock;
// the selected LKG, graph, input bodies and task transition are thus atomic.
func checkFileNodeTaskInputAdmission(ctx context.Context, state *model.State, taskType string, payload map[string]string, keys bundleauth.Keyring) error {
	refs, err := compilerInputHotTaskReferences(payload)
	if err != nil {
		return ErrConflict
	}
	var reads []PlatformInputReadSelection
	if compilerInputTaskUsesConfiguration(taskType, payload) {
		read := PlatformInputReadSelection{LKGKind: model.PlatformArtifactKindReleaseSet, LKGScope: "global"}
		read.LKG, err = fileInputReadLKG(state, read.LKGKind, read.LKGScope)
		if err != nil {
			return err
		}
		reads = append(reads, read)
		for _, root := range compilerInputReadRoots(reads) {
			refs = append(refs, root.Reference)
		}
	}
	if len(refs) == 0 && len(reads) == 0 {
		return nil
	}
	seen := map[inputarchive.HotReference]bool{}
	roots := []inputarchive.HotRoot{}
	for _, ref := range refs {
		if !seen[ref] {
			seen[ref] = true
			roots = append(roots, inputarchive.HotRoot{Source: "selected-file-task", ID: strconv.Itoa(len(roots)), Reference: ref})
		}
	}
	return checkFileCompilerInputClosure(ctx, state, roots, reads, keys)
}
