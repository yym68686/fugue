package inputarchive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
)

// HotReference is an exact ledger identity. A generation always retains its
// kind and scope: a generation label is never interpreted as an input digest.
type HotReference struct {
	Kind         string `json:"kind"`
	ID           string `json:"id"`
	ArtifactKind string `json:"artifact_kind,omitempty"`
	ScopeKey     string `json:"scope_key,omitempty"`
	ExpectedHash string `json:"expected_hash,omitempty"`
}

type HotRoot struct {
	Source    string       `json:"source"`
	ID        string       `json:"id"`
	Reference HotReference `json:"reference"`
}

// HotNode contains metadata only. LocalVerified may be set only by a resolver
// that actually checked the complete original bytes, independently of R2.
type HotNode struct {
	Identity       HotReference   `json:"identity"`
	ContentHash    string         `json:"content_hash,omitempty"`
	References     []HotReference `json:"references"`
	Format         string         `json:"format,omitempty"`
	Location       string         `json:"location,omitempty"`
	LogicalBytes   int64          `json:"logical_bytes,omitempty"`
	EncodedBytes   int64          `json:"encoded_bytes,omitempty"`
	NeedsLocalCopy bool           `json:"needs_local_copy,omitempty"`
	LocalVerified  bool           `json:"local_verified"`
}

type HotClosureNode struct {
	HotNode
	Roots []string `json:"roots"`
}

type HotClosureBlocker struct {
	Reference HotReference `json:"reference"`
	Reason    string       `json:"reason"`
}

type HotClosure struct {
	Roots                  []HotRoot           `json:"roots"`
	Nodes                  []HotClosureNode    `json:"nodes"`
	Blockers               []HotClosureBlocker `json:"blockers"`
	MetadataComplete       bool                `json:"metadata_complete"`
	LocalCopiesVerified    bool                `json:"local_copies_verified"`
	RequiredLocalCopyBytes int64               `json:"required_local_copy_bytes"`
	PlanSHA256             string              `json:"plan_sha256"`
}

type HotClosureBudget struct {
	Roots    int
	Nodes    int
	Edges    int
	Bindings int
}

// HotClosureProgress is diagnostic only. It is never part of the proof hash,
// contains no input contents, and remains useful when a bounded scan refuses.
type HotClosureProgress struct {
	Phase           string `json:"phase"`
	ResolvedNodes   int    `json:"resolved_nodes"`
	UnresolvedNodes int    `json:"unresolved_nodes"`
	Edges           int    `json:"edges"`
	RootBindings    int    `json:"root_bindings"`
	ResolverCalls   int    `json:"resolver_calls"`
}

type hotClosureFailure struct{ reason string }

func (e *hotClosureFailure) Error() string { return "hot closure incomplete: " + e.reason }

// HotClosureFailureReason never exposes resolver, SQL or connection text.
func HotClosureFailureReason(err error) string {
	var failure *hotClosureFailure
	if errors.As(err, &failure) {
		return failure.reason
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	return "resolver-unavailable"
}

// PlanHotClosure resolves each identity once, validates every declared hash
// (including converging edges), then propagates every root over the completed
// graph. Traversal order cannot lose a later root at a shared descendant.
// Bounds are refusal conditions, never permission to return a truncated proof.
func PlanHotClosure(ctx context.Context, roots []HotRoot, budget HotClosureBudget, resolve func(context.Context, HotReference) (HotNode, error)) (HotClosure, error) {
	return planHotClosure(ctx, roots, budget, resolve, nil, nil)
}

// PlanHotClosureBatched amortizes metadata transport over at most 32 unique
// identities per call. Omitted identities are unresolved; a batch error aborts
// the proof, so a canceled SQL statement cannot become thousands of synthetic
// "missing" dependencies. Returned keys must exactly match requested identities
// with ExpectedHash cleared. Every incoming expected hash is still checked.
func PlanHotClosureBatched(ctx context.Context, roots []HotRoot, budget HotClosureBudget, resolve func(context.Context, []HotReference) (map[HotReference]HotNode, error)) (HotClosure, error) {
	return planHotClosure(ctx, roots, budget, nil, resolve, nil)
}

// PlanHotClosureBatchedObserved reports aggregate progress on both success and
// refusal. Observation does not change bounds, graph contents or proof bytes.
func PlanHotClosureBatchedObserved(ctx context.Context, roots []HotRoot, budget HotClosureBudget, resolve func(context.Context, []HotReference) (map[HotReference]HotNode, error), observe func(HotClosureProgress)) (HotClosure, error) {
	return planHotClosure(ctx, roots, budget, nil, resolve, observe)
}

func planHotClosure(ctx context.Context, roots []HotRoot, budget HotClosureBudget, resolve func(context.Context, HotReference) (HotNode, error), batch func(context.Context, []HotReference) (map[HotReference]HotNode, error), observe func(HotClosureProgress)) (HotClosure, error) {
	report := HotClosure{Roots: append([]HotRoot{}, roots...), Nodes: []HotClosureNode{}, Blockers: []HotClosureBlocker{}}
	progress := HotClosureProgress{Phase: "validate-roots"}
	if observe != nil {
		defer func() { observe(progress) }()
	}
	if budget.Roots < 1 || budget.Nodes < 1 || budget.Edges < 1 || budget.Bindings < 1 || len(roots) > budget.Roots || resolve == nil && batch == nil {
		return report, &hotClosureFailure{"invalid-budget-or-resolver"}
	}
	sort.Slice(report.Roots, func(i, j int) bool { return hotRootKey(report.Roots[i]) < hotRootKey(report.Roots[j]) })
	rootKeys := map[string]bool{}
	pending := make([]HotReference, 0, len(roots))
	for _, root := range report.Roots {
		key := hotRootKey(root)
		if root.Source == "" || len(root.Source) > 128 || root.ID == "" || len(root.ID) > 512 || rootKeys[key] || !validHotReference(root.Reference) {
			return report, &hotClosureFailure{"invalid-or-duplicate-root"}
		}
		rootKeys[key] = true
		pending = append(pending, root.Reference)
	}
	nodes := map[string]HotNode{}
	failures := map[string]bool{}
	cache := map[string]*HotNode{}
	edges := 0
	progress.Phase = "resolve-identities"
	for next := 0; next < len(pending); next++ {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		ref := pending[next]
		if !validHotReference(ref) {
			return report, &hotClosureFailure{"invalid-dependency"}
		}
		key := hotReferenceKey(ref)
		if !failures[key] {
			if _, exists := nodes[key]; !exists {
				if len(nodes)+len(failures) >= budget.Nodes {
					return report, &hotClosureFailure{"node-budget"}
				}
				var node HotNode
				var err error
				if batch == nil {
					progress.ResolverCalls++
					node, err = resolve(ctx, ref)
				} else {
					if _, cached := cache[key]; !cached {
						requests := []HotReference{}
						requested := map[HotReference]bool{}
						for _, candidate := range pending[next:] {
							if !validHotReference(candidate) {
								return report, &hotClosureFailure{"invalid-dependency"}
							}
							candidate.ExpectedHash = ""
							identity := hotReferenceKey(candidate)
							_, done := nodes[identity]
							_, cached := cache[identity]
							if done || failures[identity] || cached || requested[candidate] {
								continue
							}
							requested[candidate] = true
							requests = append(requests, candidate)
							if len(requests) == 32 || len(nodes)+len(failures)+len(cache)+len(requests) >= budget.Nodes {
								break
							}
						}
						progress.ResolverCalls++
						resolved, batchErr := batch(ctx, requests)
						if batchErr != nil {
							return report, batchErr
						}
						for identity, resolvedNode := range resolved {
							if !requested[identity] || resolvedNode.Identity != identity {
								return report, &hotClosureFailure{"unrequested-batch-identity"}
							}
						}
						for _, identity := range requests {
							var entry *HotNode
							if resolvedNode, ok := resolved[identity]; ok {
								entry = &resolvedNode
							}
							cache[hotReferenceKey(identity)] = entry
						}
					}
					if cached := cache[key]; cached != nil {
						node = *cached
					} else {
						err = errors.New("hot identity unresolved")
					}
					delete(cache, key)
				}
				if err != nil {
					if ctx.Err() != nil {
						return report, ctx.Err()
					}
					// Resolver errors may contain credentials or arbitrary SQL. The
					// operator report records the exact identity, never that text.
					failures[key] = true
					progress.UnresolvedNodes = len(failures)
					report.Blockers = append(report.Blockers, HotClosureBlocker{ref, "identity-unresolved"})
					continue
				}
				if hotReferenceKey(node.Identity) != key || node.LogicalBytes < 0 || node.EncodedBytes < 0 || node.Identity.ExpectedHash != "" {
					return report, &hotClosureFailure{"invalid-resolved-identity"}
				}
				if node.Identity.Kind == "input" && (node.ContentHash != ref.ID || len(node.References) != 0) {
					return report, &hotClosureFailure{"invalid-input-identity"}
				}
				node.References = append([]HotReference{}, node.References...)
				sort.Slice(node.References, func(i, j int) bool {
					return hotReferenceBinding(node.References[i]) < hotReferenceBinding(node.References[j])
				})
				edges += len(node.References)
				progress.Edges = edges
				if edges > budget.Edges {
					return report, &hotClosureFailure{"edge-budget"}
				}
				nodes[key] = node
				progress.ResolvedNodes = len(nodes)
				pending = append(pending, node.References...)
			}
		}
		if node, ok := nodes[key]; ok && ref.ExpectedHash != "" && node.ContentHash != ref.ExpectedHash {
			report.Blockers = append(report.Blockers, HotClosureBlocker{ref, "declared-hash-mismatch"})
		}
	}
	retainedBy := map[string]map[string]bool{}
	bindings := 0
	progress.Phase = "propagate-roots"
	for _, root := range report.Roots {
		name := hotRootKey(root)
		queue := []string{hotReferenceKey(root.Reference)}
		for next := 0; next < len(queue); next++ {
			if err := ctx.Err(); err != nil {
				return report, err
			}
			key := queue[next]
			if retainedBy[key] == nil {
				retainedBy[key] = map[string]bool{}
			}
			if retainedBy[key][name] {
				continue
			}
			bindings++
			progress.RootBindings = bindings
			if bindings > budget.Bindings {
				return report, &hotClosureFailure{"root-binding-budget"}
			}
			retainedBy[key][name] = true
			for _, child := range nodes[key].References {
				queue = append(queue, hotReferenceKey(child))
			}
		}
	}
	progress.Phase = "finalize-proof"
	report.LocalCopiesVerified = true
	for key, node := range nodes {
		item := HotClosureNode{HotNode: node, Roots: []string{}}
		for root := range retainedBy[key] {
			item.Roots = append(item.Roots, root)
		}
		sort.Strings(item.Roots)
		if node.Identity.Kind == "input" {
			if node.NeedsLocalCopy {
				if node.EncodedBytes > int64(^uint64(0)>>1)-report.RequiredLocalCopyBytes {
					return report, &hotClosureFailure{"local-copy-byte-overflow"}
				}
				report.RequiredLocalCopyBytes += node.EncodedBytes
			}
			if !node.LocalVerified {
				report.LocalCopiesVerified = false
			}
		}
		report.Nodes = append(report.Nodes, item)
	}
	sort.Slice(report.Nodes, func(i, j int) bool {
		return hotReferenceKey(report.Nodes[i].Identity) < hotReferenceKey(report.Nodes[j].Identity)
	})
	sort.Slice(report.Blockers, func(i, j int) bool {
		return hotReferenceBinding(report.Blockers[i].Reference)+report.Blockers[i].Reason < hotReferenceBinding(report.Blockers[j].Reference)+report.Blockers[j].Reason
	})
	report.MetadataComplete = len(report.Blockers) == 0
	if !report.MetadataComplete {
		report.LocalCopiesVerified = false
	}
	// The hash includes reference hashes, every root and edge, representation
	// identity, byte estimates, completeness and verification. No timestamps.
	raw, err := json.Marshal(report)
	if err != nil {
		return report, err
	}
	sum := sha256.Sum256(raw)
	report.PlanSHA256 = hex.EncodeToString(sum[:])
	progress.Phase = "complete"
	return report, nil
}

func validHotReference(ref HotReference) bool {
	if ref.ID == "" || len(ref.ID) > 512 || len(ref.ArtifactKind) > 128 || len(ref.ScopeKey) > 512 || ref.ExpectedHash != "" && !digestPattern.MatchString(ref.ExpectedHash) {
		return false
	}
	switch ref.Kind {
	case "artifact", "release", "expectation", "task":
		return ref.ArtifactKind == "" && ref.ScopeKey == ""
	case "input":
		return ref.ArtifactKind == "" && ref.ScopeKey == "" && digestPattern.MatchString(ref.ID)
	case "generation":
		return ref.ArtifactKind != "" && ref.ScopeKey != ""
	}
	return false
}

func hotReferenceKey(ref HotReference) string {
	// JSON encoding is injective even for identities containing delimiters.
	raw, _ := json.Marshal([]string{ref.Kind, ref.ID, ref.ArtifactKind, ref.ScopeKey})
	return string(raw)
}

func hotReferenceBinding(ref HotReference) string {
	raw, _ := json.Marshal(ref)
	return string(raw)
}

func hotRootKey(root HotRoot) string {
	raw, _ := json.Marshal([]string{root.Source, root.ID, hotReferenceBinding(root.Reference)})
	return string(raw)
}
