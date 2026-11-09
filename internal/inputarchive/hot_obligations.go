package inputarchive

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"strings"
)

// HotObligationPath is one observed metadata path per root and selected exact
// identity. It is never byte/signature verification or a disposal permission.
type HotObligationPath struct {
	RootIndex           int            `json:"root_index"`
	Target              HotReference   `json:"target"`
	References          []HotReference `json:"references"`
	DeclaredHashesMatch bool           `json:"declared_hashes_match"`
}

// DecodeHotObligationTargets bounds and validates identities before DB access.
// ExpectedHash is preserved on paths but is not a different ledger identity.
func DecodeHotObligationTargets(raw string) ([]HotReference, error) {
	if len(raw) == 0 || len(raw) > 32768 {
		return nil, errors.New("invalid obligation targets")
	}
	var refs []HotReference
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&refs) != nil || len(refs) < 1 || len(refs) > 32 {
		return nil, errors.New("invalid obligation targets")
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return nil, errors.New("invalid obligation targets")
	}
	// encoding/json otherwise accepts duplicate keys and null string fields.
	// The wire contract is one flat array of exact string-valued identities.
	fields := json.NewDecoder(strings.NewReader(raw))
	if token, _ := fields.Token(); token != json.Delim('[') {
		return nil, errors.New("invalid obligation targets")
	}
	for fields.More() {
		if token, _ := fields.Token(); token != json.Delim('{') {
			return nil, errors.New("invalid obligation targets")
		}
		keys := map[string]bool{}
		for fields.More() {
			key, err := fields.Token()
			name, ok := key.(string)
			if err != nil || !ok || keys[name] {
				return nil, errors.New("invalid obligation targets")
			}
			keys[name] = true
			value, err := fields.Token()
			if _, ok := value.(string); err != nil || !ok {
				return nil, errors.New("invalid obligation targets")
			}
		}
		if token, _ := fields.Token(); token != json.Delim('}') {
			return nil, errors.New("invalid obligation targets")
		}
	}
	seen := map[string]bool{}
	for _, ref := range refs {
		key := hotReferenceKey(ref)
		if !validHotReference(ref) || seen[key] {
			return nil, errors.New("invalid obligation targets")
		}
		seen[key] = true
	}
	return refs, nil
}

// TraceHotObligationPaths walks only already-observed edges, including edges to
// unresolved targets. It never calls a resolver or repairs a failed snapshot.
// A single canonical shortest path witnesses each root/target relationship;
// every root is retained, including late roots of shared/cyclic descendants.
// complete means this bounded diagnostic finished, NOT that unknown graph
// edges or external retention sources are absent.
func TraceHotObligationPaths(ctx context.Context, roots []HotRoot, nodes []HotNode, targets []HotReference, maxBindings int, emit func(HotObligationPath) error) (complete bool, err error) {
	byKey := make(map[string]HotNode, len(nodes))
	for _, node := range nodes {
		byKey[hotReferenceKey(node.Identity)] = node
	}
	wanted := map[string]HotReference{}
	for _, target := range targets {
		wanted[hotReferenceKey(target)] = target
	}
	bindings := 0
	for rootIndex, root := range roots {
		queue := []HotReference{root.Reference}
		parents := map[string]string{hotReferenceKey(root.Reference): ""}
		refs := map[string]HotReference{hotReferenceKey(root.Reference): root.Reference}
		for next := 0; next < len(queue); next++ {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			bindings++
			if bindings > maxBindings {
				return false, errors.New("obligation binding budget")
			}
			ref := queue[next]
			key := hotReferenceKey(ref)
			if target, ok := wanted[key]; ok {
				path := HotObligationPath{RootIndex: rootIndex, Target: target, DeclaredHashesMatch: true}
				for at := key; at != ""; at = parents[at] {
					path.References = append(path.References, refs[at])
				}
				for i, j := 0, len(path.References)-1; i < j; i, j = i+1, j-1 {
					path.References[i], path.References[j] = path.References[j], path.References[i]
				}
				for _, step := range path.References {
					node, resolved := byKey[hotReferenceKey(step)]
					if !resolved || step.ExpectedHash != "" && step.ExpectedHash != node.ContentHash {
						path.DeclaredHashesMatch = false
					}
				}
				if target.ExpectedHash != "" && byKey[key].ContentHash != target.ExpectedHash {
					path.DeclaredHashesMatch = false
				}
				if err := emit(path); err != nil {
					return false, errors.New("obligation output budget")
				}
			}
			children := append([]HotReference(nil), byKey[key].References...)
			sort.Slice(children, func(i, j int) bool { return hotReferenceBinding(children[i]) < hotReferenceBinding(children[j]) })
			for _, child := range children {
				childKey := hotReferenceKey(child)
				if _, exists := parents[childKey]; !exists {
					parents[childKey], refs[childKey] = key, child
					queue = append(queue, child)
				}
			}
		}
	}
	return true, nil
}
