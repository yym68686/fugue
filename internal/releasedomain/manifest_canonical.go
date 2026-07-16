package releasedomain

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// CanonicalizeRenderedManifest converts a rendered manifest stream into the
// exact structural representation used by ClassifyRendered. Object and map
// ordering are canonical, list ordering remains significant, and no Secret or
// hook field is removed. The result is still strict YAML accepted by the same
// classifier.
func CanonicalizeRenderedManifest(data []byte, spec *OwnershipSpec, defaultNamespace string) ([]byte, error) {
	if spec == nil {
		return nil, fmt.Errorf("ownership is nil")
	}
	objects, unknown := decodeManifest(data, spec, defaultNamespace, "canonical")
	if len(unknown) != 0 {
		return nil, manifestEvidenceError(unknown)
	}
	indexed, duplicates := indexManifestObjects(objects, "canonical")
	if len(duplicates) != 0 {
		return nil, manifestEvidenceError(duplicates)
	}
	identities := make([]string, 0, len(indexed))
	for identity := range indexed {
		identities = append(identities, identity)
	}
	sort.Strings(identities)

	var output bytes.Buffer
	for index, identity := range identities {
		root, err := canonicalManifestNode(normalizedObject(indexed[identity]))
		if err != nil {
			return nil, fmt.Errorf("canonicalize %s: %w", indexed[identity].Identity.String(), err)
		}
		var document bytes.Buffer
		encoder := yaml.NewEncoder(&document)
		encoder.SetIndent(2)
		if err := encoder.Encode(root); err != nil {
			_ = encoder.Close()
			return nil, fmt.Errorf("encode %s: %w", indexed[identity].Identity.String(), err)
		}
		if err := encoder.Close(); err != nil {
			return nil, fmt.Errorf("close encoder for %s: %w", indexed[identity].Identity.String(), err)
		}
		if index != 0 {
			output.WriteString("---\n")
		}
		output.Write(document.Bytes())
		if output.Len() > maxRenderedManifestBytes {
			return nil, fmt.Errorf("canonical manifest bytes exceed limit %d", maxRenderedManifestBytes)
		}
	}
	return append([]byte(nil), output.Bytes()...), nil
}

func manifestEvidenceError(evidence []Evidence) error {
	evidence = canonicalEvidence(evidence)
	const detailLimit = 16
	detailCount := len(evidence)
	if len(evidence) > detailLimit {
		evidence = evidence[:detailLimit]
	}
	details := make([]string, 0, len(evidence)+1)
	for _, item := range evidence {
		detail := item.Subject + ": " + item.Reason
		if item.Status != "" {
			detail += " (status=" + item.Status + ")"
		}
		details = append(details, detail)
	}
	if detailCount > len(evidence) {
		details = append(details, fmt.Sprintf("%d additional error(s)", detailCount-len(evidence)))
	}
	return fmt.Errorf("manifest is not canonicalizable: %s", strings.Join(details, "; "))
}

func canonicalManifestNode(value any) (*yaml.Node, error) {
	switch typed := value.(type) {
	case nil:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!null", Value: "null"}, nil
	case bool:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: strconv.FormatBool(typed)}, nil
	case string:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: typed}, nil
	case manifestNumber:
		// A tagged quoted rational round-trips through strictManifestValue
		// without float64 precision loss. Integers and decimal floats already
		// share manifestNumber semantics in the classifier.
		return &yaml.Node{
			Kind: yaml.ScalarNode, Tag: "!!float", Value: string(typed), Style: yaml.DoubleQuotedStyle,
		}, nil
	case []any:
		node := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
		for _, item := range typed {
			child, err := canonicalManifestNode(item)
			if err != nil {
				return nil, err
			}
			node.Content = append(node.Content, child)
		}
		return node, nil
	case map[string]any:
		node := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			child, err := canonicalManifestNode(typed[key])
			if err != nil {
				return nil, err
			}
			node.Content = append(node.Content,
				&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key},
				child,
			)
		}
		return node, nil
	default:
		return nil, fmt.Errorf("unsupported value type %T", value)
	}
}
