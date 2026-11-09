package store

import (
	"context"
	"errors"
	"sort"
)

// Never include a chain identity, signed event, SQL text or driver message in
// the observation endpoint. Joined chain failures retain each fixed category.
type observationRetentionIntegrityError struct{ category string }

func (e *observationRetentionIntegrityError) Error() string {
	return "observation retention integrity check failed"
}

func observationRetentionFailureCategories(err error) []string {
	if err == nil {
		return nil
	}
	categories := map[string]bool{}
	visited := 0
	var visit func(error, int)
	visit = func(err error, depth int) {
		if err == nil {
			return
		}
		visited++
		if depth >= 16 || visited > 64 {
			categories["unclassified"] = true
			return
		}
		if joined, ok := err.(interface{ Unwrap() []error }); ok {
			children := joined.Unwrap()
			if len(children) > 16 {
				categories["unclassified"] = true
				return
			}
			for _, child := range children {
				visit(child, depth+1)
			}
			return
		}
		if wrapped, ok := err.(interface{ Unwrap() error }); ok {
			visit(wrapped.Unwrap(), depth+1)
			return
		}
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			categories["deadline"] = true
		case errors.Is(err, context.Canceled):
			categories["canceled"] = true
		default:
			if integrity, ok := err.(*observationRetentionIntegrityError); ok {
				switch integrity.category {
				case "anchor_integrity", "prefix_integrity", "prefix_changed":
					categories[integrity.category] = true
				default:
					categories["unclassified"] = true
				}
				return
			}
			category := "unclassified"
			if state, ok := err.(interface{ SQLState() string }); ok {
				switch state.SQLState() {
				case "57014":
					category = "statement_canceled"
				case "55P03":
					category = "lock_unavailable"
				case "53100":
					category = "disk_full"
				case "53200":
					category = "memory_exhausted"
				case "53300":
					category = "connections_exhausted"
				case "42501":
					category = "permission_denied"
				case "40001", "40P01":
					category = "transaction_conflict"
				case "08000", "08001", "08003", "08004", "08006", "08007", "08P01":
					category = "connection_failure"
				}
			}
			categories[category] = true
		}
	}
	visit(err, 0)
	if len(categories) == 0 {
		categories["unclassified"] = true
	}
	out := make([]string, 0, len(categories))
	for category := range categories {
		out = append(out, category)
	}
	sort.Strings(out)
	return out
}
