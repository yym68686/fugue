package main

import (
	"testing"

	"fugue/internal/edgegroupfront"
)

func TestFrontLKGGenerationAcceptsOnlyExactAdjacentCompensation(t *testing.T) {
	base := uint64(34)
	if !frontLKGGenerationMatches(edgegroupfront.ActivationState{Generation: base}, base, edgegroupfront.ActivationOperationPromote) {
		t.Fatal("exact baseline generation was rejected")
	}
	compensated := edgegroupfront.ActivationState{Generation: base + 2, Operation: edgegroupfront.ActivationOperationRollback, RollbackOfGeneration: base + 1}
	if !frontLKGGenerationMatches(compensated, base, edgegroupfront.ActivationOperationPromote) {
		t.Fatal("exact adjacent compensation was rejected")
	}
	for name, state := range map[string]edgegroupfront.ActivationState{
		"generation gap": {Generation: base + 3, Operation: edgegroupfront.ActivationOperationRollback, RollbackOfGeneration: base + 2},
		"wrong link":     {Generation: base + 2, Operation: edgegroupfront.ActivationOperationRollback, RollbackOfGeneration: base},
		"not rollback":   {Generation: base + 2, Operation: edgegroupfront.ActivationOperationPromote},
	} {
		t.Run(name, func(t *testing.T) {
			if frontLKGGenerationMatches(state, base, edgegroupfront.ActivationOperationPromote) {
				t.Fatal("non-adjacent compensation was accepted")
			}
		})
	}
	if frontLKGGenerationMatches(compensated, base, edgegroupfront.ActivationOperationRollback) {
		t.Fatal("restore path accepted promotion-only compensation relaxation")
	}
}
