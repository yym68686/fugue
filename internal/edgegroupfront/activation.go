package edgegroupfront

import (
	"time"

	"fugue/internal/edgegroupfront/edgefrontstate"
)

// Preserve the Front package's existing state API while keeping the
// standalone CAS executable independent of the Front network service.
type (
	ActivationState            = edgefrontstate.ActivationState
	ActivationCASRequest       = edgefrontstate.ActivationCASRequest
	ActivationReceipt          = edgefrontstate.ActivationReceipt
	ActivationRepairCASRequest = edgefrontstate.ActivationRepairCASRequest
	ActivationRepairRelease    = edgefrontstate.ActivationRepairRelease
	ActivationRepairWrite      = edgefrontstate.ActivationRepairWrite
	PublicationPreparation     = edgefrontstate.PublicationPreparation
)

const (
	ActivationStateSchemaV1        = edgefrontstate.ActivationStateSchemaV1
	ActivationReceiptSchemaV1      = edgefrontstate.ActivationReceiptSchemaV1
	ActivationAuthority            = edgefrontstate.ActivationAuthority
	ActivationOperationInit        = edgefrontstate.ActivationOperationInit
	ActivationOperationPromote     = edgefrontstate.ActivationOperationPromote
	ActivationOperationRollback    = edgefrontstate.ActivationOperationRollback
	ActivationRepairWriteSchemaV1  = edgefrontstate.ActivationRepairWriteSchemaV1
	PublicationPreparationSchema   = edgefrontstate.PublicationPreparationSchema
	PublicationPreparationLifetime = edgefrontstate.PublicationPreparationLifetime
)

var ErrActivationCASConflict = edgefrontstate.ErrActivationCASConflict

func ApplyActivationCAS(path string, request ActivationCASRequest, now time.Time) (ActivationReceipt, error) {
	return edgefrontstate.ApplyActivationCAS(path, request, now)
}

func ReadActivationState(path string) (ActivationState, bool, error) {
	return edgefrontstate.ReadActivationState(path)
}

func PreparePublication(path string, request ActivationCASRequest, now time.Time) (PublicationPreparation, error) {
	return edgefrontstate.PreparePublication(path, request, now)
}

func ReadPublicationPreparation(path string) (PublicationPreparation, bool, error) {
	return edgefrontstate.ReadPublicationPreparation(path)
}

func ApplyActivationRepairCAS(path string, request ActivationRepairCASRequest, expiresAt time.Time) (ActivationReceipt, error) {
	return edgefrontstate.ApplyActivationRepairCAS(path, request, expiresAt)
}

func PrepareActivationRepairPublication(path string, request ActivationRepairCASRequest, expiresAt time.Time) (PublicationPreparation, error) {
	return edgefrontstate.PrepareActivationRepairPublication(path, request, expiresAt)
}
