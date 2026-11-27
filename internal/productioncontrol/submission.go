package productioncontrol

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

const MaxSubmissionBytes = 3 << 20

// Submission is a portable review envelope. It includes the exact bytes
// bound by the intent, not a reference to a Git commit or mutable artifact.
type Submission struct {
	Intent  Intent `json:"intent"`
	Payload []byte `json:"payload"`
}

func DecodeSubmission(raw []byte) (Submission, error) {
	var s Submission
	if len(raw) > MaxSubmissionBytes || decodeControlJSON(raw, &s) != nil {
		return Submission{}, errors.New("invalid production intent submission")
	}
	if err := s.Intent.VerifyPayload(s.Payload); err != nil {
		return Submission{}, err
	}
	return s, nil
}

func DecodeApproval(raw []byte) (Approval, error) {
	var a Approval
	if len(raw) > 4096 || decodeControlJSON(raw, &a) != nil {
		return Approval{}, errors.New("invalid production approval document")
	}
	return a, nil
}

func decodeControlJSON(raw []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if decoder.Decode(new(json.RawMessage)) != io.EOF {
		return errors.New("unexpected data after production control document")
	}
	return nil
}
