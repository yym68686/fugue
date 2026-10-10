package clientmeasurement

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"fugue/internal/model"
)

type delayedProbeBody struct {
	reader    *bytes.Reader
	firstRead bool
}

func (body *delayedProbeBody) Read(buffer []byte) (int, error) {
	if !body.firstRead {
		body.firstRead = true
		time.Sleep(100 * time.Millisecond)
	}
	return body.reader.Read(buffer)
}

func (body *delayedProbeBody) Close() error { return nil }

func TestClientDownloadExcludesTimeBeforeFirstPayloadByte(t *testing.T) {
	report, _ := reportFixture(t)
	permit := report.Plan.Permits[0]
	encoded, _ := json.Marshal(report.Outcomes[0].Attestation)
	response := &http.Response{StatusCode: http.StatusOK, ContentLength: BodyBytes, Header: http.Header{AttestationHeader: {base64.RawURLEncoding.EncodeToString(encoded)}}, Body: &delayedProbeBody{reader: bytes.NewReader(Payload(permit.BodySeed))}}
	started := time.Now()
	outcome := receiveProbeResponse(model.EdgeClientProbeOutcome{AttemptID: permit.AttemptID, StartedAt: started}, permit, response)
	if outcome.Failure != "" || outcome.BytesReceived != BodyBytes || outcome.BodySHA256 != permit.BodySHA256 || outcome.BodySeconds <= 0 {
		t.Fatal(outcome)
	}
	if time.Since(started).Seconds()-outcome.BodySeconds < 0.09 {
		t.Fatal("initial waiting polluted downstream transfer time", outcome.BodySeconds)
	}
}

func TestIncompleteOrTransformedProbeNeverBecomesSuccessfulThroughput(t *testing.T) {
	report, _ := reportFixture(t)
	permit := report.Plan.Permits[0]
	encoded, _ := json.Marshal(report.Outcomes[0].Attestation)
	for _, scenario := range []string{"partial", "changed", "compressed", "redirect", "missing_attestation"} {
		response := &http.Response{StatusCode: http.StatusOK, ContentLength: BodyBytes, Header: http.Header{AttestationHeader: {base64.RawURLEncoding.EncodeToString(encoded)}}, Body: io.NopCloser(bytes.NewReader(Payload(permit.BodySeed)))}
		switch scenario {
		case "partial":
			response.Body = io.NopCloser(bytes.NewReader([]byte("incomplete")))
		case "changed":
			response.Body = io.NopCloser(bytes.NewReader(bytes.Repeat([]byte{1}, BodyBytes)))
		case "compressed":
			response.Header.Set("Content-Encoding", "gzip")
		case "redirect":
			response.StatusCode = http.StatusTemporaryRedirect
		case "missing_attestation":
			response.Header.Del(AttestationHeader)
		}
		outcome := receiveProbeResponse(model.EdgeClientProbeOutcome{AttemptID: permit.AttemptID, StartedAt: time.Now()}, permit, response)
		if outcome.Failure == "" {
			t.Fatal("invalid response admitted", scenario)
		}
	}
}
