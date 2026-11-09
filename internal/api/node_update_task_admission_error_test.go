package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"fugue/internal/store"
)

func TestNodeUpdateTaskAdmissionHTTPErrorContract(t *testing.T) {
	for _, test := range []struct {
		name    string
		err     error
		status  int
		message string
	}{
		{"missing dependency", fmt.Errorf("private dependency: %w", errors.Join(store.ErrNotFound, store.ErrConflict)), http.StatusConflict, "resource conflict"},
		{"changed dependency", fmt.Errorf("private tuple: %w", store.ErrConflict), http.StatusConflict, "resource conflict"},
		{"missing task or target", store.ErrNotFound, http.StatusNotFound, "resource not found"},
		{"invalid request", store.ErrInvalidInput, http.StatusBadRequest, "invalid input"},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			(&Server{}).writeNodeUpdateTaskAdmissionError(response, test.err)
			var body struct {
				Error string `json:"error"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if response.Code != test.status || body.Error != test.message {
				t.Fatalf("status=%d error=%q", response.Code, body.Error)
			}
		})
	}
}
