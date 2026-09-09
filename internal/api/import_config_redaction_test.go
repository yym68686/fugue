package api

import (
	"testing"

	"fugue/internal/model"
)

func TestImportConfigurationBaselineRedactsSecrets(t *testing.T) {
	spec := &model.AppSpec{Env: map[string]string{"TOKEN": "sensitive-value"}, Files: []model.AppFile{{Path: "/config", Content: "private-content", Secret: true}}}
	op := model.Operation{ConfigBaseSpec: spec}
	redacted := sanitizeOperationForAPI(op)
	if redacted.ConfigBaseSpec == nil || redacted.ConfigBaseSpec.Env["TOKEN"] == "sensitive-value" || redacted.ConfigBaseSpec.Files[0].Content == "private-content" {
		t.Fatal("configuration baseline leaked a secret")
	}
	if op.ConfigBaseSpec.Env["TOKEN"] != "sensitive-value" || op.ConfigBaseSpec.Files[0].Content != "private-content" {
		t.Fatal("redaction mutated the stored baseline")
	}
}
