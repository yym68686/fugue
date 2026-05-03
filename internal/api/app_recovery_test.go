package api

import (
	"testing"

	"fugue/internal/model"
)

func TestRecoverAppDeployBaselineNormalizesLegacyManagedRuntimeImage(t *testing.T) {
	t.Parallel()

	server := &Server{
		registryPushBase: "registry.push.example",
		registryPullBase: "registry.fugue.internal:5000",
	}
	app := model.App{
		Spec: model.AppSpec{
			Image:    "10.128.0.2:30500/fugue-apps/demo@sha256:abc123",
			Replicas: 2,
		},
		Source: &model.AppSource{
			Type:             model.AppSourceTypeUpload,
			UploadID:         "upload_demo",
			ArchiveSHA256:    "abcdef123456",
			ResolvedImageRef: "registry.push.example/fugue-apps/demo:upload-abcdef123456",
		},
	}

	spec, source, err := server.recoverAppDeployBaseline(app)
	if err != nil {
		t.Fatalf("recover deploy baseline: %v", err)
	}
	want := "registry.fugue.internal:5000/fugue-apps/demo@sha256:abc123"
	if spec.Image != want {
		t.Fatalf("expected normalized image %q, got %q", want, spec.Image)
	}
	if source == nil || source.ResolvedImageRef != app.Source.ResolvedImageRef {
		t.Fatalf("expected source to be preserved, got %#v", source)
	}
}
