package model

import "testing"

func TestPlanAppDataMaterializationChecksDiskLocalityAndEgress(t *testing.T) {
	spec := AppDataMaterializationSpec{
		LocalityHint:      "same-region",
		RequiredFreeBytes: 20,
		EgressEstimate: &DataEgressEstimate{
			SourceRegion:        "us-east",
			TargetRegion:        "us-west",
			Bytes:               10,
			CrossRegion:         true,
			EstimatedMicroCents: 1,
		},
		Workspaces: []AppDataWorkspaceMaterialization{{
			Workspace:            "training",
			Version:              "v1",
			TargetPath:           "/data",
			LocalityHint:         "same-region",
			EstimatedEgressBytes: 10,
		}},
	}
	plan := PlanAppDataMaterialization(spec, 10)
	if plan.DiskOK {
		t.Fatalf("expected disk check to fail, got %+v", plan)
	}
	if plan.RequiredBytes != 10 || plan.RequiredFreeBytes != 20 || plan.EstimatedEgressBytes != 10 {
		t.Fatalf("unexpected materialization byte plan: %+v", plan)
	}
	if plan.LocalityHint != "same-region" || plan.EgressEstimate == nil || !plan.EgressEstimate.CrossRegion {
		t.Fatalf("unexpected locality/egress plan: %+v", plan)
	}
	if okPlan := PlanAppDataMaterialization(spec, 25); !okPlan.DiskOK {
		t.Fatalf("expected disk check to pass, got %+v", okPlan)
	}
}

func TestHuggingFaceBackendBestEffortResumeProfile(t *testing.T) {
	caps := DataBackendCapabilitiesForProvider(DataBackendProviderHuggingFace)
	if !caps.BestEffortResume || !caps.RangeDownload || caps.StrongResume || caps.S3Compatible {
		t.Fatalf("unexpected Hugging Face resume capabilities: %+v", caps)
	}
}
