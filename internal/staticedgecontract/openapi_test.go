package staticedgecontract

import (
	"context"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestStaticEdgeOpenAPIContract(t *testing.T) {
	l := openapi3.NewLoader()
	doc, e := l.LoadFromFile("../../contracts/static-edge-management.openapi.yaml")
	if e != nil {
		t.Fatal(e)
	}
	if e = doc.Validate(context.Background()); e != nil {
		t.Fatal(e)
	}
	ops := doc.Components.Schemas["Request"].Value.Properties["operation"].Value.Enum
	for _, op := range ops {
		if !OperationAllowed(op.(string)) {
			t.Fatal("contract operation absent", op)
		}
	}
	if doc.Paths.Len() != 1 {
		t.Fatal("unexpected management surface")
	}
}
