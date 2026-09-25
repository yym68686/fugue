package entryfailover

import (
	"context"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestIndependentExecutorOpenAPIContract(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromFile("../../contracts/entry-failover.openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if err = doc.Validate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if doc.Paths.Len() != 7 {
		t.Fatalf("unexpected executor management surface: %d", doc.Paths.Len())
	}
}
