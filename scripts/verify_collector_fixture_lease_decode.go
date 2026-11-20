//go:build ignore

// This helper decodes only typed Lease requests sent to the isolated fixture.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	coordination "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
)

func run() error {
	const limit = 256 << 10
	scheme := runtime.NewScheme()
	if err := coordination.AddToScheme(scheme); err != nil {
		return err
	}
	codec := protobuf.NewSerializer(scheme, scheme)
	var raw []byte
	selfTest := len(os.Args) == 2 && os.Args[1] == "--self-test"
	if selfTest {
		var encoded bytes.Buffer
		lease := &coordination.Lease{TypeMeta: metav1.TypeMeta{Kind: "Lease", APIVersion: coordination.SchemeGroupVersion.String()},
			ObjectMeta: metav1.ObjectMeta{Name: "cursor", Namespace: "fixture", ResourceVersion: "37"}}
		if err := codec.Encode(lease, &encoded); err != nil {
			return err
		}
		raw = encoded.Bytes()
	} else {
		var err error
		raw, err = io.ReadAll(io.LimitReader(os.Stdin, limit+1))
		if err != nil {
			return err
		}
	}
	if len(raw) > limit {
		return fmt.Errorf("fixture Lease exceeds bound")
	}
	value, gvk, err := codec.Decode(raw, nil, nil)
	if err != nil {
		return err
	}
	lease, ok := value.(*coordination.Lease)
	if !ok || gvk == nil || *gvk != coordination.SchemeGroupVersion.WithKind("Lease") {
		return fmt.Errorf("unexpected fixture object type")
	}
	lease.SetGroupVersionKind(*gvk)
	if selfTest && (lease.Name != "cursor" || lease.ResourceVersion != "37") {
		return fmt.Errorf("fixture Lease round-trip mismatch")
	}
	return json.NewEncoder(os.Stdout).Encode(lease)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
