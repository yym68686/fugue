package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
)

type dnsRecheckContext struct {
	context.Context
	err   error
	reads int
}

func (c *dnsRecheckContext) Err() error {
	c.reads++
	return c.err
}

func TestDNSAuthorityRecheckPreservesEveryPredicateAndShortCircuit(t *testing.T) {
	initial := dnsFactSource{consumer: model.PlatformConsumerInstance{CredentialID: "private-original-credential"},
		parent: model.PlatformArtifact{ContentHash: "private-original-digest"}, group: "private-original-group"}
	reasons := []string{"source_unavailable", "credential", "parent_digest", "assignment", "group", "context"}
	for _, contextErr := range []error{context.Canceled, context.DeadlineExceeded} {
		for mask := 0; mask < 1<<len(reasons); mask++ {
			t.Run(fmt.Sprintf("%s/%06b", contextErr, mask), func(t *testing.T) {
				current := initial
				var readErr error
				if mask&1 != 0 {
					readErr = errors.New("private-source-error")
				}
				if mask&2 != 0 {
					current.consumer.CredentialID = "private-new-credential"
				}
				if mask&4 != 0 {
					current.parent.ContentHash = "private-new-digest"
				}
				if mask&8 != 0 {
					current.lookup.Assignment.ArtifactID = "private-new-artifact"
				}
				if mask&16 != 0 {
					current.group = "private-new-group"
				}
				ctx := &dnsRecheckContext{Context: context.Background()}
				if mask&32 != 0 {
					ctx.err = contextErr
				}
				// The exact original predicate is the compatibility oracle.
				oracle := *ctx
				wantReject := readErr != nil || current.consumer.CredentialID != initial.consumer.CredentialID || current.parent.ContentHash != initial.parent.ContentHash || !reflect.DeepEqual(current.lookup.Assignment, initial.lookup.Assignment) || current.group != initial.group || oracle.Err() != nil
				event := dnsAuthorityRecheckRejection(ctx, initial, current, readErr)
				if (event.Rejection != "") != wantReject || ctx.reads != oracle.reads {
					t.Fatalf("changed rejection or context evaluation: event=%+v reads=%d oracle=%v/%d", event, ctx.reads, wantReject, oracle.reads)
				}
				wantReason := ""
				for bit, reason := range reasons {
					if mask&(1<<bit) != 0 {
						wantReason = reason
						break
					}
				}
				if event.Rejection != wantReason || wantReject && event.Stage != "authority_recheck_rejected" {
					t.Fatal("lost the first failed predicate", event)
				}
				wantClass := ""
				if wantReason == "source_unavailable" {
					wantClass = "unavailable"
				} else if wantReason == "context" {
					wantClass = captureErrorClass(contextErr)
				}
				if event.ErrorClass != wantClass {
					t.Fatal("incorrect error classification", event)
				}
				if wantReason == "assignment" && !reflect.DeepEqual(event.Mismatches, []string{"artifact_id"}) || wantReason != "assignment" && len(event.Mismatches) != 0 {
					t.Fatal("incorrect mismatch fields", event)
				}
				if wantReason == "parent_digest" {
					want := captureDNSParent{ExpectedDigestHash: captureIdentityHash(initial.parent.ContentHash),
						ObservedDigestHash: captureIdentityHash(current.parent.ContentHash)}
					if event.DNSParent == nil || *event.DNSParent != want {
						t.Fatal("authority reread did not retain the exact compared parents", event)
					}
				} else if event.DNSParent != nil {
					t.Fatal("parent evidence attached to a different first rejection", event)
				}
				if mask == 0 && !reflect.DeepEqual(event, captureEvent{}) {
					t.Fatal("successful recheck invented a rejection", event)
				}
				raw, err := json.Marshal(event)
				if err != nil || strings.Contains(string(raw), "private-") {
					t.Fatal("diagnostic retained private values", string(raw), err)
				}
			})
		}
	}
}
