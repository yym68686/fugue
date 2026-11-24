package platformsafety

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func TestPrivateJobHostnameDataCannotHideCommands(t *testing.T) {
	const path = "deploy/releases/edge-worker-us-primary-prewarm/resources.json"
	const host = "tenant.example.test"
	const fixture = `{"apiVersion":"release.fugue.dev/v2","kind":"ComponentResourceSet","items":[{"apiVersion":"batch/v1","kind":"Job","metadata":{"name":"fugue-edge-worker-us-primary-prewarm"},"spec":{"template":{"metadata":{"annotations":{"fugue.pro/private-prewarm-hosts-v1":%s,"debug":"tenant.example.test"}},"spec":{"containers":[{"command":["tenant.example.test"]}]}}}}]}`
	body := func(list string) string { return fmt.Sprintf(fixture, strconv.Quote(list)) }
	valid := body(`["tenant.example.test"]`)
	masked := string(businessCouplingSource(path, []byte(valid)))
	if strings.Count(masked, host) != 2 || !strings.Contains(masked, "configuration-hostnames") {
		t.Fatal("hostname exemption modified an adjacent field or executable")
	}
	for _, in := range []string{body(`["tenant.example.test","echo tenant.example.test"]`), body(`["*.tenant.example.test"]`), strings.Replace(valid, `"kind":"Job"`, `"kind":"Deployment"`, 1), strings.Replace(valid, "hosts-v1", "hosts-v2", 1)} {
		if string(businessCouplingSource(path, []byte(in))) != in {
			t.Fatal("invalid hostname or declaration escaped scanning")
		}
	}
	if string(businessCouplingSource("other/resources.json", []byte(valid))) != valid {
		t.Fatal("other resources were exempted")
	}
}
