package kubeauth

import (
	"net/http"
	"strings"

	"fugue/internal/runtimeobservation"
)

var HTTPObservations runtimeobservation.HTTPRecorder

func classifyRequest(req *http.Request) runtimeobservation.HTTPRequestClass {
	c := runtimeobservation.HTTPRequestClass{Client: "kubernetes", Method: req.Method, Resource: "other", CacheMode: "unspecified"}
	parts := strings.Split(strings.Trim(req.URL.Path, "/"), "/")
	if len(parts) >= 3 && parts[0] == "api" {
		parts = parts[2:]
	} else if len(parts) >= 4 && parts[0] == "apis" {
		parts = parts[3:]
	} else {
		return c
	}
	if len(parts) >= 3 && parts[0] == "namespaces" {
		c.Namespaced = true
		parts = parts[2:]
	}
	if len(parts) > 0 {
		resource := parts[0]
		valid := len(resource) > 0 && len(resource) <= 80
		for _, ch := range resource {
			valid = valid && (ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '.')
		}
		if valid {
			c.Resource = resource
		}
		c.SingleObject = len(parts) > 1
		if len(parts) > 2 {
			// Only Kubernetes subresource names; proxy suffixes can contain
			// arbitrary paths and must never enter diagnostic records.
			switch parts[2] {
			case "status", "scale", "log", "proxy", "exec", "attach", "portforward", "eviction", "binding", "finalize":
				c.Subresource = parts[2]
			default:
				c.Subresource = "other"
			}
		}
	}
	c.MetadataOnly = strings.Contains(req.Header.Get("Accept"), "as=PartialObjectMetadata")
	q := req.URL.Query()
	c.Watch = q.Get("watch") == "true"
	c.LabelSelector = q.Get("labelSelector") != ""
	c.FieldSelector = q.Get("fieldSelector") != ""
	if v, ok := q["resourceVersion"]; ok && len(v) > 0 {
		switch v[0] {
		case "0":
			c.CacheMode = "any"
		case "":
			c.CacheMode = "latest"
		default:
			c.CacheMode = "revision"
		}
	}
	return c
}
