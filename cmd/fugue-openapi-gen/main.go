package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"
)

type routeDefinition struct {
	Method      string
	Path        string
	Pattern     string
	OperationID string
	HandlerName string
	Auth        string
}

func main() {
	var (
		specPath  string
		routesOut string
		specOut   string
		checkOnly bool
	)
	flag.StringVar(&specPath, "spec", "openapi/openapi.yaml", "path to the authoritative OpenAPI specification")
	flag.StringVar(&routesOut, "routes-out", "internal/api/routes_gen.go", "path to generated route registrations")
	flag.StringVar(&specOut, "spec-out", "internal/apispec/spec_gen.go", "path to generated embedded spec data")
	flag.BoolVar(&checkOnly, "check", false, "verify generated files are up to date without writing them")
	flag.Parse()

	doc, rawYAML, err := loadDocument(specPath)
	if err != nil {
		exitf("load OpenAPI document: %v", err)
	}

	routes, err := collectRoutes(doc)
	if err != nil {
		exitf("collect routes: %v", err)
	}

	rawJSON, err := doc.MarshalJSON()
	if err != nil {
		exitf("marshal OpenAPI document: %v", err)
	}
	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, rawJSON, "", "  "); err != nil {
		exitf("pretty-print OpenAPI JSON: %v", err)
	}
	prettyJSON.WriteByte('\n')

	routeSource, err := renderRoutesFile(routes)
	if err != nil {
		exitf("render routes file: %v", err)
	}
	specSource, err := renderSpecFile(rawYAML, prettyJSON.Bytes(), routes)
	if err != nil {
		exitf("render spec file: %v", err)
	}

	if err := writeOrCheck(routesOut, routeSource, checkOnly); err != nil {
		exitf("%v", err)
	}
	if err := writeOrCheck(specOut, specSource, checkOnly); err != nil {
		exitf("%v", err)
	}
}

func loadDocument(specPath string) (*openapi3.T, []byte, error) {
	rawYAML, err := os.ReadFile(specPath)
	if err != nil {
		return nil, nil, err
	}

	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = true

	doc, err := loader.LoadFromFile(specPath)
	if err != nil {
		return nil, nil, err
	}
	if err := doc.Validate(context.Background()); err != nil {
		return nil, nil, err
	}
	return doc, rawYAML, nil
}

func collectRoutes(doc *openapi3.T) ([]routeDefinition, error) {
	if doc.Paths == nil || doc.Paths.Len() == 0 {
		return nil, errors.New("OpenAPI document does not define any paths")
	}

	seenOperationIDs := map[string]string{}
	paths := doc.Paths.Map()
	pathKeys := make([]string, 0, len(paths))
	for path := range paths {
		pathKeys = append(pathKeys, path)
	}
	sort.Strings(pathKeys)

	var routes []routeDefinition
	for _, path := range pathKeys {
		pathItem := paths[path]
		if pathItem == nil {
			return nil, fmt.Errorf("path %q is nil", path)
		}
		for _, method := range []string{"GET", "POST", "PUT", "PATCH", "DELETE"} {
			operation := pathItem.GetOperation(method)
			if operation == nil {
				continue
			}

			handlerName, err := extensionString(operation.Extensions, "x-fugue-handler")
			if err != nil {
				return nil, fmt.Errorf("%s %s: %w", method, path, err)
			}
			if strings.TrimSpace(operation.OperationID) == "" {
				return nil, fmt.Errorf("%s %s: operationId is required", method, path)
			}
			if previous, exists := seenOperationIDs[operation.OperationID]; exists {
				return nil, fmt.Errorf("duplicate operationId %q for %s and %s %s", operation.OperationID, previous, method, path)
			}
			seenOperationIDs[operation.OperationID] = method + " " + path

			authKind, err := inferAuthKind(doc, operation)
			if err != nil {
				return nil, fmt.Errorf("%s %s: %w", method, path, err)
			}

			routes = append(routes, routeDefinition{
				Method:      method,
				Path:        path,
				Pattern:     method + " " + path,
				OperationID: operation.OperationID,
				HandlerName: handlerName,
				Auth:        authKind,
			})
		}
	}

	if len(routes) == 0 {
		return nil, errors.New("OpenAPI document does not define any operations")
	}
	return routes, nil
}

func extensionString(extensions map[string]any, name string) (string, error) {
	if extensions == nil {
		return "", fmt.Errorf("missing %s extension", name)
	}
	raw, ok := extensions[name]
	if !ok {
		return "", fmt.Errorf("missing %s extension", name)
	}
	value, ok := raw.(string)
	if !ok || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("%s extension must be a non-empty string", name)
	}
	return value, nil
}

func inferAuthKind(doc *openapi3.T, operation *openapi3.Operation) (string, error) {
	security := doc.Security
	if operation.Security != nil {
		security = *operation.Security
	}

	auth := "none"
	for _, requirement := range security {
		if _, ok := requirement["RuntimeBearerAuth"]; ok {
			if auth == "api" {
				return "", errors.New("cannot use BearerAuth and RuntimeBearerAuth on the same operation")
			}
			auth = "runtime"
		}
		if _, ok := requirement["BearerAuth"]; ok {
			if auth == "runtime" {
				return "", errors.New("cannot use BearerAuth and RuntimeBearerAuth on the same operation")
			}
			auth = "api"
		}
	}
	return auth, nil
}

func renderRoutesFile(routes []routeDefinition) ([]byte, error) {
	var b strings.Builder
	b.WriteString("// Code generated by cmd/fugue-openapi-gen; DO NOT EDIT.\n")
	b.WriteString("\n")
	b.WriteString("package api\n")
	b.WriteString("\n")
	b.WriteString("import \"net/http\"\n")
	b.WriteString("\n")
	b.WriteString("func (s *Server) registerGeneratedRoutes(mux *http.ServeMux) {\n")
	for _, route := range routes {
		handlerExpr := "http.HandlerFunc(s." + route.HandlerName + ")"
		switch route.Auth {
		case "api":
			handlerExpr = "s.auth.RequireAPI(" + handlerExpr + ")"
		case "runtime":
			handlerExpr = "s.auth.RequireRuntime(" + handlerExpr + ")"
		}
		fmt.Fprintf(&b, "\tmux.Handle(%q, %s)\n", route.Pattern, handlerExpr)
	}
	b.WriteString("}\n")

	return format.Source([]byte(b.String()))
}

func renderSpecFile(rawYAML, rawJSON []byte, routes []routeDefinition) ([]byte, error) {
	var b strings.Builder
	b.WriteString("// Code generated by cmd/fugue-openapi-gen; DO NOT EDIT.\n")
	b.WriteString("\n")
	b.WriteString("package apispec\n")
	b.WriteString("\n")
	b.WriteString("var openAPISpecYAML = []byte(")
	b.WriteString(strconv.Quote(string(rawYAML)))
	b.WriteString(")\n")
	b.WriteString("\n")
	b.WriteString("var openAPISpecJSON = []byte(")
	b.WriteString(strconv.Quote(string(rawJSON)))
	b.WriteString(")\n")
	b.WriteString("\n")
	b.WriteString("var openAPIRoutes = []Route{\n")
	for _, route := range routes {
		fmt.Fprintf(&b, "\t{Method: %q, Path: %q, Pattern: %q, OperationID: %q, HandlerName: %q, Auth: AuthKind(%q)},\n",
			route.Method, route.Path, route.Pattern, route.OperationID, route.HandlerName, route.Auth)
	}
	b.WriteString("}\n")

	return format.Source([]byte(b.String()))
}

func writeOrCheck(path string, want []byte, checkOnly bool) error {
	current, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if bytes.Equal(current, want) {
		return nil
	}
	if checkOnly {
		return fmt.Errorf("generated file is out of date: %s", path)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create output directory for %s: %w", path, err)
	}
	if err := os.WriteFile(path, want, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func exitf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
