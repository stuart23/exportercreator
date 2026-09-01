// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exportercreator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer"
	"github.com/stuart23/exportercreator/internal/metadata"
)

func TestParsePropertyPath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want []string
	}{
		{"single key", "app", []string{"app"}},
		{"nested", "pod.labels.app", []string{"pod", "labels", "app"}},
		{"bracketed dotted key", `pod.labels["app.kubernetes.io/name"]`,
			[]string{"pod", "labels", "app.kubernetes.io/name"}},
		{"adjacent brackets", `labels["a.b"]["c.d"]`, []string{"labels", "a.b", "c.d"}},
		{"escaped quote in a key", `labels["a\"b"]`, []string{"labels", `a"b`}},
		{"escaped backslash in a key", `labels["a\\b"]`, []string{"labels", `a\b`}},
		{"dot after bracket", `pod.labels["a.b"].sub`, []string{"pod", "labels", "a.b", "sub"}},
		{"key with a slash but no dots", "labels.app/name", []string{"labels", "app/name"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parsePropertyPath(tc.path)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// A path that cannot be parsed would match nothing for every resource, so it is an error rather
// than an empty result.
func TestParsePropertyPath_Rejects(t *testing.T) {
	for _, tc := range []struct{ name, path string }{
		{"empty", ""},
		{"leading dot", ".app"},
		{"trailing dot", "labels."},
		{"double dot", "pod..labels"},
		{"unterminated bracket", `labels["a.b"`},
		{"unterminated quote", `labels["a.b]`},
		{"junk after quoted key", `labels["a.b"x]`},
		{"junk after bracket", `labels["a.b"]x`},
		{"empty bracket", "labels[]"},
		{"empty quoted key", `labels[""]`},
		// OTTL quotes bracketed keys with double quotes only, and a path starts with a name.
		{"single quotes", `labels['a.b']`},
		{"unquoted bracket key", "labels[app]"},
		{"integer index", "labels[0]"},
		{"path starting with a bracket", `["a.b"].c`},
		{"unsupported escape", `labels["a\nb"]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parsePropertyPath(tc.path)
			assert.Error(t, err, "%q should not parse", tc.path)
		})
	}
}

// namespacedEndpoint is a port endpoint whose pod carries the namespaced labels a real
// Kubernetes deployment has, alongside a plain one.
func namespacedEndpoint(t *testing.T) map[string]any {
	t.Helper()
	e := observer.Endpoint{ID: "port-1", Target: "10.0.0.5:4317", Details: &observer.Port{
		Name: "otlp", Port: 4317, Transport: observer.ProtocolTCP,
		Pod: observer.Pod{Name: "checkout-1", Namespace: "default", Labels: map[string]string{
			"app":                           "checkout",
			"app.kubernetes.io/name":        "checkout",
			"topology.kubernetes.io/region": "us-east-1",
		}}}}
	env, err := e.Env()
	require.NoError(t, err)
	return flattenProperties(env)
}

// Every namespaced Kubernetes label has dots in its key, so splitting the path on dots alone
// could not address one: pod.labels.app.kubernetes.io/name looks for a "kubernetes" key inside
// the value of "app". Brackets name the key whole.
func TestGetNestedProperty_DottedKeys(t *testing.T) {
	props := namespacedEndpoint(t)

	assert.Equal(t, "checkout", getNestedProperty(props, `pod.labels["app.kubernetes.io/name"]`))
	assert.Equal(t, "us-east-1", getNestedProperty(props, `pod.labels["topology.kubernetes.io/region"]`))

	// Single quotes are not a string in OTTL, so they are not one here either.
	assert.Nil(t, getNestedProperty(props, `pod.labels['topology.kubernetes.io/region']`))

	// Splitting on dots is still how an ordinary key is addressed.
	assert.Equal(t, "checkout", getNestedProperty(props, "pod.labels.app"))
	assert.Equal(t, "checkout-1", getNestedProperty(props, "pod.name"))

	// A dotted key written without brackets still resolves to nothing - it names a path that
	// does not exist - but it is now rejected by config validation before it can be used.
	assert.Nil(t, getNestedProperty(props, "pod.labels.app.kubernetes.io/name"))

	// A key that is simply absent is still no match.
	assert.Nil(t, getNestedProperty(props, `pod.labels["nope.example.com/x"]`))
}

// The whole point: a routing rule can match on a namespaced label.
func TestRoute_MatchesOnANamespacedLabel(t *testing.T) {
	tt := componenttest.NewTelemetry()
	telemetry, err := metadata.NewTelemetryBuilder(tt.NewTelemetrySettings())
	require.NoError(t, err)
	defer telemetry.Shutdown()

	router := newTelemetryRouter([]RoutingRule{{
		ResourceAttribute: "k8s.pod.labels.app.kubernetes.io/name",
		EndpointProperty:  `pod.labels["app.kubernetes.io/name"]`,
	}}, telemetry)

	e := observer.Endpoint{ID: "port-1", Target: "10.0.0.5:4317", Details: &observer.Port{
		Name: "otlp", Port: 4317, Pod: observer.Pod{Name: "checkout-1", Labels: map[string]string{
			"app.kubernetes.io/name": "checkout",
		}}}}
	env, err := e.Env()
	require.NoError(t, err)
	router.AddExporter(e.ID, &nopExporterComponent{}, env, "otlp")

	matching := pcommon.NewMap()
	matching.PutStr("k8s.pod.labels.app.kubernetes.io/name", "checkout")
	assert.Len(t, router.Route(matching), 1, "a namespaced label must be routable")

	other := pcommon.NewMap()
	other.PutStr("k8s.pod.labels.app.kubernetes.io/name", "billing")
	assert.Empty(t, router.Route(other), "and must still discriminate")
}

// A path that cannot be parsed is rejected when the configuration is read, rather than matching
// nothing for the life of the collector.
func TestConfig_RejectsUnparseableEndpointProperty(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	err := cfg.Unmarshal(confmap.NewFromStringMap(map[string]any{
		"routing": map[string]any{
			"rules": []any{map[string]any{
				"resource_attribute": "k8s.pod.labels.app",
				"endpoint_property":  `pod.labels["app.kubernetes.io/name"`,
			}},
		},
	}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "endpoint_property")
	assert.Contains(t, err.Error(), "brackets", "the error should say how to write a dotted key")
}

// The bracket form is accepted, so the fix is reachable from configuration.
func TestConfig_AcceptsBracketedEndpointProperty(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	require.NoError(t, cfg.Unmarshal(confmap.NewFromStringMap(map[string]any{
		"routing": map[string]any{
			"rules": []any{map[string]any{
				"resource_attribute": "k8s.pod.labels.app.kubernetes.io/name",
				"endpoint_property":  `pod.labels["app.kubernetes.io/name"]`,
			}},
		},
	})))
	require.Len(t, cfg.Routing.Rules, 1)
	assert.Equal(t, `pod.labels["app.kubernetes.io/name"]`, cfg.Routing.Rules[0].EndpointProperty)
}

// A resource attribute is a flat name, so brackets in it are punctuation rather than structure:
// they mark where the prefix ends and the label key begins, and resolve to the same name.
func TestResolveAttributeName(t *testing.T) {
	for _, tc := range []struct{ name, configured, want string }{
		{"plain name, unchanged", "k8s.pod.labels.app", "k8s.pod.labels.app"},
		{"bracketed label key", `k8s.pod.labels["app.kubernetes.io/name"]`, "k8s.pod.labels.app.kubernetes.io/name"},
		{"bracketing changes nothing else", `k8s.pod.labels["app"]`, "k8s.pod.labels.app"},
		{"no dots at all", "service.name", "service.name"},
		{"chained brackets", `a["b.c"]["d.e"]`, "a.b.c.d.e"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveAttributeName(tc.configured)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// Both spellings name the same attribute, so both route the same telemetry. This is the point:
// the bracketed one is legible and the plain one keeps working.
func TestRoute_BothAttributeSpellingsMatchTheSameTelemetry(t *testing.T) {
	for _, spelling := range []string{
		"k8s.pod.labels.app.kubernetes.io/name",
		`k8s.pod.labels["app.kubernetes.io/name"]`,
	} {
		t.Run(spelling, func(t *testing.T) {
			tt := componenttest.NewTelemetry()
			telemetry, err := metadata.NewTelemetryBuilder(tt.NewTelemetrySettings())
			require.NoError(t, err)
			defer telemetry.Shutdown()

			router := newTelemetryRouter([]RoutingRule{{
				ResourceAttribute: spelling,
				EndpointProperty:  `pod.labels["app.kubernetes.io/name"]`,
			}}, telemetry)

			e := observer.Endpoint{ID: "port-1", Target: "10.0.0.5:4317", Details: &observer.Port{
				Name: "otlp", Port: 4317, Pod: observer.Pod{Name: "checkout-1",
					Labels: map[string]string{"app.kubernetes.io/name": "checkout"}}}}
			env, err := e.Env()
			require.NoError(t, err)
			router.AddExporter(e.ID, &nopExporterComponent{}, env, "otlp")

			// The attribute on the telemetry carries the flat name either way.
			attrs := pcommon.NewMap()
			attrs.PutStr("k8s.pod.labels.app.kubernetes.io/name", "checkout")
			assert.Len(t, router.Route(attrs), 1)

			other := pcommon.NewMap()
			other.PutStr("k8s.pod.labels.app.kubernetes.io/name", "billing")
			assert.Empty(t, router.Route(other))
		})
	}
}

// An unparseable bracketed attribute is rejected when the configuration is read, like an
// unparseable endpoint property.
func TestConfig_RejectsUnparseableResourceAttribute(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	err := cfg.Unmarshal(confmap.NewFromStringMap(map[string]any{
		"routing": map[string]any{
			"rules": []any{map[string]any{
				"resource_attribute": `k8s.pod.labels["app.kubernetes.io/name"`,
				"endpoint_property":  "pod.labels.app",
			}},
		},
	}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resource_attribute")
}
