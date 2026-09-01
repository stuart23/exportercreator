// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exportercreator // import "github.com/stuart23/exportercreator"

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer"
	"github.com/stuart23/exportercreator/internal/metadata"
)

// telemetryRouter routes telemetry to exporters based on resource attribute matching.
type telemetryRouter struct {
	mu    sync.RWMutex
	rules []RoutingRule
	// exporters maps an endpoint to every exporter created for it. One endpoint may match
	// several exporter templates, each producing its own exporter with its own properties.
	exporters map[observer.EndpointID][]*routedExporter
	// count is the total number of exporters across every endpoint in exporters, kept in
	// step with it under mu. Routing reads the count several times per batch, and totalling
	// the map each time made that scale with the number of exporters.
	count int
	// byType is the same total broken down by exporter type, for the exporters_count
	// attribute. A type stays in the map once seen, at zero, so that the gauge keeps
	// reporting zero for it rather than leaving its last non-zero value standing.
	byType    map[string]int
	telemetry *metadata.TelemetryBuilder
	logger    *zap.Logger
}

// routedExporter holds an exporter with its endpoint properties for matching.
type routedExporter struct {
	exporter   component.Component
	properties map[string]any // Flattened endpoint properties
	// exporterType is the type of the template this exporter was built from, e.g. "otlp".
	// Recorded so removal can decrement the right series without re-deriving it.
	exporterType string
}

// newTelemetryRouter creates a new telemetry router with the given routing rules.
func newTelemetryRouter(rules []RoutingRule, telemetry *metadata.TelemetryBuilder) *telemetryRouter {
	// Resolve each rule's attribute spelling once. Config.Unmarshal has already rejected one
	// that does not parse; a router built another way falls back to the spelling as given.
	resolved := make([]RoutingRule, len(rules))
	for i, rule := range rules {
		resolved[i] = rule
		if name, err := resolveAttributeName(rule.ResourceAttribute); err == nil {
			resolved[i].attributeName = name
		} else {
			resolved[i].attributeName = rule.ResourceAttribute
		}
	}

	return &telemetryRouter{
		rules:     resolved,
		exporters: make(map[observer.EndpointID][]*routedExporter),
		byType:    map[string]int{},
		telemetry: telemetry,
		logger:    nil, // Will be set if logger is available
	}
}

// setLogger sets the logger for routing failure logging.
func (r *telemetryRouter) setLogger(logger *zap.Logger) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logger = logger
}

// AddExporter registers an exporter with its endpoint properties. exporterType is the type of
// the template it was built from, and labels it in the exporters_count gauge.
func (r *telemetryRouter) AddExporter(id observer.EndpointID, exp component.Component, env observer.EndpointEnv, exporterType string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.exporters[id] = append(r.exporters[id], &routedExporter{
		exporter:     exp,
		properties:   flattenProperties(env),
		exporterType: exporterType,
	})
	r.count++
	r.byType[exporterType]++

	r.recordCountLocked()
}

// RemoveExporter unregisters every exporter created for an endpoint.
func (r *telemetryRouter) RemoveExporter(id observer.EndpointID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, re := range r.exporters[id] {
		r.byType[re.exporterType]--
	}
	r.count -= len(r.exporters[id])
	delete(r.exporters, id)

	r.recordCountLocked()
}

// recordCountLocked reports the exporter count per type. The caller must hold r.mu.
//
// Every known type is reported on every change, including the ones now at zero: a gauge only
// reports the series it is given, so a type that stopped being written would keep exporting
// whatever it last held.
func (r *telemetryRouter) recordCountLocked() {
	if r.telemetry == nil {
		return
	}
	for exporterType, n := range r.byType {
		r.telemetry.ExporterCreatorExportersCount.Record(context.Background(), int64(n),
			metric.WithAttributes(attribute.String("exporter_type", exporterType)))
	}
}

// Route returns all exporters that match the given resource attributes.
func (r *telemetryRouter) Route(resourceAttrs pcommon.Map) []component.Component {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Route runs once per resource on every export, so everything logged here is on the hot
	// path and belongs at debug level. Rendering attributes for logging also allocates, so it
	// is gated on debug actually being enabled.
	debug := r.logger != nil && r.logger.Core().Enabled(zapcore.DebugLevel)

	if debug {
		r.logger.Debug("routing telemetry",
			zap.Any("resource_attributes", attrsToMap(resourceAttrs)),
			zap.Int("routing_rules", len(r.rules)),
			zap.Int("available_exporters", r.count),
		)
	}

	if len(r.rules) == 0 {
		if debug {
			r.logger.Debug("telemetry could not be routed: no routing rules configured",
				zap.Any("resource_attributes", attrsToMap(resourceAttrs)),
				zap.Int("available_exporters", r.count),
			)
		}
		return nil
	}

	if r.count == 0 {
		if debug {
			r.logger.Debug("telemetry could not be routed: no exporters available",
				zap.Any("resource_attributes", attrsToMap(resourceAttrs)),
				zap.Int("routing_rules", len(r.rules)),
			)
		}
		return nil
	}

	var matched []component.Component
	for id, exps := range r.exporters {
		for _, exp := range exps {
			matchedRules := r.matchesAllRules(resourceAttrs, exp.properties)
			if matchedRules {
				matched = append(matched, exp.exporter)
			}
			if debug {
				msg := "exporter did not match routing rules"
				if matchedRules {
					msg = "exporter matched routing rules"
				}
				r.logger.Debug(msg,
					zap.String("endpoint_id", string(id)),
					zap.Any("metrics_resource_attributes", attrsToMap(resourceAttrs)),
					zap.Any("exporter_properties", resourceAttributeProps(exp.properties)),
				)
			}
		}
	}

	// Log why routing failed if no exporters matched. This is a routine condition when a
	// pipeline carries telemetry for endpoints this exporter does not serve, so it stays at
	// debug level; the volume that was dropped is reported by the non-routable metric.
	if debug && len(matched) == 0 {
		r.logger.Debug("telemetry did not match any routing rules",
			zap.Any("resource_attributes", attrsToMap(resourceAttrs)),
			zap.Int("available_exporters", r.count),
			zap.Int("routing_rules", len(r.rules)),
		)

		// Log each exporter's properties and the rules, so a mismatch can be diagnosed from
		// a single debug session without restarting.
		for id, exps := range r.exporters {
			for _, exp := range exps {
				exportProps := resourceAttributeProps(exp.properties)
				if labels, ok := exp.properties["labels"].(map[string]string); ok {
					exportProps["labels"] = labels
				}
				if len(exportProps) > 0 {
					r.logger.Debug("exporter endpoint properties",
						zap.String("endpoint_id", string(id)),
						zap.Any("properties", exportProps),
					)
				}
			}
		}

		for i, rule := range r.rules {
			r.logger.Debug("routing rule",
				zap.Int("rule_index", i),
				zap.String("resource_attribute", rule.ResourceAttribute),
				zap.String("endpoint_property", rule.EndpointProperty),
			)
		}
	}

	// Defensive check: with more than one exporter configured, every one of them matching the
	// same resource suggests the rules are not discriminating. With a single exporter this is
	// the normal case, so it is not worth reporting at all. Either way this is evaluated once
	// per resource, so it stays at debug level rather than flooding the log.
	if total := r.count; debug && total > 1 && len(matched) == total {
		r.logger.Debug("all exporters matched routing rules - this may indicate a configuration issue",
			zap.Int("exporter_count", len(matched)),
			zap.Int("rule_count", len(r.rules)),
		)
	}

	return matched
}

// resourceAttributeProps extracts an endpoint's spec.resourceAttributes for logging.
func resourceAttributeProps(properties map[string]any) map[string]any {
	props := make(map[string]any, 1)
	if spec, ok := properties["spec"].(map[string]any); ok {
		if resourceAttributes, ok := spec["resourceAttributes"].(map[string]any); ok {
			props["spec.resourceAttributes"] = resourceAttributes
		}
	}
	return props
}

// Count returns the current number of exporters across all endpoints. It takes r.mu, so code
// already holding it must read r.count directly: sync.RWMutex is not reentrant, and a writer
// queued between the two acquisitions blocks the second one forever.
func (r *telemetryRouter) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.count
}

// matchesAllRules checks if all routing rules match for the given resource attributes and endpoint properties.
// Returns true if all rules match, false otherwise.
// Logs why rules fail at debug level; a rule failing is the normal outcome for every exporter
// that a given resource is not destined for, so it is not an error.
func (r *telemetryRouter) matchesAllRules(resourceAttrs pcommon.Map, properties map[string]any) bool {
	// If there are no rules, nothing matches (this should never happen as Route() returns nil early,
	// but we handle it defensively)
	if len(r.rules) == 0 {
		return false
	}
	debug := r.logger != nil && r.logger.Core().Enabled(zapcore.DebugLevel)
	for _, rule := range r.rules {
		// Get the resource attribute value
		attrVal, ok := resourceAttrs.Get(rule.attributeName)
		if !ok {
			if debug {
				r.logger.Debug("routing rule failed: resource attribute not found in telemetry",
					zap.String("resource_attribute", rule.ResourceAttribute),
					zap.String("endpoint_property", rule.EndpointProperty),
					zap.Any("available_resource_attributes", getResourceAttributeKeys(resourceAttrs)),
				)
			}
			return false
		}

		// Get the endpoint property value using dot notation
		propVal := getNestedProperty(properties, rule.EndpointProperty)
		if propVal == nil {
			if debug {
				r.logger.Debug("routing rule failed: endpoint property not found",
					zap.String("resource_attribute", rule.ResourceAttribute),
					zap.String("resource_value", attrVal.AsString()),
					zap.String("endpoint_property", rule.EndpointProperty),
					zap.Any("available_properties", getTopLevelKeys(properties)),
				)
			}
			return false
		}

		// Compare values
		attrStr := attrVal.AsString()
		propStr := toString(propVal)
		if attrStr != propStr {
			if debug {
				r.logger.Debug("routing rule failed: values don't match",
					zap.String("resource_attribute", rule.ResourceAttribute),
					zap.String("resource_value", attrStr),
					zap.String("endpoint_property", rule.EndpointProperty),
					zap.String("endpoint_value", propStr),
					zap.Any("property_type", fmt.Sprintf("%T", propVal)),
				)
			}
			return false
		}
		if debug {
			r.logger.Debug("routing rule matched",
				zap.String("resource_attribute", rule.ResourceAttribute),
				zap.String("resource_value", attrStr),
				zap.String("endpoint_property", rule.EndpointProperty),
				zap.String("endpoint_value", propStr),
			)
		}
	}
	if debug {
		r.logger.Debug("all routing rules matched",
			zap.Int("rule_count", len(r.rules)),
		)
	}
	return true
}

// getResourceAttributeKeys returns the keys of resource attributes for logging.
func getResourceAttributeKeys(attrs pcommon.Map) []string {
	keys := make([]string, 0, attrs.Len())
	attrs.Range(func(k string, v pcommon.Value) bool {
		keys = append(keys, k)
		return true
	})
	return keys
}

// getTopLevelKeys returns the top-level keys of a map for debugging.
func getTopLevelKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// flattenProperties converts the endpoint env to a flattened map for property access.
func flattenProperties(env observer.EndpointEnv) map[string]any {
	result := make(map[string]any)
	for k, v := range env {
		result[k] = v
	}
	return result
}

// parsePropertyPath splits an endpoint property path into the keys it names.
//
// The syntax follows OTTL's, so that a path here reads the same as the equivalent path in a
// transform processor or a filter: keys are separated by dots, and a key that itself contains
// dots - which every namespaced Kubernetes label does - is written in brackets and double
// quoted, with \" and \\ escapes.
//
//	pod.labels["app.kubernetes.io/name"]
//	pod.labels["a"]["b"]
//
// See https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/pkg/ottl/contexts/ottlmetric/README.md
//
// Every bracketed and dotted form OTTL accepts is accepted here and means the same thing. The
// differences, all documented in the README, are at the edges:
//
//   - OTTL restricts an unbracketed segment to [a-z][a-z0-9_]*, so labels.appName is not a path
//     to it and labels.app/name reads as labels.app divided by name. This accepts any character
//     but a dot or a bracket, because splitting on dots alone did until now and such paths are
//     already configured. Brackets are the portable way to write either.
//   - OTTL allows an integer index and a bare identifier in brackets, for slices and for
//     dynamic lookups. Endpoint properties are string maps, so neither has anything to address;
//     ["0"] still names a key called "0".
//   - OTTL allows an empty key, [""]. Nothing can usefully be named by one here, so it is an
//     error rather than a path that matches nothing.
//
// An unparseable path is an error rather than a path that matches nothing, so that
// configuration reports the mistake instead of quietly routing no telemetry.
func parsePropertyPath(path string) ([]string, error) {
	if path == "" {
		return nil, errors.New("path is empty")
	}
	if path[0] == '[' {
		return nil, errors.New("a path starts with a name, as labels[\"a.b\"] rather than [\"a.b\"]")
	}

	var keys []string
	afterDot := false
	for i := 0; i < len(path); {
		var key string
		if path[i] == '[' {
			// A dot separates named segments, so labels.["a"] is missing the name between
			// them. OTTL rejects it for the same reason; accepting it would make it a second
			// spelling of labels["a"].
			if afterDot {
				return nil, fmt.Errorf("a dot must be followed by a name, not a bracket, at position %d", i)
			}
			var err error
			if key, i, err = parseBracketedKey(path, i); err != nil {
				return nil, err
			}
		} else {
			start := i
			for i < len(path) && path[i] != '.' && path[i] != '[' {
				i++
			}
			if i == start {
				return nil, fmt.Errorf("empty key at position %d", start)
			}
			key = path[start:i]
		}
		keys = append(keys, key)
		afterDot = false

		switch {
		case i == len(path):
		case path[i] == '.':
			i++
			if i == len(path) {
				return nil, errors.New("path ends with a dot")
			}
			afterDot = true
		case path[i] == '[':
			// An adjacent bracket, as in labels["a"]["b"], needs no separator.
		default:
			return nil, fmt.Errorf("unexpected %q at position %d", path[i], i)
		}
	}
	return keys, nil
}

// parseBracketedKey reads the ["..."] key starting at path[i], which must be an opening bracket,
// and returns the key with the index just past the closing bracket. The key is double quoted, as
// OTTL's string token is, and \" and \\ are the escapes a key can contain: a Kubernetes label
// key holds neither, but rejecting them outright would differ from OTTL silently.
func parseBracketedKey(path string, i int) (string, int, error) {
	j := i + 1
	if j == len(path) || path[j] != '"' {
		return "", 0, fmt.Errorf(`expected a double quoted key in %q, as labels["a.b"]`, path[i:])
	}
	j++

	var key strings.Builder
	for j < len(path) {
		switch path[j] {
		case '\\':
			if j+1 == len(path) {
				return "", 0, fmt.Errorf("unterminated escape in %q", path[i:])
			}
			switch path[j+1] {
			case '"', '\\':
				key.WriteByte(path[j+1])
			default:
				return "", 0, fmt.Errorf(`unsupported escape \%c in %q, only \" and \\ are`, path[j+1], path[i:])
			}
			j += 2
		case '"':
			j++
			if j == len(path) || path[j] != ']' {
				return "", 0, fmt.Errorf("expected ] after the quoted key in %q", path[i:])
			}
			if key.Len() == 0 {
				return "", 0, fmt.Errorf("empty key in %q", path[i:])
			}
			return key.String(), j + 1, nil
		default:
			key.WriteByte(path[j])
			j++
		}
	}
	return "", 0, fmt.Errorf("unterminated quote in %q", path[i:])
}

// resolveAttributeName turns the configured spelling of a resource attribute into the attribute
// name to look up.
//
// A resource attribute is a flat name, not a path: k8sattributes emits one attribute called
// "k8s.pod.labels.app.kubernetes.io/name", and nothing is nested. Written out it is a run of
// dots with no way to see where the prefix ends and the label key begins, so the same bracket
// form the endpoint side uses is accepted as an alternative spelling of the same name:
//
//	k8s.pod.labels["app.kubernetes.io/name"]   is  k8s.pod.labels.app.kubernetes.io/name
//
// The brackets are punctuation, not structure: the segments are joined back with dots. A
// spelling with no bracket in it is the attribute name verbatim, which is what every
// configuration written before this used.
func resolveAttributeName(configured string) (string, error) {
	if !strings.ContainsRune(configured, '[') {
		return configured, nil
	}
	segments, err := parsePropertyPath(configured)
	if err != nil {
		return "", err
	}
	return strings.Join(segments, "."), nil
}

// getNestedProperty retrieves a nested property by path, e.g. "labels.app", "spec.region" or
// pod.labels["app.kubernetes.io/name"]. See parsePropertyPath.
func getNestedProperty(properties map[string]any, path string) any {
	parts, err := parsePropertyPath(path)
	if err != nil {
		// Unreachable through the collector: Config.Unmarshal rejects a path that does not
		// parse. Treated as no match rather than panicking if it is reached another way.
		return nil
	}
	current := any(properties)

	for _, part := range parts {
		switch v := current.(type) {
		case map[string]any:
			current = v[part]
		case map[string]string:
			// Two-value form: the zero value of a string map is "", not nil, so a key that is
			// absent would otherwise look like a key present and empty. The caller treats nil
			// as "no such property" and anything else as a value to compare, so an absent
			// label would compare equal to an empty resource attribute.
			value, ok := v[part]
			if !ok {
				return nil
			}
			current = value
		case observer.EndpointEnv:
			current = v[part]
		default:
			return nil
		}
		if current == nil {
			return nil
		}
	}
	return current
}

// toString converts a value to string for comparison.
func toString(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", val)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", val)
	case float32, float64:
		return fmt.Sprintf("%g", val)
	case bool:
		return fmt.Sprintf("%t", val)
	default:
		// Try to convert using fmt.Sprintf as a fallback
		return fmt.Sprintf("%v", val)
	}
}
