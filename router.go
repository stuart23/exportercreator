// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exportercreator // import "github.com/stuart23/exportercreator"

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
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
	count     int
	telemetry *metadata.TelemetryBuilder
	logger    *zap.Logger
}

// routedExporter holds an exporter with its endpoint properties for matching.
type routedExporter struct {
	exporter   component.Component
	properties map[string]any // Flattened endpoint properties
}

// newTelemetryRouter creates a new telemetry router with the given routing rules.
func newTelemetryRouter(rules []RoutingRule, telemetry *metadata.TelemetryBuilder) *telemetryRouter {
	return &telemetryRouter{
		rules:     rules,
		exporters: make(map[observer.EndpointID][]*routedExporter),
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

// AddExporter registers an exporter with its endpoint properties.
func (r *telemetryRouter) AddExporter(id observer.EndpointID, exp component.Component, env observer.EndpointEnv) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.exporters[id] = append(r.exporters[id], &routedExporter{
		exporter:   exp,
		properties: flattenProperties(env),
	})
	r.count++

	// Update the gauge metric with the current count
	if r.telemetry != nil {
		r.telemetry.ExporterCreatorExportersCount.Record(context.Background(), int64(r.count))
	}
}

// RemoveExporter unregisters every exporter created for an endpoint.
func (r *telemetryRouter) RemoveExporter(id observer.EndpointID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.count -= len(r.exporters[id])
	delete(r.exporters, id)

	// Update the gauge metric with the current count
	if r.telemetry != nil {
		r.telemetry.ExporterCreatorExportersCount.Record(context.Background(), int64(r.count))
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
		attrVal, ok := resourceAttrs.Get(rule.ResourceAttribute)
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

// getNestedProperty retrieves a nested property using dot notation (e.g., "labels.app", "spec.region").
func getNestedProperty(properties map[string]any, path string) any {
	parts := strings.Split(path, ".")
	current := any(properties)

	for _, part := range parts {
		switch v := current.(type) {
		case map[string]any:
			current = v[part]
		case map[string]string:
			current = v[part]
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
