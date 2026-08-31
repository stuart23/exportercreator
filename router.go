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
	// Always log when routing is attempted (at debug level to avoid spam)
	if r.logger != nil {
		resourceAttrsMap := make(map[string]string)
		resourceAttrs.Range(func(k string, v pcommon.Value) bool {
			resourceAttrsMap[k] = v.AsString()
			return true
		})
		r.logger.Debug("routing metrics",
			zap.Any("resource_attributes", resourceAttrsMap),
			zap.Int("routing_rules", len(r.rules)),
			// Count, not r.count: this runs before Route takes r.mu, and an
			// unsynchronized read races the observer callbacks that maintain it.
			zap.Int("available_exporters", r.Count()),
		)
	}

	if len(r.rules) == 0 {
		r.mu.RLock()
		exportersCount := r.count
		r.mu.RUnlock()
		if r.logger != nil {
			// Convert resource attrs to map for logging
			resourceAttrsMap := make(map[string]string)
			resourceAttrs.Range(func(k string, v pcommon.Value) bool {
				resourceAttrsMap[k] = v.AsString()
				return true
			})
			r.logger.Info("metrics could not be routed: no routing rules configured",
				zap.Any("resource_attributes", resourceAttrsMap),
				zap.Int("available_exporters", exportersCount),
			)
		}
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.count == 0 {
		if r.logger != nil {
			// Convert resource attrs to map for logging
			resourceAttrsMap := make(map[string]string)
			resourceAttrs.Range(func(k string, v pcommon.Value) bool {
				resourceAttrsMap[k] = v.AsString()
				return true
			})
			r.logger.Info("metrics could not be routed: no exporters available",
				zap.Any("resource_attributes", resourceAttrsMap),
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
				if r.logger != nil {
					// Log the properties that matched for debugging
					exportProps := make(map[string]any)
					if spec, ok := exp.properties["spec"].(map[string]any); ok {
						if resourceAttrs, ok := spec["resourceAttributes"].(map[string]any); ok {
							exportProps["spec.resourceAttributes"] = resourceAttrs
						}
					}
					// Also log the resource attributes from the metrics for comparison
					metricsAttrs := make(map[string]string)
					resourceAttrs.Range(func(k string, v pcommon.Value) bool {
						metricsAttrs[k] = v.AsString()
						return true
					})
					r.logger.Info("exporter matched routing rules",
						zap.String("endpoint_id", string(id)),
						zap.Any("matched_properties", exportProps),
						zap.Any("metrics_resource_attributes", metricsAttrs),
					)
				}
			} else {
				// Log why this exporter didn't match at DEBUG level to help diagnose routing issues
				if r.logger != nil {
					metricsAttrs := make(map[string]string)
					resourceAttrs.Range(func(k string, v pcommon.Value) bool {
						metricsAttrs[k] = v.AsString()
						return true
					})
					// Extract exporter properties for comparison
					exportProps := make(map[string]any)
					if spec, ok := exp.properties["spec"].(map[string]any); ok {
						if resourceAttrs, ok := spec["resourceAttributes"].(map[string]any); ok {
							exportProps["spec.resourceAttributes"] = resourceAttrs
						}
					}
					r.logger.Debug("exporter did not match routing rules",
						zap.String("endpoint_id", string(id)),
						zap.Any("metrics_resource_attributes", metricsAttrs),
						zap.Any("exporter_properties", exportProps),
					)
				}
			}
		}
	}

	// Log why routing failed if no exporters matched
	// Always log at DEBUG level when routing fails to help diagnose issues
	if len(matched) == 0 && r.count > 0 {
		// Convert resource attrs to map for logging
		resourceAttrsMap := make(map[string]string)
		resourceAttrs.Range(func(k string, v pcommon.Value) bool {
			resourceAttrsMap[k] = v.AsString()
			return true
		})

		// Log summary of why routing failed
		if r.logger != nil {
			r.logger.Debug("metrics did not match any routing rules",
				zap.Any("resource_attributes", resourceAttrsMap),
				zap.Int("available_exporters", r.count),
				zap.Int("routing_rules", len(r.rules)),
			)

			// Log details about each exporter's properties for debugging
			for id, exps := range r.exporters {
				for _, exp := range exps {
					exportProps := make(map[string]any)
					if spec, ok := exp.properties["spec"].(map[string]any); ok {
						if resourceAttrs, ok := spec["resourceAttributes"].(map[string]any); ok {
							exportProps["spec.resourceAttributes"] = resourceAttrs
						}
					}
					// Also include other relevant properties
					if labels, ok := exp.properties["labels"].(map[string]string); ok {
						exportProps["labels"] = labels
					}
					if len(exportProps) > 0 {
						r.logger.Info("exporter endpoint properties",
							zap.String("endpoint_id", string(id)),
							zap.Any("properties", exportProps),
						)
					}
				}
			}

			// Log the routing rules for reference
			for i, rule := range r.rules {
				r.logger.Info("routing rule",
					zap.Int("rule_index", i),
					zap.String("resource_attribute", rule.ResourceAttribute),
					zap.String("endpoint_property", rule.EndpointProperty),
				)
			}
		}
	}

	// Defensive check: ensure we never return all exporters when rules don't match
	// This should never happen, but we check to prevent bugs
	if len(matched) == r.count && len(r.rules) > 0 {
		// This would mean all exporters matched, which is suspicious
		// Log a warning but still return the matched exporters
		if r.logger != nil {
			resourceAttrsMap := make(map[string]string)
			resourceAttrs.Range(func(k string, v pcommon.Value) bool {
				resourceAttrsMap[k] = v.AsString()
				return true
			})
			r.logger.Warn("all exporters matched routing rules - this may indicate a configuration issue",
				zap.Any("resource_attributes", resourceAttrsMap),
				zap.Int("exporter_count", len(matched)),
				zap.Int("rule_count", len(r.rules)),
			)
		}
	}

	return matched
}

// Count returns the current number of exporters across all endpoints.
func (r *telemetryRouter) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.count
}

// matchesAllRules checks if all routing rules match for the given resource attributes and endpoint properties.
// Returns true if all rules match, false otherwise.
// Logs detailed information about why rules fail at INFO level.
func (r *telemetryRouter) matchesAllRules(resourceAttrs pcommon.Map, properties map[string]any) bool {
	// If there are no rules, nothing matches (this should never happen as Route() returns nil early,
	// but we handle it defensively)
	if len(r.rules) == 0 {
		return false
	}
	for _, rule := range r.rules {
		// Get the resource attribute value
		attrVal, ok := resourceAttrs.Get(rule.ResourceAttribute)
		if !ok {
			if r.logger != nil {
				r.logger.Info("routing rule failed: resource attribute not found in metrics",
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
			if r.logger != nil {
				r.logger.Info("routing rule failed: endpoint property not found",
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
			if r.logger != nil {
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
		if r.logger != nil {
			r.logger.Debug("routing rule matched",
				zap.String("resource_attribute", rule.ResourceAttribute),
				zap.String("resource_value", attrStr),
				zap.String("endpoint_property", rule.EndpointProperty),
				zap.String("endpoint_value", propStr),
			)
		}
	}
	if r.logger != nil {
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
