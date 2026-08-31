// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exportercreator // import "github.com/stuart23/exportercreator"

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/service/hostcapabilities"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer"
	"github.com/stuart23/exportercreator/internal/metadata"
)

var (
	_ exporter.Logs    = (*exporterCreator)(nil)
	_ exporter.Metrics = (*exporterCreator)(nil)
	_ exporter.Traces  = (*exporterCreator)(nil)
	_ consumer.Logs    = (*exporterCreator)(nil)
	_ consumer.Metrics = (*exporterCreator)(nil)
	_ consumer.Traces  = (*exporterCreator)(nil)
)

// exporterCreator implements the exporter that dynamically creates sub-exporters at runtime.
type exporterCreator struct {
	params           exporter.Settings
	cfg              *Config
	observerHandler  *observerHandler
	observables      []observer.Observable
	router           *telemetryRouter
	telemetry        *metadata.TelemetryBuilder
	defaultExporters map[component.ID]component.Component
	// shared is the wrapper handed to every pipeline referencing this creator; see factory.go.
	shared *sharedExporter
}

// host is an interface that the component.Host passed to exportercreator's Start function must implement
type host interface {
	component.Host
	hostcapabilities.ComponentFactory
}

func newExporterCreator(params exporter.Settings, cfg *Config) (*exporterCreator, error) {
	telemetry, err := metadata.NewTelemetryBuilder(params.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	router := newTelemetryRouter(cfg.Routing.Rules, telemetry)
	router.setLogger(params.Logger)
	ec := &exporterCreator{
		params:    params,
		cfg:       cfg,
		router:    router,
		telemetry: telemetry,
	}
	return ec, nil
}

// Start exporter_creator.
func (ec *exporterCreator) Start(ctx context.Context, h component.Host) error {
	ecHost, ok := h.(host)
	if !ok {
		return errors.New("the exporter_creator is not compatible with the provided component.host")
	}

	// Initialize the gauge metric to 0
	if ec.telemetry != nil {
		ec.telemetry.ExporterCreatorExportersCount.Record(ctx, 0)
	}

	// Get default exporters from host
	// Note: Default exporters should be static exporters configured in the service pipelines
	// For now, we'll store the IDs and look them up when needed
	ec.defaultExporters = make(map[component.ID]component.Component)
	// TODO: Implement proper default exporter lookup from host
	// This requires access to the service's exporter registry which may not be available
	// through the standard component.Host interface

	// Create observer handler
	ec.observerHandler = &observerHandler{
		config:              ec.cfg,
		params:              ec.params,
		exportersByEndpoint: make(exporterMap),
		router:              ec.router,
		runner:              newExporterRunner(ec.params, ecHost),
	}

	observers := map[component.ID]observer.Observable{}

	// Match all configured observables to the extensions that are running.
	for _, watchObserver := range ec.cfg.WatchObservers {
		for cid, ext := range ecHost.GetExtensions() {
			if cid != watchObserver {
				continue
			}

			obs, ok := ext.(observer.Observable)
			if !ok {
				return fmt.Errorf("extension %q in watch_observers is not an observer", watchObserver.String())
			}
			observers[watchObserver] = obs
		}
	}

	// Make sure all observables are present before starting any.
	for _, watchObserver := range ec.cfg.WatchObservers {
		if observers[watchObserver] == nil {
			return fmt.Errorf("failed to find observer %q in the extensions list", watchObserver.String())
		}
	}

	if len(observers) == 0 {
		ec.params.Logger.Warn("no observers were configured and no subexporters will be started. exporter_creator will be disabled")
	}

	// Start all configured watchers.
	for _, observable := range observers {
		ec.observables = append(ec.observables, observable)
		observable.ListAndWatch(ec.observerHandler)
	}

	return nil
}

// Shutdown stops the exporter_creator and all its exporters started at runtime.
func (ec *exporterCreator) Shutdown(ctx context.Context) error {
	var errs []error

	// Unsubscribe from all observers
	for _, observable := range ec.observables {
		observable.Unsubscribe(ec.observerHandler)
	}

	// Shutdown observer handler (which shuts down all sub-exporters)
	if ec.observerHandler != nil {
		if err := ec.observerHandler.shutdown(); err != nil {
			errs = append(errs, err)
		}
	}

	if ec.telemetry != nil {
		ec.telemetry.Shutdown()
	}

	if len(errs) > 0 {
		return multierr.Combine(errs...)
	}
	return nil
}

// Capabilities implements consumer.Logs, consumer.Metrics, consumer.Traces.
func (ec *exporterCreator) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// ConsumeLogs routes logs to the appropriate sub-exporter based on resource attributes.
func (ec *exporterCreator) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	if ec.router == nil {
		return nil
	}

	var errs []error
	exportersByComponent := make(map[component.Component]plog.Logs)
	unmatchedLogs := plog.NewLogs()

	// Group logs by matching exporters
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		resourceAttrs := rl.Resource().Attributes()

		// Find matching exporters
		matchedExporters := ec.router.Route(resourceAttrs)

		if len(matchedExporters) > 0 {
			// Route to matched exporters
			for _, exp := range matchedExporters {
				var logsExp exporter.Logs
				// Check if it's a wrappedExporter and extract the logs exporter
				if we, ok := exp.(*wrappedExporter); ok && we.logs != nil {
					logsExp = we.logs
				} else if le, ok := exp.(exporter.Logs); ok {
					logsExp = le
				}

				if logsExp != nil {
					if _, exists := exportersByComponent[exp]; !exists {
						exportersByComponent[exp] = plog.NewLogs()
					}
					rl.CopyTo(exportersByComponent[exp].ResourceLogs().AppendEmpty())
				}
			}
		} else {
			// No match, add to unmatched
			rl.CopyTo(unmatchedLogs.ResourceLogs().AppendEmpty())
		}
	}

	// Send to matched exporters
	for exp, logs := range exportersByComponent {
		var logsExp exporter.Logs
		// Check if it's a wrappedExporter and extract the logs exporter
		if we, ok := exp.(*wrappedExporter); ok && we.logs != nil {
			logsExp = we.logs
		} else if le, ok := exp.(exporter.Logs); ok {
			logsExp = le
		}

		if logsExp != nil {
			if err := logsExp.ConsumeLogs(ctx, logs); err != nil {
				errs = append(errs, fmt.Errorf("failed to export logs to exporter: %w", err))
			}
		}
	}

	// Route unmatched logs to default exporters
	if unmatchedLogs.ResourceLogs().Len() > 0 && len(ec.defaultExporters) > 0 {
		for _, defaultExp := range ec.defaultExporters {
			if logsExp, ok := defaultExp.(exporter.Logs); ok {
				if err := logsExp.ConsumeLogs(ctx, unmatchedLogs); err != nil {
					errs = append(errs, fmt.Errorf("failed to export logs to default exporter: %w", err))
				}
			}
		}
	}

	if len(errs) > 0 {
		return multierr.Combine(errs...)
	}
	return nil
}

// ConsumeMetrics routes metrics to the appropriate sub-exporter based on resource attributes.
func (ec *exporterCreator) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	if ec.router == nil {
		return nil
	}

	var errs []error
	exportersByComponent := make(map[component.Component]pmetric.Metrics)
	unmatchedMetrics := pmetric.NewMetrics()

	// Group metrics by matching exporters
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		resourceAttrs := rm.Resource().Attributes()

		// Find matching exporters
		// The router will log detailed information at INFO level about why routing fails
		matchedExporters := ec.router.Route(resourceAttrs)

		if len(matchedExporters) > 0 {
			// Route to matched exporters
			for _, exp := range matchedExporters {
				var metricsExp exporter.Metrics
				// Check if it's a wrappedExporter and extract the metrics exporter
				if we, ok := exp.(*wrappedExporter); ok && we.metrics != nil {
					metricsExp = we.metrics
				} else if me, ok := exp.(exporter.Metrics); ok {
					metricsExp = me
				}

				if metricsExp != nil {
					if _, exists := exportersByComponent[exp]; !exists {
						exportersByComponent[exp] = pmetric.NewMetrics()
					}
					// Log which ResourceMetrics are being added to which exporter
					attrs := make(map[string]string)
					resourceAttrs.Range(func(k string, v pcommon.Value) bool {
						attrs[k] = v.AsString()
						return true
					})
					ec.params.Logger.Debug("adding ResourceMetrics to exporter",
						zap.String("exporter_type", fmt.Sprintf("%T", exp)),
						zap.Any("resource_attributes", attrs),
					)
					rm.CopyTo(exportersByComponent[exp].ResourceMetrics().AppendEmpty())
				}
			}
		} else {
			// No match, add to unmatched
			// The router will log detailed information about why routing failed at INFO level
			// Log the resource attributes for debugging
			attrs := make(map[string]string)
			resourceAttrs.Range(func(k string, v pcommon.Value) bool {
				attrs[k] = v.AsString()
				return true
			})
			ec.params.Logger.Info("metrics did not match any routing rules, adding to unmatched",
				zap.Any("resource_attributes", attrs),
			)
			rm.CopyTo(unmatchedMetrics.ResourceMetrics().AppendEmpty())
		}
	}

	// Send to matched exporters
	if len(exportersByComponent) > 0 {
		ec.params.Logger.Debug("sending metrics to matched exporters",
			zap.Int("exporter_count", len(exportersByComponent)),
			zap.Int("total_metric_points", int(countMetricPoints(md))),
		)
	}

	for exp, metrics := range exportersByComponent {
		var metricsExp exporter.Metrics
		// Check if it's a wrappedExporter and extract the metrics exporter
		if we, ok := exp.(*wrappedExporter); ok {
			if we.metrics != nil {
				metricsExp = we.metrics
				ec.params.Logger.Info("extracted metrics exporter from wrappedExporter",
					zap.String("exporter_type", fmt.Sprintf("%T", we.metrics)),
					zap.Int("metrics_to_send", metrics.ResourceMetrics().Len()),
				)
			} else {
				ec.params.Logger.Warn("wrappedExporter has no metrics exporter",
					zap.String("exporter_type", fmt.Sprintf("%T", exp)),
				)
			}
		} else if me, ok := exp.(exporter.Metrics); ok {
			metricsExp = me
			ec.params.Logger.Debug("using direct metrics exporter",
				zap.String("exporter_type", fmt.Sprintf("%T", exp)),
			)
		} else {
			ec.params.Logger.Warn("exporter does not support metrics or is not a wrappedExporter",
				zap.String("exporter_type", fmt.Sprintf("%T", exp)),
			)
		}

		if metricsExp != nil {
			// Log the resource attributes of metrics being sent to help diagnose routing issues
			var resourceAttrsList []map[string]string
			for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
				rm := metrics.ResourceMetrics().At(i)
				attrs := make(map[string]string)
				rm.Resource().Attributes().Range(func(k string, v pcommon.Value) bool {
					attrs[k] = v.AsString()
					return true
				})
				resourceAttrsList = append(resourceAttrsList, attrs)
			}
			ec.params.Logger.Debug("sending metrics to exporter",
				zap.Int("resource_metrics_count", metrics.ResourceMetrics().Len()),
				zap.Any("resource_attributes", resourceAttrsList),
				zap.String("exporter_type", fmt.Sprintf("%T", metricsExp)),
			)
			if err := metricsExp.ConsumeMetrics(ctx, metrics); err != nil {
				ec.params.Logger.Error("failed to export metrics to exporter",
					zap.Error(err),
					zap.String("exporter_type", fmt.Sprintf("%T", metricsExp)),
					zap.Int("metrics_count", metrics.ResourceMetrics().Len()),
				)
				errs = append(errs, fmt.Errorf("failed to export metrics to exporter: %w", err))
			} else {
				ec.params.Logger.Debug("successfully exported metrics to exporter",
					zap.Int("metric_count", metrics.ResourceMetrics().Len()),
					zap.String("exporter_type", fmt.Sprintf("%T", metricsExp)),
				)
			}
		} else {
			ec.params.Logger.Info("no metrics exporter available for component",
				zap.String("component_type", fmt.Sprintf("%T", exp)),
				zap.Int("metrics_lost", metrics.ResourceMetrics().Len()),
			)
		}
	}

	// Route unmatched metrics to default exporters
	nonRoutableCount := int64(0)
	if unmatchedMetrics.ResourceMetrics().Len() > 0 {
		// Log unmatched metrics for debugging
		var unmatchedAttrsList []map[string]string
		for i := 0; i < unmatchedMetrics.ResourceMetrics().Len(); i++ {
			rm := unmatchedMetrics.ResourceMetrics().At(i)
			attrs := make(map[string]string)
			rm.Resource().Attributes().Range(func(k string, v pcommon.Value) bool {
				attrs[k] = v.AsString()
				return true
			})
			unmatchedAttrsList = append(unmatchedAttrsList, attrs)
		}
		ec.params.Logger.Info("processing unmatched metrics",
			zap.Int("unmatched_resource_metrics", unmatchedMetrics.ResourceMetrics().Len()),
			zap.Any("unmatched_resource_attributes", unmatchedAttrsList),
			zap.Int("default_exporters_count", len(ec.defaultExporters)),
		)

		if len(ec.defaultExporters) > 0 {
			hasMetricsExporter := false
			exportSucceeded := false
			for _, defaultExp := range ec.defaultExporters {
				if metricsExp, ok := defaultExp.(exporter.Metrics); ok {
					hasMetricsExporter = true
					if err := metricsExp.ConsumeMetrics(ctx, unmatchedMetrics); err != nil {
						errs = append(errs, fmt.Errorf("failed to export metrics to default exporter: %w", err))
					} else {
						exportSucceeded = true
					}
				}
			}
			// If no default exporter supports metrics, or export failed, count all unmatched as non-routable
			if !hasMetricsExporter || !exportSucceeded {
				nonRoutableCount = countMetricPoints(unmatchedMetrics)
				ec.params.Logger.Info("counting unmatched metrics as non-routable (no default exporter or export failed)",
					zap.Int64("non_routable_count", nonRoutableCount),
					zap.Bool("has_metrics_exporter", hasMetricsExporter),
					zap.Bool("export_succeeded", exportSucceeded),
				)
			}
		} else {
			// No default exporters configured, count all unmatched as non-routable
			nonRoutableCount = countMetricPoints(unmatchedMetrics)
			ec.params.Logger.Info("counting unmatched metrics as non-routable (no default exporters configured)",
				zap.Int64("non_routable_count", nonRoutableCount),
			)
		}
	}

	// Record non-routable metric points
	if nonRoutableCount > 0 && ec.telemetry != nil {
		ec.telemetry.ExporterCreatorNonroutableMetricPointsTotal.Add(ctx, nonRoutableCount)
		ec.params.Logger.Info("recorded non-routable metric points",
			zap.Int64("non_routable_count", nonRoutableCount),
			zap.Int("unmatched_resource_metrics", unmatchedMetrics.ResourceMetrics().Len()),
		)
	} else if unmatchedMetrics.ResourceMetrics().Len() > 0 {
		// Log why non-routable metric wasn't recorded
		ec.params.Logger.Warn("unmatched metrics found but non-routable metric not recorded",
			zap.Int("unmatched_resource_metrics", unmatchedMetrics.ResourceMetrics().Len()),
			zap.Int64("non_routable_count", nonRoutableCount),
			zap.Int("default_exporters_count", len(ec.defaultExporters)),
			zap.Bool("telemetry_nil", ec.telemetry == nil),
		)
	}

	if len(errs) > 0 {
		return multierr.Combine(errs...)
	}
	return nil
}

// countMetricPoints counts the total number of data points in a pmetric.Metrics.
func countMetricPoints(md pmetric.Metrics) int64 {
	var count int64
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
			for k := 0; k < sm.Metrics().Len(); k++ {
				metric := sm.Metrics().At(k)
				//exhaustive:enforce
				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					count += int64(metric.Gauge().DataPoints().Len())
				case pmetric.MetricTypeSum:
					count += int64(metric.Sum().DataPoints().Len())
				case pmetric.MetricTypeHistogram:
					count += int64(metric.Histogram().DataPoints().Len())
				case pmetric.MetricTypeExponentialHistogram:
					count += int64(metric.ExponentialHistogram().DataPoints().Len())
				case pmetric.MetricTypeSummary:
					count += int64(metric.Summary().DataPoints().Len())
				case pmetric.MetricTypeEmpty:
					// Empty metrics have no data points
				}
			}
		}
	}
	return count
}

// ConsumeTraces routes traces to the appropriate sub-exporter based on resource attributes.
func (ec *exporterCreator) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	if ec.router == nil {
		return nil
	}

	var errs []error
	exportersByComponent := make(map[component.Component]ptrace.Traces)
	unmatchedTraces := ptrace.NewTraces()

	// Group traces by matching exporters
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		resourceAttrs := rs.Resource().Attributes()

		// Find matching exporters
		matchedExporters := ec.router.Route(resourceAttrs)

		if len(matchedExporters) > 0 {
			// Route to matched exporters
			for _, exp := range matchedExporters {
				var tracesExp exporter.Traces
				// Check if it's a wrappedExporter and extract the traces exporter
				if we, ok := exp.(*wrappedExporter); ok && we.traces != nil {
					tracesExp = we.traces
				} else if te, ok := exp.(exporter.Traces); ok {
					tracesExp = te
				}

				if tracesExp != nil {
					if _, exists := exportersByComponent[exp]; !exists {
						exportersByComponent[exp] = ptrace.NewTraces()
					}
					rs.CopyTo(exportersByComponent[exp].ResourceSpans().AppendEmpty())
				}
			}
		} else {
			// No match, add to unmatched
			rs.CopyTo(unmatchedTraces.ResourceSpans().AppendEmpty())
		}
	}

	// Send to matched exporters
	for exp, traces := range exportersByComponent {
		var tracesExp exporter.Traces
		// Check if it's a wrappedExporter and extract the traces exporter
		if we, ok := exp.(*wrappedExporter); ok && we.traces != nil {
			tracesExp = we.traces
		} else if te, ok := exp.(exporter.Traces); ok {
			tracesExp = te
		}

		if tracesExp != nil {
			if err := tracesExp.ConsumeTraces(ctx, traces); err != nil {
				errs = append(errs, fmt.Errorf("failed to export traces to exporter: %w", err))
			}
		}
	}

	// Route unmatched traces to default exporters
	if unmatchedTraces.ResourceSpans().Len() > 0 && len(ec.defaultExporters) > 0 {
		for _, defaultExp := range ec.defaultExporters {
			if tracesExp, ok := defaultExp.(exporter.Traces); ok {
				if err := tracesExp.ConsumeTraces(ctx, unmatchedTraces); err != nil {
					errs = append(errs, fmt.Errorf("failed to export traces to default exporter: %w", err))
				}
			}
		}
	}

	if len(errs) > 0 {
		return multierr.Combine(errs...)
	}
	return nil
}
