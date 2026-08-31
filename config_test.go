// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exportercreator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
)

// confWithSignals builds a config for one subexporter, optionally carrying a "signals" block.
func confWithSignals(signals any) *confmap.Conf {
	subexporter := map[string]any{
		"rule":   `type == "port"`,
		"config": map[string]any{"endpoint": "localhost:4317"},
	}
	if signals != nil {
		subexporter["signals"] = signals
	}
	return confmap.NewFromStringMap(map[string]any{
		"exporters": map[string]any{"otlp/test": subexporter},
	})
}

func templateFor(t *testing.T, conf *confmap.Conf) exporterTemplate {
	t.Helper()
	cfg := createDefaultConfig().(*Config)
	require.NoError(t, cfg.Unmarshal(conf))
	tmpl, ok := cfg.exporterTemplates["otlp/test"]
	require.True(t, ok, "subexporter template should be parsed")
	return tmpl
}

// Omitting the block enables every signal.
func TestConfig_SignalsAbsentEnablesAll(t *testing.T) {
	tmpl := templateFor(t, confWithSignals(nil))
	assert.True(t, tmpl.signals.metrics)
	assert.True(t, tmpl.signals.logs)
	assert.True(t, tmpl.signals.traces)
}

// A signals block must actually take effect: only what it names is enabled.
func TestConfig_SignalsPartialEnablesOnlyNamed(t *testing.T) {
	tmpl := templateFor(t, confWithSignals(map[string]any{"metrics": true}))
	assert.True(t, tmpl.signals.metrics, "metrics was requested")
	assert.False(t, tmpl.signals.logs, "logs was not requested")
	assert.False(t, tmpl.signals.traces, "traces was not requested")
}

// A block enabling nothing used to be silently replaced by the all-enabled default, because
// it is indistinguishable from an absent block once parsed. Accepting it is no better: the
// runner rejects an exporter with no signals, once per matching endpoint. Reject it up front.
func TestConfig_SignalsAllDisabledIsRejected(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	err := cfg.Unmarshal(confWithSignals(map[string]any{
		"metrics": false, "logs": false, "traces": false,
	}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one")
}

func TestConfig_SignalsRejectsUnknownKey(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	err := cfg.Unmarshal(confWithSignals(map[string]any{"profiles": true}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "profiles")
}

func TestConfig_SignalsRejectsNonBoolean(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	err := cfg.Unmarshal(confWithSignals(map[string]any{"metrics": "yes please"}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "metrics")
}

// A signals block written as a list (or anything else that is not a map) reaches the parser
// as-is, since the block has no mapstructure field to reject it earlier.
func TestConfig_SignalsRejectsNonMap(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	err := cfg.Unmarshal(confWithSignals([]any{"metrics", "logs"}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be a map")
}
