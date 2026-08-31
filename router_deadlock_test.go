// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exportercreator

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	obs "github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer"
	"github.com/stuart23/exportercreator/internal/metadata"
)

// Route holds a read lock for its whole body, so anything it calls must not take that lock
// again. sync.RWMutex is not reentrant: a writer queued between the two acquisitions blocks the
// second RLock, while the writer itself waits for the first, and routing and endpoint updates
// deadlock together. Debug logging is enabled here because the offending call sat behind it.
func TestRoute_DoesNotReacquireItsOwnLock(t *testing.T) {
	core, _ := observer.New(zapcore.DebugLevel)
	telemetry, err := metadata.NewTelemetryBuilder(componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)

	router := newTelemetryRouter([]RoutingRule{
		{ResourceAttribute: "app", EndpointProperty: "labels.app"},
	}, telemetry)
	router.setLogger(zap.New(core))

	env := obs.EndpointEnv{"labels": map[string]string{"app": "test"}}
	router.AddExporter("seed", &nopExporterComponent{}, env)

	attrs := pmetric.NewMetrics().ResourceMetrics().AppendEmpty().Resource().Attributes()
	attrs.PutStr("app", "test")

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 2000; j++ {
					router.Route(attrs)
				}
			}()
		}
		// Writers contending for the same mutex are what turns the reentrant read into a
		// deadlock rather than a harmless second acquisition.
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				id := obs.EndpointID(string(rune('a' + i)))
				for j := 0; j < 2000; j++ {
					router.AddExporter(id, &nopExporterComponent{}, env)
					router.RemoveExporter(id)
				}
			}(i)
		}
		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Route deadlocked: it re-acquires the read lock it already holds, " +
			"so a queued writer blocks the second acquisition")
	}
}
