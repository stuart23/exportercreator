// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exportercreator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component"
)

func TestExporterMap(t *testing.T) {
	em := exporterMap{}
	assert.Equal(t, 0, em.Size())

	e1 := &nopExporter{}
	e2 := &nopExporter{}
	e3 := &nopExporter{}

	em.Put("a", e1)
	assert.Equal(t, 1, em.Size())

	em.Put("a", e2)
	assert.Equal(t, 2, em.Size())

	em.Put("b", e3)
	assert.Equal(t, 3, em.Size())

	assert.Equal(t, []component.Component{e1, e2}, em.Get("a"))
	assert.Nil(t, em.Get("missing"))

	em.RemoveAll("missing")
	assert.Equal(t, 3, em.Size())

	em.RemoveAll("b")
	assert.Equal(t, 2, em.Size())

	em.RemoveAll("a")
	assert.Equal(t, 0, em.Size())

	em.Put("a", e1)
	em.Put("b", e2)
	assert.Equal(t, 2, em.Size())
	assert.ElementsMatch(t, []component.Component{e1, e2}, em.Values())
}
