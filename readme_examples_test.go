package exportercreator

import (
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer"
)

// The README's examples are only useful if they work. These evaluate the expressions and rules
// it documents against the endpoint types it documents them for, so an example cannot claim
// something the expansion or the rule engine will not do - a mistake that is invisible until
// someone copies it into a collector.
//
// Every expression the README uses, against the endpoint type it is shown with.
func TestREADMEExamples_ExpressionsExpand(t *testing.T) {
	podEP := observer.Endpoint{ID: "p", Target: "10.0.0.5", Details: &observer.Pod{
		Name: "checkout-1", Namespace: "default", UID: "u",
		Labels: map[string]string{"app": "checkout", "otel-export": "true",
			"collector-endpoint":            "checkout-collector:4317",
			"topology.kubernetes.io/region": "us-east-1"}}}
	pod, _ := podEP.Env()
	portEP := observer.Endpoint{ID: "pt", Target: "10.0.0.5:4317", Details: &observer.Port{
		Name: "otlp", Port: 4317, Transport: observer.ProtocolTCP, Pod: observer.Pod{
			Name: "checkout-1", Labels: map[string]string{"app": "checkout",
				"topology.kubernetes.io/region": "us-east-1"}}}}
	port, _ := portEP.Env()
	svcWithEP := observer.Endpoint{ID: "s", Target: "10.0.0.1", Details: &observer.K8sService{
		Name: "prom", ClusterIP: "10.0.0.1",
		Annotations: map[string]string{"prometheus.io/port": "9090"}}}
	svcWith, _ := svcWithEP.Env()
	svcWithoutEP := observer.Endpoint{ID: "s2", Target: "10.0.0.1", Details: &observer.K8sService{
		Name: "prom", ClusterIP: "10.0.0.1"}}
	svcWithout, _ := svcWithoutEP.Env()

	for _, tc := range []struct {
		name, expr string
		env        observer.EndpointEnv
		want       any
	}{
		{"simple endpoint", "`labels[\"collector-endpoint\"]`", pod, "checkout-collector:4317"},
		{"port endpoint", "`endpoint`", port, "10.0.0.5:4317"},
		{"port pod region", "`pod.labels[\"topology.kubernetes.io/region\"]`", port, "us-east-1"},
		{"svc labels region", "`labels[\"topology.kubernetes.io/region\"]`", pod, "us-east-1"},
		{"prw with annotation", "http://`endpoint`:`\"prometheus.io/port\" in annotations ? annotations[\"prometheus.io/port\"] : 9090`/api/v1/write", svcWith, "http://10.0.0.1:9090/api/v1/write"},
		{"prw default port", "http://`endpoint`:`\"prometheus.io/port\" in annotations ? annotations[\"prometheus.io/port\"] : 9090`/api/v1/write", svcWithout, "http://10.0.0.1:9090/api/v1/write"},
		{"static value", "us-east-1", pod, "us-east-1"},
	} {
		got, err := evalBackticksInConfigValue(tc.expr, tc.env)
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: got %#v want %#v", tc.name, got, tc.want)
		}
	}
}

// Every rule the README uses must compile and match the endpoint it describes. A rule that
// silently fails to match is the easiest kind of documentation error to ship: comparing the
// named type of transport to a plain string is false rather than an error, for instance.
func TestREADMEExamples_RulesMatch(t *testing.T) {
	podE := observer.Endpoint{ID: "p", Target: "10.0.0.5", Details: &observer.Pod{
		Name: "c", Labels: map[string]string{"otel-export": "true"}}}
	portE := observer.Endpoint{ID: "pt", Target: "10.0.0.5:4317", Details: &observer.Port{
		Name: "otlp", Port: 4317, Transport: observer.ProtocolTCP,
		Pod: observer.Pod{Name: "c", Labels: map[string]string{"otel-export": "true"}}}}
	svcE := observer.Endpoint{ID: "s", Target: "10.0.0.1", Details: &observer.K8sService{
		Name: "prom", Annotations: map[string]string{"prometheus.io/remote-write": "true"}}}
	hostE := observer.Endpoint{ID: "h", Target: "localhost:4318", Details: &observer.HostPort{
		ProcessName: "otelcol", Port: 4318, Transport: observer.ProtocolTCP}}

	for _, tc := range []struct {
		name, rule string
		e          observer.Endpoint
	}{
		{"per-app pod", `type == "pod" && labels["otel-export"] == "true"`, podE},
		{"sidecar port", `type == "port" && port == 4317 && pod.labels["otel-export"] == "true"`, portE},
		{"prw service", `type == "k8s.service" && annotations["prometheus.io/remote-write"] == "true"`, svcE},
		{"host agent", `type == "hostport" && port == 4318 && string(transport) == "TCP"`, hostE},
	} {
		r, err := newRule(tc.rule)
		if err != nil {
			t.Errorf("%s: compile: %v", tc.name, err)
			continue
		}
		env, err := tc.e.Env()
		if err != nil {
			t.Fatal(err)
		}
		matched, err := r.eval(env)
		if err != nil {
			t.Errorf("%s: eval: %v", tc.name, err)
			continue
		}
		if !matched {
			t.Errorf("%s: rule did not match the endpoint it documents", tc.name)
		}
	}
}
