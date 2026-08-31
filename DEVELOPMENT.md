# Development

```bash
go test ./...
```

`go test ./...` uses the published observer module from the proxy; no local contrib checkout is required.

## Regenerating metadata

`documentation.md`, `README.md`'s status table and everything under `internal/metadata` are
generated from `metadata.yaml` by `mdatagen`, pinned to **v0.159.0** and verified in CI.

It cannot be installed with `go install`, because its `go.mod` carries replace directives. Build
it from a collector checkout:

```bash
git clone --depth 1 --branch v0.159.0 \
  https://github.com/open-telemetry/opentelemetry-collector.git /tmp/collector
(cd /tmp/collector/cmd/mdatagen && go build -o /tmp/mdatagen .)
/tmp/mdatagen metadata.yaml
```

Run it from a checkout directory named `exportercreator`. mdatagen takes the package name and the
README badge labels from the directory, so generating from a differently named directory (a git
worktree, say) rewrites `generated_component_test.go`, `generated_package_test.go` and the README
status table to match that name.
