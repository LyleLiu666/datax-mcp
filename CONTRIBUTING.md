# Contributing

Start from a concrete failure or user need. Add a failing test before changing the
execution contract. Keep transport in upstream DataX, state in `internal/state`,
publication in `internal/postgres`, and MCP/CLI as thin control interfaces.

Run `go test -race ./...` and `go vet ./...`. Changes to transport/publication also
require `scripts/test-integration.sh` with the pinned DataX runtime. Integration
tests must use synthetic, disposable databases; never point test environment
variables at business data. Explain untested type or database-version boundaries.

New database pairs need a defined idempotent publication/receipt strategy and
crash tests. Merely adding an upstream plugin name is not enough to promise resume.
Never include credentials, real data, organization-specific endpoints or native
DataX logs in commits, issues, or test fixtures.
