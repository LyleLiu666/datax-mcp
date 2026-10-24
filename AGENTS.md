# Development contract

- Keep DataX as the transfer engine. Do not add a second row transport in Go.
- Changes to restart/publication behavior require failure-first tests against the actual boundary.
- MCP calls enqueue/control jobs; never block a tool call on a data transfer.
- Preserve original values. Unsupported source types need explicit projection and integration evidence.
- Keep domain-specific datasets, endpoints, credentials, logs and classification logic outside this public repository.
- State must survive MCP disconnect and worker restart. Never advance a checkpoint before verified destination commit.
- Do not call an append watermark CDC or claim independent source verification from stage/target comparison.
- Use the documented synthetic integration entry point; never run tests against business databases.
