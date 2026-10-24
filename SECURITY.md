# Security

This is an early operator tool, not a multi-tenant or untrusted-SQL service.
Only trusted users and MCP clients should register profiles or run workers.
Registration permits native reader SQL/JDBC options; validation is not a sandbox.
Use a SELECT-only source identity, a dedicated destination database/role, private
state storage, explicit TLS settings and a restricted worker environment.

Never commit real reader configurations, credentials, source rows, state databases,
resolved DataX jobs or raw diagnostic logs. A worker resolves environment variables
into a private job file, removed on normal exit. SIGKILL/power loss may leave that
file. DataX logs can contain credentials or row values and remain private on disk;
redact them before sharing. Do not assume password-field validation finds every
possible inline secret in arbitrary SQL/JDBC configuration.

The example non-TLS JDBC URLs and passwords are for disposable loopback fixtures.
Review the pinned upstream engine and transitive JDBC dependencies against your
own security requirements before using production data. Client certificate formats
must be supported by both PostgreSQL JDBC and pgx; configuration acceptance alone
is not evidence of a working TLS connection.

For a vulnerability, use this repository's GitHub **Report a vulnerability** flow
if enabled. Do not post sensitive reproduction data in a public issue. For ordinary
bugs, provide an isolated synthetic fixture and redacted configuration.
