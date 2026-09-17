# Security Policy

## Reporting a vulnerability

Report suspected vulnerabilities privately through GitHub's
[private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability)
on this repository. Please do not open a public issue for a security problem.

Include what you need to make the report actionable: affected version or commit,
the deployment shape you tested (storage backend, transports, identity mode), a
reproduction, and the impact you believe it has. We will acknowledge receipt and
tell you whether we consider it in scope.

## Supported versions

openSensus is at `0.1.x`. Security fixes go to the latest `0.1.x` release only; there
are no maintained older branches yet.

## Deployment assumptions

openSensus is an MVP and its security model rests on assumptions a deployment has to
keep. If any of the following does not hold for your deployment, treat the
corresponding risk as yours to mitigate.

**Producers are trusted to describe their own data.** The `access` policy on an
Observation — `classification`, `allow`, `deny`, `inherit_from_source` — is
asserted by the Producer in the request body. The Runtime stores and enforces it
but does not verify that the Producer was entitled to declare it. A Producer that
can ingest can therefore publish a restricted fact as `public`, or re-assert a
field another source owns with a lower classification.

**Ingestion is not isolated per principal.** `POST /v1/observations`,
`/v1/syncs`, and `/v1/signal-rules` authenticate the caller but do not
distinguish roles. Any credential that can ingest can also open a
reconciliation sync with `authoritative_deletion: true` — which synthesizes
deletion Observations for entities a snapshot omitted — and can replace or
delete a tenant's Signal rules, which triggers a full re-evaluation of that
tenant's Signals. Do not hand the ingestion credential to an agent or a
semi-trusted integration.

**Tenant isolation has to be configured, and fails closed until it is.** With
`SENSUS_TENANT_ID` set, one process serves exactly that tenant and a request
naming another is rejected. Without it, the tenant must come from the identity
resolver's `tenant.claim`; a request that names its own tenant — the body on
write routes, `x-sensus-tenant` on read routes — is refused with `403`, because
nothing vouches for it. `SENSUS_ALLOW_REQUEST_TENANT=true` restores the older
behaviour of trusting that name, and with it a single shared API key can act
across every tenant it can spell. Multi-tenant deployments should use an identity
provider with `mapping.tenant.allowed` configured and treat that allowlist as a
security boundary, rather than enabling the escape hatch.

**Unauthenticated modes exist and are opt-in.** Omitting `SENSUS_API_KEY`
disables bearer authentication on the ingestion API. `SENSUS_MCP_ALLOW_ANONYMOUS=true`
lets the MCP HTTP endpoint start with no resolver. Both bind to `127.0.0.1` by
default; changing `HOST` with either of them set exposes an unauthenticated
endpoint. The ingestion API logs which mode it started in — read that line.

**Signals are derived, not authoritative.** A Signal is a Runtime conclusion from
Producer-supplied data. Evidence references hand deep inspection to a vertical
MCP server; they are not independently re-verified by openSensus.
