# Contributing

Thanks for looking at openSensus. This is an MVP at `0.1.x`, so the most useful
contributions right now are bug reports with reproductions, protocol feedback,
and fixes for documented limits.

## Getting set up

Node.js 22 or later, and npm.

```bash
npm ci
npm run check     # tsc --noEmit
npm test          # builds, then runs the integration suite
```

`npm test` runs against SQLite out of the box. The two PostgreSQL suites skip
themselves when no database is reachable, so the command stays green on a machine
without one. To run them:

```bash
SENSUS_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:5432/sensus_test \
SENSUS_TEST_DATABASE_URL_E2E=postgres://postgres@127.0.0.1:5432/sensus_e2e_test \
npm test
```

The end-to-end suite creates its own database; the conformance suite expects to
own its schema, so point the two variables at different databases.

Useful scripts while developing:

```bash
npm run dev            # ingestion API, reload on change
npm run mcp:dev        # stdio MCP
npm run mcp:http:dev   # Streamable HTTP MCP
SENSUS_DB_PATH=./data/demo.db npm run seed
```

## Before you open a pull request

- `npm run check` and `npm test` both pass.
- New behavior has a test. For storage behavior, add it to
  `test/conformance.ts` — that suite is what holds the two backends to identical
  semantics, and a change that only passes on SQLite is a change that breaks
  PostgreSQL.
- If you change a documented guarantee, update the document that states it.
  `docs/sensus-protocol-v0.1.md` is normative and English-only on purpose;
  `docs/architecture.md` and `docs/integration-guide.md` are mirrored in
  `*.zh-CN.md`, so keep the pair in sync.

## The lockfile rule

`package-lock.json` must only reference `registry.npmjs.org`. If you develop
behind a corporate mirror, npm rewrites the `resolved` URLs on every install, so
run `npm run check:lockfile` before committing — and if it fails, regenerate:

```bash
npm install --package-lock-only --registry=https://registry.npmjs.org/
```

CI enforces this. A lockfile full of internal hostnames cannot be installed
outside your network, which makes it a hard blocker for everyone else.

## Where things live

| Path | What it is |
| --- | --- |
| `src/protocol.ts` | The wire contract as Zod schemas |
| `src/store.ts` | SQLite storage: ingestion, projection, corrections, reconciliation, rules, ACL-filtered reads |
| `src/storage.ts` | The backend-neutral contract, including the two invariants any implementation must hold |
| `src/storage-postgres.ts` | The PostgreSQL implementation of that contract |
| `src/world.ts` | The bounded read model behind the six MCP tools |
| `src/http.ts` | The ingestion API |
| `src/access.ts`, `src/identity*.ts` | Authorization decisions and identity resolution |
| `test/conformance.ts` | The shared storage suite both backends must pass |

`src/storage.ts` states two invariants — projection is transactional, and rebuild
order is total and deterministic. Read them before changing projection or
Correction handling; most of the subtle behavior in `store.ts` exists to keep
them true.

## Reporting bugs

Include the storage backend, the deployment shape (which transports, which
identity mode), and a minimal reproduction. For a projection or ordering problem,
the Observation payloads in insertion order are usually enough to reproduce it.

Security problems: see [SECURITY.md](SECURITY.md) and report privately.

## License

Contributions are accepted under the MIT License, as described in
[LICENSE](LICENSE).
