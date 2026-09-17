#!/usr/bin/env node
/**
 * Fails when package-lock.json pins a registry other than the public npm
 * registry. Corporate mirrors rewrite `resolved` on every install, so this has
 * to be checked rather than assumed: a lockfile full of internal hostnames
 * breaks `npm ci` for everyone outside the network and leaks those hostnames
 * into the published tree.
 */
import { readFileSync } from "node:fs";

const allowedHosts = new Set(["registry.npmjs.org"]);
const lockfile = JSON.parse(
  readFileSync(new URL("../package-lock.json", import.meta.url), "utf8"),
);

const offenders = [];
for (const [name, entry] of Object.entries(lockfile.packages ?? {})) {
  if (!entry?.resolved) continue;
  let host;
  try {
    host = new URL(entry.resolved).host;
  } catch {
    offenders.push([name, entry.resolved, "unparseable URL"]);
    continue;
  }
  if (!allowedHosts.has(host)) offenders.push([name, entry.resolved, host]);
}

if (offenders.length) {
  console.error(
    `package-lock.json resolves ${offenders.length} package(s) from a non-public registry:`,
  );
  for (const [name, url] of offenders.slice(0, 10)) {
    console.error(`  ${name}\n    ${url}`);
  }
  if (offenders.length > 10) {
    console.error(`  ... and ${offenders.length - 10} more`);
  }
  console.error(
    "\nRegenerate the lockfile against the public registry:\n" +
      "  npm install --package-lock-only --registry=https://registry.npmjs.org/",
  );
  process.exit(1);
}

console.log(
  `package-lock.json is clean: ${Object.keys(lockfile.packages ?? {}).length} entries, all from the public npm registry.`,
);
