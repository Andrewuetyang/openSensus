import { McpServer } from "@modelcontextprotocol/server";
import { serveStdio } from "@modelcontextprotocol/server/stdio";
import {
  describeStorage,
  openStorageFromEnvironment,
  selectStorage,
} from "./storage-factory.js";
import { consumerFromEnvironment } from "./access.js";
import {
  SensusWorld,
  compareInputSchema,
  getEvidenceInputSchema,
  inspectInputSchema,
  observeInputSchema,
  queryInputSchema,
  timelineInputSchema,
} from "./world.js";

export function buildMcpServer(world: SensusWorld): McpServer {
  const server = new McpServer(
    { name: "openSensus", version: "0.1.0" },
    {
      instructions:
        "Observe the current enterprise world before investigating details. " +
        "Treat Signals as derived conclusions and verify important claims through evidence.",
    },
  );

  server.registerTool(
    "observe",
    {
      description:
        "Get a bounded first view of current state, metric changes, and open Signals for one scope.",
      inputSchema: observeInputSchema,
    },
    async (input) => toolResult(() => world.observe(input)),
  );
  server.registerTool(
    "inspect",
    {
      description:
        "Inspect one entity or Signal, including state, relations, metric changes, and evidence.",
      inputSchema: inspectInputSchema,
    },
    async (input) => toolResult(() => world.inspect(input)),
  );
  server.registerTool(
    "timeline",
    {
      description: "Read an entity's events and state changes in occurrence-time order.",
      inputSchema: timelineInputSchema,
    },
    async (input) => toolResult(() => world.timeline(input)),
  );
  server.registerTool(
    "query",
    {
      description: "Find entities or open Signals with structured predicates.",
      inputSchema: queryInputSchema,
    },
    async (input) => toolResult(() => world.query(input)),
  );
  server.registerTool(
    "compare",
    {
      description:
        "Compare a metric between two time windows, optionally grouped by dimensions.",
      inputSchema: compareInputSchema,
    },
    async (input) => toolResult(() => world.compare(input)),
  );
  server.registerTool(
    "get_evidence",
    {
      description:
        "Resolve an evidence reference or return the external MCP capability needed to inspect it.",
      inputSchema: getEvidenceInputSchema,
    },
    async (input) => toolResult(() => world.getEvidence(input)),
  );

  return server;
}

async function toolResult(handler: () => Promise<Record<string, unknown>>) {
  try {
    const result = await handler();
    return {
      content: [{ type: "text" as const, text: JSON.stringify(result, null, 2) }],
    };
  } catch (error) {
    return {
      isError: true,
      content: [
        {
          type: "text" as const,
          text: JSON.stringify({
            error: {
              code: error instanceof Error ? error.name : "INTERNAL",
              message: error instanceof Error ? error.message : "Unknown error",
              retryable: false,
            },
          }),
        },
      ],
    };
  }
}

async function main(): Promise<void> {
  const tenantId = process.env.SENSUS_TENANT_ID ?? "demo";
  const selection = selectStorage();
  const store = await openStorageFromEnvironment();
  const consumer = consumerFromEnvironment();
  const world = new SensusWorld(store, tenantId, consumer);
  const handle = serveStdio(() => buildMcpServer(world));
  console.error(`Sensus MCP ready for tenant ${tenantId}`);
  console.error(`Storage: ${describeStorage(selection)}`);
  console.error(
    `Principals: ${[...consumer.principals].join(",") || "none"}; clearance: ${consumer.clearance}`,
  );

  const shutdown = (): void => {
    void handle.close().finally(() => {
      void store.close().finally(() => process.exit(0));
    });
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

if (process.argv[1] && import.meta.url === new URL(`file://${process.argv[1]}`).href) {
  void main();
}
