import type { IncomingMessage, ServerResponse } from "node:http";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

export type FetchHandler = (request: Request) => Promise<Response>;

/**
 * Serves a web-standard `fetch` handler from Node's `http` module, streaming in
 * both directions. Streaming matters here: the MCP Streamable HTTP transport
 * answers with server-sent events, and buffering them would stall the client.
 */
export function createNodeFetchBridge(handler: FetchHandler) {
  return async function serve(
    request: IncomingMessage,
    response: ServerResponse,
  ): Promise<void> {
    let webRequest: Request;
    try {
      webRequest = toWebRequest(request);
    } catch {
      sendError(response, 400, "INVALID_REQUEST", "Malformed request");
      return;
    }

    let result: Response;
    try {
      result = await handler(webRequest);
    } catch (error) {
      console.error(error);
      if (!response.headersSent) {
        sendError(response, 500, "INTERNAL", "An unexpected error occurred");
      } else {
        response.end();
      }
      return;
    }

    await writeResponse(response, result);
  };
}

function toWebRequest(request: IncomingMessage): Request {
  const host = request.headers.host ?? "localhost";
  const url = new URL(request.url ?? "/", `http://${host}`);

  const headers = new Headers();
  for (const [name, value] of Object.entries(request.headers)) {
    if (value === undefined) continue;
    if (Array.isArray(value)) {
      for (const item of value) headers.append(name, item);
    } else {
      headers.set(name, value);
    }
  }

  const method = request.method ?? "GET";
  const hasBody = method !== "GET" && method !== "HEAD";

  return new Request(url, {
    method,
    headers,
    ...(hasBody
      ? {
          body: Readable.toWeb(request) as ReadableStream<Uint8Array>,
          duplex: "half",
        }
      : {}),
  } as RequestInit);
}

async function writeResponse(
  response: ServerResponse,
  result: Response,
): Promise<void> {
  if (response.headersSent) {
    await drain(result, response);
    return;
  }

  const headers: Record<string, string | string[]> = {};
  for (const [name, value] of result.headers) {
    // Set-Cookie is the one header that legitimately repeats, and iterating
    // Headers collapses it into a single comma-joined value.
    if (name.toLowerCase() === "set-cookie") continue;
    headers[name] = value;
  }
  const cookies = result.headers.getSetCookie();
  if (cookies.length) headers["set-cookie"] = cookies;

  response.writeHead(result.status, headers);
  await drain(result, response);
}

async function drain(
  result: Response,
  response: ServerResponse,
): Promise<void> {
  if (!result.body) {
    response.end();
    return;
  }
  try {
    await pipeline(
      Readable.fromWeb(result.body as Parameters<typeof Readable.fromWeb>[0]),
      response,
    );
  } catch (error) {
    // A client that disconnects mid-stream is routine, not an error worth
    // reporting; anything else during the body is worth knowing about.
    if (!isClientDisconnect(error)) console.error(error);
    response.destroy();
  }
}

function isClientDisconnect(error: unknown): boolean {
  const code = (error as { code?: unknown } | null)?.code;
  return (
    code === "ERR_STREAM_PREMATURE_CLOSE" ||
    code === "ECONNRESET" ||
    code === "EPIPE"
  );
}

function sendError(
  response: ServerResponse,
  status: number,
  code: string,
  message: string,
): void {
  const payload = JSON.stringify({ error: { code, message, retryable: false } });
  response.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(payload),
  });
  response.end(payload);
}
