#!/usr/bin/env node

// Deterministic local OpenAI-compatible streaming fixture used only by the
// V9 public-API performance gate. It isolates Gateway/Runtime overhead from
// remote model and network jitter; real-provider behavior is tested separately
// by v9-terminal-gate and its archived live scenarios.
import http from "node:http";

const port = Number.parseInt(process.env.COWD_V9_PERFORMANCE_PROVIDER_PORT ?? "8877", 10);
const defaultModel = "cowd-v9-performance-fixture";

const server = http.createServer((request, response) => {
  if (request.method !== "POST" || request.url !== "/v1/chat/completions") {
    response.writeHead(404, { "content-type": "application/json" });
    response.end(JSON.stringify({ error: { message: "fixture route not found" } }));
    return;
  }

  let body = "";
  request.setEncoding("utf8");
  request.on("data", (chunk) => {
    body += chunk;
  });
  request.on("end", () => {
    let model = defaultModel;
    try {
      model = JSON.parse(body).model || defaultModel;
    } catch {
      // The fixture is strict about the route, not incidental request shape.
    }

    response.writeHead(200, {
      "cache-control": "no-cache",
      "content-type": "text/event-stream",
      connection: "keep-alive",
      "x-request-id": "cowd-v9-performance-fixture",
    });
    response.write(`data: ${JSON.stringify({
      id: "chatcmpl-cowd-v9-performance-fixture",
      model,
      choices: [{ index: 0, delta: { content: "56" }, finish_reason: null }],
    })}\n\n`);
    response.write(`data: ${JSON.stringify({
      id: "chatcmpl-cowd-v9-performance-fixture",
      model,
      choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
      usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
    })}\n\n`);
    response.end("data: [DONE]\n\n");
  });
});

server.listen(port, "127.0.0.1", () => {
  process.stdout.write(`cowd-v9-performance-provider listening on ${port}\n`);
});

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => server.close(() => process.exit(0)));
}
