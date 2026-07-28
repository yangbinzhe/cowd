#!/usr/bin/env node

/**
 * Deterministic OpenAI-compatible provider used by the TUI production
 * acceptance gates. It records exactly which conversation roles/texts reached
 * the provider, streams in multiple chunks, reports usage and can deliberately
 * emit a split invalid DSML frame to verify the fail-closed protocol boundary.
 */
import fs from "node:fs";
import http from "node:http";

const port = Number.parseInt(process.env.COWD_TUI_ACCEPTANCE_PROVIDER_PORT ?? "18784", 10);
const logPath = process.env.COWD_TUI_ACCEPTANCE_PROVIDER_LOG;
const fixtureModel = "cowd-tui-acceptance-model";

function textOf(content) {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content
    .map((part) => {
      if (typeof part === "string") return part;
      if (!part || typeof part !== "object") return "";
      return typeof part.text === "string" ? part.text : "";
    })
    .join("");
}

function exposedToolNames(request) {
  return (Array.isArray(request.tools) ? request.tools : [])
    .map((tool) => tool?.function?.name)
    .filter((name) => typeof name === "string" && name.length > 0);
}

function recordRequest(request, messages) {
  if (!logPath) return;
  const record = {
    received_at: new Date().toISOString(),
    model: request.model ?? fixtureModel,
    stream: request.stream === true,
    exposed_tools: exposedToolNames(request),
    messages: messages.map((message) => ({
      role: message?.role ?? "unknown",
      text: textOf(message?.content),
      tool_call_id: message?.tool_call_id ?? null,
    })),
  };
  fs.appendFileSync(logPath, `${JSON.stringify(record)}\n`, "utf8");
}

function responseFor(messages, tools) {
  const userMessages = messages
    .filter((message) => message?.role === "user")
    .map((message) => textOf(message.content))
    // Runtime context packets deliberately use provider-user role so they
    // cannot gain system authority. They are not durable human turns and must
    // not inflate the fixture's causal-history assertion.
    .filter((text) => !text.startsWith("## Runtime context data\n"));
  const latest = userMessages.at(-1) ?? "";
  const prior = userMessages.slice(0, -1).join("\n");
  const allUserText = userMessages.join("\n");

  if (allUserText.includes("[cowd-e2e:explicit-team-negative]")) {
    const teamRole = allUserText.match(/## Team role\s+Role:\s*([^\n]+)/)?.[1]?.trim();
    if (teamRole === "researcher") {
      const focus = allUserText.match(/\nFocus:\s*([^\n]+)/)?.[1]?.trim();
      const suffix = allUserText.match(
        /\[cowd-e2e:explicit-team-negative\]\s+([A-Za-z0-9.-]+)/,
      )?.[1];
      const focusRoots = {
        "crates-gateway": "crates/gateway",
        "crates-runtime": "crates/runtime",
        "surfaces-webui": "surfaces/webui",
      };
      const focusRoot = focusRoots[focus];
      const roleStart = messages.findLastIndex(
        (message) =>
          message?.role === "user" &&
          textOf(message.content).includes("## Team role"),
      );
      const toolResults = messages
        .slice(Math.max(0, roleStart))
        .filter((message) => message?.role === "tool");
      if (toolResults.length === 0 && focusRoot && suffix) {
        const pattern = `e2e-team-${suffix}.md`;
        return {
          chunks: [
            '<｜｜DSML｜｜tool_calls><｜｜DSML｜｜invoke name="glob_search">',
            `<｜｜DSML｜｜parameter name="pattern" string="true">${pattern}</｜｜DSML｜｜parameter>`,
            `<｜｜DSML｜｜parameter name="path" string="true">${focusRoot}</｜｜DSML｜｜parameter>`,
            "</｜｜DSML｜｜invoke></｜｜DSML｜｜tool_calls>",
          ],
          finishReason: "stop",
        };
      }
      if (toolResults.length === 1 && focusRoot && suffix) {
        const path = `${focusRoot}/e2e-team-${suffix}.md`;
        return {
          chunks: [
            '<｜｜DSML｜｜tool_calls><｜｜DSML｜｜invoke name="read_file">',
            `<｜｜DSML｜｜parameter name="path" string="true">${path}</｜｜DSML｜｜parameter>`,
            "</｜｜DSML｜｜invoke></｜｜DSML｜｜tool_calls>",
          ],
          finishReason: "stop",
        };
      }
      return {
        chunks: [
          JSON.stringify({
            findings: [`verified bounded focus ${focus ?? "unknown"}`],
            evidence: [`scoped discovery receipt for ${focusRoot ?? focus ?? "bounded focus"}`],
            unresolved: ["none in deterministic acceptance scope"],
          }),
        ],
        finishReason: "stop",
      };
    }
    if (teamRole === "synthesizer") {
      return {
        chunks: [
          JSON.stringify({
            summary: "three bounded Team focuses were executed and synthesized",
            evidence: ["checked Team child receipts"],
            unresolved: ["none in deterministic acceptance scope"],
          }),
        ],
        finishReason: "stop",
      };
    }
    if (
      latest.startsWith("Original objective:")
      && latest.includes("Checked evidence receipts:")
    ) {
      return {
        chunks: [
          JSON.stringify({
            summary: "three bounded Team focuses were executed and synthesized",
            evidence: ["checked Team child receipts"],
            unresolved: ["none in deterministic acceptance scope"],
          }),
        ],
        finishReason: "stop",
      };
    }
  }

  if (latest.includes("TUI_ACCEPTANCE_TURN_1")) {
    const nonce = latest.match(/TUI_ACCEPTANCE-NONCE-[A-Z0-9-]+/)?.[0] ?? "NONCE-MISSING";
    return {
      chunks: [`TUI_ACCEPTANCE-TURN1-ACK nonce=${nonce}`],
      finishReason: "stop",
    };
  }
  if (latest.includes("TUI_ACCEPTANCE_TURN_2")) {
    const nonce = prior.match(/TUI_ACCEPTANCE-NONCE-[A-Z0-9-]+/)?.[0] ?? "NONCE-MISSING";
    return {
      chunks: [
        "TUI_ACCEPTANCE-TURN2-ACK ",
        `recalled=${nonce} `,
        `provider_user_history=${userMessages.length}`,
      ],
      finishReason: "stop",
      delayMs: 120,
    };
  }
  if (latest.includes("TUI_ACCEPTANCE_LONG_WRAP")) {
    const matrix = Array.from({ length: 48 }, (_, index) => {
      const row = String(index).padStart(2, "0");
      return (
        `ROW-${row} 中文自然折行必须保留字符与顺序；` +
        `URL https://example.invalid/terminal/${row}/a-very-long-path?alpha=1234567890&beta=折行；` +
        `JSON {"row":${index},"emoji":"🚀🧪","完整":true}; ` +
        `CODE verifyTerminalWidth(${40 + index});\n`
      );
    }).join("");
    return {
      chunks: [
        "TUI_ACCEPTANCE-LONG-BEGIN 中文折行验证：这是不能丢失末尾的超长句子，",
        "它必须在不同终端宽度内自然换行且继续显示全部文字。\n",
        matrix,
        "TUI_ACCEPTANCE-LONG-END 🚀🧪 END-OF-LONG-RESPONSE",
      ],
      finishReason: "stop",
      delayMs: 120,
    };
  }
  if (latest.includes("TUI_ACCEPTANCE_SLOW_STATUS")) {
    return {
      chunks: ["TUI_ACCEPTANCE-SLOW-BEGIN ", "stream-progress-visible ", "TUI_ACCEPTANCE-SLOW-END"],
      finishReason: "stop",
      delayMs: 650,
    };
  }
  if (latest.includes("TUI_ACCEPTANCE_VALID_DSML")) {
    const toolResultAfterRequest = messages
      .slice(
        Math.max(
          0,
          messages.findLastIndex(
            (message) =>
              message?.role === "user" &&
              textOf(message.content).includes("TUI_ACCEPTANCE_VALID_DSML"),
          ),
        ),
      )
      .some((message) => message?.role === "tool");
    if (toolResultAfterRequest) {
      return {
        chunks: ["TUI_ACCEPTANCE-DSML-TOOL-COMPLETE"],
        finishReason: "stop",
      };
    }
    const exposed = exposedToolNames({ tools });
    const toolName =
      exposed.find((name) => /list.*mcp.*resource/i.test(name)) ??
      exposed.find((name) => name === "tool_cache_stats") ??
      exposed.find((name) => name === "workspace_snapshot") ??
      exposed.find((name) => name === "runtime_capabilities");
    if (!toolName) {
      return {
        chunks: ["TUI_ACCEPTANCE-DSML-NO-SAFE-EXPOSED-TOOL"],
        finishReason: "stop",
      };
    }
    const invocation =
      toolName === "runtime_capabilities"
        ? `<｜｜DSML｜｜invoke name="${toolName}"><｜｜DSML｜｜parameter name="intent" string="true">verify the production DSML read-only tool boundary</｜｜DSML｜｜parameter></｜｜DSML｜｜invoke>`
        : `<｜｜DSML｜｜invoke name="${toolName}"></｜｜DSML｜｜invoke>`;
    return {
      chunks: [
        "<｜｜DSML｜｜tool_",
        `calls>${invocation}</｜｜DSML｜｜tool_calls>`,
      ],
      finishReason: "stop",
      delayMs: 80,
    };
  }
  if (latest.includes("TUI_ACCEPTANCE_OBSERVER_SYNC")) {
    if (latest.includes("publish one answer")) {
      return {
        chunks: [
          "TUI_ACCEPTANCE-OBSERVER-SYNC-BEGIN ",
          "live-progress-visible ",
          "TUI_ACCEPTANCE-OBSERVER-SYNC-ACK",
        ],
        finishReason: "stop",
        // Keep a deterministic pre-terminal window long enough for two
        // independent tmux surfaces to redraw and be captured serially even
        // while the machine is linking other Rust targets.
        delayMs: 2500,
      };
    }
    const marker = latest.includes("from WebUI")
      ? "TUI_ACCEPTANCE-WEBUI-TO-TUI-ACK"
      : latest.includes("from TUI")
        ? "TUI_ACCEPTANCE-TUI-TO-WEBUI-ACK"
        : latest.includes("after WebUI disconnect")
          ? "TUI_ACCEPTANCE-WEBUI-DISCONNECT-ACK"
          : latest.includes("after reconnect")
            ? "TUI_ACCEPTANCE-RECONNECT-ACK"
            : "TUI_ACCEPTANCE-OBSERVER-SYNC-ACK";
    return {
      chunks: [marker],
      finishReason: "stop",
    };
  }
  if (latest.includes("TUI_ACCEPTANCE_INVALID_DSML")) {
    return {
      chunks: [
        "<｜｜DSML｜｜tool_",
        'calls><｜｜DSML｜｜invoke name="bash"></｜｜DSML｜｜invoke></｜｜DSML｜｜tool_calls>',
      ],
      finishReason: "stop",
      delayMs: 80,
    };
  }
  return {
    chunks: ["TUI_ACCEPTANCE-DEFAULT-ACK"],
    finishReason: "stop",
  };
}

function streamResponse(response, model, requestId, userCount) {
  const { chunks, finishReason, delayMs = 0 } = response;
  let index = 0;
  const emit = () => {
    if (index < chunks.length) {
      const chunk = {
        id: requestId,
        object: "chat.completion.chunk",
        model,
        choices: [
          {
            index: 0,
            delta: { content: chunks[index] },
            finish_reason: null,
          },
        ],
      };
      this.write(`data: ${JSON.stringify(chunk)}\n\n`);
      index += 1;
      if (delayMs > 0) {
        setTimeout(emit, delayMs);
      } else {
        emit();
      }
      return;
    }

    const terminal = {
      id: requestId,
      object: "chat.completion.chunk",
      model,
      choices: [{ index: 0, delta: {}, finish_reason: finishReason }],
      usage: {
        prompt_tokens: 120 + userCount * 17,
        completion_tokens: Math.max(
          1,
          Math.ceil(chunks.join("").length / 4),
        ),
        total_tokens:
          120 +
          userCount * 17 +
          Math.max(1, Math.ceil(chunks.join("").length / 4)),
      },
    };
    this.write(`data: ${JSON.stringify(terminal)}\n\n`);
    this.end("data: [DONE]\n\n");
  };
  emit();
}

let requestSequence = 0;
const server = http.createServer((request, response) => {
  if (logPath) {
    fs.appendFileSync(
      logPath,
      `${JSON.stringify({
        received_at: new Date().toISOString(),
        phase: "request_start",
        method: request.method,
        url: request.url,
      })}\n`,
      "utf8",
    );
  }
  if (request.method === "GET" && request.url === "/health") {
    response.writeHead(200, { "content-type": "application/json" });
    response.end('{"ok":true}');
    return;
  }
  if (
    request.method !== "POST" ||
    !["/v1/chat/completions", "/chat/completions"].includes(request.url)
  ) {
    response.writeHead(404, { "content-type": "application/json" });
    response.end('{"error":{"message":"fixture route not found"}}');
    return;
  }

  let body = "";
  request.setEncoding("utf8");
  request.on("data", (chunk) => {
    body += chunk;
  });
  request.on("end", () => {
    let parsed;
    try {
      parsed = JSON.parse(body);
    } catch {
      response.writeHead(400, { "content-type": "application/json" });
      response.end('{"error":{"message":"invalid JSON"}}');
      return;
    }
    const messages = Array.isArray(parsed.messages) ? parsed.messages : [];
    recordRequest(parsed, messages);
    requestSequence += 1;
    const requestId = `chatcmpl-tui-acceptance-${requestSequence}`;
    const model =
      typeof parsed.model === "string" && parsed.model.length > 0
        ? parsed.model
        : fixtureModel;
    const fixtureResponse = responseFor(messages, parsed.tools);
    response.writeHead(200, {
      "cache-control": "no-cache",
      "content-type": "text/event-stream",
      connection: "keep-alive",
      "x-request-id": requestId,
    });
    streamResponse.call(
      response,
      fixtureResponse,
      model,
      requestId,
      messages.filter((message) => message?.role === "user").length,
    );
  });
});

server.listen(port, "127.0.0.1", () => {
  process.stdout.write(`cowd TUI acceptance provider listening on ${port}\n`);
});

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => server.close(() => process.exit(0)));
}
