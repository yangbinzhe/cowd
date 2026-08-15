(() => {
  "use strict";
  const SCHEMA = 1;
  const APP_ID = "reference-app";
  const parentOrigin = (() => {
    try { return new URL(document.referrer).origin; } catch (_) { return null; }
  })();
  let host = null;
  let port = null;
  let frameNonce = null;
  const seen = new Set();
  const pending = new Map();
  const status = document.getElementById("status");
  const output = document.getElementById("output");
  const echo = document.getElementById("echo");

  function validInit(event, message) {
    return parentOrigin !== null && event.origin === parentOrigin &&
      event.source === window.parent && message &&
      message.kind === "host_init" && message.schema_version === SCHEMA &&
      message.app_id === APP_ID && typeof message.frame_nonce === "string" &&
      message.frame_nonce.length >= 1 && typeof message.protocol_digest === "string" &&
      typeof message.catalog_generation === "string" && event.ports && event.ports.length === 1;
  }

  function onPort(event) {
    const message = event.data;
    if (!message || message.schema_version !== SCHEMA || typeof message.request_id !== "string") return;
    const replayKey = `${message.request_id}:${message.kind}:${message.sequence ?? -1}`;
    if (seen.has(replayKey)) return;
    seen.add(replayKey);
    if (seen.size > 1024) seen.delete(seen.values().next().value);
    const request = pending.get(message.request_id);
    if (!request) return;
    if (message.kind === "host_api_data" && Number.isSafeInteger(message.sequence) &&
        message.sequence === request.nextSequence && typeof message.data_base64url === "string") {
      request.nextSequence += 1;
      request.chunks.push(message.data_base64url);
      port.postMessage({kind:"app_api_credit", schema_version:SCHEMA, request_id:message.request_id, bytes:65536});
    } else if (message.kind === "host_api_end" && message.sequence === request.nextSequence) {
      pending.delete(message.request_id);
      output.textContent = request.chunks.join("\n");
    } else if (message.kind === "host_api_error") {
      pending.delete(message.request_id);
      output.textContent = JSON.stringify(message.error, null, 2);
    }
  }

  window.addEventListener("message", (event) => {
    if (host || !validInit(event, event.data)) return;
    host = event.source;
    frameNonce = event.data.frame_nonce;
    port = event.ports[0];
    port.onmessage = onPort;
    port.start();
    status.textContent = "Verified MessageChannel connected.";
    echo.disabled = false;
    host.postMessage({kind:"app_ready", schema_version:SCHEMA, app_id:APP_ID,
      frame_nonce:frameNonce, message_id:crypto.randomUUID()}, parentOrigin);
  });

  echo.addEventListener("click", () => {
    if (!port) return;
    const requestId = crypto.randomUUID();
    pending.set(requestId, {nextSequence:0, chunks:[]});
    port.postMessage({kind:"app_api_request", schema_version:SCHEMA, request_id:requestId, method:"POST",
      path:"/operations/reference-app.echo/invoke", deadline_unix_ms:Date.now()+30000,
      headers:{"content-type":"application/json"}, body:{message:"hello from the reference APP"}});
    port.postMessage({kind:"app_api_credit", schema_version:SCHEMA, request_id:requestId, bytes:65536});
  });

  window.addEventListener("pagehide", () => {
    if (port) for (const requestId of pending.keys()) port.postMessage({kind:"app_api_cancel", schema_version:SCHEMA, request_id:requestId});
    pending.clear();
    if (port) port.close();
    port = null; host = null; frameNonce = null;
  });
})();
