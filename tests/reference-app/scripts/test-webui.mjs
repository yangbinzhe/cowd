import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";
import {MessageChannel} from "node:worker_threads";
import crypto from "node:crypto";

const listeners = new Map();
const elements = new Map(["status", "echo", "output"].map((id) => [id, {
  disabled: id === "echo", textContent: "", listeners: new Map(),
  addEventListener(kind, callback) { this.listeners.set(kind, callback); }
}]));
const parentMessages = [];
const parent = {
  postMessage(message, targetOrigin) { parentMessages.push({message, targetOrigin}); }
};
const window = {
  parent,
  addEventListener(kind, callback) { listeners.set(kind, callback); }
};
const context = vm.createContext({window, URL,
  document:{referrer:"https://edge.example.test/apps/reference-app", getElementById:(id)=>elements.get(id)},
  crypto, console, setTimeout, clearTimeout});
vm.runInContext(readFileSync(new URL("../webui/app.js", import.meta.url), "utf8"), context,
  {filename:"webui/app.js"});

function receive(port) {
  return new Promise((resolve) => port.once("message", resolve));
}

const rejected = new MessageChannel();
listeners.get("message")({origin:"null", source:parent, ports:[rejected.port1], data:{
  kind:"host_init", schema_version:1, app_id:"reference-app", frame_nonce:"nonce-1",
  message_id:"init-rejected", protocol_digest:"sha256:"+"a".repeat(64),
  catalog_generation:"sha256:"+"b".repeat(64)}});
await new Promise((resolve)=>setTimeout(resolve, 10));
assert.equal(elements.get("echo").disabled, true);
rejected.port1.close(); rejected.port2.close();

const channel = new MessageChannel();
listeners.get("message")({origin:"https://edge.example.test", source:parent, ports:[channel.port1], data:{
  kind:"host_init", schema_version:1, app_id:"reference-app", frame_nonce:"nonce-1",
  message_id:"init-1", protocol_digest:"sha256:"+"a".repeat(64),
  catalog_generation:"sha256:"+"b".repeat(64)}});
await new Promise((resolve)=>setTimeout(resolve, 10));
assert.equal(parentMessages.length, 1);
const [{message:ready, targetOrigin}] = parentMessages;
assert.deepEqual({...ready}, {kind:"app_ready", schema_version:1, app_id:"reference-app",
  frame_nonce:"nonce-1", message_id:ready.message_id});
assert.equal(targetOrigin, "https://edge.example.test");
assert.equal(elements.get("echo").disabled, false);

const firstRequestPromise = receive(channel.port2);
elements.get("echo").listeners.get("click")();
const firstRequest = await firstRequestPromise;
const firstCredit = await receive(channel.port2);
assert.equal(firstRequest.kind, "app_api_request");
assert.equal(firstCredit.kind, "app_api_credit");
assert.equal(firstCredit.request_id, firstRequest.request_id);
channel.port2.postMessage({kind:"host_api_headers", schema_version:1,
  request_id:firstRequest.request_id, status:200, headers:{}});
channel.port2.postMessage({kind:"host_api_data", schema_version:1,
  request_id:firstRequest.request_id, sequence:0, data_base64url:"eyJvayI6dHJ1ZX0"});
const replenished = await receive(channel.port2);
assert.equal(replenished.kind, "app_api_credit");
channel.port2.postMessage({kind:"host_api_data", schema_version:1,
  request_id:firstRequest.request_id, sequence:0, data_base64url:"cmVwbGF5"});
channel.port2.postMessage({kind:"host_api_end", schema_version:1,
  request_id:firstRequest.request_id, sequence:1});
await new Promise((resolve)=>setTimeout(resolve, 10));
assert.equal(elements.get("output").textContent, "eyJvayI6dHJ1ZX0");

const secondRequestPromise = receive(channel.port2);
elements.get("echo").listeners.get("click")();
const secondRequest = await secondRequestPromise;
await receive(channel.port2);
const cancelPromise = receive(channel.port2);
listeners.get("pagehide")();
const cancel = await cancelPromise;
assert.equal(cancel.kind, "app_api_cancel");
assert.equal(cancel.request_id, secondRequest.request_id);
channel.port2.close();

console.log(JSON.stringify({valid:true, parent_origin:true, opaque_child_origin:true,
  window_ready:true, port_api_only:true, exact_source:true, nonce_bound:true,
  replay_rejected:true, credit:true, cancel:true}));
