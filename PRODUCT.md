# Product

## Register

product

## Users

Cowd is used by developers and operators who want an AI runtime that can work across codebases, sessions, memory, tools, local terminals, web interfaces, and external channels. Users are usually in an active work loop: inspect state, delegate work, review evidence, continue prior sessions, and decide when automation may proceed.

## Product Purpose

Cowd provides a durable AI agent runtime with session storage, memory infrastructure, context assembly, tool execution, multi-agent coordination, permission control, TUI, WebUI, and channel adapters. Success means users can understand what the agent knows, what it is doing, what it is allowed to do, and how to resume or audit the work without guessing from logs.

As of v0.9.42, the connector runtime is a first-class product surface. Provider accounts, service/channel/MCP capabilities, durable external resource refs, policy decisions, execution receipts, and audit evidence must be visible through the same daemon contract and projected consistently to API, TUI, and WebUI.

## Runtime Contract

- `GET /api/runtime/control-plane` is the operator summary for readiness, blocked/degraded reasons, session leases, provider routing, connector status, task state, and diagnostics.
- `GET /api/connectors/*` is the connector management surface for account readiness, declared capabilities, durable resource refs, service tools, and service execution.
- `POST /api/cross-plane/*` is the governance surface for identity bindings, grants, policy simulation, execution, and audit.
- TUI and WebUI should not invent separate state. They project these contracts and add controls, filters, commands, and operator affordances.

## Connector Product Model

- Channel capabilities move messages or media between Cowd and external conversations.
- Service capabilities read or operate on external workspaces such as documents, drives, wikis, or future office suites.
- MCP capabilities describe tool/resource servers without requiring control-plane inspection to start external MCP processes.
- Resource refs store durable pointers and metadata, not full external document bodies by default.
- Permissions apply across channels and services through identity bindings and grants, so cross-channel actions remain auditable and controllable.

## Brand Personality

Precise, capable, calm.

## Anti-references

Avoid marketing-page composition inside the app, decorative dashboards that hide operational state, fragile novelty controls, opaque automation, and UI that makes permissions or runtime degradation hard to inspect.

## Design Principles

- Make runtime state explicit: show session, memory, context, task, permission, channel, and degradation signals where decisions happen.
- Keep control surfaces compact: dense, scannable panels are preferred over large explanatory layouts.
- Preserve operational trust: every automation path should expose status, evidence, permissions, and auditability.
- Separate boundaries clearly: channels, services, memory, context, and agents should cooperate through visible contracts, not hidden coupling.
- Optimize for continuation: users should be able to leave, return, resume, and understand the current state quickly.

## Accessibility & Inclusion

Default to readable contrast in both dark and light themes, keyboard-accessible controls, clear focus states, reduced-motion-safe transitions, and text labels that remain understandable without color alone.
