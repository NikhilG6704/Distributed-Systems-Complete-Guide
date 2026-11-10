# Chapter 3 — Communication in Distributed Systems

This bundle contains the chapter Markdown and its editable SVG diagrams.

## Files

- `03-communication.md` — complete chapter with relative image links.
- `assets/*.svg` — 20 standalone, editable SVG diagrams.

Open the Markdown file from this folder so the relative paths resolve in VS Code, GitHub, and compatible Markdown viewers.

## Diagram inventory

- `distributed-communication-architecture.svg` — E-commerce synchronous and event-driven service architecture.
- `rpc-request-response-lifecycle.svg` — RPC client/server request and response lifecycle with timeout ambiguity.
- `grpc-streaming-modes.svg` — Unary, server-streaming, client-streaming and bidirectional gRPC.
- `grpc-http2-architecture.svg` — HTTP/2 multiplexing, framing, flow control and TCP head-of-line effects.
- `rest-api-request-response.svg` — REST request/response plus ETag conditional caching.
- `graphql-query-execution.svg` — GraphQL parse/validate/execute pipeline and DataLoader batching.
- `message-passing-vs-shared-memory.svg` — Comparison of explicit message transfer and shared memory coordination.
- `serialization-lifecycle.svg` — Object-to-bytes-to-object pipeline and schema checks.
- `protobuf-wire-format.svg` — Protobuf tag, length-delimited payload and wire type examples.
- `schema-evolution-compatibility.svg` — Safe rolling schema evolution across producer and consumer versions.
- `message-delivery-semantics.svg` — At-most-once, at-least-once and transactional inbox timelines.
- `at-least-once-duplicate-delivery.svg` — Crash after DB commit but before ACK leading to redelivery.
- `exactly-once-processing.svg` — Kafka transactional consume-process-produce boundary.
- `idempotency-payment-workflow.svg` — Payment idempotency key, uncertain outcome and reconciliation.
- `retry-exponential-backoff.svg` — Capped exponential backoff and deadline cutoff.
- `retry-jitter-comparison.svg` — No jitter, full jitter and equal jitter retry distributions.
- `retry-storm-cascading-failure.svg` — Retry amplification across gateway, services and database.
- `gossip-push-pull.svg` — Push, pull and push-pull gossip state dissemination.
- `gossip-anti-entropy.svg` — Merkle tree comparison and partial repair between replicas.
- `epidemic-propagation.svg` — Ten-node randomized gossip spreading rounds and illustrative curve.

## Notes

- All SVGs use a consistent palette, typography, and viewBox-based sizing.
- Text and shapes remain editable vector elements.
- Diagrams are conceptual teaching illustrations; consult the chapter references for protocol specifications and implementation details.
