# Distributed Systems Fieldnotes

Distributed systems are hard to reason about because messages take time, clocks disagree, machines fail independently, and data is copied across places. This guide is designed to help make those ideas easier to see and understand.

It brings the notes into a visual, chapter-based learning path. Follow a request through a network, watch replicas converge, compare consistency guarantees, and trace what happens when a component or connection fails. The aim is to build intuition for how these systems work, why their trade-offs exist, and how to use that understanding when designing real services.

**[Open the learning guide](index.html)** · **[Visit the project on GitHub](https://github.com/NikhilG6704/Distributed-Systems-Complete-Guide)**

## A path through distributed systems

The chapters build from first principles toward production architecture and research. Start at the beginning for a guided progression, or jump to a topic you want to explore.

### Foundations: machines, time, and agreement

1. **Distributed Systems Foundations** — Learn the basic models, guarantees, and failure assumptions used to describe distributed software.
2. **Time, Clocks, and Ordering** — Understand why there is no shared clock, how causality differs from wall-clock time, and how systems order events.
3. **Communication in Distributed Systems** — Explore requests, messages, timeouts, retries, idempotency, and what a caller can know when a response is lost.
4. **Consistency Models** — See how replicas can return different versions, and what guarantees such as eventual consistency and linearizability mean to clients.
5. **Consensus and Coordination** — Follow how nodes choose leaders, agree on shared state, and use coordination tools such as locks and leases.

### Data: copies, partitions, and durable state

6. **Replication** — Study how data copies are updated, how replicas recover, and how replication choices affect availability and freshness.
7. **Partitioning (Sharding)** — Learn how data is split across machines, how requests find the right shard, and why hot partitions are difficult.
8. **Distributed Transactions** — Compare ways to keep multi-step work correct when services can fail partway through, including sagas and compensation.
9. **Storage Systems** — Trace storage read and write paths, from data structures and indexes to the engines that support large applications.
10. **Messaging and Streaming** — Understand queues and logs, delivery guarantees, event-driven services, and continuous stream processing.
11. **Batch and Large-Scale Computation** — See how large jobs are divided among workers, coordinated, recovered, and optimized.
12. **Caching and Content Delivery** — Learn when caches help, how invalidation and freshness work, and how content is served closer to users.

### Production: traffic, reliability, and operations

13. **Load Balancing and Traffic Management** — Explore request routing, health checks, admission control, and managing traffic across services and regions.
14. **Fault Tolerance and Reliability** — Build intuition for timeouts, retries, circuit breakers, graceful degradation, and designing for partial failure.
15. **Scalability and Architecture Patterns** — Find bottlenecks and consider how caching, partitioning, replication, and service boundaries change capacity.
16. **Observability** — Connect metrics, logs, and traces to diagnosing behavior and responding to production incidents.
17. **Security in Distributed Systems** — Follow identity, authentication, authorization, encryption, and trust boundaries between services.
18. **Verification and Testing** — Explore how specifications, model checking, simulation, fault injection, and property-based tests reveal subtle failures.

### Research and perspective

19. **Landmark Papers** — Meet influential ideas and papers that shaped consensus, storage, databases, messaging, and observability.

## Learn by exploring

Each chapter is a standalone page with its own table of contents and diagrams. Use the **Flow** view to trace a process step by step, or generate a mind map from a chapter or a Markdown file to see how its topics connect. Open a local file to preview it, and convert HTML pages to Markdown when you want a portable text version.

Search and filtering help you find a topic quickly. Read progress, bookmarks, and personal notes make it easy to return to an idea later. Notes are saved in the current browser profile; use **Download .txt** to keep a copy outside the browser. Light and dark themes and collapsible sidebars let you set up a comfortable reading view.

## Open the guide

Open `index.html` in a browser. To serve it locally instead, run:

```sh
python3 -m http.server 8000
```

Then visit [http://localhost:8000](http://localhost:8000).

## Help the guide grow

If this guide helps you understand a concept, **please [star the project on GitHub](https://github.com/NikhilG6704/Distributed-Systems-Complete-Guide)**. A star helps other learners find it, and your support means a lot.

Corrections, clearer explanations, new examples, and better diagrams are welcome. You can contribute by opening an issue or pull request. The original chapter notes and diagrams live in `content/`; generated chapter pages live in `chapters/`.

## Where it could go next

The next step is to make the visuals interactive: let readers change network delays, replica counts, quorum sizes, or failure patterns and watch the system respond. Future versions could also offer optional accounts so learners can sync notes and progress across devices, alongside more guided exercises and simulations.
