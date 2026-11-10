# Distributed Systems Fieldnotes

A visual, chapter-based guide to the supplied distributed-systems notes. The site includes standalone chapter pages, hundreds of source diagrams, animated topic flows, searchable chapter summaries, mind maps, a local file reader with HTML-to-Markdown export, explicit read/unread controls, bookmarks, collapsible sidebars, light and dark themes, and private notes. Formulas are typeset as MathML for offline rendering.

Visit the [GitHub project](https://github.com/NikhilG6704/Distributed-Systems-Complete-Guide) for the source and updates. When sharing or adapting these notes, please credit the project and link back to its repository.

This is a static site, so it cannot securely prevent visitors from copying text, downloading files, or viewing page source. A browser-side lock or disabled copy controls would be easy to bypass and would interfere with normal reading. The site instead makes the project link and reuse request visible on the index and chapter pages.

## Open the site

Open `index.html` directly. The site works from `file://` without a build step. Or serve this folder locally:

```sh
python3 -m http.server 8000
```

Then visit [http://localhost:8000](http://localhost:8000).

## Project files

| File or folder | Purpose |
| --- | --- |
| `index.html` | Home page and navigation to every chapter. |
| `app.js` | Home page interactions, search, chapter listing, and reading progress. |
| `chapters.json` | Chapter titles, summaries, source paths, and page identifiers. |
| `chapters/` | One standalone, linked HTML page for each of the 19 chapters. |
| `content/` | Source Markdown notes and local diagrams/images. |
| `build_chapters.py` | Builds the chapter HTML pages and embeds chapter data into the index. Uses Pandoc and Bleach. |
| `styles.css` | Main responsive layout and dark theme. |
| `light-mode.css` | High-contrast light theme overrides. |
| `chapter-tools.js` | Notes, mind maps, file opening and HTML-to-Markdown conversion, read state, themes, and sidebar controls. |
| `chapter-tools.css` | Chapter tools, transitions, overlays, and control layout. |
| `visual-tools.css` | Flow diagrams and local file preview styles. |
| `README.md` | Project guide, contribution notes, and roadmap. |

## Chapter guide

The 19 chapters move from core distributed-systems concepts to practical design, operations, and research:

1. **Distributed Systems Foundations** — Core vocabulary, system properties, failure models, and the reasoning tools used throughout the guide.
2. **Time, Clocks, and Ordering** — Physical and logical clocks, causality, event ordering, and the limits of synchronized time.
3. **Communication in Distributed Systems** — Network calls, serialization, retries, timeouts, idempotency, and messaging patterns.
4. **Consistency Models** — How replicas expose data, order concurrent operations, and trade coordination for latency and availability.
5. **Consensus and Coordination** — Agreement, replicated state machines, leader election, distributed commit, locks, and leases.
6. **Replication** — Copying data across machines, replication strategies, recovery, and consistency trade-offs.
7. **Partitioning (Sharding)** — Dividing data and traffic, choosing partition keys, routing, rebalancing, and hot spots.
8. **Distributed Transactions** — Maintaining correctness across services with commit protocols, sagas, and compensating actions.
9. **Storage Systems** — Storage engines, data layouts, read/write paths, indexing, and distributed data platforms.
10. **Messaging and Streaming** — Queues, logs, event-driven architectures, delivery semantics, and stream processing.
11. **Batch and Large-Scale Computation** — Distributed work scheduling, MapReduce-style processing, recovery, and optimization.
12. **Caching and Content Delivery** — Cache strategies, invalidation, freshness, CDNs, and performance trade-offs.
13. **Load Balancing and Traffic Management** — Routing, admission control, traffic shaping, health checks, and regional distribution.
14. **Fault Tolerance and Reliability** — Failure detection, retries, timeouts, circuit breakers, graceful degradation, and reliability practices.
15. **Scalability and Architecture Patterns** — Finding bottlenecks and choosing ways to scale services, data, and workloads.
16. **Observability** — Metrics, logs, traces, alerting, incident response, and production debugging.
17. **Security in Distributed Systems** — Workload identity, authentication, authorization, encryption, and security operations.
18. **Verification and Testing** — Specifications, model checking, fault injection, property-based testing, and operational verification.
19. **Landmark Papers** — Influential research and the ideas that shaped consensus, storage, databases, messaging, and observability.

### Notes and browser data

Chapter notes autosave to this browser's local storage, keyed by chapter. They return when that chapter is reopened in the same browser profile, but they are not saved into the HTML file or synchronized to another device. Use **Download .txt** for a portable copy. Theme and read/unread preferences are also kept in browser storage.

## Contributions and support

Contributions, corrections, clearer diagrams, accessibility improvements, and feature ideas are welcome. To contribute, open an issue describing the change or submit a pull request with a focused update. For chapter content, update the relevant source Markdown in `content/`, then regenerate the pages with:

```sh
python3 build_chapters.py
```

If you find this field guide useful, **please [star the project on GitHub](https://github.com/NikhilG6704/Distributed-Systems-Complete-Guide)** and share it with other learners. Stars help more people discover the guide, and your support is much appreciated.

## Future ideas

- Add live, interactive visuals that let readers adjust assumptions and see how latency, replication, quorum size, and failures change system behavior.
- Add optional login facilities for accounts, synchronized notes, bookmarks, and reading progress across devices.
- Expand guided exercises, simulations, and accessible diagram descriptions.

Login and synchronization would require a hosted service and a secure account system; the current site is static and stores personal state locally in the browser.
