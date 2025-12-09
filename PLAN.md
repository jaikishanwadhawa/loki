# Metaquery & Distributed Metastore Plan

## Context for Operators
- The query engine currently builds physical plans in `pkg/engine/engine.go`, lines 297-335, by instantiating `physical.NewMetastoreCatalog`. The planner synchronously calls `catalog.ResolveShardDescriptorsWithShard`, which in turn calls `metastore.Sections` inside `pkg/dataobj/metastore/object.go`.
- `ObjectMetastore.Sections` downloads TOC/index files from object storage, enumerates streams/sections, and applies predicate Bloom filters/AMQs (`lines 227-248`). This work happens on the query front-end, so planning latency and object store load scale with coordinator capacity.
- Goal: turn metadata lookups into “metaqueries” executed via the engine workflow (eventually distributed across workers), while keeping the existing metastore as a fallback. We want incremental steps so each phase can ship independently and be validated.
- Constraints: keep feature-flag fallback to today’s behavior; preserve Bloom filter semantics; keep planner/catalog interfaces stable until distributed path is ready.

## Phase 1 – Metaquery Flow (Coordinator-Only)
**Definition of Done:** Physical planning calls an explicit metaquery stage that still uses the local metastore, guarded by a feature flag; with the flag on/off the system produces identical query results; planner no longer reaches into object storage directly when the flag is on.

1. **Metaquery Abstractions**
   - Introduce `MetaQueryRequest`/`MetaQueryResult` types (tenant, time range, selector/predicate matchers, shard hints).
   - Create `MetaQueryRunner` interface and update `MetastoreCatalog` to call `runner.Resolve(request)` instead of `metastore.Sections` directly.
   - Provide `LocalRunner` implementation that simply calls the existing metastore; default to this runner so behavior matches current code.

2. **Metaquery Execution Stage**
   - Extend `Engine.buildPhysicalPlan` to derive a `MetaQueryRequest` from the logical plan/time params before invoking `physical.NewPlanner`.
   - Execute the runner, cache descriptors for the request, and pass them to the planner via a lightweight `ResolvedCatalog` that satisfies `physical.Catalog` without additional lookups.
   - Add metrics/logging for metaquery duration and descriptor counts; feature flag to bypass the new stage.

3. **Testing & Validation**
   - Unit tests for catalog → runner interactions and feature-flag fallback.
   - Integration/e2e tests ensuring identical query results with/without the metaquery stage enabled.

## Phase 2 – Metastore Redesign Prep (Arrow Artifacts, still Local)
**Definition of Done:** Metaquery results can be materialized as Arrow batches, catalog can consume either structs or Arrow, and we can switch between implementations at runtime; tests cover both paths.

1. **Arrow Schema & Conversion**
   - Define Arrow schema matching `DataobjSectionDescriptor` (object path, section, stream IDs, row count, size, start/end timestamps).
   - Implement converters between descriptors and Arrow record batches; add fuzz/unit tests for schema parity.

2. **Runner/Catalog Refactor**
   - Split responsibilities into `MetaQueryPlanner` and `MetaQueryResultReader` (or similar) so downstream stages can work with Arrow streams.
   - Provide `LocalRunner` (returns structs) and `ArrowRunner` (builds Arrow batches locally) implementations; catalog should accept both via adapters.

3. **Observability & Tests**
   - Metrics for Arrow conversion latency and batch sizes.
   - Integration tests that run both runner types (feature flag or config knob) to ensure identical outputs.

## Phase 3 – Distributed Metastore (New Physical Nodes)
**Definition of Done:** Metaqueries run as distributed workflows using new physical nodes (`MetaLookupScan`, optional `MetaLookupCollect`), planner never performs object-store lookups directly, and there is a fallback to local runners; integration tests cover distributed execution.

1. **Physical Nodes & Workflow Support**
   - Add `MetaLookupScan` node/executor encapsulating current metastore logic (TOC/index reads, Bloom filters). Optionally add `MetaLookupCollect` for merging results.
   - Teach the workflow scheduler to run a meta workflow (build meta physical plan → workflow → execution) prior to the main query workflow.

2. **Distributed Runner Implementation**
   - Implement `DistributedMetaQueryRunner` that builds/executes the meta workflow, streams Arrow batches back, and feeds them to the catalog.
   - Handle retries and fallback to local runner when failures or feature flags dictate.

3. **End-to-End Validation**
   - Integration tests forcing distributed metaqueries (multiple windows, Bloom filter coverage) and comparing outputs to local runner results.
   - Benchmark planning latency/object-store load; document rollout strategy.

## Rollout & Notes
- Each phase should merge independently with feature flags and full test coverage.
- Keep current metastore code untouched wherever possible to ease fallback.
- Bloom filter (AMQ) logic stays within whatever component reads index sections; when distributed, each `MetaLookupScan` applies AMQ filtering before emitting results so global intersections remain accurate.
- AMQ = Approximate Membership Query data structure (Bloom/Cuckoo filters) used for predicate pruning.
