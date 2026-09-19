# MySQL schema history and anchoring

This document defines how the phase-one MySQL connector obtains and evolves the schema state required to decode row events without creating source objects or taking explicit source locks.

## Authority model

Every row event is decoded against the immutable Table Definition identified by its Schema Fingerprint. After a Schema Anchor exists, the authoritative historical state is the Schema History in Kafka plus the ordered normalized DDL published by the Capture Stream. `INFORMATION_SCHEMA`, `SHOW` output, and other current-catalog reads may validate the live tail, but they never overwrite or reinterpret historical definitions.

Complete Table and Schema Context Definitions live in the Schema Topic rather than being duplicated inside each Change Event. Events contain lineage-aware Definition References. New definitions, referencing events, and the Capture Checkpoint are transactionally published together; independent consumers still materialize the Schema Topic before applying a reference and block if its immutable record cannot be obtained or verified.

Definitions describe the Source's Effective Definition after MySQL has accepted and normalized DDL. Original spelling, redundant clauses, and syntax provenance remain Native Statement Evidence and do not override effective catalog semantics.

A Schema Anchor pairs one transaction-safe Source Cursor with:

- the Source, Capture Stream, and Capture Stream Generation identities and Source Incarnation;
- the effective Capture Scope revision;
- the source database-level Schema Context needed to interpret DDL;
- normalized Table Definitions and their Schema Fingerprints for every included table; and
- a digest covering the complete ordered anchor contents.

At the anchor, each included table receives a deterministic Object Lineage ID and each observable column, index, key, and durable constraint receives a deterministic Element Lineage ID. These identities are part of the canonical Table Definitions and Schema History keys. Clauses that an older source parsed but did not retain cannot be reconstructed from current metadata and therefore remain absent from the anchored definition rather than being guessed.

All identifiers are captured as source bytes plus their declared encoding. Catalog display strings may be retained for diagnostics but are not authoritative and are never normalized into identity. Comments and other semantic text use the same encoded-text rule and must be recovered losslessly or the affected definition is unsupported.

## Metadata assembly and cross-checks

Each qualified MySQL release has its own metadata adapter. It constructs a candidate definition from independent public interfaces rather than treating one formatted SQL string as truth:

- `INFORMATION_SCHEMA.COLUMNS` and the relevant table views are primary for columns, ordering, types, nullability, generation, visibility, engine, and table semantics;
- parsed `SHOW CREATE TABLE` or `SHOW CREATE DATABASE` output supplements explicit default presence and release-specific attributes that the catalog cannot distinguish, but its text is never a Schema Fingerprint input;
- `SHOW INDEX` and `INFORMATION_SCHEMA.STATISTICS` cross-check Access Index identity, parts, prefixes, effective direction, visibility, and method;
- `INFORMATION_SCHEMA.KEY_COLUMN_USAGE` and `REFERENTIAL_CONSTRAINTS` jointly establish foreign-key columns, referenced objects, ordering, match behavior, and referential actions;
- `TABLE_CONSTRAINTS` and, on releases that store CHECK constraints, `CHECK_CONSTRAINTS` jointly establish constraint identity, expression, and enforcement; and
- `SCHEMATA` establishes database defaults, with parsed `SHOW CREATE DATABASE` used only where explicit presence or a release-specific semantic requires it.

System tables under `mysql.*`, InnoDB internal identifiers, statistics estimates, and physical dictionary state are evidence at most, never definition authority. The adapter reconciles all observations into one candidate and cross-checks overlapping fields. A transient disagreement restarts the capture-scan-stabilize attempt; a repeated or non-transient disagreement blocks anchoring with the conflicting evidence recorded.

Metadata sessions fix their character set, collation, quoting behavior, time zone, and relevant visibility settings before reading expressions. `GENERATION_EXPRESSION`, `CHECK_CLAUSE`, and the effective expression recovered for a generated default become `SOURCE_CATALOG_EXPRESSION` bytes after lossless transport validation. Submitted DDL substrings never substitute for these effective expressions.

For live DDL, the exact statement and DDL Parse Context drive a version-specific transition from the verified before definition. The connector buffers the following log while it obtains a stable catalog observation and verifies the resulting definition chain. MySQL's silent normalization is incorporated into the Effective Definition. If the adapter's predicted state and the stabilized catalog disagree, capture stops before publishing a definition that subsequent row events could misinterpret.

The anchor proves an interpretation boundary, not a transactionally locked catalog snapshot. Ordinary metadata queries may encounter MySQL's normal short-lived metadata locking, but the tool issues no explicit table, global read, backup, or instance lock.

## First live-tail anchor

For a first start at the live tail, the connector uses capture-scan-stabilize:

1. Establish a complete binlog boundary `C0` and begin buffering the raw replication stream before reading catalog metadata.
2. Read the candidate Schema Context and normalized definitions for the effective Capture Scope.
3. Continue buffering through a complete boundary `C1`.
4. Inspect the ordered interval `C0..C1` for DDL or another event that could change an included definition, its database-level Schema Context, scope interpretation, or transaction boundaries.
5. If the catalog scan was unstable or the interval contains a relevant change, discard the candidate and repeat from a later boundary. An inconclusive event is not treated as harmless.
6. When stable, publish all initial Schema History records, the Schema Anchor, and a Capture Checkpoint at `C1` in one Kafka transaction.
7. Begin semantic decoding only after `C1`; no event can reference the anchor before the transaction containing it is visible.

The bounded raw buffer follows the same memory, local Spool, security, and quota rules as transaction buffering. If stabilization cannot complete within configured limits, the Capture Stream blocks with evidence instead of claiming a schema snapshot.

## Explicit historical starts

A first-ever start at historical cursor `H` is valid only while the source logs needed from `H` remain available and one of these proofs exists:

1. Establish a stable live Schema Anchor `A`, inspect the retained log from `H` through `A`, and prove that no relevant DDL or Schema Context change occurred. The definition at `A` is then also valid at `H`.
2. Supply a Schema Bundle whose identity, Source Incarnation, exact cursor `H`, normalized definitions, Schema Context, fingerprints, and bundle digest all validate. DDL after `H` then evolves that starting state in source order.

A current schema dump without an exact cursor and fingerprints is not a Schema Bundle. If neither proof is available, the historical start is rejected rather than guessed.

Normal restart is different from first start: it resumes from the Kafka Capture Checkpoint and existing Schema History. It does not rescan the current catalog to reconstruct past state.

## Table Map events

Table Map events bind native table identifiers to row-event metadata. They supplement the applicable fingerprinted Table Definition and may provide additional native metadata for cross-checking. Missing optional Table Map metadata is allowed, so phase one does not require `binlog_row_metadata` to be enabled. A conflict in table identity, column count, type semantics, signedness, metadata, or another correctness-bearing attribute blocks the Capture Stream before the row is published.

## Database-level Schema Context

The connector retains source-faithful database defaults needed to normalize table DDL, including character-set and collation semantics. It parses relevant `CREATE DATABASE`, `ALTER DATABASE`, and `DROP DATABASE` statements to evolve that context even though the Sink never creates, alters, or drops the target database itself.

Dropping a source database that contains captured tables produces the corresponding ordered table-object removals. A database statement whose impact on an included table or future DDL interpretation cannot be proven blocks the Capture Stream.

## Conditional and unknown DDL

`IF EXISTS` or `IF NOT EXISTS` that makes a source statement a no-op is still a committed source boundary. The connector emits a `SOURCE_DDL` Synthetic Transaction whose Schema Change outcome is `NO_CHANGE` and whose ordered object-change list is empty. The Sink validates and checkpoints it without executing the retained raw SQL.

An unrecognized Query Event follows a three-way rule:

- provably irrelevant to the Capture Scope, Schema Context, and transaction interpretation: audit and skip;
- successfully parsed and normalized, but unsupported by a particular Sink: publish it and block only that Replication Route;
- uncertain in scope, schema, or transaction impact: block the Capture Stream.

A Schema History event is never force-skipped. Recovery must add support, retry after correction, perform a verified Schema Adoption where valid, or rebuild from a new baseline.
