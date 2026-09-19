# Change Event contract v1

This document defines the phase-one database-independent event contract. Binary Protocol Buffers in package `cdc.v1` is canonical; JSON is diagnostic output only.

## Envelope

Every `ChangeEvent` contains:

| Part | Required content |
| --- | --- |
| `metadata` | 32-byte event identity, 32-byte Event Content Digest, contract major `1`, UTC capture and emit timestamps, effective capture Revision Reference, and Capture Scope revision |
| `source` | Source, Capture Stream, and Capture Stream Generation UUIDs; source kind and version; Connector Identity; Source Incarnation; Source Environment Epoch ID and definition digest; and optional native event cursor |
| `transaction` | deterministic transaction identity, kind, sequence, optional native transaction identity and commit time, and begin/commit Source Cursors |
| `objects[]` | ordered Object References with required lineage identities, source-faithful names, and Schema Fingerprints |
| `extensions[]` | ordered, namespaced, versioned binary diagnostic extensions that are not required for generic correctness |
| `payload` | exactly one recognized payload variant |

UUIDs are 16 raw bytes and SHA-256 values are 32 raw bytes. A Revision Reference contains its entity UUID, monotonic revision number, and 32-byte canonical configuration digest. Times use UTC seconds plus normalized nanoseconds and never determine order or identity.

Payload variants are `TransactionBegin`, `RowChange`, `SchemaChange`, `TableReset`, `TransactionCommit`, `Heartbeat`, `GenerationActivated`, `CaptureScopeActivated`, `SourceEnvironmentChanged`, and the reserved future `SnapshotBegin`, `SnapshotRow`, and `SnapshotEnd`. There is no redundant operation discriminator outside the payload union.

## Source coordinates

`SourceCursor` contains a required namespaced format, opaque binary value, and optional display text. Transaction events carry a required begin cursor and commit-safe cursor; a native event cursor is optional. Because Capture buffers through commit, every emitted member of a committed Transaction Batch can carry its final commit cursor. Only the commit cursor may advance a Capture Checkpoint. Components other than the matching Source Connector must compare or transport cursors as opaque values and must not infer binlog filenames, positions, LSNs, SCNs, or vendor ordering rules.

## Transaction contract

Transaction kind is one of `SOURCE_DML`, `SOURCE_DDL`, `STREAM_CONTROL`, or the reserved `SNAPSHOT`. Sequence zero is `TransactionBegin`, changes occupy contiguous sequence numbers `1..N`, and `TransactionCommit` is `N+1`. The commit declares `change_count` and a digest of the ordered `(event_id, payload_digest)` pairs for changes. Missing, duplicate, reordered, cross-transaction, count-mismatched, or digest-mismatched events invalidate the complete batch before apply.

MySQL DDL and Capture Scope activation use Synthetic Transactions. A scope activation transaction contains one `CaptureScopeActivated` payload with old and new revision references, its effective Source Cursor, and ordered added and removed rules. One source DDL statement remains one ordered transaction even when it contains multiple clauses or affects multiple objects; a Sink may not split it into independently committed target statements.

The first transaction in every Capture Stream Generation is a `STREAM_CONTROL` Synthetic Transaction containing one `GenerationActivated` payload. It references the immutable Generation Baseline and digest, Deployment and generation identities, exact baseline Source Cursor, resolved event/schema/checkpoint topic names and roles, initial Source Environment Epoch Definition, Schema Anchor, Effective Configuration, and Capture Scope revision. It is interpreted under the initial source environment. The producer atomically publishes this transaction, the full initial schema records and Generation Baseline, and the initial Capture Checkpoint across the three generation topics. No ordinary source transaction may precede it or share its sequence.

A verified source-environment transition is a `STREAM_CONTROL` Synthetic Transaction containing one `SourceEnvironmentChanged` payload. It is published at the last complete boundary before any transaction interpreted under the new environment. The payload contains references to the old and new Source Environment Epoch Definitions; the boundary Source Cursor; log-continuity and schema-reconciliation evidence digests; and the observation time. A proven transition preserves Source Incarnation. If continuity, definition reconciliation, or qualification fails, Capture blocks without publishing the marker or any event under the candidate environment.

Every Change Event, including a heartbeat or control event, references exactly one Source Environment Epoch ID and definition digest. The environment-change transaction is itself interpreted under the old epoch while introducing the new one; the immediately following transaction must reference the new epoch. A consumer rejects an event whose epoch reference is absent, unknown, conflicts with immutable history, or changes without the ordered transition marker.

Conditional source DDL that has no object effect still produces a `SOURCE_DDL` Synthetic Transaction. Its Schema Change outcome is `NO_CHANGE`, its ordered object changes are empty, and the source statement remains diagnostic evidence; a Sink validates the event and advances without executing that statement.

`TRUNCATE` produces a `TableReset` rather than a Schema Change or a set of Row Changes. It references one table, carries equal before and after Schema Fingerprints, declares removal of all rows and reset of applicable Table Runtime State, and uses the DDL `PREPARED`/`APPLIED` recovery path.

A prepared Table Reset with an unknown execution outcome is recovered by first validating the expected unchanged Table Definition and then executing the reset again. This is safe only because the route applies serially, no later route transaction can pass the prepared event, and the Target Object Owner contract excludes concurrent writers. A definition mismatch or ownership violation blocks recovery.

## Schema lineage

Every table Object Reference contains a required 32-byte Object Lineage ID. Every column, index, key, and constraint in a Table Definition contains a required 32-byte Element Lineage ID. Native identifiers, names, ordinals, and Schema Fingerprints remain separate attributes and are never substituted for lineage.

Lineage IDs use versioned, typed, length-prefixed SHA-256 inputs. An object first observed in a Schema Anchor derives its identity from the Source, Capture Stream, and Capture Stream Generation identities, Source Incarnation, anchor cursor, object kind, and exact qualified name. An object created later derives it from the creating transaction and ordered Object Change coordinate. Nested elements derive from their owning object lineage and their deterministic first-observation or creation coordinate.

Rename, alteration, column reordering, and Table Reset preserve lineage. Drop followed by create, and `CREATE TABLE ... LIKE`, create new lineages. Removing only a Route Projection preserves the captured lineage. Removing an object from Capture Scope and later adding it creates a new lineage unless uninterrupted history has been independently proven. Temporary MySQL Table Map identifiers and storage-engine internal identifiers are diagnostic evidence only.

## Native statement evidence

Definition-changing and reset payloads retain bounded Native Statement Evidence containing the original bytes, source dialect and exact version, source character-set identity, DDL Parse Context reference, raw-statement SHA-256 digest, parsed statement kind, executable-version-comment indicator, ordered Ignored Source Clauses with reasons, and normalized-result digest. The raw bytes and all correctness-bearing interpretation fields participate in the Event Content Digest.

Native Statement Evidence is subject to the Transport Envelope and is never truncated. It is hidden from ordinary diagnostic JSON and requires an explicit privileged `--include-native-sql` request; runtime logs never emit it.

## Schema Change payload

`SchemaChange` contains a required effect, optional Native Statement Evidence, and ordered `object_changes`. Effect is `CHANGED` or `NO_CHANGE`; `NO_CHANGE` requires an empty change list, while `CHANGED` requires at least one change. MySQL DDL requires Native Statement Evidence, but the field remains optional for future sources that expose structured schema transitions without a native statement.

Each `ObjectChange` contains:

- operation `CREATE`, `ALTER`, `RENAME`, or `DROP`;
- an optional `before_object_index` and optional `after_object_index`, both with explicit field presence;
- ordered `mutations` in source statement order.

CREATE requires only after, DROP requires only before, and ALTER or RENAME requires both with the same Object Lineage ID. RENAME changes the qualified name while preserving lineage. The referenced before and after Schema Fingerprints are authoritative; mutations explain intent and ordering but may not contradict the definitions.

`SchemaMutation` is a correctness-bearing oneof with `AddElement`, `DropElement`, `RenameElement`, `AlterElement`, `MoveElement`, `SetSemanticOption`, and `RemoveSemanticOption`. Element mutations carry the applicable Element Lineage ID and references into the before or after definition. Each mutation classifies its existing-data effect as `NONE`, `INITIALIZE_VALUES`, `REMOVE_VALUES`, or `REWRITE_VALUES`; inability to classify an in-scope operation blocks capture. Phase one rejects executable rewrites it cannot prove equivalent.

The payload contains no target identifier, target SQL, target capability result, or Route policy. Those belong exclusively to a Target Schema Plan.

## Definition references and Schema History

Full Table Definitions and Schema Context Definitions are not embedded in Change Events. An event's ordered `objects[]` supplies Definition References through Object Lineage ID, qualified name, definition kind, and Schema Fingerprint. Object Changes point to before and after entries by index. A Sink resolves each reference from the Capture Stream Generation's Schema Topic and never infers a missing historical definition from either current catalog.

A Schema History record contains the Capture Stream and Capture Stream Generation IDs, Source Incarnation, definition kind, Object Lineage ID, Schema Fingerprint, definition contract major, effective Source Cursor, origin anchor or event identity, and exactly one full normalized definition. Its Kafka key is `(stream_id, stream_generation_id, definition_kind, object_lineage_id, schema_fingerprint)`. Every state has a distinct key, no record is tombstoned, and its content digest must equal the declared fingerprint under the applicable canonical definition encoding.

The producer publishes new Schema History records, the referencing Change Event transaction, and the Capture Checkpoint in one Kafka transaction. Cross-topic observation order is not assumed: a Sink that sees the event first waits for its schema consumer. If the required record is unavailable, corrupt, or conflicts with an already materialized record under the same key, the route blocks.

The Schema Topic also stores the immutable Generation Baseline and Source Environment Epoch Definitions. The baseline is keyed by `(stream_id, stream_generation_id, generation_baseline_digest)`. Environment definitions are keyed by `(stream_id, stream_generation_id, source_incarnation, epoch_id)` and contain the Server Build Identity, Semantic Environment Fingerprint, Connector Identity, canonical definition digest, effective Source Cursor, and origin anchor or `SourceEnvironmentChanged` event identity. The initial baseline and environment definition are published atomically with the Schema Anchor, `GenerationActivated` transaction, and initial Capture Checkpoint. A later environment definition is published atomically with its transition transaction and Capture Checkpoint. Records are never overwritten or tombstoned, allowing a Route that starts after a transition marker to resolve every event's environment without querying the current Source.

## Schema Context definitions

Database-level Schema Context is a first-class definition kind with its own Object Lineage ID, immutable versions, and Schema Fingerprints. `CREATE DATABASE`, `ALTER DATABASE`, and `DROP DATABASE` produce normal Object Changes for that context. A source database drop also carries ordered drops for captured table lineages.

Schema Context changes are retained to interpret later definitions, but the Sink never creates, alters, or drops its configured target database. A context-only transition is planned as `VALIDATE_ONLY` and advances route metadata without executing target DDL. Dropping and recreating a source database with the same name creates a new context lineage.

## Objects and rows

Payloads reference ordered `objects[]` entries by zero-based index. A RowChange references exactly one table lineage whose Object Reference includes the exact Schema Fingerprint used to decode it. Each RowImage contains Column Datums ordered and identified by both `element_lineage_id` and `column_ordinal` in that immutable Table Definition; the ordinal binds native row-image layout while lineage prevents rename or drop-and-recreate ambiguity. The Sink does not resolve values by current column name.

RowChange cardinality is one source row per event. INSERT has only an after image, UPDATE has before and reconstructed after images, and DELETE has only a before image. Every writable ordinary column is complete and accepts only `VALUE` or `NULL`. A generated column also carries `VALUE` or `NULL` when its result is present in the native row bitmap, but may carry `NOT_CAPTURED` when the source does not log that result; `NOT_CAPTURED` is not permitted for an ordinary column. Such a value is a Generated Observation, not writable data. `UNCHANGED` and `REDACTED` remain reserved and are not accepted in phase-one canonical MySQL images. MySQL native event type and column bitmaps are authoritative even when current global variables report `ROW` and `FULL`; an in-scope statement DML event or incomplete required image blocks Capture. `PARTIAL_JSON` patches are accepted only with a complete before document and are merged at capture before publication. Canonical Capture Streams do not redact included data.

A RowChange contains Materialized Row Values, not the source DML statement, assignment expression, predicate, default expression, generated-column expression, or CHECK evaluation. Expressions that are durable schema semantics live only in the referenced Table Definition. Native DDL text lives only in Native Statement Evidence. Consequently an ordinary assignment such as `SET amount = price * quantity` is represented by the resulting before and after values, not by an expression tree.

The Sink binds every writable after value explicitly, including source-generated defaults, automatic timestamps, invisible ordinary columns, and explicit `AUTO_INCREMENT` values. It never binds generated columns. When a Generated Observation is available, the Sink compares the target-computed result inside the same apply transaction and rolls back on mismatch; when it is absent, exact application requires an already qualified equivalent target expression and never triggers a source-row lookup.

A Semantic Key containing an expression-generated column is a usable Row Locator only when every generated key part has a Generated Observation and the target expression is qualified. Otherwise the Sink tries another key and then the complete ordinary before-image strategy. Failure of every strategy is an Unlocatable Row Change. A MySQL generated invisible primary key is not an expression-generated column: it is a `SYSTEM` ordinary identity column whose captured value can locate rows when its target representation is qualified.

## Logical values

The Logical Value union does not coerce database values to generic strings or JSON:

- integers use a minimal two's-complement big-endian byte representation interpreted with the Logical Type's width and signedness;
- decimals use an unscaled arbitrary-precision integer plus scale;
- floats use exact IEEE-754 `fixed32` or `fixed64` bits;
- text carries source bytes, charset identity, and encoding-validity state, while binary carries bytes directly;
- date, time of day, duration, local datetime, instant, zoned timestamp, interval, and year have separate structured representations;
- JSON uses a typed tree whose number union distinguishes signed integer, unsigned integer, decimal, and exact double bits; arrays remain ordered and object entries are canonically ordered by key bytes;
- enum carries label and native ordinal, set carries labels and native bitmap, and bit strings carry bit length and bytes;
- spatial values carry standard WKB or EWKB plus SRID;
- opaque values carry source type identity, format version, and bytes and require an explicit Sink mapping.

Invalid temporal values remain tagged raw values rather than becoming NULL or corrected dates. Display strings are diagnostic and do not replace semantic fields.

## Integrity

`payload_digest` is SHA-256 over the custom typed, framed `cdc.change-event-content.v1` encoding of correctness-bearing metadata, source and transaction contexts, ordered objects, payload, and controlled extensions. It excludes itself, display-only cursor text, and other diagnostic rendering. No Protocol Buffers serialization, including a deterministic mode, is treated as canonical, and correctness-bearing collections do not use Protobuf maps.

The Sink verifies event identities, content digests, contiguous sequence, declared count, and commit digest before opening a target apply transaction. The same event identity with a different content digest is a determinism fault and blocks processing.

## Evolution

Compatible optional fields may be added in `cdc.v1`. Removed field numbers and names, and removed enum values, remain reserved permanently. Unknown noncritical optional fields and extensions may be ignored, but an unknown payload, Presence, Logical Value, transaction kind, or other correctness-bearing enum blocks processing. Extensions may not be the only representation of data required for generic application.

An incompatible semantic or wire-type change uses `cdc.v2` and a new topic generation with a bounded dual-write transition. Binary events are never converted through JSON and written back to the data stream.

An integrity fault in an already published interval, such as discovering an unbounded non-self-describing source-environment change, cannot be repaired by appending a retroactive marker. That immutable Capture Stream Generation is terminated and a new baseline and generation are required. The new generation has a fresh UUID, topics, initial environment definition, Schema Anchor, and Checkpoint; its identity participates in transaction, event, lineage, schema, and checkpoint identities. A worker restart, qualified Source Environment Epoch transition, or normal cursor resume does not by itself create a new generation.

## Diagnostic JSON

CLI JSON output uses lowercase hexadecimal UUID and SHA-256 text, unpadded Base64URL for arbitrary bytes, decimal strings for 64-bit and arbitrary-precision integers, tagged objects for non-finite floats and negative zero, and both structured and ISO-8601 temporal displays. It uses a stable display order for readable diffs but is neither an input format nor a binary compatibility promise.

## Kafka protection

Kafka event and Schema Topics are sensitive production data. Nonlocal and production connections require TLS, authenticated mTLS or SASL/SCRAM identities, and least-privilege ACLs. An isolated POC may explicitly set `insecure_plaintext = true` and receives a startup warning. Phase one does not encrypt individual ChangeEvent fields; broker storage, backups, and local Spools require deployment-layer protection.
