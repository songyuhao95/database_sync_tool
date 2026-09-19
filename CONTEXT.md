# Change Data Replication

This context covers moving committed database changes from source systems to sinks through a vendor-neutral change representation while preserving recoverability, ordering, and transaction boundaries.

## Language

**Deployment**:
An independently administered CDC installation whose immutable identity namespaces its durable control records and stream resources.
_Avoid_: Runtime process, database environment

**Source**:
A database system whose committed changes are captured for replication.
_Avoid_: Source side, upstream database

**Sink**:
A database system that receives and applies replicated changes.
_Avoid_: Destination log, target database log

**Capture Stream**:
An ordered feed of committed changes captured once from a Source and published independently of any Sink.
_Avoid_: Source task, upstream job

**Runtime Role**:
The capture, sink, or combined responsibility assigned to one running CDC worker.
_Avoid_: Crate, deployment platform

**Configuration Revision**:
An immutable, numbered configuration snapshot for a Source, Sink, Capture Stream, or Replication Route.
_Avoid_: Local file version, process settings

**Desired Configuration**:
The approved Configuration Revision that an entity is expected to adopt at its next valid activation boundary.
_Avoid_: Effective configuration, latest edited file

**Effective Configuration**:
The Configuration Revision currently governing an entity's capture or apply behavior.
_Avoid_: Desired configuration, pending revision

**Configuration Activation**:
The recorded transition from one Effective Configuration to another at a transaction-safe boundary.
_Avoid_: File reload, retroactive reinterpretation

**Retired Entity**:
A Source, Sink, Capture Stream, or Replication Route that is no longer active but retains its immutable identity and recovery records.
_Avoid_: Deleted entity, reusable ID

**Capture Scope**:
The versioned set of source schemas and tables whose changes a Capture Stream publishes.
_Avoid_: Route filter, target tables

**Replication Route**:
A configured path that consumes one Capture Stream, applies its changes to one Sink, and tracks its own progress.
_Avoid_: Replication task, target consumer

**Route Projection**:
The versioned selection and transformation of a Capture Stream that one Replication Route applies to its Sink.
_Avoid_: Capture Scope, partial transaction

**Replication Topology**:
The graph formed by Capture Streams and Replication Routes, including one-to-one, one-to-many, and many-to-one arrangements.
_Avoid_: Task list, pipeline set

**Change Event**:
A vendor-neutral representation of one committed data or schema change. Row values
refer to the fingerprinted source definitions that give their columns meaning;
the event does not embed a target-specific type or conversion decision.
_Avoid_: Common log, target log

**Event Content Digest**:
A stable digest of the canonical semantic content of a Change Event, independent of ordinary Protocol Buffers serialization.
_Avoid_: Kafka checksum, event identity

**Schema Change**:
A Change Event that describes a database-independent object transition and may retain the source statement as optional dialect-specific evidence.
_Avoid_: Raw DDL, DDL string

**Object Change**:
One ordered object lifecycle transition within a Schema Change, identified by lineage and authoritative before and after definition references.
_Avoid_: DDL clause, target statement

**Schema Mutation**:
One ordered, database-independent element or semantic-option transition that explains how an Object Change moves between its before and after definitions.
_Avoid_: SQL fragment, schema diff guess

**Target Schema Plan**:
A Sink-specific, validated transformation of a Schema Change into expected target definitions and executable target DDL.
_Avoid_: Raw DDL replay, SQL rewrite

**Target Name Binding**:
A deterministic association between a source Object or Element Lineage ID and the identifier used for it by one Replication Route at its Sink.
_Avoid_: Source rename, user alias

**Schema Fingerprint**:
A stable digest of a normalized object definition used to recognize whether a Schema Change has already taken effect.
_Avoid_: DDL hash, SQL hash

**Schema History**:
The immutable set of fingerprint-addressed object definitions needed to interpret Change Events captured under both current and superseded schemas.
_Avoid_: Latest schema, target catalog

**Schema Anchor**:
A verified pairing of a Source Cursor with the Schema Context and object fingerprints that establish the starting state for interpreting subsequent changes, without claiming a locked source snapshot.
_Avoid_: Metadata read time, current catalog snapshot

**Schema Bundle**:
An immutable set of fingerprinted object definitions and Schema Context tied to one exact Source Cursor and supplied when historical schema cannot be derived safely.
_Avoid_: Current schema dump, unversioned DDL file

**Schema Context**:
The source database-level semantic defaults and settings needed to interpret table definitions and schema changes correctly.
_Avoid_: Target database configuration, connection settings

**Schema Context Definition**:
An immutable, fingerprinted state of one Schema Context lineage at a Source Cursor.
_Avoid_: Database object DDL, target database settings

**No-op Schema Change**:
A committed conditional DDL operation that leaves the tracked object state unchanged while retaining its place in the source order.
_Avoid_: Ignored DDL, skipped event

**DDL Parse Context**:
The source version, default namespace, and session semantics under which one native DDL statement has meaning.
_Avoid_: Sink session, current connector settings

**Table Reset**:
A committed operation that removes every row from a table while retaining its definition and resetting source-defined mutable table state.
_Avoid_: Schema Change, bulk row deletes, no-op

**Table Runtime State**:
Mutable table-level state, such as the next generated identity value, that is not part of the immutable Table Definition.
_Avoid_: Table option, Schema Fingerprint

**Object Lineage ID**:
A stable database-independent identity that follows one source object through rename, alteration, and reset until that object is dropped.
_Avoid_: Qualified name, native table ID, Schema Fingerprint

**Element Lineage ID**:
A stable database-independent identity that follows one column, index, key, or constraint through supported changes within its owning object.
_Avoid_: Element name, ordinal, definition hash

**Native Statement Evidence**:
The bounded source statement and its interpretation context retained for audit and narrowly validated same-dialect replay without replacing the normalized change.
_Avoid_: Canonical Schema Change, generated target SQL

**Ignored Source Clause**:
A source DDL clause that was accepted syntactically but created no durable source definition or enforced behavior.
_Avoid_: Unsupported target feature, No-op Schema Change

**Target Schema Drift**:
A difference between the Sink's actual object definition and the definition expected by its Replication Route that was not produced by that route.
_Avoid_: Schema Change, source DDL

**Schema Adoption**:
An audited reconciliation decision that accepts an already existing target definition as the applied result of a Schema Change.
_Avoid_: Automatic DDL recovery, schema overwrite

**Table Definition**:
An immutable, database-independent description of a table lineage and its element lineages, constraints, relevant type attributes, and source comparison semantics at one schema state.
_Avoid_: CREATE statement, table metadata row

**Effective Definition**:
The durable schema semantics that exist after a Source has accepted and normalized a schema statement, independent of how that statement was originally spelled.
_Avoid_: Parsed DDL intent, raw CREATE text

**Definition Reference**:
An immutable link from a Change Event to one fingerprinted Table or Schema Context Definition in Schema History; it is the authority used to interpret the source column semantics of that event.
_Avoid_: Embedded schema, catalog lookup hint

**Encoded Identifier**:
A database identifier preserved as its source bytes and declared encoding, with optional human-readable display text that does not determine identity.
_Avoid_: Normalized name, lowercase name, display label

**Semantic Key**:
A primary or unique relational constraint that defines row identity or uniqueness independently of any access structure used to enforce it.
_Avoid_: Index, key name

**Access Index**:
An ordered physical access structure whose identity and attributes are modeled separately from any Semantic Key it backs.
_Avoid_: Unique constraint, primary key

**Normalized Expression**:
A database-independent expression whose operators, functions, references, types, and evaluation semantics are fully represented by the definition contract.
_Avoid_: SQL text, parsed token list

**Materialized Row Value**:
The resulting value captured after the Source has evaluated the statement, defaults, and applicable database behavior for one row change.
_Avoid_: DML expression, target-side recomputation

**Generated Observation**:
A source-provided result for a generated column that may verify equivalent Sink computation but is never treated as a writable input value.
_Avoid_: Generated-column assignment, ordinary Column Datum

**Opaque Native Expression**:
A source-faithful expression that cannot be represented by the current Normalized Expression contract and is therefore portable only through a separately validated source-dialect path.
_Avoid_: Extension, best-effort expression

**Unsupported Feature**:
A correctness- or security-bearing source semantic that can be diagnosed but cannot be preserved completely by the current Change Event contract.
_Avoid_: Opaque Native Expression, target incompatibility

**Capture Block Reason**:
The recorded reason a Capture Stream cannot establish a complete source interpretation or safely continue decoding changes.
_Avoid_: Unsupported Feature, target incompatibility

**Capture Recovery Class**:
The declared recovery path for a Capture Block Reason based on whether its source history remains complete and replayable.
_Avoid_: Error severity, retry count

**Target Capability Failure**:
A Replication Route's evidence that one complete source semantic cannot be represented or executed equivalently by its Sink.
_Avoid_: Capture error, unsupported source syntax

**Known Omission**:
An explicitly authorized removal of a source semantic whose resulting guarantee downgrade is recorded for one Replication Route.
_Avoid_: Silent fallback, Unsupported Feature

**Capability Qualification**:
Version-specific evidence that a normalized source semantic and its Sink representation behave equivalently for a defined source-target pair.
_Avoid_: Major-version assumption, successful parse

**Conversion Rule**:
A versioned, release-owned behavior for representing one Logical Type shape in a Sink representation under declared constraints; it does not choose a task-specific column binding or infer constraints from observed values.
_Avoid_: Pair mapping table, runtime cast

**Capability Manifest**:
A release-owned immutable set of Capability Qualifications and Conversion Rules accepted by a particular connector build, including the target representations and constraints they prove.
_Avoid_: Operator allowlist, runtime feature guess

**Server Build Identity**:
The exact database product, distribution, version, and build provenance used to select build-specific evidence, excluding mutable runtime settings and logical database history.
_Avoid_: Major version, Source Incarnation, environment fingerprint

**Semantic Environment Fingerprint**:
A digest of the role-specific settings that can change how a fixed server build interprets source history or executes replicated changes.
_Avoid_: All-variable snapshot, Server Build Identity

**Environment Profile**:
A versioned connector declaration of the role-wide correctness settings measured to form a Semantic Environment Fingerprint.
_Avoid_: Server configuration dump, capability result

**Environment Requirement**:
A classified connector rule that states how one observed database property contributes to identity, continuity, preflight, event validation, session control, or operations.
_Avoid_: Unclassified variable, configuration option

**Connector Identity**:
The connector release, event-contract generation, and Capability Manifest identity under which database semantics are interpreted and qualified.
_Avoid_: Worker process, Server Build Identity

**External Object Reference**:
A source-faithful qualified reference to a dependency outside the known captured lineage, without inventing an Object Lineage ID for it.
_Avoid_: Placeholder lineage, target object reference

**Native Type**:
The source database's exact type name and declaration retained with the source definition for lossless inspection and connector-specific translation; a Native Type that has no proven Logical Type remains unsupported rather than being silently widened or stringified.
_Avoid_: Logical type, Rust type

**Source Type Mapping**:
A versioned Source-owned rule that uses a precise Native Type, source definition semantics, and the verified source build/environment to produce a Logical Type and its decoding evidence without consulting a Sink or observed row values.
_Avoid_: Cross-database type table, value inference

**Logical Value**:
A lossless database-independent representation of one captured column value, including only value-level semantics; source and target column definitions are resolved separately.
_Avoid_: JSON value, stringified value

**Logical Type**:
A database-independent type description that preserves value semantics such as signedness, precision, temporal meaning, encoding, and spatial reference without relying on a target database type name. It may be scalar, temporal, enum-like, spatial, JSON, or recursively structured; it is produced only when the source mapping can prove the represented semantics.
_Avoid_: Native Type, Rust type

**Normalized JSON Document**:
A JSON value profile that preserves array order, typed numeric exactness, and canonical object keys, without promising source whitespace, object key order, or duplicate-key preservation; a source JSON text type is not equivalent to it unless a separate explicit rule proves or accepts that loss.
_Avoid_: Raw JSON text, JSONB name

**Column Conversion Plan**:
The immutable decision for how one source column's schema and values can be represented by its mapped target column without runtime guesswork, instantiating one qualified Conversion Rule with the exact target representation, parameters, policy, and evidence for that binding. It is selected at Replication Route activation or requalification and reused for every applicable row until its inputs change.
_Avoid_: Value coercion, per-row type inference

**Invalid Temporal**:
A lossless representation of a source temporal value, such as a MySQL zero date, that cannot be represented as a valid standard date or time.
_Avoid_: NULL date, corrected date

**Column Datum**:
A column value together with an explicit presence state that distinguishes a value, SQL NULL, unavailable data, unchanged data, and redacted data; column definitions and target conversion decisions are not part of the datum.
_Avoid_: Field value, nullable value

**Row Locator**:
The source-derived column values and strategy used by a Sink to identify the row affected by an update or delete, including full-before-image matching when no usable key exists.
_Avoid_: Primary key, WHERE clause

**Unlocatable Row Change**:
A keyless update or delete whose exact before row cannot be distinguished safely under the target database's comparison semantics.
_Avoid_: Missing row, duplicate row

**Blocked Route**:
A Replication Route that has stopped before an unapplied transaction because continuing would violate its configuration or consistency guarantees, while its Capture Stream continues publishing changes.
_Avoid_: Failed stream, paused capture

**Degraded Route**:
A Replication Route whose operator explicitly accepted an incomplete baseline or advancement past an unapplied change, so it carries no end-to-end consistency guarantee until reconciled or rebuilt.
_Avoid_: Successful route, ignored warning

**Future-only Activation**:
Adding a table at a transaction-safe Source Cursor and guaranteeing only changes after that cursor, without claiming that earlier table contents were copied.
_Avoid_: Initial load, incremental snapshot

**Transaction Batch**:
An ordered group of Change Events that preserves one source transaction boundary.
_Avoid_: Event batch, message batch

**Transaction Spool**:
A bounded local append-only buffer used to hold a transaction that is too large for memory before it is atomically published or applied.
_Avoid_: Event store, Checkpoint

**Transport Envelope**:
The configured size and duration limits within which one Change Event and one Transaction Batch can be published atomically without truncation or fragmentation.
_Avoid_: Row size, Kafka retention

**Recovery Window**:
The bounded interval in which the exact records required by a Capture Stream or Replication Route remain readable from that stage's authoritative durable source; source logs and Kafka are not interchangeable authorities.
_Avoid_: Guaranteed retention, recovery time objective

**Recovery Window Status**:
The evidence-based state `OPEN`, `AT_RISK`, or `EXPIRED` describing whether every record needed for exact resume is still available.
_Avoid_: Retryability, failure severity

**Source Logging Contract**:
The operational prerequisite that every committed in-scope source change is written to the retained native change log consumed by Capture.
_Avoid_: Connector permission, end-to-end guarantee

**Source Logging Attestation**:
An immutable audited operator assertion that the writers covered by one Source Incarnation, Capture Scope revision, and writer-policy digest satisfy its Source Logging Contract, without claiming database-enforced proof.
_Avoid_: Connector verification, guarantee certificate

**Source Cursor**:
An opaque, source-specific coordinate that identifies a resumable boundary in a source change stream.
_Avoid_: Source position, binlog position

**Source Incarnation**:
An opaque identity for one continuous history of a Source that changes when logs are reset, the database is restored to a divergent history, or the physical source is replaced.
_Avoid_: Server UUID, source version

**Source Environment Epoch**:
A versioned boundary within one continuous Source history at which its verified server build or interpretation environment changes.
_Avoid_: Source Incarnation, process restart

**Source Environment Epoch Definition**:
An immutable description of the verified source build, semantic environment, and connector identity that governs one Source Environment Epoch.
_Avoid_: Change Event, current server settings

**Capture Stream Generation**:
One immutable continuity and event-contract lineage of a Capture Stream whose published history is never retracted or silently reinterpreted.
_Avoid_: Worker restart, Source Environment Epoch

**Generation Baseline**:
The immutable source-side boundary from which one Capture Stream Generation begins, identified by an exact Source Cursor, Schema Anchor, and source environment.
_Avoid_: Target snapshot, Route Baseline

**Generation Activation**:
The first immutable control transaction that binds a Capture Stream Generation to its Generation Baseline before ordinary source changes become visible.
_Avoid_: Worker start, process readiness

**Generation Topic Binding**:
The durable association between one generation-owned transport resource and its Deployment, Capture Stream Generation, resource role, and event-contract major.
_Avoid_: Topic name, consumer subscription

**Route Baseline**:
The immutable assertion describing how one Replication Route's target state relates to its bound Generation Baseline, either exact through an external baseline or explicitly future-only.
_Avoid_: Generation Baseline, Route Start Point

**External Baseline Attestation**:
An audited operator assertion, supported by an evidence digest, that one Route's target data is equivalent to its Source at an exact baseline boundary.
_Avoid_: Tool-verified equality, Generation Baseline

**Target Environment Epoch**:
A versioned boundary in Replication Metadata at which a Sink's verified server build or execution environment changes.
_Avoid_: Route Fence, connection generation

**Sink Session Profile**:
One complete, prequalified set of target-session semantics selected as a whole for a Sink Transaction Plan.
_Avoid_: Server defaults, connection pool settings

**History Epoch**:
A tool-maintained component of Source Incarnation that changes when source-log continuity can no longer be proven.
_Avoid_: Process generation, restart count

**Object Reference**:
A required Object Lineage ID plus the source-faithful qualified name, optional native evidence, and Schema Fingerprint for one object state; target-side name mapping is not part of the reference.
_Avoid_: Target table, mapped name

**Checkpoint**:
A durable record that a consumer has successfully processed through a particular Source Cursor.
_Avoid_: Offset, saved position

**Route Start Point**:
An explicitly validated transaction boundary in a Capture Stream from which a new Replication Route begins applying changes.
_Avoid_: Default offset, arbitrary Kafka offset

**Capture Checkpoint**:
A Kafka-resident Checkpoint atomically published with a committed source transaction and used by a Capture Stream to resume source-log reading.
_Avoid_: Route Checkpoint, consumer offset

**Replication Metadata**:
Sink-owned records that identify the last committed Source Cursor, source transaction, and Kafka resume coordinate for each Replication Route, together with any blocking state.
_Avoid_: Offset table, internal state

**Route Fence**:
A monotonically advancing token stored with Replication Metadata that prevents an obsolete Sink writer from committing changes for a Replication Route.
_Avoid_: Kafka ownership, process lock

**Sink Epoch Fence**:
A Sink-wide monotonically advancing token that prevents any Route writer qualified under an obsolete Target Environment Epoch from committing.
_Avoid_: Route Fence, pause notification

**Requalification Attempt**:
A leased, fenced effort by one Worker to verify and activate a candidate Target Environment Epoch for a Sink.
_Avoid_: Worker session, retry counter

**Target Object Owner**:
The sole Replication Route authorized to apply changes to a particular physical target object in phase one.
_Avoid_: Database owner, source table owner

**Sink Apply Transaction**:
One target-database transaction that applies an entire source Transaction Batch and advances the corresponding Replication Metadata together.
_Avoid_: Kafka transaction, per-event commit

**Sink Transaction Plan**:
An immutable Sink-specific execution plan for one source Transaction Batch, including its single qualified Sink Session Profile and recovery evidence.
_Avoid_: Target Schema Plan, dynamic statement cache

**Synthetic Transaction**:
A Transaction Batch constructed around a committed DDL or stream-control boundary that is not exposed as an ordinary source DML transaction.
_Avoid_: Multi-statement transaction, target transaction

**Exactly-once Effect**:
The guarantee that replaying an already committed source transaction does not change the Sink a second time, even though transport delivery may occur more than once.
_Avoid_: Exactly-once delivery, real-time strong consistency

## ChangeEvent contract decisions

The first portable ChangeEvent contract is transaction-oriented. A published Transaction Batch contains one source transaction's ordered row changes and preserves its begin and commit boundaries. A single row change is not an independent commit unit.

The contract uses a structured Logical Type rather than asking a Sink to infer semantics from Native Type. The Logical Type family includes scalar, temporal, enum-like, spatial, JSON, and recursively structured types; it preserves value semantics and is produced only by a versioned Source Type Mapping with sufficient evidence. Native Type remains source-definition evidence, not an embedded target or event type.

The first contract is definition-referenced for DML replay. A Change Event carries source object and element identity, a Definition Reference to the fingerprinted source definition, column values, and their explicit presence states; it does not embed the full Logical Type, Native Type, or source column definition.

Source Type Mapping is strict and versioned. MySQL and PostgreSQL native types map to Logical Types only when the exact connector/build, declaration, and source semantics prove the mapping; aliases such as MySQL `TINYINT(1)` are not treated as Boolean without unambiguous evidence. Integer width and signedness, Decimal precision and signed scale, text/binary bounds, temporal meaning, JSON profile, enum members, and spatial or recursive structure are preserved. PostgreSQL `json` is not mapped to the normalized JSON profile, and an unproven native type remains unsupported rather than being inferred from row values or downgraded.

Target representation is Sink-owned and selected through a Column Conversion Plan for one existing source-to-target column binding. A qualified mapping is `EXACT` when source and target semantics are equivalent, `RANGE_CHECKED` when a proven wider representation requires declared-domain checks, `EXPLICIT_CONVERSION` when an accepted encoding or semantic downgrade is required, and `UNSUPPORTED` when no qualified representation exists. ChangeEvent never selects a target type, and runtime values never select or widen a plan.

Column Datum preserves four presence states: Value, Null, Unchanged, and Unavailable. Unchanged is valid for a source update image; Unavailable means the Source did not provide the historical value. A Sink must report Target Capability Failure when it cannot apply a state without changing semantics.

The JSON encoding is a versioned replay format. `cdc.change-event-json.v0.3` is the next contract generation and must be owned and deserializable; readers remain compatible with v0.1 and v0.2 where the source adapter can establish the missing contract fields. ChangeEvent does not receive or parse database SQL.

The core model treats Source Cursor and source identity as opaque values. MySQL and PostgreSQL connectors validate their own cursor syntax and native event rules at the source adapter boundary. The first contract remains DML-only, requires a usable primary-key Row Locator, and relies on fingerprinted source definitions without designing DDL event production or automatic target schema changes in this scope.

## Validation boundary decisions

The change_event core performs only vendor-neutral validation. It validates transaction shape, operation and image relationships, identifiers, LogicalType and LogicalValue consistency, Row Locator requirements, presence-state rules, and the non-empty shape of an opaque Source Cursor. It accepts any non-empty source kind and does not maintain a list of supported database vendors.

Each database version connector owns its Source Contract validation. That validation covers server-version support, source identity syntax, native cursor syntax and ordering, transaction rules imposed by the source protocol, native-type to LogicalType correspondence, and source-specific image rules. A connector first calls the core validator, then applies its own validator to the resulting ValidatedTransaction.

Snapshot validation follows the same boundary. The core validates table identity, batch boundaries, complete values, and the phase-one primary-key requirement. A database connector validates how its Snapshot Boundary relates to its native change log and recovery rules. SnapshotBoundary uses an opaque Source Cursor and does not expose MySQL file, position, or GTID fields in the common model; those details are encoded and checked by the source connector.

Validation failures are classified by responsibility. ChangeEventValidationError means the vendor-neutral contract is invalid, SourceContractError means the source connector's native rules are invalid, and Target Capability Failure means a Sink cannot represent or execute an otherwise valid Change Event equivalently. This separation is part of the API contract and replaces vendor-specific dispatch in the core validator.

## Adapter and target capability decisions

The change_event crate owns the database-neutral SourceAdapter and SinkAdapter boundaries. Database version crates implement those boundaries and may depend on their native drivers; the core crate never depends on a database driver or a concrete version crate.

A Sink processes a Change Event in explicit stages: Capability Qualification, Target Apply Plan construction, parameterized SQL generation, and target transaction execution. A target connector's Capability Manifest is version-specific and determines whether Logical Types, presence states, Row Locators, transaction semantics, and target-session requirements can preserve the source semantics. Source kind is not a reason for a target renderer to reject an otherwise valid event.

Rendered SQL is parameterized. A target plan contains SQL templates and typed or lossless parameter values; a human-readable SQL script is diagnostic output only and is not the execution contract. This keeps binary, text encoding, Decimal, JSON, and identifier handling in the target driver boundary.

The Sink Adapter owns the atomic apply boundary. One target transaction applies the business changes and advances the route's replication checkpoint/control record together, then returns Applied, CommitUnknown, or Failed. Web orchestration selects and invokes the adapter but does not split data commit from checkpoint commit.

Source and Sink connectors are selected through independent registries keyed by connector identity and target version. Adding a source or Sink connector must not require source-target pair branches in Web or in existing database crates.

Adding a database version follows role-specific extension contracts. A Source version supplies capture, Source Contract validation, Source Type Mapping, and source definitions; a Sink version supplies target catalog access, Capability Manifest qualifications, Conversion Rules, Column Conversion Plan construction and execution, and its target session requirements. Either role may be added independently. A version that introduces semantics outside the existing Logical Type family requires a separate common-contract decision rather than a pair-specific or opaque fallback.

## Verification and Web registry decisions

The local verification matrix covers every combination of the four currently supported connectors: MySQL 5.7, MySQL 8.0, MySQL 8.4, and PostgreSQL 15. This is a 4-by-4 matrix for ChangeEvent-to-Sink qualification and rendering. Live database tests cover the deployed and operationally important combinations rather than requiring every local combination to run against a live server.

The standard test fixture is a canonical ChangeEvent fixture paired with the referenced source definitions. Each Sink renderer consumes that fixture independently, while source tests separately verify that native MySQL and PostgreSQL capture produces the same semantic model. JSON serialization and replay are tested as part of the canonical fixture path.

Tests must assert Target Capability Failure for semantics outside a Sink's qualified capability. An unsupported value must not be silently coerced, partially rendered, or applied to the target.

Web uses independent SourceRegistry and SinkRegistry lookups keyed by Connector Identity, including database kind and version. The Web task worker and UI read connector metadata and capabilities from those registries instead of maintaining source-target pair branches. PostgreSQL 15 is included as a Web Source connector, with live validation first covering PostgreSQL 15 to the three MySQL Sink versions.
