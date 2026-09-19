# MySQL value semantics

Phase-one MySQL routes preserve stored database values and schema semantics across the three qualified 5.7, 8.0, and 8.4 instances. A statement being accepted by the target is not evidence that its result is equivalent.

## Schema and names

Table Definitions retain exact source identifier case, native `lower_case_table_names` behavior, character set, collation identity, Unicode/collation version, and padding behavior. Planning resolves names under target normalization and rejects collisions instead of lowercasing automatically. A target charset or collation must be semantically equivalent; there is no phase-one lossy fallback such as mapping `utf8mb4_0900_ai_ci` to a similarly named 5.7 collation. The positive nine-direction POC matrix uses explicitly compatible collations and includes negative tests for incompatible 8.x-to-5.7 definitions.

Every mapped column has an immutable activation-time Column Conversion Plan: `EXACT`, `RANGE_CHECKED`, or `UNSUPPORTED`. Range-checked values are validated before SQL execution; an unsupported column prevents activation. Per-row inference cannot override the plan.

## Sink session

Each Sink Transaction Plan selects one immutable, content-addressed Sink Session Profile covering UTC time zone, `READ COMMITTED`, enabled foreign-key and unique checks, explicit connection charset, strict default SQL mode, and every other qualified execution semantic required by the complete source transaction. Every connection checkout establishes and reads back the complete profile before beginning the apply transaction; a connection whose state cannot be restored or verified is discarded from the pool. Replication Metadata binds the profile digest. Server-wide settings outside worker control belong to the target Semantic Environment Fingerprint rather than the session profile. Sink sessions never use `INSERT IGNORE`, `UPDATE IGNORE`, `REPLACE`, or `sql_log_bin=0`. INSERT and UPDATE bind the complete writable after image explicitly so target defaults and automatic-update expressions do not regenerate source values; generated columns are omitted and target CHECK constraints remain enabled.

When the native row bitmap supplies a generated-column result, the Change Event retains it as a Generated Observation. The Sink compares that observation with the target-computed value immediately after applying its Row Change, on the same connection and inside the same Sink Apply Transaction, before a later Row Change can replace that intermediate state. A mismatch rolls back and becomes a deterministic Target Capability Failure. When no generated result is logged, the route does not query the Source; it may proceed only from a qualified equivalent expression and the verified target Schema Fingerprint.

Generated-result equality never uses target SQL equality or display strings. The Column Conversion Plan first projects the source observation to one expected Target Logical Value, then the target adapter decodes the selected result to that same representation. Integers, decimals and scales, nulls, exact floating bits, encoded text, typed JSON, spatial type and SRID, temporals, and every other Logical Value follow their existing canonical equality. Any target normalization not declared by the conversion plan is a mismatch.

Every available Generated Observation is checked; runtime sampling cannot satisfy exact application. One immediate read may validate all generated columns supplied by that Row Change. Cross-row batching is permitted only when dependency analysis proves that candidate row sets are disjoint and that no intervening or later event can mutate any candidate before validation; otherwise the Sink performs the immediate single-row read. A type-aware complete ordinary after-image predicate may validate an indistinguishable duplicate because equal deterministic inputs must produce the same generated result. An ambiguous set with differing generated results, a missing row, or an unreadable result rolls back the complete source transaction.

Database deadlocks, connection loss, and lock-wait timeouts during DML or validation are transient attempts: the Sink rolls back and retries the complete Transaction Batch, including all immediate validations. A reproducible generated-value mismatch, missing row, or irreducible locator ambiguity is deterministic and blocks before the transaction. Exhausting the transient retry budget produces a retryable Blocked Route state without converting the incident into a Target Capability Failure. No attempt can advance Replication Metadata after a partial apply.

An expression-generated key is eligible for row location only when all of its generated parts are observed and qualified. A GIPK is instead a system-origin ordinary `AUTO_INCREMENT` value and follows normal key-location rules.

## Invalid temporal values

Invalid Temporal values follow `PRESERVE_OR_BLOCK`. A dedicated connection may use only the qualified narrowly relaxed Sink Session Profile needed for the target to store the exact native value. The Sink permits only specifically predicted warnings and verifies the native representation after write. The connection is destroyed after the operation and never returned to the ordinary pool. The Sink never converts an invalid value to NULL, a minimum date, or the current time.

## JSON

MySQL's stored JSON value, rather than the user's original textual spelling, is authoritative. JSON numbers preserve whether the source representation is signed integer, unsigned integer, decimal, or approximate double, with exact integer/decimal magnitude and double bits. Object key order and discarded source whitespace are not claimed as recoverable; arrays remain ordered. Source-specific opaque JSON scalars receive typed representations and block if the target API cannot reconstruct them. Partial JSON diffs are applied in order to the full before value by the Source Connector before a complete after value is emitted.

## ENUM, SET, and spatial values

ENUM carries its label and native ordinal; SET carries ordered labels and the native bitmap. Their target definitions must preserve member order. Normal values are written by label and verified against native numeric state; historical error values may use a narrowly relaxed dedicated session only when expected warnings and readback both match. `IGNORE` is never used.

Spatial values travel as standard WKB plus SRID, geometry kind, and dimension. The version-specific Sink uses parameterized geometry constructors and compares WKB and SRID after write. Missing target SRS definitions block without creating target objects. A source column-level SRID restriction that the target cannot express is schema-incompatible even if individual geometries could be inserted.

## Floating point

FLOAT and DOUBLE values retain IEEE-754 bits. A target write must reproduce those bits, including negative zero, subnormal values, and any source-accepted non-finite or NaN payload; target rejection or normalization blocks instead of coercing. The same exactness applies when validating a keyless before image.

## Keyless row location

The Sink builds type-specific, binary-safe candidate predicates and locks candidates before mutation. It then decodes and compares complete before images in Rust. Multiple candidates are safe only when they are exact indistinguishable duplicates, in which case one source RowChange mutates one row. SQL-equivalent but byte-distinct candidates that cannot be distinguished form an Unlocatable Row Change and block. Candidate work is bounded by configurable resource limits; exhausting a bound blocks with remediation rather than broadening the predicate.
