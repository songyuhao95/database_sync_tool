# MySQL DDL contract v1

This document defines the phase-one source-capture and Sink-apply boundary for DDL from the three qualified MySQL instances: 5.7, 8.0, and 8.4. Recognition by the source parser does not imply that every target version can apply the operation; a complete normalized event may be published while an incompatible Replication Route blocks.

Canonical definitions record MySQL's Effective Definition after server normalization, not the submitted clause spelling. Native Statement Evidence retains the submitted statement. A version adapter predicts the transition and a stabilized catalog observation verifies it; disagreement blocks capture.

## Object boundary

The executable scope contains permanent, nonpartitioned InnoDB base tables and database-level Schema Context needed to interpret their definitions. Temporary tables, views, triggers, routines, events, accounts, grants, tablespaces, spatial-reference-system administration, plugins, and other server objects are outside the replication model. A confidently identified excluded-object statement is audited and skipped. A captured table that becomes non-InnoDB or whose object kind becomes unsupported blocks the Capture Stream because subsequent row and transaction guarantees no longer satisfy the contract.

## CREATE TABLE

Supported forms are:

- an explicit table definition whose every correctness-bearing feature can be normalized;
- `CREATE TABLE ... LIKE` when the referenced base table's exact Table Definition is already present in Schema History; and
- either form with `IF NOT EXISTS`, producing a normal Schema Change or a No-op Schema Change according to the source result.

`LIKE` is evaluated with MySQL source semantics rather than implemented as a blind definition copy: omitted foreign keys and physical options, regenerated constraint names, and retained defaults or generated attributes are reflected in the resulting Table Definition. A reference without historical definition blocks capture. `CREATE TEMPORARY TABLE` and `CREATE TABLE ... SELECT/AS SELECT`, including `IGNORE` or `REPLACE`, are unsupported in phase one.

## ALTER TABLE columns

The column-operation syntax whitelist contains `ADD COLUMN`, `DROP COLUMN`, `MODIFY COLUMN`, `CHANGE COLUMN`, `RENAME COLUMN`, `ALTER COLUMN SET DEFAULT`, and `ALTER COLUMN DROP DEFAULT`, including `FIRST` and `AFTER` placement. A multi-clause statement retains clause order and remains one source operation.

Default presence is explicit in the Effective Definition: absent default, SQL `NULL`, literal, and expression are distinct states regardless of whether MySQL derived or the user wrote them. Optional declaration origin `EXPLICIT`, `SERVER_IMPLICIT`, or `UNKNOWN` is audit-only and excluded from fingerprints because existing metadata cannot always recover syntax provenance. Literal and NULL defaults are normalized as Logical Values. General expression defaults are available only on qualified 8.x instances that support them; a route to a target without equivalent semantics blocks.

Current-time defaults and `ON UPDATE` clauses use dedicated semantic forms carrying temporal kind, fractional precision, time-zone behavior, and automatic-update trigger and override rules. Synonyms such as `NOW()` normalize only when their semantics are identical. Legacy implicit `TIMESTAMP` behavior, including assignment of `NULL` as current time when applicable, is an explicit semantic rather than an assumption. Sink DML supplies captured final values for ordinary columns and does not depend on target defaults or `ON UPDATE` to reproduce a source row.

Generated columns and CHECK constraints permit only role-qualified deterministic Normalized Expressions. The initial portable qualification candidates are literals, Element Lineage column references, null tests, exact integral or decimal unary signs and addition/subtraction/multiplication, same-type exact numeric or binary-string comparisons, equal-result-type `CASE`, and proven lossless explicit casts. Division, floating arithmetic, implicit string/numeric conversion, general functions, JSON, spatial, regular-expression, bitwise, and temporal operations remain unqualified until exact nine-direction tests prove them. Capability is keyed by expression role, normalized operation, operand and result types, and exact source and target versions rather than function name alone.

Nondeterministic defaults such as `RAND()` or `UUID()` are Opaque Native Expressions; the specialized current-time forms are the only initial exception. An expression outside the qualified contract is a first-class blocking feature for generated cross-version DDL rather than an extension. Same-release-series raw DDL may carry it only through the already constrained validation path. A target that lacks an expression, generation, visibility, or evaluation semantic blocks rather than approximating it.

The first runnable DML slice nevertheless fixes the complete expression wire boundary. It implements literal and SQL `NULL` defaults and dedicated current-time forms first, represents other retained expressions opaquely, and enables deterministic AST families only after the applicable nine-direction tests pass. An opaque expression blocks only the affected Route when source row decoding remains provably safe; inability to establish the Effective Definition or decode later rows blocks the Capture Stream.

MySQL `AUTO_INCREMENT` is modeled as an identity whose generated value may still be supplied explicitly, while the current next counter is excluded as Table Runtime State. A generated invisible primary key is represented as a real `SYSTEM` column, Identity, Primary Semantic Key, and backing Access Index; the table is not classified as keyless. Source metadata sessions must expose it or preflight fails. Target DDL disables automatic GIPK creation for its session and creates only the explicit expected structure when the target can preserve visibility, identity, and system-origin semantics. A target that cannot do so blocks; the column is never made visible as a fallback.

Type, nullability, default, generation, charset, collation, and ordinal changes require an exact before-to-after Column Conversion Plan. Phase one does not execute `CONVERT TO CHARACTER SET`, `ALTER TABLE ... ORDER BY`, a narrowing or lossy conversion, or another operation whose existing-data transformation is not proven equivalent.

## Indexes and keys

Primary and unique Semantic Keys are modeled separately from Access Indexes, including when MySQL uses one index to back a key. Each has its own Element Lineage ID and a key may reference its backing index lineage. The whitelist contains ordinary column-based BTREE parts with ordered column lineage, optional prefix length and its `CHARACTERS` or `BYTES` unit, and effective `ASC` or `DESC` direction. Standalone `CREATE INDEX` and `DROP INDEX` normalize to the same table-definition transitions as their `ALTER TABLE` forms. Index name, method, visibility, and comment are correctness-bearing; cardinality and optimizer statistics are not. An older target that parses but ignores an attribute is not considered equivalent.

Function or expression indexes, FULLTEXT indexes, SPATIAL indexes, plugin parsers, HASH, and other specialized index forms are unsupported in phase one. They are not reduced to ordinary BTREE indexes.

## Foreign keys

An exact foreign key records its Element Lineage ID, encoded name, ordered child column lineages, referenced object lineage, ordered parent column lineages, match behavior, update and delete actions, enforcement, and optional backing-index lineage. It requires its child and parent objects to be owned by the same Route Projection, compatible mapped types and indexes, the same referential actions, and enabled target foreign-key checks. A parent outside Capture Scope is retained as an External Object Reference with a qualified source name and no invented lineage; exact replication requires that dependency to become resolvable. `CASCADE` and `SET NULL` may not be omitted because their data effects can depend on target-side constraint execution. If either cannot be preserved, the route blocks.

An operator may explicitly omit only `RESTRICT` or `NO ACTION`. The resulting expected target definition records the omission and the route becomes `DEGRADED`; omission is not implemented by globally or session-locally disabling target checks.

## CHECK constraints

A CHECK definition exists only when the exact source version stores it as a durable constraint. It carries its Element Lineage ID, encoded name, Normalized Expression, referenced column lineages, and `ENFORCED` or `NOT_ENFORCED` state. A route applies the constraint only if the target preserves the expression, enforcement behavior, identity needed by later DDL, and SQL-mode-dependent semantics. It never silently weakens or strengthens enforcement.

A CHECK clause parsed but ignored by MySQL 5.7 or a MySQL 8.0 release before 8.0.16 creates no constraint in Table Definition or Schema Fingerprint. It is retained as an ordered Ignored Source Clause in Native Statement Evidence, and generated cross-series target DDL does not turn it into a real constraint.

## Table options and partitioning

Semantic table options in phase one are InnoDB engine identity, default character set, default collation, and table comment. Setting a compatible default character set or collation is supported; converting existing columns with `CONVERT TO CHARACTER SET` is not.

The column-level `AUTO_INCREMENT` attribute belongs to Table Definition, but the next counter value is Table Runtime State and is excluded from Schema Fingerprints. Explicit `AUTO_INCREMENT = N` changes block in phase one. `TRUNCATE` resets the counter through a Table Reset rather than a schema option change.

Partitioned tables and all partition clauses or operations fail preflight or block before unsafe continuation. Physical and security options such as row format, compression, encryption, tablespace placement, data or index directories, statistics, and secondary-engine attributes are parsed as explicit unsupported features; they are never silently discarded or executed.

## Rename, drop, and reset

`RENAME TABLE`, table rename through `ALTER TABLE`, and `DROP TABLE` are supported when all affected objects and dependencies are resolvable. Multi-object source statements preserve their order and cannot be partly projected. A MySQL 5.7 Sink defaults to blocking multi-object DDL until that exact form has demonstrated deterministic crash recovery.

Rename preserves the Object Lineage ID; drop retires it, and a later same-name create receives a new identity. `TRUNCATE TABLE` preserves the logical Object and Element Lineage IDs even though MySQL implements it by dropping and recreating physical storage. It emits a Table Reset with equal before and after Schema Fingerprints, all-row removal semantics, and reset of applicable Table Runtime State. It is not expanded into deletes and is not a No-op Schema Change.

A prepared Table Reset is recovered by validating the expected schema and executing `TRUNCATE` again. The operation is effect-idempotent while the route is stopped at that event and the table has one Target Object Owner. Recovery verifies that the table is empty and that its definition remains correct before marking the event applied; an external write or definition mismatch blocks.

## Statement unit and target execution

One successful source DDL statement forms one `SOURCE_DDL` Synthetic Transaction regardless of its clause or object count. A Sink may use the validated original statement only under the existing same-release-series and no-mapping rules; otherwise it generates target SQL from the normalized operation. It never splits the source operation into independently committed target statements.

A Schema Change records `CHANGED` or `NO_CHANGE`, ordered Object Changes, and ordered Schema Mutations. Object Changes carry before and after Definition References, while complete definitions are transactionally published to the Schema Topic. Mutations preserve clause intent and classify effects on existing data; neither source event contains target names, generated target SQL, nor target-version capability decisions.

The target DDL must preserve the operation as one equivalent unit. MySQL 5.7 enables only single-object forms qualified by crash-injection tests. After an interrupted apply, a target state matching neither the expected before nor after definitions blocks for repair instead of being guessed or automatically overwritten.

Source `ALGORITHM` and `LOCK` clauses are evidence, not target instructions. The target planner recomputes capability under `REQUIRE_CONCURRENT_DML`, requiring target-supported `INSTANT` or `INPLACE` plus `LOCK=NONE` unless an audited route policy explicitly permits blocking DDL.

## DDL Parse Context

DDL is interpreted with the exact source server version and the Query Event's default database, connection character set, SQL mode, and other available correctness-bearing session state. Executable MySQL version comments are evaluated against the source version. Ordinary comments and hints do not alter the normalized model, although the bounded native statement may remain diagnostic evidence.

The connector never parses a historical event under its current SQL connection settings. If context needed to distinguish identifiers, string literals, escapes, defaults, or executable clauses is absent, the Capture Stream blocks.

Native Statement Evidence retains the exact bounded source bytes and character set, DDL Parse Context reference, source dialect and version, statement kind, raw digest, executable-comment flag, ordered Ignored Source Clauses, and normalized-result digest. It is never used as a substitute for normalized semantics, never truncated to fit transport, hidden from default diagnostics, and never emitted to ordinary logs.

## Target names

Target database and table names come from the Route Mapping, while phase-one column names remain source-faithful. Table-scoped ordinary index names are retained when legal. For every generated Target Schema Plan, schema-scoped foreign-key and CHECK names are deterministically derived from Route ID, Element Lineage ID, and element kind using a target-safe prefix and the full Base32-encoded digest when the identifier limit permits. Later operations recompute the same name from lineage; no growing mutable name map is required.

The same-release-series raw-statement path retains native names only after proving that all names are legal and collision-free in the target namespace. If that proof fails, the route must use a generated plan when supported or block.
