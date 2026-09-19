# Table Definition contract v1

This document defines the database-independent, immutable schema records used by `cdc.v1`. It describes source meaning; target names, generated target SQL, capability decisions, observations, and mutable runtime state do not belong in these definitions.

## Common encoded primitives

`EncodedIdentifier` contains required `bytes`, required `encoding`, and optional `display`. Identity uses the exact byte sequence plus encoding. Producers and consumers must not case-fold, Unicode-normalize, trim, transliterate, or replace invalid data. `display` is diagnostic and excluded from canonical fingerprints.

`QualifiedName` contains explicitly present or absent `catalog` and `schema` Encoded Identifiers plus a required `object` identifier. Absence is different from a present empty value. Element names are also Encoded Identifiers.

`EncodedText` contains required source bytes and encoding plus optional display text. It is used for comments and other definition text whose bytes carry source meaning. If text cannot be recovered losslessly, the producer records an Unsupported Feature and does not invent replacement text.

## TableDefinition

`TableDefinition` has:

- `contract_major`, fixed at `1`;
- `object_lineage_id` and source-faithful `qualified_name`;
- a Definition Reference to the applicable Schema Context Definition;
- `table_kind`, normalized `engine_semantics`, and exact `native_engine`;
- ordered `columns`, Semantic Keys, Access Indexes, foreign keys, and CHECK constraints;
- normalized semantic table options and optional encoded comment;
- controlled namespaced extensions that are never required for generic correctness.

The enclosing Schema History record carries the Schema Fingerprint; the definition does not self-authorize a digest. A publishable definition contains no Unsupported Feature: if a selected source semantic cannot be represented completely by a normalized field or declared opaque carrier, Capture records diagnostics and blocks before publishing Schema History. Statistics, cardinality, current `AUTO_INCREMENT` counter, creation/update timestamps, files, paths, tablespaces, and observed physical state are excluded.

## ColumnDefinition

Each ordered column has:

- Element Lineage ID, ordinal, and Encoded Identifier;
- Logical Type and exact Native Type;
- nullability;
- `DefaultSpec`: `ABSENT`, `NULL`, `LITERAL(LogicalValue)`, or `EXPRESSION(Expression)`;
- generation: `NONE`, `VIRTUAL(Expression)`, or `STORED(Expression)`;
- identity semantics, including whether explicit values are allowed;
- optional `ON_UPDATE` expression;
- optional charset and full collation identity;
- visibility and generation origin `USER` or `SYSTEM`;
- optional Encoded Text comment;
- controlled extensions that are never required for generic correctness.

`DefaultSpec` records the Effective Definition rather than whether the user typed a `DEFAULT` clause. Optional declaration origin `EXPLICIT`, `SERVER_IMPLICIT`, or `UNKNOWN` is audit-only and is excluded from the Schema Fingerprint. `ABSENT` means that the effective column has no default; it remains distinct from an effective SQL `NULL` default. Source-specific insertion behavior that is not determined by `DefaultSpec`, such as legacy MySQL `TIMESTAMP` null assignment, is an explicit semantic field.

MySQL `AUTO_INCREMENT` is an explicit-value-allowed identity. A MySQL generated invisible primary-key column is retained as a `SYSTEM` column with its Identity, Primary Semantic Key, and backing Access Index. Neither may be inferred merely from a name or hidden from the definition.

## Expressions

`Expression` is either `NormalizedExpression` or `OpaqueNativeExpression`.

A Normalized Expression is a typed AST consisting only of contract-defined literals, Element Lineage column references, unary and binary operators, comparisons, boolean composition, casts, case expressions, normalized functions, and current-time forms. Operator/function identity, operand order, result Logical Type, nullability, collation and coercibility, error behavior, numeric conversion and rounding, and temporal precision and time-zone semantics must be explicit. SQL source text is never the normalized form. Producers preserve evaluation order and do not constant-fold, reorder commutative operands, simplify algebraically, or introduce implicit casts. Parentheses and syntax aliases disappear only where semantic identity is proven.

Portability is role-qualified. A capability entry identifies expression role, normalized operation, operand and result types, and exact source and target versions. Generated columns and CHECK constraints accept only qualified deterministic expressions. Default expressions also retain column-dependency ordering; nondeterministic forms are opaque except for dedicated current-time semantics. The phase-one candidate allowlist is deliberately small and expands only through cross-version golden and execution tests.

`CurrentTimeSpec` and `AutoUpdateSpec` represent current-time defaults and updates separately from general function calls. They carry temporal kind, fractional precision, time-zone behavior, evaluation semantics, trigger conditions, and explicit-assignment overrides. Source aliases normalize only when equivalent.

An Opaque Native Expression carries source database kind, exact source version, raw bytes and encoding, digest, and portability `SOURCE_DIALECT_ONLY`. It is a complete core representation, not an extension or Unsupported Feature. It may be used only by the separately validated same-release-series raw-statement path; a Route without that qualified path reports a Target Capability Failure.

## Semantic keys and access indexes

`KeyDefinition` represents `PRIMARY` or `UNIQUE` relational semantics. It has its own Element Lineage ID and optional Encoded Identifier, ordered Key Parts, and an optional backing Access Index lineage.

`IndexDefinition` represents an access structure. It has a distinct Element Lineage ID, Encoded Identifier, method, visibility, optional comment, and ordered Key Parts. Cardinality, selectivity, statistics, and optimizer observations are excluded.

A column Key Part references an Element Lineage ID and carries optional prefix length with explicit unit `CHARACTERS` or `BYTES`, plus effective direction `ASC` or `DESC`. Expression parts and specialized indexes are Unsupported Features in phase one.

## Foreign keys and CHECK constraints

A foreign key contains its Element Lineage ID, Encoded Identifier, ordered child column lineages, referenced Object Lineage ID, ordered parent column lineages, match behavior, update and delete actions, enforcement, and optional backing-index lineage. If the parent is outside known captured lineage, the reference is instead an `ExternalObjectReference` containing its source-faithful Qualified Name and no fabricated lineage. A dependency required for exact application must be resolvable before planning can continue.

A CHECK constraint contains its Element Lineage ID, Encoded Identifier, Normalized Expression, referenced column lineages, and enforcement state. A clause ignored by the exact source release is not a constraint and belongs only in Native Statement Evidence.

## SchemaContextDefinition

`SchemaContextDefinition` has contract major `1`, Object Lineage ID, qualified schema name, default charset, full default-collation identity and semantics, and controlled extensions that are never required for generic correctness. MySQL `lower_case_table_names` is recorded as source identifier policy rather than used to rewrite identifiers. Session `sql_mode` belongs to DDL Parse Context, not the definition. Default encryption or another unmodeled correctness/security behavior is an Unsupported Feature that blocks publication rather than becoming a nonsemantic option or incomplete definition field.

## Canonical encoding and fingerprint boundary

The definition encoding reuses the Change Event digest codec's typed, length-prefixed scalar rules under a distinct domain tag and contract major. Core numeric field tags are emitted in canonical order. Optional presence and union variants are explicit; semantic defaults are never omitted. Ordered collections retain order, unordered collections sort by canonical bytes, and exact IEEE-754 bits are preserved. Duplicate fields, unknown core fields, and unknown enum values invalidate v1; a new correctness-bearing field requires a new contract major.

The canonical bytes include every lineage, encoded identifier byte sequence and encoding, encoded comment, explicit presence state, ordered semantic element, and expression. Optional display strings, declaration provenance, Native Statement Evidence, connector observations, statistics, mutable Table Runtime State, and all namespaced extensions are excluded because generic correctness cannot depend on them. Cross-language golden vectors lock every encoding rule.
