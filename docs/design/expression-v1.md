# Expression contract v1

This contract represents expressions that are durable schema semantics. It does not represent the SQL expressions or predicates that caused ordinary DML row changes; Row Changes contain only Materialized Row Values.

## Normalized form

`NormalizedExpression` is a typed AST with contract-defined variants for literals, Element Lineage column references, unary and binary operators, comparisons, boolean composition, casts, case expressions, normalized function calls, and dedicated current-time forms. Each node has an explicit result Logical Type and nullability. String semantics include charset, collation identity, repertoire or coercibility where relevant. Numeric and temporal nodes carry every conversion, rounding, overflow, error, precision, and time-zone rule needed to establish equivalence.

Canonicalization resolves column names to lineage and converts only proven syntax aliases to one semantic operation. It preserves operand and evaluation order. It never folds constants, sorts commutative operands, simplifies algebraically, or inserts an implicit cast, because those rewrites can change errors, floating results, collation behavior, or version-dependent coercion.

## Role-qualified capability

An expression is portable only when a capability entry matches its role, every normalized operation, operand and result types, and the exact source and target versions. Generated columns and CHECK constraints require deterministic entries. Default expressions additionally retain ordered column dependencies and reject unsupported forward references.

The initial portable candidates are:

- literals and Element Lineage column references;
- `IS NULL` and `IS NOT NULL`;
- unary sign and addition, subtraction, and multiplication over qualified exact integer or decimal types;
- comparisons over qualified same-type exact numeric or binary-string operands;
- `CASE` whose branches have one identical result type; and
- explicit casts proven lossless for the specific source-target pair.

Division, floating arithmetic, implicit string/numeric conversion, general functions, JSON, spatial, regular-expression, bitwise, and temporal operations are initially unqualified. This list expands only after golden normalization vectors and all applicable source-target execution tests pass.

## Delivery sequence

The expression union, opaque fallback, role metadata, and canonical encoding are part of the first `cdc.v1` contract so later support does not require a wire redesign. The first runnable DML slice implements literal and SQL `NULL` defaults plus dedicated current-time semantics. It can classify every other retained expression as opaque without evaluating it. The small deterministic AST subset is enabled operation family by operation family only after its normalization vectors and all applicable directions in the qualified-instance matrix pass.

An opaque schema expression does not by itself stop source capture when the connector can still decode the table's row events and preserve the complete effective definition. It does prevent any Replication Route from crossing the corresponding schema boundary unless that route has the separately validated same-release-series raw-DDL path. If the expression prevents reliable source definition or row interpretation, the Capture Stream blocks instead.

## Current time and automatic update

Current-time defaults use `CurrentTimeSpec`, not a general function name. The form records temporal kind, fractional precision, time-zone behavior, and evaluation semantics. `AutoUpdateSpec` separately records when an ordinary column is updated automatically and how explicit assignment suppresses or requests that update. Equivalent source aliases may normalize to the same form.

Legacy source behavior such as assigning `NULL` to mean current time is explicit schema semantics. It is never inferred from the target's current server settings. Sink row application supplies the source's captured final value for ordinary columns, while target DDL reproduces these expressions only after capability validation.

## Opaque native form

`OpaqueNativeExpression` contains source kind, exact source version, raw encoded bytes, digest, and portability `SOURCE_DIALECT_ONLY`. Nondeterministic defaults such as random or UUID generation are opaque in the initial contract; dedicated current-time forms are the only exception. Opaque expressions are visible correctness-bearing features, not extensions. They block generated cross-version or heterogeneous plans and can execute only through the constrained same-release-series raw-DDL path.

The canonical opaque bytes describe the Source's effective catalog expression, captured under a fixed metadata character-set and quoting session and tagged with representation kind `SOURCE_CATALOG_EXPRESSION`. Initial anchors and live DDL therefore produce the same representation. A substring from the submitted DDL is Native Statement Evidence only and never becomes the opaque semantic value or its Schema Fingerprint input. If the connector cannot recover one stable, lossless effective representation, it records a Capture Block Reason instead of publishing a guessed definition.

## Canonical encoding

The AST uses explicit variant tags and field presence under the definition canonical codec. Node and operand order is preserved. All semantic annotations participate in the containing definition's Schema Fingerprint; source spelling and diagnostic display text do not.
