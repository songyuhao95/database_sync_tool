# DML type and version qualification

Issue #59 establishes `scripts/type-inventory.json` as the connector and
native-type roster. Rust qualification tests and both PowerShell test runners
read that file; suite configuration contains test registrations, not another
copy of the connector list. `qualification-matrix.json` qualifies the six
versioned Source and Sink fixtures. Their Cartesian product is generated on
every run as 36 offline directions. PostgreSQL 16/17 use their own
SourceTypeMapping and Sink manifests; neither role falls back to PostgreSQL
15.

Live qualification is deliberately not a 36-route database-to-database
matrix. It reports six independent Source → ChangeEvent components, six
ChangeEvent → Sink components, one common transaction-recovery qualification,
and live target-capability invalidation evidence. The live suite registers
Source and Sink adapter tests for MySQL 5.7/8.0/8.4 and PostgreSQL 15/16/17.
PostgreSQL Source suites capture and replay from an actual server of the named
major version. PostgreSQL Sink suites apply every one of the six canonical
source fixtures to an actual server of that version, then verify DML and
atomic checkpoint behavior across restart. A separate MySQL 5.7 → PostgreSQL
15 Web test changes a temporary target table after saving its plan, verifies
requalification marks the plan stale, and verifies task start is blocked.
Representative end-to-end route smoke tests are reported separately.

Run from the repository root:

```powershell
./scripts/qualify.ps1
./scripts/qualify.ps1 -Live -ConfigFile ./scripts/test.txt
./scripts/qualify.ps1 -BaselineFile ./previous-summary.json
```

The default does not read credentials or connect to databases. `-Live` opts in
to the DML capture/apply/checkpoint, capability-invalidation, and route-smoke
tests, using process environment variables or an explicitly supplied
`KEY=VALUE` configuration. Missing configuration produces `REQUIRES_LIVE`. A
configured test that fails, or a filter executing zero tests, fails the run.
Live tests prepare and alter only their own isolated test objects; no
production schema-evolution feature is introduced.

PostgreSQL 15 uses `PG_CDC_*`; PostgreSQL 16 and 17 use independent
`PG_CDC16_*` and `PG_CDC17_*` values (`HOST`, `PORT`, `ADMIN_USER`,
`READER_USER` for Source, `WRITER_USER`, and `TEST_PASSWORD`). The newer major
versions never borrow PostgreSQL 15 connection settings, and server-version
assertions reject a misrouted endpoint. Without those settings the live
component status remains `REQUIRES_LIVE`.

The console contains only an ordered final matrix, missing-direction count,
and report location. Cargo output stays in per-suite logs. `summary.json`
contains the complete offline matrix, per-case qualification and status,
stable reason codes, plan/manifest digests, recovery evidence scope,
missing/new directions, a stable summary digest, a password scan result, and
the four separate live report sections. Each direction separates Native
Equivalent, Value Preserved, Explicit Conversion, `UNSUPPORTED`, `BLOCKED`,
`REQUIRES_LIVE`, and `FAIL` evidence.
`live-source.json`, `live-sink.json`, `transaction-recovery.json`,
`live-capability-invalidation.json`, and `route-smoke.json` retain the
structured component reports. `types.json` and `recovery.json` retain the
underlying offline evidence. Reports contain no credentials or fixture row
values. Output goes to a unique directory under
`target/qualification`; use `-OutputDirectory` to choose another location.

## Reading results

`offline=PASS` means executed assertions passed, including negative cases. It
does **not** mean every type can replicate. Each field case separately reports
`EXACT`, `RANGE_CHECKED`, `EXPLICIT_CONVERSION`, or `UNSUPPORTED/BLOCKED`, together
with the actual planner status (`COMPATIBLE`, `NEEDS_CONFIGURATION`, etc.).
Only a selectable plan is tested as an accepted runtime conversion. A blocked
source mapping stops before target planning. Unsupported versions receive no
synthetic mapping, plan, execution, or recovery result.

In `summary.json`, `success` and `offline_success` describe the registered
qualification suites and direction matrix. They can be true while
`type_inventory_complete` is false; only the latter, backed by
`type_inventory.status == PASS`, means this all-types gate has no remaining
type-level gaps.

The same canonical transaction exercises each implemented Source Contract,
JSON replay, and Sink parameterized INSERT/UPDATE/DELETE. It includes
composite keys, a key change, NULL, PostgreSQL Unchanged/Unavailable, exact
numeric, text, binary, temporal and JSON values. Field fixtures independently
cover all represented LogicalType families, declared parameters, range checks,
field confirmation, rejected keys/generated/nullability mismatches, and
unsupported spatial/recursive declarations. Existing #28–#36 boundary and
Web regressions are executed and reported as shared regression suites.

Recovery tests use the **production Web recovery loop** with a fake transactional
executor and durable metadata store. Every implemented direction checks a
persisted, confirmed RANGE_CHECKED plan, real conversion failure, same-table
and cross-table transactions, atomic DML/checkpoint publication, rollback,
constraint/SQL/metadata failure, connection/lock retries, the three
CommitUnknown resolutions, stop, duplicate delivery, and a reopened writer.
Fake executor results cannot qualify a driver's real commit path.

Live component PASS is scoped to the named adapter test. An implemented
direction is `live=PASS` only when its registered Source component and Sink
component both pass; that status is evidence composition, not execution of
that database-to-database pair. Missing component evidence is
`REQUIRES_LIVE`, a failed component is `FAIL`. Offline PASS is never promoted
to live PASS.

`live_qualified=true` requires all six Source components, all six Sink
components, the common transaction-recovery report, and capability
invalidation evidence to pass. Route smoke is reported in
`route_smoke_qualified` and is required for a successful `-Live` run, but it
is not counted as an additional source/sink direction.
The report therefore does not claim that all 36 database-to-database links
were run on real servers. Float NaN, positive and negative infinity, signed
zero, and MySQL zero-date capture are recorded as offline edge fixtures; the
zero-date fixture is capture evidence and marks date writes as blocked.

`types.json` and `summary.json` also include `type_inventory`. Rust probes each
listed native declaration with its versioned SourceTypeMapping and records
rejections as `MISSING_IMPLEMENTATION`. Protocol capture, semantic codec,
ChangeEvent value evidence, representation capture, Sink target/carrier,
Web candidate, and per-type live evidence remain separate. A logical-family
fixture is marked `FAMILY_FIXTURE_ONLY`; a type without a fixture is
`MISSING_TEST`. Dynamic PostgreSQL catalog types and unmatched MySQL
declarations remain `REQUIRES_LIVE_CATALOG_ENUMERATION` and unmatched types
are `MISSING_IMPLEMENTATION` until catalog identity and protocol framing are
qualified. `offline=PASS` describes the registered tests; the independent
`type_inventory.status` reports whether the all-types goal is complete.

`type_inventory.status` is `MISSING_IMPLEMENTATION` when any listed declaration
or dynamic catalog class has an implementation gap, `REQUIRES_PER_TYPE_QUALIFICATION`
when code exists but exact-type evidence is incomplete, and `PASS` only after
every source declaration has a `PER_TYPE_QUALIFIED` receipt. Receipts are keyed
by `<type_id>@<source_connector_id>` in `per_type_qualification.entries` and
must cite passing source protocol, semantic codec, ChangeEvent, and live
evidence; all six sinks must record an accepted preservation outcome with
offline and live evidence; Web must cite explicit selection and risk
confirmation. Evidence references must resolve uniquely in the versioned
evidence registry and match the receipt's type, source connector, selected
sink, axis, passing status, non-empty run ID, and SHA-256 report digest.
Each reference must resolve a repository-relative `artifact_path`; the gate
parses the report, recomputes its stable SHA-256 digest, and verifies the report
schema, embedded run ID, and scoped evidence entry. Paths that escape the
workspace, missing reports, stale IDs, forged digests, and ambiguous or
cross-scope references do not qualify. Persisted receipts must point to
checked-in reports under `docs/qualification/evidence/`; generated test reports
under `target/` are only fixtures and cannot qualify a committed receipt. A
`SOURCE_REPRESENTATION_PRESERVED`
sink outcome additionally cites that sink's carrier and write/readback
evidence, while its source receipt cites source representation capture and
protocol framing. Those requirements apply only to the selected representation
path. Semantic value preservation does not depend on raw-byte framing. Empty
evidence references or a missing sink keep the type at `FAMILY_FIXTURE_ONLY`;
family-level fixtures never qualify a native declaration by themselves.
Each declaration row also reports `per_type_live_evidence.source`,
`per_type_live_evidence.all_sinks`, and `per_type_web_status`. The existing
connector-level live suites do not automatically promote those type-level
states; absent a resolvable type receipt, the report keeps them pending.

The Issue #58 representation-only route is tracked independently. Each
versioned source declaration points to a protocol framing profile and records
the required envelope identity, definition digest, type metadata, exact payload
length/digest, and source cursor. MySQL row events require the versioned
`Table_map` type metadata, row encoding, NULL/present-column bitmaps, and a
decoder boundary that exposes validated field bytes. PostgreSQL `pgoutput`
requires the `Relation` OID/type modifier plus the `TupleData` tag and length;
NULL and unchanged-TOAST tags stay separate from payload bytes. These profiles
are currently `MISSING_IMPLEMENTATION`, so this inventory does not claim raw
capture works. PostgreSQL documents the `pgoutput` tuple framing in its
[logical replication message format](https://www.postgresql.org/docs/17/protocol-logicalrep-message-formats.html);
MySQL documents row type metadata and row images in its
[binary logging options](https://dev.mysql.com/doc/refman/8.4/en/replication-options-binary-log.html).

Each sink separately reports native `VALUE_PRESERVED` qualification and
representation-carrier qualification (`LONGBLOB` or `bytea` candidates only).
Carrier write/readback plus length and digest checks may establish
`SOURCE_REPRESENTATION_PRESERVED`; that result never implies value recovery or
native behavior. The representation-only Web path remains a gap until it
requires per-field selection and risk confirmation and blocks primary,
unique, and row-locator use without separate equivalence evidence.

## Adding a version

Add its identity and documentation evidence to `type-inventory.json`, then
declare implementation or unsupported status in the qualification configuration.
An implemented version must supply its own source fixture,
mapping and Sink manifest dispatch, plus registry entries; configuration and
registry tests reject a registration without fixed fixture support. Production
Web and connector code gain no source-target pair branches.

The Rust test pins a per-version digest over static type declarations,
parameter axes, aliases, source identities, code seams, dynamic type classes,
per-type qualification receipts, the connector-scoped evidence registry, and
exclusions. A source digest covers that source's decoding/Web evidence; a Sink
digest covers only its own target evidence. When a type is added or corrected,
first verify its official evidence and mapping probes, then update the affected
pinned digest in the same change. Removing a listed type or alias or changing
a qualification receipt without an intentional reviewed digest update fails
the coverage test.

The Cartesian product automatically adds `2N+1` directions when a seventh
version is added to six versions (13 additions). `-BaselineFile` lists those
directions and verifies the count; adding K versions uses `2NK+K²`. Missing
type or recovery evidence becomes `MISSING_TEST`, is listed in
`missing_directions`, and causes a nonzero exit. The previous matrix remains
part of every run. The extension-rule regression checks both incoming,
outgoing and self directions.

This scope is DML against prepared targets. It adds no DDL replication, full
load, automatic schema alteration, or bidirectional conflict resolution.
