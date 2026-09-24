# DML type and version qualification

Issue #57 qualifies the six versioned Source and Sink fixtures in
`scripts/qualification-matrix.json`. Their Cartesian product is generated on
every run as 36 offline directions. PostgreSQL 16/17 use their own
SourceTypeMapping and Sink manifests; neither role falls back to PostgreSQL
15.

Live qualification is deliberately not a 36-route database-to-database
matrix. It reports six independent Source → ChangeEvent components, six
ChangeEvent → Sink components, and one common transaction-recovery
qualification. The current live suite registers MySQL 5.7/8.0/8.4 and
PostgreSQL 15; PostgreSQL 16/17 remain `REQUIRES_LIVE` until their live suites
are registered and run. Representative end-to-end route smoke tests are
reported separately.

Run from the repository root:

```powershell
./scripts/qualify.ps1
./scripts/qualify.ps1 -Live -ConfigFile ./scripts/test.txt
./scripts/qualify.ps1 -BaselineFile ./previous-summary.json
```

The default does not read credentials or connect to databases. `-Live` opts in
to the existing DML capture/apply/checkpoint and route-smoke tests, using
process environment variables or an explicitly supplied `KEY=VALUE`
configuration. Missing configuration produces `REQUIRES_LIVE`. A configured
test that fails, or a filter executing zero tests, fails the run. Live tests
prepare their own isolated test objects; no production schema-evolution
feature is introduced.

The console contains only an ordered final matrix, missing-direction count,
and report location. Cargo output stays in per-suite logs. `summary.json`
contains the complete offline matrix, per-case qualification and status,
stable reason codes, plan/manifest digests, recovery evidence scope,
missing/new directions, a stable summary digest, a password scan result, and
the four separate live report sections. Each direction separates Native
Equivalent, Value Preserved, Explicit Conversion, `UNSUPPORTED`, `BLOCKED`,
`REQUIRES_LIVE`, and `FAIL` evidence.
`live-source.json`, `live-sink.json`, `transaction-recovery.json`, and
`route-smoke.json` retain the structured component reports. `types.json` and
`recovery.json` retain the underlying offline evidence. Reports contain no
credentials or fixture row values. Output goes to a unique directory under
`target/qualification`; use `-OutputDirectory` to choose another location.

## Reading results

`offline=PASS` means executed assertions passed, including negative cases. It
does **not** mean every type can replicate. Each field case separately reports
`EXACT`, `RANGE_CHECKED`, `EXPLICIT_CONVERSION`, or `UNSUPPORTED/BLOCKED`, together
with the actual planner status (`COMPATIBLE`, `NEEDS_CONFIGURATION`, etc.).
Only a selectable plan is tested as an accepted runtime conversion. A blocked
source mapping stops before target planning. Unsupported versions receive no
synthetic mapping, plan, execution, or recovery result.

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
components, and the common transaction-recovery report to pass. Route
smoke is reported in `route_smoke_qualified` and is required for a successful
`-Live` run, but it is not counted as an additional source/sink direction.
The report therefore does not claim that all 16 database-to-database links
were run on real servers.

## Adding a version

Add its identity to the roster and explicitly declare implementation or
unsupported status. An implemented version must supply its own source fixture,
mapping and Sink manifest dispatch, plus registry entries; configuration and
registry tests reject a registration without fixed fixture support. Production
Web and connector code gain no source-target pair branches.

The Cartesian product automatically adds `2N+1` directions when a seventh
version is added to six versions (13 additions). `-BaselineFile` lists those
directions and verifies the count; adding K versions uses `2NK+K²`. Missing
type or recovery evidence becomes `MISSING_TEST`, is listed in
`missing_directions`, and causes a nonzero exit. The previous matrix remains
part of every run. The extension-rule regression checks both incoming,
outgoing and self directions.

This scope is DML against prepared targets. It adds no DDL replication, full
load, automatic schema alteration, or bidirectional conflict resolution.
