# Target Schema Plan

A Target Schema Plan is the immutable, Sink-specific interpretation of one Schema Change or Table Reset for one Replication Route. It never feeds back into the canonical Capture Stream.

## Plan contents

Every plan contains:

- Replication Route ID and Effective Configuration Revision;
- source transaction and event identities plus Event Content Digest;
- outcome `EXECUTE_DDL`, `VALIDATE_ONLY`, or `BLOCKED`;
- ordered target operations and their canonical digest;
- deterministic Target Name Bindings;
- expected before and after target Definition References and Schema Fingerprints;
- Column Conversion Plans and other target capability evidence;
- Sink Session Profile identity and digest;
- effective DDL lock policy;
- recovery class `VERIFY_BEFORE_AFTER`, `REEXECUTE_RESET`, or `MANUAL_ONLY`; and
- an overall versioned, typed, length-prefixed plan digest.

`BLOCKED` contains a stable reason code and evidence and is never executed. No-op Schema Changes and source-only Schema Context transitions use `VALIDATE_ONLY`; they advance route metadata in an ordinary database transaction without entering the DDL prepared state.

Each positive capability result references the unique exact-match entry in the connector-embedded Capability Manifest containing the source and target Server Build Identities, Connector Identity, operation or expression role and type signature, and evidence-suite revision. The plan records the manifest digest. Wildcards, nearest-version fallback, parse success, a release-series match, or successful target execution without postcondition verification are not qualification. A missing or stale entry makes the plan `BLOCKED`; duplicate applicable entries invalidate the Manifest before planning. The runtime does not fetch capabilities or run mutating feature probes against the production Sink. Operators may narrow but never broaden the embedded qualifications.

A Target Capability Failure belongs to this plan and its Route state rather than the source Table Definition. An Opaque Native Expression remains a complete captured source representation but requires a qualified target path. An Unsupported Feature means no complete normalized or declared opaque representation exists; an in-scope occurrence therefore creates a Capture Block Reason and no guessed Schema History record. An explicitly accepted Known Omission is a separate route policy and marks the Route degraded.

Generated Observation validation compares the source value projected through its Column Conversion Plan with a target value decoded through the target adapter. SQL equality and formatted output are not evidence. All available observations must pass immediately after their Row Change inside the Sink Apply Transaction; a deterministic mismatch, missing row, or irreducible ambiguity becomes a Target Capability Failure and records the applicable event, column lineage, conversion-plan digest, expected-value digest, and observed-value digest without exposing values in ordinary logs. A deadlock, connection failure, or lock timeout rolls back and retries the complete source transaction; exhausting its retry budget blocks in a retryable operational state and is not reclassified as a semantic mismatch.

The Sink Session Profile is immutable and content-addressed. Its complete state is established and read back after a connection checkout and before opening the apply transaction. A plan may select a dedicated narrowly relaxed profile for one qualified legacy-value operation; that connection is destroyed after use rather than returned to the ordinary pool. Replication Metadata records the active profile digest so restart cannot silently substitute a different session contract.

## Deterministic names

Route Mapping supplies target database and table names. Phase-one columns retain source names, and legal table-scoped index names remain source-faithful. Generated schema-scoped foreign-key and CHECK names use a target-safe kind prefix plus Base32 of SHA-256 over a versioned frame containing Route ID, Element Lineage ID, and kind. MySQL's 64-byte limit permits the full digest for the chosen prefixes; a future target with a shorter limit truncates deterministically and blocks on a detected collision.

The same-series raw SQL path may preserve native constraint names only if there is no mapping and every target name is legal and collision-free. Otherwise the Sink generates normalized DDL or blocks.

## Preparation and recovery

Before executable DDL, the route and Sink metadata are locked, Sink state must be `ACTIVE`, and both the Route Fence and Sink Epoch Fence are checked. `PREPARED` stores the source Kafka coordinate, transaction and event identities, Event Content Digest, Effective Configuration Revision, Target Environment Epoch, plan digest, recovery class, expected target fingerprints, and both fences. It does not store executable SQL.

Recovery rereads the exact Kafka event and Schema History, rebuilds the plan under the recorded configuration, and requires the same digest before any SQL runs. Missing or expired input moves the route to `RESNAPSHOT_REQUIRED`; corruption, a digest mismatch, or an unavailable historical configuration blocks as an integrity fault.

For `VERIFY_BEFORE_AFTER`, all affected target objects matching before permits retry and all matching after permits marking applied. A mixed set, a third state, or an object that cannot be assigned the expected lineage blocks. `REEXECUTE_RESET` validates the unchanged definition and performs the guarded reset again. `MANUAL_ONLY` never executes automatically.

After target execution, the Sink introspects and fingerprints every affected target definition. Only a complete after-state match permits `APPLIED` and route progress advancement. Kafka consumer-group offset acknowledgement follows the authoritative target metadata update.
