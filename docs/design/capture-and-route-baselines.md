# Capture Stream and Route baselines

A Capture Stream Generation and each Replication Route bound to it have different baseline responsibilities. The Generation Baseline establishes the source history and schema interpretation boundary. A Route Baseline states how one particular Sink's data relates to that boundary. They are never represented by one shared assertion because one generation can feed multiple independently prepared targets.

## Generation Baseline

Every generation has one immutable Generation Baseline containing:

- Source, Capture Stream, Capture Stream Generation, and Source Incarnation identities;
- an exact complete-transaction Source Cursor from which the generation begins;
- the initial Source Environment Epoch Definition reference;
- the effective Capture Scope and Configuration Revision references;
- the complete Schema Anchor reference and digest;
- creation actor, UTC time, evidence references, and a canonical baseline digest; and
- the generated event, schema, and checkpoint topic identities.

The Generation Baseline makes no claim about any Sink. It is published before ordinary source transactions become visible and cannot be replaced inside the generation. A continuity or published-integrity failure that needs a different baseline creates a fresh generation.

## Route Baseline

Every Route has one immutable Route Baseline bound to exactly one Generation Baseline and one transaction boundary. It records the Route, Sink, generation, Source Cursor and Kafka coordinate, Target Environment Epoch, Effective Configuration and Route Projection, expected target definitions and name bindings, mode, actor, UTC time, evidence references, and canonical digest.

The modes are:

- `EXACT_EXTERNAL_BASELINE`: an external process has established target data equivalent to the source at the declared cursor. The operator supplies an External Baseline Attestation. Exact downstream guarantees are conditional on that assertion because phase one does not copy or compare all baseline rows.
- `FUTURE_ONLY`: the Route claims only to process changes after the declared cursor and makes no statement that pre-existing source rows are present at the Sink. It starts as a Degraded Route and remains strict during apply: a missing row or before-image mismatch blocks, and the Sink never converts UPDATE to INSERT, upserts automatically, or ignores a missing DELETE. It is suitable only when that behavior is acceptable, such as qualified append-only data or tables created after the boundary.

An External Baseline Attestation contains an immutable Baseline ID, external snapshot or backup method and reference, exact Source Cursor, Generation, Capture Scope, and Schema Anchor digests, Sink and Route identities, external load completion identity and UTC time, actor, reason, evidence-artifact SHA-256, and a non-secret evidence reference. Optional external row checksums remain evidence rather than tool proof. Phase one validates identities, cursor and Kafka boundaries, schema fingerprints, mappings, environment qualifications, target definitions, and attestation shape. It does not perform the external data copy, fetch the evidence during normal runtime, or represent operator evidence as database proof. Missing required evidence prevents an exact Route from activating.

## Route activation

After baseline validation, one target-database transaction takes the normal shared Sink metadata lock, verifies `ACTIVE`, Target Environment Epoch, and Sink Epoch Fence, and inserts the new Route record with its Route Baseline digest, initial Source Cursor, Kafka input and next-offset coordinates, Effective Configuration, Route Projection, environment and profile identities, and initial Route Fence. It executes no replicated business DML. Target commit makes activation authoritative; the Kafka consumer-group offset is committed afterward and can be reconstructed from Replication Metadata after a crash.

## Rebuilds

A Replication Route cannot change its bound generation or baseline in place. Rebuilding after `RESNAPSHOT_REQUIRED` creates a new Route identity and Route Baseline; the failed Route and its metadata remain immutable recovery history. A new Generation UUID by itself never authorizes a Sink to resume.
