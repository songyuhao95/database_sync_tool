# Capability qualification

Capability Qualification is release evidence that one source semantic can be represented and executed equivalently for an exact source build, target build, and connector build. It is not inferred from a version prefix, successful parsing, or a statement that happened to execute.

## Manifest

Phase one embeds one immutable, content-addressed Capability Manifest in each connector binary. It is not downloaded at runtime. Each entry contains:

- exact source and target database kinds, versions, build comments, and relevant platform/build identity;
- connector identity and build;
- a stable capability key, expression or operation role, and operand/result type signature where applicable;
- required schema predicates and acceptable complete Sink Session Profile identities;
- evidence-suite identity and revision; and
- the expected postcondition class.

Configuration may disable an entry but cannot create, widen, or replace positive evidence. A Target Schema Plan records the manifest digest and selected entries. New support is delivered by a connector release, and diagnostic tooling can display the complete manifest without contacting a service.

The manifest has one deterministic canonical byte encoding and a SHA-256 content digest. A connector verifies the embedded bytes against that digest at startup. The digest is an identity and accidental-integrity mechanism, not proof against a party capable of replacing the binary and its embedded digest. Artifact signing and remotely distributed signed manifests are outside the POC and require a later release-supply-chain design.

## Environment identity

Environment identity is intentionally factored rather than hidden behind one opaque version hash:

- Server Build Identity contains the database kind and distribution, exact version and build comment, compile operating system and machine, and available package or immutable image provenance. A MySQL `server_uuid` is continuity evidence and not build identity.
- Semantic Environment Fingerprint contains the values selected by a versioned, role-specific Environment Profile. It excludes volatile status and unrelated tuning variables.
- Connector Identity contains the connector release and build, Change Event contract generation, and Capability Manifest digest.

Each component has a canonical encoding and digest so a build replacement, semantic-setting drift, and connector upgrade remain distinguishable and can receive different recovery treatment.

Semantic settings are assigned to the narrowest authority that can prove them. Each connector build supplies a fixed Environment Profile for an exact database build; its field set does not change with current Capture Scope or Routes. The Semantic Environment Fingerprint contains the role-wide correctness settings from that profile that are neither fully self-describing in each event nor pinned by the worker. Capability entries declare additional environment predicates, semantic requirement tags, and references to complete Sink Session Profiles; runtime never unions profile fragments or invents a new combination. Contradictory requirements invalidate the Manifest. DDL-specific source session semantics belong to DDL Parse Context. Row-event format, column bitmaps, and partial-value markers are validated from the native event itself. Target connection semantics belong to a qualified Sink Session Profile and are checked on every checkout. Volatile status, resource tuning, and unrelated variables do not enter semantic identity.

## Deterministic qualification lookup

Phase-one positive entries do not use release-series, minimum-version, or wildcard matching. The complete lookup key contains exact source and target Server Build Identities, Connector Identity, stable capability code, semantic role, and operand and result type signatures. Planning must obtain exactly one positive entry. No match is unqualified and blocks the plan; multiple applicable or conflicting entries make the Manifest invalid and fail connector startup. Negative evidence remains in the evidence catalog for diagnostics and cannot authorize execution.

## Sink Session Profile selection

For each exact target and Connector Identity, the Manifest enumerates complete content-addressed profiles. Each profile declares every target session variable and value, readback procedure, whether it requires a dedicated disposable connection, its semantic requirement tags, its explicitly qualified operation and type signatures, and its relaxation set relative to the strict normal profile.

A Sink Transaction Plan first collects the requirements of every operation in the complete source Transaction Batch. A profile is eligible only when every operation positively qualifies under that complete profile. The planner selects the unique eligible profile with the subset-minimal relaxation set; it never applies an arbitrary priority or changes session state between operations. Zero eligible profiles produce a Target Capability Failure before DML. Multiple incomparable or equally minimal candidates are an ambiguity: a statically detectable case invalidates the Manifest at startup, and any residual batch-specific case blocks planning. A non-normal eligible profile uses one dedicated connection for the entire transaction and destroys it afterward.

## Environment changes

A source build, semantic environment, or Connector Identity change is detected before reading beyond the last complete Source Cursor. Preflight, log-continuity validation, and current-definition reconciliation run again. A proven continuous history with unchanged definitions and an exact transition boundary retains Source Incarnation and publishes the immutable Source Environment Epoch Definition plus a `SourceEnvironmentChanged` stream-control Synthetic Transaction before the first event interpreted under the new environment. Every subsequent event references the new epoch. Event-self-describing encoding changes are validated per event and do not require an epoch. A non-self-describing correctness change with no provable Source Cursor blocks and is never assigned to the most recent poll; if its uncertain interval was already published, a new baseline and Capture Stream Generation are required. An unlogged definition difference, discontinuity, or unqualified build also blocks without publishing either record.

A Sink build, semantic environment, or Connector Identity change atomically marks its metadata state `REQUALIFYING` and increments the Sink Epoch Fence before pausing and revalidating every Route targeting that Sink. Each apply transaction must verify both its Route Fence and current Sink Epoch Fence and require Sink state `ACTIVE`, so a stale worker cannot commit merely because it missed a pause notification. Revalidation covers the metadata table, expected target definitions, Sink Session Profiles, and selected qualifications. A metadata-only target transaction creates one Sink-owned Target Environment Epoch, updates every Route's epoch reference at its existing Kafka coordinate and Route Fence, and restores `ACTIVE` before any Route resumes. Failure leaves the Sink `REQUALIFYING`. The transition is audited but does not enter the source-owned Capture Stream. A different logical database still requires a new identity; an environment epoch cannot disguise replacement or divergent history.

## Supported distribution boundary

The phase-one manifest recognizes only the three pinned Oracle MySQL Community Server instances. MariaDB, Percona Server, Aurora MySQL, HeatWave, and other protocol-compatible distributions are separate future database kinds. A familiar version number or protocol handshake cannot select an Oracle MySQL qualification for them.

## Failure records

An `OpaqueNativeExpression` is not also an `UnsupportedFeature` when its exact native representation and interpretation context preserve the source definition completely. It remains captured, and a Route without a qualified same-dialect path reports a `TargetCapabilityFailure`. `UnsupportedFeature` is reserved for a correctness-bearing source semantic that the Change Event contract cannot preserve completely through either its normalized model or a declared opaque carrier. For an in-scope object this produces a `CaptureBlockReason` rather than a placeholder definition.

An `UnsupportedFeature` diagnostic contains a stable namespaced code, affected Object or Element Lineage path, exact source kind and version, canonical semantic descriptor and digest, and evidence digest. It contains no target identity, remediation prose, or localized message. A selected in-scope object with such a feature cannot produce a publishable Table or Schema Context Definition, so Capture blocks. An out-of-scope occurrence may be audited and ignored only after proving that it cannot affect in-scope objects, Schema Context, or transaction interpretation.

A `CaptureBlockReason` contains its stable code, blocking Source Cursor, applicable object reference, evidence digest, retryability classification, and first and latest observation times. It is operational state and never fills a missing definition with a placeholder.

A `TargetCapabilityFailure` contains its stable code, Route and event identities, Definition Reference, exact target build, missing or failed capability key, and Target Schema Plan digest. A generated-value mismatch additionally records column lineage, Column Conversion Plan digest, and expected and observed value digests.

A `KnownOmission` contains the authorizing route policy and Configuration Revision, affected lineage, actor, reason, approval time, and explicit guarantee downgrade. Human-readable and localized rendering is derived from these records and is excluded from canonical digests.
