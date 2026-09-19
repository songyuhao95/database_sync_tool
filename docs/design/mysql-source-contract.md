# MySQL source contract

Phase-one MySQL Capture is non-invasive: it reads public metadata and the native replication stream but does not create source objects, change server settings, lock source tables, or stop business traffic. Lossless capture therefore depends on an explicit Source Logging Contract as well as connector validation.

## Logging prerequisite

Every committed change to an in-scope object must be present in the retained MySQL binary log. Binary logging must remain enabled, and every application, administrative, scheduled, or replication session that can modify an in-scope object must leave `sql_log_bin=ON`. The Capture account has no authority to enforce this rule. A write performed with logging disabled has no Source Cursor or event for the connector to detect, order, retry, or recover; later continuity checks cannot prove that it never occurred.

The end-to-end no-loss claim is therefore conditional on MySQL successfully recording the change and on either the source log or Kafka retaining every coordinate needed by the Recovery Window. Known or suspected violation requires an independent data reconciliation or a new baseline. Resuming from the previous Checkpoint alone is not repair.

Activation requires a Source Logging Attestation bound to the Source, Source Incarnation, Capture Scope revision, and writer-policy digest. It records those identities, the governing Source Configuration Revision, actor, time, policy reference, evidence references, and canonical attestation digest. It has no arbitrary time expiry: a Source identity or Incarnation change, endpoint replacement, Scope expansion, writer-policy change, or confirmed or suspected unlogged in-scope write invalidates it, while a worker restart or unrelated configuration edit does not. The CLI separately labels each check as directly observed, operator-attested, or unverifiable. Current global settings and the Capture account's own privileges can be observed; the attestation cannot prove the future behavior of every application or administrator. An invalid attestation blocks lossless continuation.

## Preflight and event-time gates

Preflight requires `log_bin=ON`, a nonzero server identity, a qualified server and connector build, `binlog_format=ROW`, and `binlog_row_image=FULL`, in addition to cursor, metadata, scope, engine, continuity, and Binlog-retention checks. `binlog_row_value_options=PARTIAL_JSON` is allowed because the connector supports reconstruction from a complete before value. The retention check observes the version-appropriate expiration settings and earliest available log coordinate, compares them with the configured outage objective, and warns when the Recovery Window is at risk; it never claims that a configured duration prevents an administrative purge.

Preflight values are necessary defaults, not evidence for a later event. Every native transaction is validated from the stream itself:

- an in-scope DML Query Event is unsupported and blocks Capture before the transaction is published;
- INSERT requires complete ordinary writable after values, UPDATE requires complete ordinary writable before and reconstructed after values, and DELETE requires complete ordinary writable before values;
- generated-column results may be absent under the Generated Observation rules, but ordinary columns may not;
- a partial JSON after value is accepted only with a complete before document and a valid ordered patch whose application yields one complete Logical Value; and
- DDL Query Events are interpreted only with their complete DDL Parse Context.

Global `STATEMENT` or `MIXED` format and `MINIMAL` or `NOBLOB` row images fail preflight. A privileged source session can nevertheless override global defaults, so actual Query Events, row bitmaps, and partial-value markers remain authoritative. A missing value or unsupported statement affects the canonical Capture Stream, not merely one Route.

## Environment profiles and drift

Each exact connector build owns a fixed, versioned Environment Profile for each source build. It lists all role-wide correctness settings on which that connector might rely, independent of current Capture Scope or Route selection. Every Environment Requirement has a stable key, source and read method, scope, expected predicate, failure action, and exactly one class: `BUILD_IDENTITY`, `CONTINUITY_IDENTITY`, `SEMANTIC_FINGERPRINT`, `REQUIRED_PRECONDITION`, `EVENT_VALIDATED`, `SESSION_PROFILE`, or `OPERATIONAL_ONLY`. Only `SEMANTIC_FINGERPRINT` values enter the Source Environment Epoch digest. Capability Manifest entries declare additional capability preconditions, while DDL-specific session state and self-describing native encoding facts remain outside the role-wide fingerprint. Contradictory requirements invalidate the Manifest. A Connector Identity change is required to change the profile's field set.

Polling and reconnect checks can detect environment drift but do not invent its Source Cursor. Self-describing differences are handled from each native event and do not create an epoch. Connector upgrades, server restarts, and other transitions with a provable complete boundary may create a new Source Environment Epoch normally. An unexpected non-self-describing correctness change without an exact boundary blocks Capture and is never backdated to the latest poll.

If every event in the uncertain interval is still uncommitted to Kafka, Capture may discard its local work and restart verification from the last unambiguous Capture Checkpoint. If an unprovable interval has already entered the immutable stream, that Capture Stream Generation has an integrity fault and cannot be repaired by inserting a late marker; a new baseline and generation are required.

Environment observations run at startup, reconnect, Schema Anchor creation, each native format-description or rotation boundary, and before and after relevant DDL stabilization. A periodic check defaults to 30 seconds and is configurable with a connector-defined lower bound. It records the last successful time and digest for health reporting but remains detection rather than proof; event type, row bitmap, and partial-value validation still run for every native event.

## Capture recovery classes

Every Capture Block Reason declares one recovery class:

- `TRANSIENT` retries from the previous Capture Checkpoint after a temporary connection or metadata failure;
- `REPLAYABLE_IMPLEMENTATION_GAP` waits for a connector that can decode the still-retained exact log bytes, then replays from the previous Checkpoint;
- `IRRECOVERABLE_SOURCE_EVENT` covers a committed statement DML event, missing required row image, invalid unreconstructable patch, or another historical event whose missing semantics cannot be restored by changing future source settings, and requires a new baseline; and
- `PUBLISHED_INTEGRITY_FAULT` terminates the affected Capture Stream Generation because an already published interval can no longer be proven.

The blocking record states the earliest safe Checkpoint, offending or uncertain cursor range, required operator action, and whether exact replay bytes are still retained. Phase one does not durably archive a second copy of the native Binlog. While blocked it compares the required earliest Source Cursor with the currently earliest available cursor and expiration policy and reports `OPEN`, `AT_RISK`, or `EXPIRED`; the predicted expiration time is advisory. If a replayable gap becomes `EXPIRED`, its original cause remains auditable but the generation transitions to `RESNAPSHOT_REQUIRED`. Capture never skips a transaction or changes Scope retroactively to step over it.
