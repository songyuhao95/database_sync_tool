# Kafka transport contract

Phase one uses the deployed Apache Kafka 3.7.2 broker and the Rust `rdkafka` 0.39.0 client with its bundled `librdkafka`. It relies only on Kafka 3.7 transactional producers, producer fencing, and `read_committed` consumers; Kafka 4 transaction protocol version 2 is not required. The POC runs one KRaft broker and explicitly has no broker-node-failure durability claim. Production qualification requires a replicated cluster and separate evidence.

## Client safety profile

Capture uses an idempotent transactional producer with `acks=all`. Every consumer of generation data uses `isolation.level=read_committed` and disables automatic offset commits. Startup reads back or otherwise validates all correctness-bearing client and broker settings, including transaction timeout compatibility, record-size limits, topic partition count, cleanup policy, and the transaction-state internal-topic settings. The single-node POC explicitly configures transaction-state and consumer-offset internal topics for replication factor and minimum ISR one; those values are never production defaults.

The producer transaction timeout must exceed the configured Transport Envelope's maximum transaction duration while remaining no greater than the broker's maximum transaction timeout. A fatal or fenced transactional error terminates that producer instance. Retriable and abort-required errors follow the client classification but never advance a Capture Checkpoint outside a committed Kafka transaction.

## Generation topics

One Deployment prefix and immutable IDs derive exactly three topics per Capture Stream Generation:

```text
cdc.<deployment_id>.<stream_id>.<generation_id>.events.v1
cdc.<deployment_id>.<stream_id>.<generation_id>.schema.v1
cdc.<deployment_id>.<stream_id>.<generation_id>.checkpoint.v1
```

The events topic has one partition in phase one. The schema and checkpoint topics are compacted: Schema History uses immutable state keys without tombstones, while the checkpoint key retains the latest committed Capture Checkpoint for the generation. Event deletion retention must satisfy the Route Recovery Window. Individual topic names cannot be overridden. Bootstrap uses an administrative identity, disables automatic topic creation, and verifies partition count, cleanup and retention settings, record limits, and ACLs before generation activation.

## Generation activation and binding

Topic creation is administrative preparation, not generation activation. The first committed producer transaction atomically writes:

- a `GenerationActivated` STREAM_CONTROL Synthetic Transaction to the events topic;
- the Generation Baseline, initial Source Environment Epoch Definition, Schema Anchor, and all initial immutable definitions to the schema topic; and
- the initial Capture Checkpoint to the checkpoint topic.

Together these records are the Generation Topic Binding. Each record identifies the Deployment, Capture Stream, generation, topic role, and contract major as applicable. The activation payload references the Generation Baseline digest and resolved topic names and declares the exact baseline Source Cursor. Ordinary source transactions cannot precede it.

An existing nonempty topic is reusable only when its retained records, immutable control-plane generation definition, role, and settings all match. A conflicting topic is never adopted. If event retention removed the activation transaction, the retained schema baseline and checkpoint still bind the generation, and every remaining Change Event must carry the same generation identity. Missing records required by a Checkpoint or Route make the relevant Recovery Window `EXPIRED`; bootstrap never recreates history under the same generation.

## Producer fencing

Each generation uses exactly one stable transactional producer identity:

```text
cdc.<deployment_id>.<stream_id>.<generation_id>.capture.v1
```

The identity excludes Worker identity so a replacement Worker initializes the same transactional producer and Kafka fences its predecessor. A fenced Worker treats the error as fatal and exits rather than selecting another identity. One producer owns publication to all three generation topics so a source Transaction Batch, referenced definitions, and its Capture Checkpoint commit or abort together.

## Recovery objectives

`capture_recovery_objective` and `route_recovery_objective` default to seven days. Exact activation blocks when observable source-log or Kafka retention is below its applicable objective or cannot be verified. A runtime reduction marks the stage `AT_RISK` and raises an alert but does not stop useful forward progress. `EXPIRED` is reached only when a required authoritative record is actually unavailable and results in `RESNAPSHOT_REQUIRED` rather than offset guessing or topic reuse.

## Route progress

The target Replication Metadata row, not the Kafka consumer-group offset, is the authoritative Route resume point. A Sink commits the complete source transaction and next Kafka coordinate in the target database first, then acknowledges that coordinate to Kafka. After a crash it seeks to the target-recorded coordinate and tolerates Kafka redelivery; it never skips forward to a newer consumer-group offset.
