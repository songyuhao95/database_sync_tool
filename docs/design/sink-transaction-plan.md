# Sink Transaction Plan

A Sink Transaction Plan is the immutable, Sink-specific plan for applying one complete source Transaction Batch. It is distinct from a Target Schema Plan, which governs one Schema Change or Table Reset. The planner consumes and validates the whole batch before opening a target transaction.

## Contents and digest

The plan binds:

- Replication Route, Effective Configuration Revision, Capture Stream Generation, and source transaction identity and commit digest;
- Source Environment Epoch Definition and Target Environment Epoch identities;
- ordered source events, Definition References, target Schema Fingerprints, Target Name Bindings, Row Locator strategies, and Column Conversion Plans;
- Capability Manifest digest and selected capability-entry identities;
- exactly one content-addressed Sink Session Profile;
- expected Route Fence, Sink Epoch Fence, Kafka input and next-offset coordinates;
- bounded resource expectations and deterministic failure classifications; and
- a versioned canonical plan digest.

The Sink regenerates the same plan on retry and refuses to execute if its digest, definitions, environment epochs, capabilities, or input coordinates differ. A successful Sink Apply Transaction records the plan, profile, and Capability Manifest digests plus the bound identities with Replication Metadata. The complete DML plan is runtime state reconstructed from Kafka events, immutable definitions, Effective Configuration, and the Manifest; it is not stored in the target metadata table or audit topic. Ordinary logs and durable diagnostics contain neither row values, bound parameters, nor rendered SQL. The existing Target Schema Plan recovery record remains the narrow exception for nontransactional DDL and Table Reset recovery.

## One profile per transaction

Planning scans every event and collects its semantic requirement tags before target `BEGIN`. The batch executes under exactly one complete Sink Session Profile already present and positively qualified in the Capability Manifest; session semantics never change mid-transaction and runtime never synthesizes a profile by merging variable fragments. The planner chooses the unique eligible profile with the subset-minimal relaxation set. If that is the normal profile, an ordinary verified pool connection is used. A qualified legacy value may instead select one prequalified relaxed profile only when every other operation in the batch is also qualified under that complete profile.

Incompatible profile requirements, no eligible profile, or an ambiguous minimal selection produce a Target Capability Failure before DML. A dedicated connection establishes and reads back the complete profile, executes or rolls back the entire batch, and is then destroyed. It is never returned to the ordinary pool. Connection loss, deadlock, or lock timeout retries planning and the complete transaction; no retry resumes within the batch.

## Fencing

After the profile is established and the target transaction begins, the Sink takes a shared locking read on the Sink metadata row and verifies `ACTIVE`, Target Environment Epoch, and Sink Epoch Fence. It then locks its Route row exclusively and verifies Route Fence, prior source transaction, and Kafka coordinate. Locks are acquired in this order and held through DML, validation, metadata advancement, and commit. A target environment transition takes the Sink row exclusively, so it waits for all in-flight shared holders without serializing normal Routes against each other.
