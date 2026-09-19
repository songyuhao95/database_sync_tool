# Schema lineage

Schema lineage separates the identity of a database object from its current name, physical implementation, and immutable definition state.

## Identity domains

An Object Lineage ID is a required 32-byte SHA-256 value for a table or future top-level schema object. An Element Lineage ID is the same size for a column, index, key, or constraint owned by an object. The two domains use different canonical tags and cannot be interchanged.

The canonical encodings are versioned, typed, and length-prefixed:

- `cdc.object-lineage.v1` for objects;
- `cdc.element-lineage.v1` for nested elements.

Names, ordinals, native IDs, definitions, and diagnostic text are framed as distinct fields rather than concatenated. No wall-clock time, connector processing order, or random value participates.

## First observation

For a pre-existing object first captured in Schema Anchor `A`, object lineage derives from Source ID, Capture Stream ID, Source Incarnation, anchor Source Cursor, object kind, and exact source-qualified name. Its initial elements derive from the object lineage, element kind, source ordinal, effective native name, and anchor coordinate.

For an object created after the anchor, lineage derives from Source ID, Capture Stream ID, Source Incarnation, creating transaction ID, and ordered Object Change coordinate. Elements created with it derive from that object lineage and their ordered creation coordinates. An element added later derives from the owning object lineage, creating Schema Change event identity, and ordered mutation coordinate.

The same committed history therefore regenerates the same identities after replay. Two independent Capture Streams may assign different lineage identities to the same physical source table because each owns an independent schema history.

## Lifecycle rules

- table or element rename preserves lineage;
- column reorder, definition alteration, index visibility change, and constraint enforcement change preserve lineage;
- Table Reset preserves every logical lineage despite a source engine's physical recreation;
- drop retires the affected lineage;
- drop followed by same-name create receives a new lineage;
- `CREATE TABLE ... LIKE` creates a new object and new nested lineages;
- removing only a Route Projection preserves lineage because capture continues;
- removing from Capture Scope and later re-adding creates new lineage unless uninterrupted identity and schema history are independently proven.

Schema Fingerprints identify immutable definition states within a lineage and include the Object and Element Lineage IDs. Object References pair the lineage with the source-faithful name and applicable fingerprint. Native identifiers may be retained as evidence and cross-checks but never determine generic identity. Structurally identical replacement objects therefore have different fingerprints; a lineage-free comparison may be shown diagnostically but is never authoritative for apply or recovery.

## Collision handling

All lineage hashes retain the full 32 bytes. Encountering the same lineage ID with incompatible first-observation evidence or history is a blocking integrity fault; the connector never resolves a collision by allocating a random replacement.
