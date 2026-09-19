# Schema Change contract v1

This document specifies the database-independent definition and change records used by `cdc.v1`. Target SQL and compatibility decisions are deliberately excluded.

## SchemaChange

`SchemaChange` has:

- `effect`: `CHANGED` or `NO_CHANGE`;
- optional `native_statement`: Native Statement Evidence; and
- ordered `object_changes`.

`NO_CHANGE` requires an empty object list. `CHANGED` requires at least one Object Change. MySQL source DDL requires Native Statement Evidence, while future structured-log connectors may omit it. The event's ordinary metadata, source context, transaction context, ordered Object References, and extensions remain in the enclosing ChangeEvent.

## ObjectChange

An Object Change has operation `CREATE`, `ALTER`, `RENAME`, or `DROP`, explicit-presence `before_object_index` and `after_object_index`, and ordered Schema Mutations.

| Operation | Before | After | Lineage rule |
| --- | --- | --- | --- |
| `CREATE` | absent | required | starts a new lineage |
| `ALTER` | required | required | same lineage |
| `RENAME` | required | required | same lineage, different qualified name |
| `DROP` | required | absent | retires lineage |

The indexed Object References are Definition References carrying definition kind, Object Lineage ID, source-faithful qualified name, and Schema Fingerprint. The enclosing Source Context supplies the Capture Stream ID that completes the immutable Schema History key; a reference never embeds a full definition or authorizes inference from the current catalog. Before and after definitions are authoritative. Duplicate, contradictory, wrong-lineage, name-inconsistent, or operation-inconsistent references invalidate the event.

## SchemaMutation

Schema Mutation is a oneof:

- `AddElement` starts an Element Lineage present only after;
- `DropElement` retires an Element Lineage present only before;
- `RenameElement` preserves lineage and changes its name;
- `AlterElement` preserves lineage and changes its semantic definition;
- `MoveElement` preserves lineage and changes ordered placement;
- `SetSemanticOption` adds or replaces a normalized object option;
- `RemoveSemanticOption` removes one.

Mutations retain source statement order and reference elements in the immutable before or after definitions rather than duplicating them. Each carries existing-data effect `NONE`, `INITIALIZE_VALUES`, `REMOVE_VALUES`, or `REWRITE_VALUES`. An unclassifiable effect is a capture error, not an `UNKNOWN` value that a consumer may ignore.

Mutations explain how to plan a transition but do not override its authoritative definitions. Applying them to the before definition must deterministically produce the after definition; capture validates this invariant before publication.

## Schema History records

Schema History records use key:

```text
(stream_id, definition_kind, object_lineage_id, schema_fingerprint)
```

The value contains Source Incarnation, definition contract major, effective Source Cursor, origin anchor or Change Event identity, and one full Table Definition or Schema Context Definition. The declared Schema Fingerprint is the record's canonical content digest. Reusing a key for different bytes is an integrity fault.

Schema records are immutable and never tombstoned. A drop is represented by the ordered Object Change, not by deleting history. The producer commits every new definition, its referencing transaction, and the Capture Checkpoint atomically across Kafka topics.

## Schema Context

Schema Context Definition is a first-class definition kind. It represents database-level semantic defaults needed to normalize later table definitions and has its own lineage and fingerprint. Context CREATE, ALTER, and DROP use ordinary Object Changes. A database drop also lists captured table drops in source order.

Sinks retain and validate context history but plan a context-only source change as `VALIDATE_ONLY`; they never create, alter, or drop the configured target database.

## Fingerprints

The canonical fingerprint includes all Object and Element Lineage IDs, Encoded Identifier bytes and encodings, encoded comments, explicit field presence, and every correctness-bearing definition field. It performs no identifier case folding, Unicode normalization, or trimming, and excludes optional display text, Native Statement Evidence, ignored clauses, connector observations, mutable runtime state, and nonsemantic extensions. Source and target definitions are separately normalized and fingerprinted. A target catalog is assigned the expected lineage through the Target Schema Plan before its target fingerprint is calculated.

No lineage-free digest is authoritative. Diagnostic tooling may display a structural comparison, but it cannot satisfy apply preconditions, recovery, or Schema Adoption.
