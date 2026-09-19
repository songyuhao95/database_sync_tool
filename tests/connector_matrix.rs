//! Legacy adapter regression, sharing the canonical fixture with qualification.
#[allow(dead_code)]
#[path = "support/matrix_fixture.rs"]
mod fixture;
use change_event::{Datum, Operation, SinkAdapter as _, validate};
use fixture::*;

#[test]
fn local_four_by_four_matrix() {
    assert_source_adapters_exist();
    for source in SourceVersion::ALL {
        let validated = validate_source(source, transaction(source, "cdc_matrix")).unwrap();
        let replayed = roundtrip(&validated).unwrap();
        assert_eq!(replayed.transaction().changes.len(), 3);
        assert!(matches!(
            replayed.transaction().changes[0].operation,
            Operation::Insert
        ));
        assert!(matches!(
            replayed.transaction().changes[1].operation,
            Operation::Update
        ));
        assert!(matches!(
            replayed.transaction().changes[2].operation,
            Operation::Delete
        ));
        assert_eq!(
            replayed.transaction().changes[0]
                .after
                .as_ref()
                .unwrap()
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            COLUMN_NAMES
        );

        macro_rules! assert_sink {
            ($adapter:ident, $placeholder:literal, $target:literal) => {{
                let sink = $adapter::SinkAdapter::new();
                assert_eq!(sink.capability_manifest().target, $target);
                sink.qualify(&replayed).unwrap();
                let plan = sink.plan(&replayed).unwrap();
                let statements = plan.statements().collect::<Vec<_>>();
                assert_eq!(statements.len(), 3);
                assert!(
                    statements
                        .iter()
                        .all(|statement| statement.contains($placeholder))
                );
                assert!(plan.parameters().any(|parameters| !parameters.is_empty()));
                assert!(statements[0].starts_with("INSERT"));
                assert!(statements[1].starts_with("UPDATE"));
                assert!(statements[2].starts_with("DELETE"));
                assert!(
                    !statements
                        .iter()
                        .any(|statement| statement.contains("中文"))
                );
            }};
        }
        assert_sink!(mysql_5_7, "?", "mysql-5.7");
        assert_sink!(mysql_8_0, "?", "mysql-8.0");
        assert_sink!(mysql_8_4, "?", "mysql-8.4");
        assert_sink!(postgresql_15, "$", "postgresql-15");
    }
}

#[test]
fn local_matrix_rejects_keyless_events_before_any_sink_plan() {
    let validated = validate_source(
        SourceVersion::Postgresql15,
        transaction(SourceVersion::Postgresql15, "cdc_matrix"),
    )
    .unwrap();
    let mut keyless = validated.transaction().clone();
    for change in &mut keyless.changes {
        for image in [&mut change.before, &mut change.after]
            .into_iter()
            .flatten()
        {
            for column in image {
                column.primary_key_ordinal = None;
            }
        }
    }
    let keyless = validate(keyless).unwrap();

    macro_rules! reject {
        ($adapter:ident) => {{
            let error = $adapter::SinkAdapter::new().plan(&keyless).unwrap_err();
            assert_target_capability_failure(error);
        }};
    }
    reject!(mysql_5_7);
    reject!(mysql_8_0);
    reject!(mysql_8_4);
    reject!(postgresql_15);
}

#[test]
fn local_matrix_preserves_postgresql_presence_states_for_every_sink() {
    let validated = validate_source(
        SourceVersion::Postgresql15,
        transaction(SourceVersion::Postgresql15, "cdc_matrix"),
    )
    .unwrap();
    let update = &validated.transaction().changes[1];
    assert!(matches!(
        update.before.as_ref().unwrap()[2].datum,
        Datum::Unavailable
    ));
    assert!(matches!(
        update.after.as_ref().unwrap()[3].datum,
        Datum::Unchanged
    ));

    macro_rules! check {
        ($adapter:ident) => {{
            let plan = $adapter::SinkAdapter::new().plan(&validated).unwrap();
            let update = plan.statements().nth(1).unwrap();
            assert!(
                !update.contains("payload"),
                "unchanged values must not be bound"
            );
            assert!(
                update.contains("message"),
                "changed values must remain writable"
            );
        }};
    }
    check!(mysql_5_7);
    check!(mysql_8_0);
    check!(mysql_8_4);
    check!(postgresql_15);
}
