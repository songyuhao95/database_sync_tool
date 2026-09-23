use super::task_ui_tests::fixture;
use crate::{Error, Store};

#[test]
fn legacy_task_plans_are_persisted_as_unsafe_and_block_start() {
    let (_dir, store, actor, task) = fixture();
    let (status, version, plan_count, confirmation_count): (String, Option<String>, i64, i64) =
        store
            .db()
            .unwrap()
            .query_row(
                "SELECT plan_status,plan_version,
                    (SELECT COUNT(*) FROM task_conversion_plans WHERE task_id=replication_tasks.id),
                    json_array_length(risk_confirmations_json)
             FROM replication_tasks WHERE id=?1",
                [&task.id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
    assert_eq!(status, "legacy");
    assert_eq!(version, None);
    assert_eq!(plan_count, 0);
    assert_eq!(confirmation_count, 0);
    assert!(matches!(
        store.start_task(actor, task.id.clone()),
        Err(Error::Conflict(message)) if message.contains("ColumnConversionPlan")
    ));
    assert!(store.workers.lock().unwrap().is_empty());
}

#[test]
fn stale_task_plan_status_blocks_start_without_rewriting_the_snapshot() {
    let (_dir, store, actor, task) = fixture();
    store
        .db()
        .unwrap()
        .execute(
            "UPDATE replication_tasks SET plan_status='stale',plan_invalid_reason='测试元数据变化' WHERE id=?1",
            [&task.id],
        )
        .unwrap();
    assert!(matches!(
        store.start_task(actor, task.id.clone()),
        Err(Error::Conflict(message)) if message.contains("ColumnConversionPlan")
    ));
    let current = store.task(&task.id).unwrap();
    assert_eq!(current.plan_status, "stale");
    assert_eq!(
        current.plan_invalid_reason.as_deref(),
        Some("测试元数据变化")
    );
}

#[test]
fn task_configuration_revision_is_persisted_with_desired_and_effective_pointers() {
    let (dir, store, actor, task) = fixture();
    assert_eq!(task.configuration_revision, 1);
    assert_eq!(task.desired_configuration_revision, 1);
    assert_eq!(task.effective_configuration_revision, None);

    let revision: (i64, String, String) = store
        .db()
        .unwrap()
        .query_row(
            "SELECT revision,plan_status,plans_json
             FROM task_configuration_revisions WHERE task_id=?1",
            [&task.id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(revision, (1, "legacy".into(), "[]".into()));

    store.begin_task(actor, &task.id).unwrap();
    let activated = store.task(&task.id).unwrap();
    assert_eq!(activated.effective_configuration_revision, Some(1));
    store.finish_task(&task.id, None).unwrap();

    drop(store);
    let reopened = Store::open(dir.path().join("web.sqlite"), [7; 32]).unwrap();
    let persisted = reopened.task(&task.id).unwrap();
    assert_eq!(persisted.desired_configuration_revision, 1);
    assert_eq!(persisted.effective_configuration_revision, Some(1));
}

#[test]
fn corrupted_saved_plan_is_rejected_before_activation() {
    let (_dir, store, actor, task) = fixture();
    store
        .db()
        .unwrap()
        .execute(
            "UPDATE replication_tasks
             SET plan_status='valid',plan_version=?2,plans_json='[]'
             WHERE id=?1",
            rusqlite::params![task.id, crate::tasks::TASK_PLAN_VERSION],
        )
        .unwrap();
    assert!(matches!(
        store.start_task(actor, task.id),
        Err(Error::Conflict(message)) if message.contains("ColumnConversionPlan")
    ));
}

#[test]
fn task_page_exposes_plan_status_and_requalification_action() {
    let script = include_str!("../assets/tasks.js");
    assert!(script.contains("taskRequalifyAction"));
    assert!(script.contains("/requalify"));
    assert!(script.contains("/requalify/preview"));
    assert!(script.contains("task.plan_status"));
    assert!(script.contains("ColumnConversionPlan"));
    assert!(script.contains("兼容计划预览"));
    assert!(script.contains("转换参数"));
    assert!(script.contains("示例转换"));
}
