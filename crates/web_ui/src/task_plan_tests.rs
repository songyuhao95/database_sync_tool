use super::task_ui_tests::fixture;
use crate::Error;

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
fn task_page_exposes_plan_status_and_requalification_action() {
    let script = include_str!("../assets/tasks.js");
    assert!(script.contains("taskRequalifyAction"));
    assert!(script.contains("/requalify"));
    assert!(script.contains("task.plan_status"));
    assert!(script.contains("ColumnConversionPlan"));
    assert!(script.contains("兼容计划预览"));
    assert!(script.contains("转换参数"));
    assert!(script.contains("示例转换"));
}
