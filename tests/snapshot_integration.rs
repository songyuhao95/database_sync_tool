//! Real snapshots and concurrent DML; only unique test tables/task IDs are touched.
use change_event::SnapshotTable;
use mysql::{Conn, OptsBuilder, Row, prelude::Queryable};
use std::{
    env,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
fn writer(port: u16) -> Conn {
    let mut conn = Conn::new(
        OptsBuilder::new()
            .ip_or_hostname(Some("192.168.0.10"))
            .tcp_port(port)
            .user(Some("mysql_writer"))
            .pass(Some(env::var("CDC_MYSQL_WRITER_PASSWORD").unwrap()))
            .tcp_connect_timeout(Some(Duration::from_secs(5)))
            .read_timeout(Some(Duration::from_secs(10))),
    )
    .unwrap();
    conn.query_drop(
        "SET SESSION time_zone='+00:00', sql_mode='STRICT_ALL_TABLES,NO_AUTO_VALUE_ON_ZERO'",
    )
    .unwrap();
    conn
}
fn rows(conn: &mut Conn, table: &str) -> Vec<Row> {
    conn.query(format!(
        "SELECT * FROM CDC_test.{table} ORDER BY {}",
        if table.ends_with("_composite") {
            "1,2"
        } else {
            "1"
        }
    ))
    .unwrap()
}
#[test]
#[ignore = "requires source reader RELOAD permission on all three test instances"]
fn snapshot_handoff_types_composite_keys_and_concurrent_dml() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let reader_password = env::var("CDC_MYSQL_READER_PASSWORD").unwrap();
    let writer_password = env::var("CDC_MYSQL_WRITER_PASSWORD").unwrap();
    macro_rules! run {
        ($source:ident,$sp:expr,$sink:ident,$dp:expr) => {{
            for mode in ["binlog","gtid"] {
                let name=format!("snap_{nonce}_{}_{}",$sp,mode);
                let empty=format!("{name}_empty");
                let composite=format!("{name}_composite");
                let task=name.clone();
                let names=[name.clone(),empty.clone(),composite.clone()];
                let mut src=writer($sp);let mut dst=writer($dp);
                for conn in [&mut src,&mut dst] {
                    conn.query_drop("CREATE DATABASE IF NOT EXISTS CDC_test").unwrap();
                    conn.query_drop(format!("CREATE TABLE CDC_test.{name}(id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,message VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,n DECIMAL(30,9),u BIGINT UNSIGNED,f FLOAT,d DOUBLE,payload VARBINARY(20),da DATE,dt DATETIME(6),ts TIMESTAMP(6) NULL DEFAULT NULL,tm TIME(6),yr YEAR,j JSON,message_len INT AS (CHAR_LENGTH(message)) STORED) ENGINE=InnoDB")).unwrap();
                    conn.query_drop(format!("CREATE TABLE CDC_test.{empty}(id INT PRIMARY KEY) ENGINE=InnoDB")).unwrap();
                    conn.query_drop(format!("CREATE TABLE CDC_test.{composite}(a VARCHAR(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,b INT,v VARCHAR(40),PRIMARY KEY(a,b)) ENGINE=InnoDB")).unwrap();
                }
                let values=(0..513).map(|id|format!("({id},'原始',-1234567890123456789.123456789,18446744073709551615,1.25,-1.234567890123456,X'00ff275c','2026-09-12','2026-09-12 01:02:03.123456','2026-09-12 01:02:03.123456','-123:45:56.123456',2026,JSON_OBJECT('id',18446744073709551615,'v',JSON_ARRAY(true,NULL,'汉字',1.25)))")).collect::<Vec<_>>().join(",");
                src.query_drop(format!("INSERT INTO CDC_test.{name}(id,message,n,u,f,d,payload,da,dt,ts,tm,yr,j) VALUES {values}")).unwrap();
                let values=(0..520).map(|i|format!("('{}',{},'old')",if i<260 {"a"} else {"中"},i%260)).collect::<Vec<_>>().join(",");
                src.query_drop(format!("INSERT INTO CDC_test.{composite} VALUES {values}")).unwrap();
                let initial:Vec<_>=names.iter().map(|n|rows(&mut src,n)).collect();
                let tables:Vec<_>=names.iter().map(|table|SnapshotTable {schema:"CDC_test".into(),table:table.clone(),columns:vec![]}).collect();
                let source_config=|| {
                    let mut c=$source::BinlogConfig::new("192.168.0.10",$sp,"mysql_reader",&reader_password);
                    c.start_mode=if mode=="gtid" {$source::BinlogStartMode::Gtid} else {$source::BinlogStartMode::Position};c
                };
                let mut snapshot=$source::snapshot(source_config(),tables.clone()).unwrap();
                let boundary=snapshot.boundary().clone();
                assert!(boundary.cursor.display.starts_with(&format!("{mode}:")));
                let target=$sink::TargetConfig {host:"192.168.0.10".into(),port:$dp,user:"mysql_writer".into(),password:writer_password.clone()};
                let mut sink=$sink::CheckpointWriter::open(&target,&task,&boundary.source.id,&"c".repeat(64)).unwrap();
                sink.prepare_snapshot(&boundary).unwrap();
                let mut changed=false;
                let cp=sink.copy_snapshot(&tables,&mut snapshot,&mut |_,_| {
                    if !changed {
                        // The source lock must already be released. This transaction commits
                        // while the remaining tables/batches still use the earlier read view.
                        let mut tx=src.start_transaction(mysql::TxOpts::default()).unwrap();
                        tx.query_drop(format!("UPDATE CDC_test.{name} SET message='changed' WHERE id=512")).unwrap();
                        tx.query_drop(format!("DELETE FROM CDC_test.{name} WHERE id=511")).unwrap();
                        tx.query_drop(format!("INSERT INTO CDC_test.{name}(id,message) VALUES(600,'new')")).unwrap();
                        tx.query_drop(format!("UPDATE CDC_test.{composite} SET v='changed' WHERE a='中' AND b=259")).unwrap();
                        tx.query_drop(format!("INSERT INTO CDC_test.{empty} VALUES(1)")).unwrap();
                        tx.commit().unwrap();changed=true;
                    }
                    Ok(())
                }).unwrap();
                drop(snapshot);
                assert_eq!(cp.phase,"incremental");assert_eq!(cp.snapshot_rows,1033);
                for (i,n) in names.iter().enumerate() { assert_eq!(rows(&mut dst,n),initial[i],"full result must remain at the common boundary: {n}"); }
                let mut capture=source_config();capture.server_id=880000+$sp as u32;
                capture.non_blocking=true;capture.tables=tables.iter().map(|t|(t.schema.clone(),t.table.clone())).collect();
                if mode=="gtid" {capture.gtid_set=cp.gtid_set.clone();} else {capture.start=Some($source::BinlogPosition {file:cp.file.clone(),position:cp.position});}
                let mut applied=0;
                for transaction in $source::binlog(capture).unwrap() {
                    let mut transaction=transaction.unwrap();
                    transaction.changes.retain(|c|c.schema=="CDC_test" && names.contains(&c.table));
                    if transaction.changes.is_empty() {continue;}
                    let validated=change_event::validate(transaction).unwrap();
                    let plan=$sink::sql(&validated).unwrap();
                    let result=sink.apply(&plan).unwrap();applied+=result.statements_executed;
                    assert!(sink.apply(&plan).unwrap().already_applied,"replay must not write twice");
                }
                assert_eq!(applied,5);
                for n in &names {assert_eq!(rows(&mut dst,n),rows(&mut src,n),"full + incremental mismatch: {n}");}

                src.query_drop(format!("UPDATE CDC_test.{name} SET j=JSON_OBJECT('date',CAST('2026-09-12' AS DATE)) WHERE id=0")).unwrap();
                let mut unsupported=$source::snapshot(source_config(),tables.clone()).unwrap();
                assert!(unsupported.next().unwrap().unwrap_err().to_string().contains("round-trip"));
                drop(unsupported);
                drop(sink);
                dst.exec_drop("DELETE FROM CDC.log_info WHERE task_id=?",(&task,)).unwrap();
                for conn in [&mut src,&mut dst] {for n in &names {conn.query_drop(format!("DROP TABLE CDC_test.{n}")).unwrap();}}
                println!("{} -> {} {mode}: 1033 snapshot rows, empty table, composite key, exact types, concurrent DML and replay passed",stringify!($source),stringify!($sink));
            }
        }};
    }
    run!(mysql_5_7, 33061, mysql_8_0, 33062);
    run!(mysql_8_0, 33062, mysql_8_4, 33063);
    run!(mysql_8_4, 33063, mysql_5_7, 33061);
}

#[test]
#[ignore = "read-only permission inspection of live source accounts"]
fn reader_lock_permissions() {
    let mut complete = true;
    for port in [33061, 33062, 33063] {
        let mut conn = Conn::new(
            OptsBuilder::new()
                .ip_or_hostname(Some("192.168.0.10"))
                .tcp_port(port)
                .user(Some("mysql_reader"))
                .pass(Some(env::var("CDC_MYSQL_READER_PASSWORD").unwrap()))
                .tcp_connect_timeout(Some(Duration::from_secs(5)))
                .read_timeout(Some(Duration::from_secs(5))),
        )
        .unwrap();
        let principal: String = conn.query_first("SELECT CURRENT_USER()").unwrap().unwrap();
        let grants: Vec<String> = conn.query("SHOW GRANTS").unwrap();
        let reload = grants
            .iter()
            .any(|g| g.contains("RELOAD") || g.contains("ALL PRIVILEGES ON *.*"));
        println!("{port}: {principal} RELOAD={reload}");
        complete &= reload;
    }
    assert!(complete, "source reader still needs RELOAD permission");
}
