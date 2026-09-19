//! Live fault tests use only uniquely named CDC_test tables and their own task rows.
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::{
    ChangeTransaction, ColumnDatum, Datum, LogicalValue, Operation, RowChange, Source, SourceCursor,
};
use mysql::{Conn, OptsBuilder, prelude::Queryable};
use std::{
    env,
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const SOURCE_UUID: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
fn cursor(p: u32) -> SourceCursor {
    let mut raw = b"mysql-bin.000001\0".to_vec();
    raw.extend_from_slice(&p.to_be_bytes());
    SourceCursor {
        format: "mysql.binlog.file-position.v1".into(),
        value: URL_SAFE_NO_PAD.encode(raw),
        display: format!("mysql-bin.000001:{p}"),
    }
}
fn transaction(
    table: &str,
    n: u32,
    key: u32,
    duplicate: bool,
) -> change_event::ValidatedTransaction {
    let row = |p| RowChange {
        database: None,
        operation: Operation::Insert,
        schema: "CDC_test".into(),
        table: table.into(),
        source_cursor: cursor(p),
        source_timestamp: 1_700_000_000,
        schema_basis: "test_fixture".into(),
        before: None,
        after: Some(vec![ColumnDatum {
            ordinal: 0,
            name: "id".into(),
            native_type: "int unsigned".into(),
            primary_key_ordinal: Some(0),
            generated: false,
            collation: None,
            datum: Datum::Value(LogicalValue::Integer {
                signed: false,
                bits: 32,
                value: key.to_string(),
            }),
        }]),
    };
    change_event::validate(ChangeTransaction {
        source: Source {
            kind: "mysql".into(),
            version: "5.7.44-log".into(),
            id: SOURCE_UUID.into(),
        },
        id: format!("{SOURCE_UUID}:{n}"),
        begin_cursor: cursor(n * 100 + 5),
        commit_cursor: cursor(n * 100 + 50),
        changes: if duplicate {
            vec![row(n * 100 + 10), row(n * 100 + 20)]
        } else {
            vec![row(n * 100 + 10)]
        },
    })
    .unwrap()
}
fn connection(port: u16) -> Conn {
    Conn::new(
        OptsBuilder::new()
            .ip_or_hostname(Some("192.168.0.10"))
            .tcp_port(port)
            .user(Some("mysql_writer"))
            .pass(Some(
                env::var("CDC_MYSQL_WRITER_PASSWORD").expect("set CDC_MYSQL_WRITER_PASSWORD"),
            ))
            .tcp_connect_timeout(Some(Duration::from_secs(5)))
            .read_timeout(Some(Duration::from_secs(10))),
    )
    .unwrap()
}
fn packet(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut h = [0; 4];
    stream.read_exact(&mut h)?;
    let size = h[0] as usize | ((h[1] as usize) << 8) | ((h[2] as usize) << 16);
    let mut bytes = vec![0; size + 4];
    bytes[..4].copy_from_slice(&h);
    stream.read_exact(&mut bytes[4..])?;
    Ok(bytes)
}
/// Forward a COMMIT to MySQL, consume its response, and sever the client connection
/// without delivering the response. The arm switch excludes initialization commits.
fn lost_ack_proxy(port: u16) -> (u16, Arc<AtomicBool>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let local = listener.local_addr().unwrap().port();
    let arm = Arc::new(AtomicBool::new(false));
    let armed = arm.clone();
    let handle = thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        let mut server = TcpStream::connect(("192.168.0.10", port)).unwrap();
        server
            .set_read_timeout(Some(Duration::from_secs(20)))
            .unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(20)))
            .unwrap();
        let mut client_read = client.try_clone().unwrap();
        let mut server_write = server.try_clone().unwrap();
        let drop_response = Arc::new(AtomicBool::new(false));
        let marker = drop_response.clone();
        let upstream = thread::spawn(move || {
            while let Ok(bytes) = packet(&mut client_read) {
                if bytes.get(4) == Some(&3)
                    && bytes[5..].eq_ignore_ascii_case(b"COMMIT")
                    && armed.swap(false, Ordering::SeqCst)
                {
                    marker.store(true, Ordering::SeqCst);
                }
                if server_write.write_all(&bytes).is_err() {
                    break;
                }
            }
            let _ = server_write.shutdown(Shutdown::Both);
        });
        while let Ok(bytes) = packet(&mut server) {
            if drop_response.swap(false, Ordering::SeqCst) {
                assert_eq!(
                    bytes.get(4),
                    Some(&0),
                    "server must have acknowledged a successful COMMIT"
                );
                let _ = client.shutdown(Shutdown::Both);
                let _ = server.shutdown(Shutdown::Both);
                break;
            }
            if client.write_all(&bytes).is_err() {
                break;
            }
        }
        let _ = client.shutdown(Shutdown::Both);
        let _ = server.shutdown(Shutdown::Both);
        upstream.join().unwrap();
    });
    (local, arm, handle)
}
#[test]
#[ignore = "requires configured MySQL test instances; creates isolated test objects"]
fn checkpoint_atomicity_replay_isolation_and_lost_ack_all_versions() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let password = env::var("CDC_MYSQL_WRITER_PASSWORD").unwrap();
    macro_rules! verify { ($a:ident,$port:literal)=>{{
        let mut db=connection($port);
        db.query_drop("CREATE DATABASE IF NOT EXISTS CDC_test").unwrap();
        for mode in ["binlog","gtid"] {
            let table=format!("cp_{nonce}_{}_{}", $port,mode);
            db.query_drop(format!("CREATE TABLE CDC_test.{table}(id INT UNSIGNED PRIMARY KEY) ENGINE=InnoDB")).unwrap();
            let task=format!("test_{table}");let other=format!("{task}_other");let binding="a".repeat(64);
            let config=$a::TargetConfig{host:"192.168.0.10".into(),port:$port,user:"mysql_writer".into(),password:password.clone()};
            let mut writer=$a::CheckpointWriter::open(&config,&task,SOURCE_UUID,&binding).unwrap();
            let initial=writer.initialize(mode,"mysql-bin.000001",4,(mode=="gtid").then_some("")).unwrap();
            assert_eq!(initial.position,4);
            assert!($a::CheckpointWriter::open(&config,&task,SOURCE_UUID,&binding).is_err(),"same task must have only one writer");
            let plan=$a::sql(&transaction(&table,1,1,false)).unwrap();
            let first=writer.apply(&plan).unwrap();assert_eq!(first.statements_executed,1);
            assert_eq!(first.checkpoint.position,150);
            assert!(writer.apply(&plan).unwrap().already_applied);
            let bad=$a::sql(&transaction(&table,2,2,true)).unwrap();
            assert!(writer.apply(&bad).is_err());
            assert_eq!(writer.checkpoint().unwrap().position,150);
            let absent:Option<u32>=db.query_first(format!("SELECT id FROM CDC_test.{table} WHERE id=2")).unwrap();
            assert!(absent.is_none(),"first row of failed transaction must roll back");
            let position:u64=db.exec_first("SELECT binlog_position FROM CDC.log_info WHERE task_id=?",(&task,)).unwrap().unwrap();
            assert_eq!(position,150,"progress must roll back with failed data");
            let mut second=$a::CheckpointWriter::open(&config,&other,SOURCE_UUID,&binding).unwrap();
            second.initialize(mode,"mysql-bin.000001",4,(mode=="gtid").then_some("")).unwrap();
            second.apply(&$a::sql(&transaction(&table,1,3,false)).unwrap()).unwrap();
            assert_eq!(second.checkpoint().unwrap().applied_rows,1);
            drop(second);drop(writer);
            assert!($a::CheckpointWriter::open(&config,&task,SOURCE_UUID,&"b".repeat(64)).is_err(),"task configuration must be bound");
            let (proxy_port,arm,proxy)=lost_ack_proxy($port);
            let proxy_config=$a::TargetConfig{host:"127.0.0.1".into(),port:proxy_port,user:"mysql_writer".into(),password:password.clone()};
            let mut writer=$a::CheckpointWriter::open(&proxy_config,&task,SOURCE_UUID,&binding).unwrap();
            let next=$a::sql(&transaction(&table,3,4,false)).unwrap();
            arm.store(true,Ordering::SeqCst);
            let error=writer.apply(&next).unwrap_err();
            assert!($a::commit_outcome_unknown(&error),"{error}");
            drop(writer);proxy.join().unwrap();
            let mut recovered=$a::CheckpointWriter::open(&config,&task,SOURCE_UUID,&binding).unwrap();
            assert_eq!(recovered.checkpoint().unwrap().position,350,"recover target commit despite missing acknowledgement");
            assert_eq!(recovered.checkpoint().unwrap().applied_rows,2);
            assert!(recovered.apply(&next).unwrap().already_applied);
            assert!(recovered.apply(&plan).unwrap().already_applied,"older commits must also be skipped");
            let count:u64=db.query_first(format!("SELECT COUNT(*) FROM CDC_test.{table}")).unwrap().unwrap();
            assert_eq!(count,3);
            if mode=="gtid" {
                let cp=recovered.checkpoint().unwrap();
                let included:u8=db.exec_first("SELECT GTID_SUBSET(?,?)",(format!("{SOURCE_UUID}:1:3"),cp.gtid_set.as_ref().unwrap())).unwrap().unwrap();
                assert_eq!(included,1);
            }
            let mut filtered=transaction(&table,4,88,false).transaction().clone();
            filtered.changes.clear();
            recovered.observe(&filtered).unwrap();
            let before:u64=db.exec_first("SELECT binlog_position FROM CDC.log_info WHERE task_id=?",(&task,)).unwrap().unwrap();
            assert_eq!(before,350,"observing a filtered transaction cannot advance durable progress");
            recovered.apply(&$a::sql(&transaction(&table,5,6,false)).unwrap()).unwrap();
            if mode=="gtid" {
                let included:u8=db.exec_first("SELECT GTID_SUBSET(?,gtid_set) FROM CDC.log_info WHERE task_id=?",(format!("{SOURCE_UUID}:4-5"),&task)).unwrap().unwrap();
                assert_eq!(included,1,"filtered GTIDs commit together with the next business transaction");
            }
            db.exec_drop("DELETE FROM CDC.log_info WHERE task_id=?",(&task,)).unwrap();
            assert!(recovered.apply(&$a::sql(&transaction(&table,4,5,false)).unwrap()).is_err(),"missing progress cannot establish a new baseline");
            drop(recovered);
            db.exec_drop("DELETE FROM CDC.log_info WHERE task_id=?",(&other,)).unwrap();
            db.query_drop(format!("DROP TABLE CDC_test.{table}")).unwrap();
            println!("{} {mode}: atomic commit, rollback, replay, isolation, lost ACK and missing checkpoint passed",stringify!($a));
        }
    }}; }
    verify!(mysql_5_7, 33061);
    verify!(mysql_8_0, 33062);
    verify!(mysql_8_4, 33063);
}

#[test]
#[ignore = "requires live Sink instances; no source lock or source writes"]
fn snapshot_atomicity_restart_truncation_and_lost_ack_all_versions() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let password = env::var("CDC_MYSQL_WRITER_PASSWORD").unwrap();
    macro_rules! run {
        ($a:ident,$port:expr) => {{
            let table=format!("full_atomic_{nonce}_{}",$port);
            let task=format!("full_atomic_{nonce}_{}",$port);
            let mut conn=connection($port);
            conn.query_drop("CREATE DATABASE IF NOT EXISTS CDC_test").unwrap();
            conn.query_drop(format!("CREATE TABLE CDC_test.{table}(id INT UNSIGNED PRIMARY KEY) ENGINE=InnoDB")).unwrap();
            let config=$a::TargetConfig {host:"192.168.0.10".into(),port:$port,user:"mysql_writer".into(),password:password.clone()};
            let boundary=change_event::SnapshotBoundary {source:transaction(&table,1,1,false).transaction().source.clone(),cursor:SourceCursor {format:"mysql.snapshot-boundary.v1".into(),value:serde_json::json!({"mode":"gtid","file":"mysql-bin.000001","position":150,"executed_gtids":format!("{SOURCE_UUID}:1")}).to_string(),display:"gtid:mysql-bin.000001:150".into()}};
            let scope=vec![change_event::SnapshotTable {schema:"CDC_test".into(),table:table.clone(),columns:vec!["id".into()]}];
            let batch=|id,last|change_event::SnapshotBatch {source:boundary.source.clone(),schema:"CDC_test".into(),table:table.clone(),rows:vec![transaction(&table,1,id,false).transaction().changes[0].after.clone().unwrap()],last_in_table:last};
            let binding="b".repeat(64);
            let mut writer=$a::CheckpointWriter::open(&config,&task,SOURCE_UUID,&binding).unwrap();
            writer.check_snapshot_targets(&scope).unwrap();
            let prepared=writer.prepare_snapshot(&boundary).unwrap();
            assert_eq!(prepared.phase,"snapshot");
            let error=writer.copy_snapshot(&scope,&mut vec![Ok(batch(1,false)),Ok(batch(2,true))].into_iter(),&mut |_,_|Err(std::io::Error::new(std::io::ErrorKind::Interrupted,"injected stop"))).unwrap_err();
            assert_eq!(error.kind(),std::io::ErrorKind::Interrupted);
            assert_eq!(conn.query_first::<u64,_>(format!("SELECT COUNT(*) FROM CDC_test.{table}")).unwrap(),Some(0));
            drop(writer);
            let mut writer=$a::CheckpointWriter::open(&config,&task,SOURCE_UUID,&binding).unwrap();
            assert_eq!(writer.checkpoint().unwrap().phase,"snapshot");
            writer.prepare_snapshot(&boundary).unwrap();
            assert!(writer.copy_snapshot(&scope,&mut vec![Ok(batch(1,false))].into_iter(),&mut |_,_|Ok(())).is_err(),"truncated full scan must not commit");
            assert_eq!(conn.query_first::<u64,_>(format!("SELECT COUNT(*) FROM CDC_test.{table}")).unwrap(),Some(0));
            drop(writer);
            let (proxy_port,arm,proxy)=lost_ack_proxy($port);
            let proxy_config=$a::TargetConfig {host:"127.0.0.1".into(),port:proxy_port,user:"mysql_writer".into(),password:password.clone()};
            let mut writer=$a::CheckpointWriter::open(&proxy_config,&task,SOURCE_UUID,&binding).unwrap();
            arm.store(true,Ordering::SeqCst);
            let error=writer.copy_snapshot(&scope,&mut vec![Ok(batch(1,false)),Ok(batch(2,true))].into_iter(),&mut |_,_|Ok(())).unwrap_err();
            assert!($a::commit_outcome_unknown(&error));
            drop(writer);proxy.join().unwrap();
            let mut recovered=$a::CheckpointWriter::open(&config,&task,SOURCE_UUID,&binding).unwrap();
            let cp=recovered.checkpoint().unwrap();
            assert_eq!(cp.phase,"incremental");assert_eq!(cp.snapshot_rows,2);assert_eq!(cp.position,150);
            let expected_gtids = format!("{SOURCE_UUID}:1");
            assert_eq!(cp.snapshot_gtids.as_deref(),Some(expected_gtids.as_str()));
            assert_eq!(conn.query_first::<u64,_>(format!("SELECT COUNT(*) FROM CDC_test.{table}")).unwrap(),Some(2));
            assert!(recovered.prepare_snapshot(&boundary).is_err(),"must never overwrite a committed full snapshot");
            assert!(recovered.check_snapshot_targets(&scope).is_err(),"nonempty target must be protected");
            drop(recovered);
            conn.exec_drop("DELETE FROM CDC.log_info WHERE task_id=?",(&task,)).unwrap();
            conn.query_drop(format!("DROP TABLE CDC_test.{table}")).unwrap();
            println!("{}: snapshot stop rollback, restart, truncation, lost ACK and target protection passed",stringify!($a));
        }};
    }
    run!(mysql_5_7, 33061);
    run!(mysql_8_0, 33062);
    run!(mysql_8_4, 33063);
}
