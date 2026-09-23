use crate::{Result, catalog, decoder::Decoder, invalid};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::{Source, ValidatedTransaction};
use pg_walstream::{
    CancellationToken, LogicalReplicationParser, PgReplicationConnection, ReplicationSlotOptions,
    SlotType,
};
use sqlx::{
    Connection, PgConnection, Row,
    postgres::{PgConnectOptions, PgSslMode},
};
use std::time::{Duration, Instant};

/// Password is deliberately excluded from Debug/logging.
#[derive(Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
    pub publication: String,
    pub slot: String,
    /// Explicit first initialization. Existing slots are never replaced.
    pub create_slot: bool,
    /// Existing slot: defaults to confirmed_flush_lsn. Never jumps to current WAL.
    pub start_lsn: Option<String>,
    /// Check the source identity printed by a previous run when resuming elsewhere.
    pub expected_source_id: Option<String>,
    pub max_transaction_bytes: usize,
}
impl Config {
    pub fn new(
        host: impl Into<String>,
        port: u16,
        database: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
        publication: impl Into<String>,
        slot: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            database: database.into(),
            username: username.into(),
            password: password.into(),
            publication: publication.into(),
            slot: slot.into(),
            create_slot: false,
            start_lsn: None,
            expected_source_id: None,
            max_transaction_bytes: 32 * 1024 * 1024,
        }
    }
}
pub struct Replication {
    connection: PgReplicationConnection,
    parser: LogicalReplicationParser,
    decoder: Decoder,
    start: u64,
    acknowledged: u64,
    delivered: Option<change_event::SourceCursor>,
    feedback_at: Instant,
    failed: bool,
}
/// Opens an existing slot, or explicitly creates a new one. No automatic reconnect/slot recreation.
pub async fn replication(config: Config) -> Result<Replication> {
    replication_for_version(config, 15).await
}

/// Open a PostgreSQL logical replication source for one supported server
/// major. The protocol and recovery contract are shared by 15/16/17, while
/// the server major remains part of Source identity and catalog evidence.
pub async fn replication_for_version(config: Config, expected_major: u16) -> Result<Replication> {
    if !matches!(expected_major, 15..=17) {
        return Err(invalid(
            "PostgreSQL SourceAdapter supports versions 15, 16, and 17",
        ));
    }
    if config.slot.is_empty()
        || config.slot.len() > 63
        || !config
            .slot
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        || config.publication.is_empty()
        || config.max_transaction_bytes == 0
        || [
            &config.host,
            &config.database,
            &config.username,
            &config.password,
            &config.publication,
        ]
        .iter()
        .any(|s| s.contains('\0'))
    {
        return Err(invalid("invalid replication configuration"));
    }
    if config.create_slot && config.start_lsn.is_some() {
        return Err(invalid(
            "cannot specify historical LSN while creating a new slot",
        ));
    }
    let options = PgConnectOptions::new()
        .host(&config.host)
        .port(config.port)
        .username(&config.username)
        .password(&config.password)
        .database(&config.database)
        .ssl_mode(PgSslMode::Prefer);
    let mut sql = tokio::time::timeout(
        Duration::from_secs(10),
        PgConnection::connect_with(&options),
    )
    .await??;
    sqlx::query("SET statement_timeout = '10s'")
        .execute(&mut sql)
        .await?;
    let settings=sqlx::query("SELECT current_setting('server_version_num')::integer AS version,current_setting('wal_level') AS wal_level,current_setting('server_encoding') AS encoding,(SELECT oid::bigint FROM pg_database WHERE datname=current_database()) AS database_oid")
        .fetch_one(&mut sql).await?;
    let version: i32 = settings.try_get("version")?;
    if version / 10000 != i32::from(expected_major)
        || settings.try_get::<String, _>("wal_level")? != "logical"
        || settings.try_get::<String, _>("encoding")? != "UTF8"
    {
        return Err(invalid(
            "requires PostgreSQL 15, 16, or 17, wal_level=logical and UTF8 database encoding",
        ));
    }
    let tables = catalog::load(&mut sql, &config.publication, expected_major).await?;
    let slot=sqlx::query("SELECT database,plugin,slot_type,active,temporary,wal_status,confirmed_flush_lsn::text AS confirmed FROM pg_replication_slots WHERE slot_name=$1")
        .bind(&config.slot).fetch_optional(&mut sql).await?;
    if config.create_slot && slot.is_some() {
        return Err(invalid("slot already exists; omit --create-slot to resume"));
    }
    if !config.create_slot && slot.is_none() {
        return Err(invalid(
            "slot is missing; refusing to recreate it during resume (use --create-slot only for first initialization)",
        ));
    }
    let confirmed = if let Some(slot) = slot {
        if slot.try_get::<Option<String>, _>("database")?.as_deref() != Some(&config.database)
            || slot.try_get::<Option<String>, _>("plugin")?.as_deref() != Some("pgoutput")
            || slot.try_get::<String, _>("slot_type")? != "logical"
            || slot.try_get::<bool, _>("active")?
            || slot.try_get::<bool, _>("temporary")?
            || !matches!(
                slot.try_get::<Option<String>, _>("wal_status")?.as_deref(),
                Some("reserved" | "extended")
            )
        {
            return Err(invalid(
                "slot is active, invalid, temporary, or bound to another database/plugin",
            ));
        }
        pg_walstream::parse_lsn(
            &slot
                .try_get::<Option<String>, _>("confirmed")?
                .ok_or_else(|| invalid("slot has no confirmed position"))?,
        )?
    } else {
        0
    };
    let info = format!(
        "host={} port={} dbname={} user={} password={} replication=database sslmode=prefer connect_timeout=10",
        conn_value(&config.host),
        config.port,
        conn_value(&config.database),
        conn_value(&config.username),
        conn_value(&config.password)
    );
    let mut connection = PgReplicationConnection::connect(&info)?;
    let identified = connection.identify_system()?;
    let system = identified
        .get_value(0, 0)
        .ok_or_else(|| invalid("IDENTIFY_SYSTEM missing system id"))?;
    let timeline = identified
        .get_value(0, 1)
        .ok_or_else(|| invalid("IDENTIFY_SYSTEM missing timeline"))?;
    if identified.get_value(0, 3).as_deref() != Some(config.database.as_str())
        || connection.server_version() != version
    {
        return Err(invalid(
            "SQL and replication connections identify different sources",
        ));
    }
    let source = Source {
        kind: "postgresql".into(),
        version: format!("{expected_major}.{}", version % 10000),
        id: format!(
            "postgresql:{system}:{timeline}:{}:{}",
            settings.try_get::<i64, _>("database_oid")?,
            URL_SAFE_NO_PAD.encode(config.database.as_bytes())
        ),
    };
    if let Some(expected) = &config.expected_source_id
        && expected != &source.id
    {
        return Err(invalid("source identity changed; refusing cursor reuse"));
    }
    // These session settings must run on the replication connection itself.
    for setting in [
        "SET client_encoding='UTF8'",
        "SET DateStyle='ISO, YMD'",
        "SET TimeZone='UTC'",
        "SET bytea_output='hex'",
        "SET extra_float_digits=3",
        "SET row_security=off",
    ] {
        connection.exec(setting)?;
    }
    let start = if config.create_slot {
        let result = connection.create_replication_slot_with_options(
            &config.slot,
            SlotType::Logical,
            Some("pgoutput"),
            &ReplicationSlotOptions {
                snapshot: Some("nothing".into()),
                ..Default::default()
            },
        )?;
        pg_walstream::parse_lsn(
            &result
                .get_value(0, 1)
                .ok_or_else(|| invalid("CREATE_REPLICATION_SLOT missing consistent point"))?,
        )?
    } else if let Some(lsn) = &config.start_lsn {
        let requested = pg_walstream::parse_lsn(lsn)?;
        if requested < confirmed {
            return Err(invalid(
                "requested LSN predates confirmed_flush_lsn; the slot can no longer guarantee replay from that position",
            ));
        }
        requested
    } else {
        confirmed
    };
    if start == 0 {
        return Err(invalid("a nonzero starting LSN is required"));
    }
    let publication = format!("\"{}\"", config.publication.replace('"', "\"\""));
    connection.start_replication(
        &config.slot,
        start,
        &[
            ("proto_version", "1"),
            ("publication_names", &publication),
            ("binary", "false"),
            ("messages", "false"),
        ],
    )?;
    sql.close().await?;
    Ok(Replication {
        connection,
        parser: LogicalReplicationParser::with_protocol_version(1),
        decoder: Decoder::new(
            source,
            config.database,
            tables,
            config.max_transaction_bytes,
        ),
        start,
        acknowledged: 0,
        delivered: None,
        feedback_at: Instant::now(),
        failed: false,
    })
}
impl Replication {
    pub fn source(&self) -> &Source {
        &self.decoder.source
    }
    pub fn start_lsn(&self) -> String {
        pg_walstream::format_lsn(self.start)
    }
    /// Returns only complete validated transactions. Cancellation or any failure poisons this stream.
    pub async fn next_transaction(
        &mut self,
        cancel: &CancellationToken,
    ) -> Result<ValidatedTransaction> {
        if self.failed {
            return Err(invalid(
                "capture stopped after an earlier error/cancellation; reopen the existing slot",
            ));
        }
        let result = self.read_transaction(cancel).await;
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    async fn read_transaction(
        &mut self,
        cancel: &CancellationToken,
    ) -> Result<ValidatedTransaction> {
        loop {
            // The library requires cancellation through this token, not dropping the read future.
            let data = self.connection.get_copy_data_async(cancel).await?;
            if data.is_empty() {
                return Err(invalid("replication stream ended"));
            }
            if data.len() > 64 * 1024 * 1024 {
                return Err(invalid("replication message exceeds 64 MiB"));
            }
            if data[0] == b'k' {
                let keepalive = pg_walstream::parse_keepalive_message(&data)?;
                if keepalive.reply_requested || self.feedback_at.elapsed() >= Duration::from_secs(5)
                {
                    self.feedback().await?;
                }
                continue;
            }
            if data[0] != b'w' || data.len() < 26 {
                return Err(invalid("invalid XLogData frame"));
            }
            let message = self.parser.parse_wal_message_bytes(data.slice(25..))?;
            if self.feedback_at.elapsed() >= Duration::from_secs(5) {
                self.feedback().await?;
            }
            if let Some(tx) = self.decoder.push(message.message, data.len())? {
                self.delivered = Some(tx.transaction().commit_cursor.clone());
                return Ok(tx);
            }
        }
    }
    /// Call ONLY after durably persisting this complete transaction and its checkpoint.
    /// JSON/console tailing deliberately never calls this.
    pub async fn acknowledge(&mut self, transaction: &ValidatedTransaction) -> Result<()> {
        let tx = transaction.transaction();
        if self.failed
            || tx.source != self.decoder.source
            || self.delivered.as_ref() != Some(&tx.commit_cursor)
        {
            return Err(invalid(
                "can only acknowledge this stream's most recently delivered complete transaction",
            ));
        }
        let lsn = pg_walstream::parse_lsn(&tx.commit_cursor.value)?;
        if lsn < self.acknowledged {
            return Err(invalid("acknowledgement moved backwards"));
        }
        self.acknowledged = lsn;
        self.feedback().await
    }
    async fn feedback(&mut self) -> Result<()> {
        // Never use server wal_end as a durable-consumer checkpoint.
        self.connection
            .send_standby_status_update(
                self.acknowledged,
                self.acknowledged,
                self.acknowledged,
                false,
            )
            .await?;
        self.feedback_at = Instant::now();
        Ok(())
    }
}

/// Synchronous SourceAdapter bridge for workers that use the database-neutral
/// blocking boundary. PostgreSQL's protocol reader remains async internally;
/// each boundary call drives it on a helper thread.
impl change_event::SourceAdapter for Replication {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn next_transaction(
        &mut self,
    ) -> std::result::Result<Option<ValidatedTransaction>, Self::Error> {
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|error| Box::new(error) as Self::Error)?;
                    let cancel = CancellationToken::new();
                    runtime
                        .block_on(Replication::next_transaction(self, &cancel))
                        .map(Some)
                })
                .join()
                .map_err(|_| invalid("PostgreSQL SourceAdapter worker panicked"))?
        })
    }
}
fn conn_value(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}
