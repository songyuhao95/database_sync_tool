use aes_gcm::aead::{OsRng, rand_core::RngCore};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use std::{
    env,
    fs::{self, OpenOptions},
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};
use web_ui::{Store, WebConfig, router};

fn load_key(database: &Path) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let path = database.with_extension("key");
    let encoded = if let Ok(value) = env::var("CDC_WEB_SECRET_KEY") {
        value
    } else if path.exists() {
        fs::read_to_string(&path)?
    } else {
        if database.exists() {
            return Err("SQLite 已存在但密钥文件丢失；请恢复原密钥".into());
        }
        let mut key = [0u8; 32];
        OsRng.fill_bytes(&mut key);
        let value = URL_SAFE_NO_PAD.encode(key);
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&path)?;
        file.write_all(value.as_bytes())?;
        file.sync_all()?;
        value
    };
    URL_SAFE_NO_PAD
        .decode(encoded.trim())?
        .try_into()
        .map_err(|_| "密钥必须是 URL-safe base64 编码的 32 个随机字节".into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind: SocketAddr = env::var("CDC_WEB_BIND")
        .unwrap_or_else(|_| "127.0.0.1:8080".into())
        .parse()?;
    let origin = env::var("CDC_WEB_ORIGIN").unwrap_or_else(|_| format!("http://{bind}"));
    if !bind.ip().is_loopback() && !origin.starts_with("https://") {
        return Err("非本机访问必须通过 HTTPS 反向代理，并设置 CDC_WEB_ORIGIN".into());
    }
    let database =
        PathBuf::from(env::var("CDC_WEB_DB").unwrap_or_else(|_| "data/web-ui.sqlite".into()));
    if let Some(parent) = database.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    let store = Arc::new(Store::open(&database, load_key(&database)?)?);
    if store.needs_admin()? {
        store.bootstrap_default_admin()?;
        println!("[web] default administrator created: admin / admin; change it after login");
    }
    let app = router(
        store.clone(),
        WebConfig {
            origin: origin.clone(),
        },
    )?;
    let listener = tokio::net::TcpListener::bind(bind).await?;
    println!("[web] {origin}  SQLite={}", database.display());
    let startup_store = store.clone();
    match tokio::task::spawn_blocking(move || startup_store.start_auto_tasks()).await? {
        Ok(count) => println!("[web] dispatched {count} auto-start task(s)"),
        Err(error) => eprintln!("[web] auto-start dispatch failed: {error}"),
    }
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    tokio::task::spawn_blocking(move || store.shutdown_tasks()).await?;
    Ok(())
}
