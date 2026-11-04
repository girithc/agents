use agenthub_api::ServeConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let host = std::env::var("AGENTHUB_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port: u16 = std::env::var("AGENTHUB_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);
    let data_dir = std::env::var("AGENTHUB_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("./data"));

    agenthub_api::serve(ServeConfig {
        host,
        port,
        data_dir,
    })
    .await
}
