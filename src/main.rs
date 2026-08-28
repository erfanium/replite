#[tokio::main]
async fn main() -> anyhow::Result<()> {
    replite::serve().await
}
