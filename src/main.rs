use clap::Parser;
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;
use tokio_postgres::{Client, Config, Error as PgError, NoTls};

#[derive(Deserialize, Debug, Clone)]
struct DatabaseConfig {
    host: String,
    port: u16,
    dbname: String,
    user: String,
    password: String,
    #[serde(default)]
    connection_string: Option<String>,
}

#[derive(Deserialize, Debug)]
struct WaldoConfig {
    remote: DatabaseConfig,
    local: DatabaseConfig,
    tables: Vec<String>,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the configuration file
    #[arg(short, long, default_value = "waldo.yml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = load_config()?;
    let (remote_client, local_client) = connect_databases(&config).await?;

    for table in &config.tables {
        clone_table(&remote_client, &local_client, table).await?;
        println!("Table '{}' cloned successfully", table);

        let remote_config = config.remote.clone();
        let local_config = config.local.clone();
        tokio::spawn(tail_table(remote_config, local_config, table.to_string()));
    }

    // Keep the main thread running
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
    }
}

fn load_config() -> Result<WaldoConfig, Box<dyn std::error::Error>> {
    let args = Args::parse();
    let config_content = fs::read_to_string(args.config)?;
    Ok(serde_yml::from_str(&config_content)?)
}

async fn connect_databases(config: &WaldoConfig) -> Result<(Client, Client), PgError> {
    let remote_client = connect_to_database(&config.remote, "Remote").await?;
    let local_client = connect_to_database(&config.local, "Local").await?;
    Ok((remote_client, local_client))
}

async fn connect_to_database(config: &DatabaseConfig, label: &str) -> Result<Client, PgError> {
    let connect_config = if let Some(ref conn_string) = config.connection_string {
        conn_string.parse::<Config>()?
    } else {
        Config::new()
            .host(&config.host)
            .port(config.port)
            .dbname(&config.dbname)
            .user(&config.user)
            .password(&config.password)
            .to_owned()
    };

    let (client, connection) = connect_config.connect(NoTls).await?;

    let label = label.to_string(); // Clone the label
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("{} database connection error: {}", label, e);
        }
    });

    Ok(client)
}

async fn clone_table(
    remote_client: &Client,
    local_client: &Client,
    table_name: &str,
) -> Result<(), PgError> {
    let schema = fetch_table_schema(remote_client, table_name).await?;
    create_table(local_client, table_name, &schema).await?;
    copy_table_data(remote_client, local_client, table_name).await?;
    Ok(())
}

async fn fetch_table_schema(
    client: &Client,
    table_name: &str,
) -> Result<Vec<(String, String)>, PgError> {
    let schema_query = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position";
    let rows = client.query(schema_query, &[&table_name]).await?;
    Ok(rows
        .into_iter()
        .map(|row| (row.get("column_name"), row.get("data_type")))
        .collect())
}

async fn create_table(
    client: &Client,
    table_name: &str,
    schema: &[(String, String)],
) -> Result<(), PgError> {
    let create_table_query = format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        table_name,
        schema
            .iter()
            .map(|(name, type_)| format!("{} {}", name, type_))
            .collect::<Vec<_>>()
            .join(", ")
    );
    client.execute(&create_table_query, &[]).await?;
    Ok(())
}

async fn copy_table_data(
    remote_client: &Client,
    local_client: &Client,
    table_name: &str,
) -> Result<(), PgError> {
    let copy_data_query = format!("INSERT INTO {} SELECT * FROM {}", table_name, table_name);
    local_client.execute(&copy_data_query, &[]).await?;
    Ok(())
}

async fn tail_table(
    remote_config: DatabaseConfig,
    local_config: DatabaseConfig,
    table_name: String,
) {
    loop {
        println!("Tailing table: {}", table_name);
        // Implement your WAL replication logic here
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    }
}
