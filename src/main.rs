use chrono::Utc;
use clap::Parser;
use log::{debug, error, info, warn};
use serde::Deserialize;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use tokio::task;
use tokio_postgres::{Client, Config, Error as PgError, NoTls};

// Configuration structs
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

// Command-line arguments
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the configuration file
    #[arg(short, long, default_value = "waldo.yml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init();

    info!("Starting Waldo database cloning tool");

    // Load configuration
    let config = load_config()?;
    info!("Configuration loaded successfully");

    // Connect to databases
    let (remote_client, mut local_client) = connect_databases(&config).await?;
    info!("Connected to remote and local databases");

    // Clone tables
    for table in &config.tables {
        info!("Starting to clone table: {}", table);
        match clone_table(
            &remote_client,
            &mut local_client,
            table,
            &config.remote,
            &config.local,
        )
        .await
        {
            Ok(_) => info!("Table '{}' cloned successfully", table),
            Err(e) => {
                error!("Failed to clone table '{}': {}", table, e);
                continue;
            }
        }
    }

    // Start tailing all tables after cloning is complete
    start_wal_tailing(
        config.remote.clone(),
        config.local.clone(),
        config.tables.clone(),
    )
    .await;

    // Keep the main thread running
    info!("Waldo is now running. Press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await?;
    info!("Shutting down Waldo");

    Ok(())
}

async fn start_wal_tailing(
    remote_config: DatabaseConfig,
    local_config: DatabaseConfig,
    tables: Vec<String>,
) {
    for table in tables {
        let remote_config = remote_config.clone();
        let local_config = local_config.clone();
        let table_clone = table.clone(); // Clone the table name
        tokio::spawn(async move {
            tail_table(remote_config, local_config, table_clone).await;
        });
        info!("Started tailing table: {}", table);
    }
}

// Load configuration from file
fn load_config() -> Result<WaldoConfig, Box<dyn std::error::Error>> {
    let args = Args::parse();
    debug!("Loading configuration from file: {:?}", args.config);
    let config_content = fs::read_to_string(args.config)?;
    Ok(serde_yml::from_str(&config_content)?)
}

// Connect to both remote and local databases
async fn connect_databases(config: &WaldoConfig) -> Result<(Client, Client), PgError> {
    debug!("Connecting to remote database");
    let remote_client = connect_to_database(&config.remote, "Remote").await?;
    debug!("Connecting to local database");
    let local_client = connect_to_database(&config.local, "Local").await?;
    Ok((remote_client, local_client))
}

// Connect to a single database
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

    debug!("Establishing connection to {} database", label);
    let (client, connection) = connect_config.connect(NoTls).await?;

    let label = label.to_string();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("{} database connection error: {}", label, e);
        }
    });

    Ok(client)
}

// Clone a single table from remote to local
async fn clone_table(
    remote_client: &Client,
    local_client: &mut Client,
    table_name: &str,
    remote_config: &DatabaseConfig,
    local_config: &DatabaseConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Cloning table: {}", table_name);

    // Fetch remote schema
    let remote_schema = fetch_table_schema(remote_client, table_name).await?;
    debug!("Fetched remote schema for table: {}", table_name);

    if table_exists(local_client, table_name).await? {
        let local_schema = fetch_table_schema(local_client, table_name).await?;
        debug!("Local table '{}' exists, comparing schemas", table_name);

        if !schemas_match(&remote_schema, &local_schema) {
            warn!("Schemas don't match for table: {}", table_name);
            return Err(format!(
                "Table '{}' exists but schemas don't match. Manual intervention required.",
                table_name
            )
            .into());
        }

        // Truncate the existing table
        local_client
            .execute(&format!("TRUNCATE TABLE {}", table_name), &[])
            .await?;
        info!("Existing data in table '{}' has been truncated", table_name);
    } else {
        create_table(local_client, table_name, &remote_schema).await?;
        info!("Table '{}' created successfully", table_name);
    }

    copy_table_data(table_name, remote_config, local_config).await?;
    info!("Data copied successfully for table '{}'", table_name);
    Ok(())
}

// Check if a table exists in the database
async fn table_exists(client: &Client, table_name: &str) -> Result<bool, PgError> {
    let query = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)";
    let exists: bool = client.query_one(query, &[&table_name]).await?.get(0);
    debug!("Checked existence of table '{}': {}", table_name, exists);
    Ok(exists)
}

// Fetch the schema of a table
async fn fetch_table_schema(
    client: &Client,
    table_name: &str,
) -> Result<Vec<(String, String)>, PgError> {
    let schema_query = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position";
    let rows = client.query(schema_query, &[&table_name]).await?;
    let schema: Vec<(String, String)> = rows
        .into_iter()
        .map(|row| (row.get("column_name"), row.get("data_type")))
        .collect();
    debug!("Fetched schema for table '{}': {:?}", table_name, schema);
    Ok(schema)
}

// Compare two schemas
fn schemas_match(schema1: &[(String, String)], schema2: &[(String, String)]) -> bool {
    let match_result = schema1.len() == schema2.len()
        && schema1
            .iter()
            .zip(schema2.iter())
            .all(|((name1, type1), (name2, type2))| name1 == name2 && type1 == type2);
    debug!("Schema match result: {}", match_result);
    match_result
}

// Create a new table
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
    debug!(
        "Creating table '{}' with query: {}",
        table_name, create_table_query
    );
    client.execute(&create_table_query, &[]).await?;
    Ok(())
}

// Copy data from remote table to local table using pg_dump and pg_restore
async fn copy_table_data(
    table_name: &str,
    remote_config: &DatabaseConfig,
    local_config: &DatabaseConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
    let dump_file = format!("/tmp/{}_dump_{}.sql", table_name, timestamp);

    let remote_conn_string = create_connection_string(remote_config);
    let local_conn_string = create_connection_string(local_config);

    // Dump the table from the source database
    dump_table(&remote_conn_string, table_name, &dump_file).await?;

    // Restore the table to the destination database
    restore_table(&local_conn_string, &dump_file).await?;

    // Clean up dump file
    std::fs::remove_file(&dump_file)?;
    debug!("Cleaned up dump file for table '{}'", table_name);

    Ok(())
}

// Create a connection string for a database
fn create_connection_string(config: &DatabaseConfig) -> String {
    format!(
        "postgresql://{}:{}@{}:{}/{}",
        config.user, config.password, config.host, config.port, config.dbname
    )
}

// Dump a table from the source database to a file
async fn dump_table(
    conn_string: &str,
    table_name: &str,
    dump_file: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Starting pg_dump for table '{}'. This may take a while...",
        table_name
    );

    let output = Command::new("pg_dump")
        .args([
            "-t",
            table_name,
            "--data-only",
            "-f",
            dump_file,
            conn_string,
        ])
        .output()?;

    if !output.status.success() {
        error!(
            "pg_dump failed for table '{}': {}",
            table_name,
            String::from_utf8_lossy(&output.stderr)
        );
        return Err("pg_dump failed".into());
    }

    info!(
        "Table '{}' successfully dumped to {}",
        table_name, dump_file
    );
    Ok(())
}

// Restore a table from a file to the destination database
async fn restore_table(
    conn_string: &str,
    dump_file: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Starting psql restore from file '{}'. This may take a while...",
        dump_file
    );

    let output = Command::new("psql")
        .args(["-v", "ON_ERROR_STOP=1", "-f", dump_file, conn_string])
        .output()?;

    if !output.status.success() {
        error!(
            "psql restore failed for file '{}': {}",
            dump_file,
            String::from_utf8_lossy(&output.stderr)
        );
        return Err("psql restore failed".into());
    }

    info!(
        "Table successfully restored to destination database from file '{}'",
        dump_file
    );
    Ok(())
}

// Tail a table for changes
#[allow(dead_code)]
async fn tail_table(
    remote_config: DatabaseConfig,
    local_config: DatabaseConfig,
    table_name: String,
) {
    // ... rest of the function remains the same ...
}
