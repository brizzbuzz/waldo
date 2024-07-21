use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use clap::Parser;
use log::{debug, error, info, warn};
use postgres_types::{ToSql, Type as PgType};
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;
use tokio_postgres::{Client, Config, Error as PgError, NoTls, Row};
use uuid::Uuid;

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
        match clone_table(&remote_client, &mut local_client, table).await {
            Ok(_) => info!("Table '{}' cloned successfully", table),
            Err(e) => {
                error!("Failed to clone table '{}': {}", table, e);
                continue;
            }
        }

        // Start tailing the table
        let remote_config = config.remote.clone();
        let local_config = config.local.clone();
        tokio::spawn(tail_table(remote_config, local_config, table.to_string()));
        info!("Started tailing table: {}", table);
    }

    // Keep the main thread running
    info!("Waldo is now running. Press Ctrl+C to stop.");
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
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

    copy_table_data(remote_client, local_client, table_name).await?;
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

// Copy data from remote table to local table
async fn copy_table_data(
    remote_client: &Client,
    local_client: &mut Client,
    table_name: &str,
) -> Result<(), PgError> {
    let row_count_query = format!("SELECT COUNT(*) FROM {}", table_name);
    let row_count: i64 = remote_client.query_one(&row_count_query, &[]).await?.get(0);
    debug!(
        "Fetched row count for table '{}': {}",
        table_name, row_count
    );

    if row_count == 0 {
        info!("No data to copy for table '{}'", table_name);
        return Ok(());
    }

    // TODO: Pagination

    let select_query = format!("SELECT * FROM {}", table_name);
    let rows = remote_client.query(&select_query, &[]).await?;

    if rows.is_empty() {
        info!("No data to copy for table: {}", table_name);
        return Ok(());
    }

    let columns = rows[0].columns();
    let column_count = columns.len();
    let insert_query = format!(
        "INSERT INTO {} VALUES ({})",
        table_name,
        (1..=column_count)
            .map(|i| format!("${}", i))
            .collect::<Vec<_>>()
            .join(", ")
    );

    debug!("Preparing insert statement for table '{}'", table_name);
    let stmt = local_client.prepare(&insert_query).await?;

    debug!("Starting transaction for table '{}'", table_name);
    let tx = local_client.transaction().await?;

    for (i, row) in rows.iter().enumerate() {
        debug!("Processing row {} for table '{}'", i + 1, table_name);
        let params: Vec<Box<dyn ToSql + Sync>> = columns
            .iter()
            .enumerate()
            .map(|(idx, column)| as_sql_type(row, idx, column.type_()))
            .collect();
        tx.execute(
            &stmt,
            &params
                .iter()
                .map(|p| &**p as &(dyn ToSql + Sync))
                .collect::<Vec<_>>(),
        )
        .await?;
        if (i + 1) % 1000 == 0 {
            debug!("Copied {} rows for table '{}'", i + 1, table_name);
        }
    }

    debug!("Committing transaction for table '{}'", table_name);
    tx.commit().await?;

    info!("Copied {} rows for table '{}'", rows.len(), table_name);
    Ok(())
}

// Convert a Postgres value to a Rust type
fn as_sql_type(row: &Row, idx: usize, ty: &PgType) -> Box<dyn ToSql + Sync> {
    match *ty {
        PgType::BOOL => Box::new(row.get::<_, Option<bool>>(idx)),
        PgType::INT2 => Box::new(row.get::<_, Option<i16>>(idx)),
        PgType::INT4 => Box::new(row.get::<_, Option<i32>>(idx)),
        PgType::INT8 => Box::new(row.get::<_, Option<i64>>(idx)),
        PgType::FLOAT4 => Box::new(row.get::<_, Option<f32>>(idx)),
        PgType::FLOAT8 => Box::new(row.get::<_, Option<f64>>(idx)),
        PgType::VARCHAR | PgType::TEXT => Box::new(row.get::<_, Option<String>>(idx)),
        PgType::UUID => Box::new(row.get::<_, Option<Uuid>>(idx)),
        PgType::TIMESTAMP => Box::new(row.get::<_, Option<NaiveDateTime>>(idx)),
        PgType::TIMESTAMPTZ => Box::new(row.get::<_, Option<DateTime<Utc>>>(idx)),
        PgType::DATE => Box::new(row.get::<_, Option<NaiveDate>>(idx)),
        PgType::BYTEA => Box::new(row.get::<_, Option<Vec<u8>>>(idx)),
        PgType::JSON => Box::new(row.get::<_, Option<serde_json::Value>>(idx)),
        PgType::JSONB => Box::new(row.get::<_, Option<serde_json::Value>>(idx)),
        _ => {
            warn!("Unsupported type: {:?}", ty);
            Box::new(None::<&str>)
        }
    }
}

// Tail a table for changes (placeholder function)
async fn tail_table(
    remote_config: DatabaseConfig,
    local_config: DatabaseConfig,
    table_name: String,
) {
    loop {
        debug!("Tailing table: {}", table_name);
        // Implement your WAL replication logic here
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    }
}
