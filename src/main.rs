use clap::Parser;
use postgres_types::{ToSql, Type as PgType};
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;
use tokio_postgres::{Client, Config, Error as PgError, NoTls, Row};

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
    let (remote_client, mut local_client) = connect_databases(&config).await?;

    for table in &config.tables {
        clone_table(&remote_client, &mut local_client, table).await?;
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
    local_client: &mut Client,
    table_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let remote_schema = fetch_table_schema(remote_client, table_name).await?;

    if table_exists(local_client, table_name).await? {
        let local_schema = fetch_table_schema(local_client, table_name).await?;

        if !schemas_match(&remote_schema, &local_schema) {
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
        println!(
            "Existing data in table '{}' has been truncated.",
            table_name
        );
    } else {
        create_table(local_client, table_name, &remote_schema).await?;
        println!("Table '{}' created successfully.", table_name);
    }

    copy_table_data(remote_client, local_client, table_name).await?;
    println!("Data copied successfully for table '{}'.", table_name);
    Ok(())
}

async fn table_exists(client: &Client, table_name: &str) -> Result<bool, PgError> {
    let query = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)";
    let exists: bool = client.query_one(query, &[&table_name]).await?.get(0);
    Ok(exists)
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

fn schemas_match(schema1: &[(String, String)], schema2: &[(String, String)]) -> bool {
    if schema1.len() != schema2.len() {
        return false;
    }

    schema1
        .iter()
        .zip(schema2.iter())
        .all(|((name1, type1), (name2, type2))| name1 == name2 && type1 == type2)
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
    local_client: &mut Client,
    table_name: &str,
) -> Result<(), PgError> {
    let select_query = format!("SELECT * FROM {}", table_name);
    let rows = remote_client.query(&select_query, &[]).await?;

    if rows.is_empty() {
        println!("No data to copy for table: {}", table_name);
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

    // Prepare the statement
    let stmt = local_client.prepare(&insert_query).await?;

    // Start a transaction
    let tx = local_client.transaction().await?;

    // Copy the data
    for row in &rows {
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
    }

    // Commit the transaction
    tx.commit().await?;

    Ok(())
}

fn as_sql_type(row: &Row, idx: usize, ty: &PgType) -> Box<dyn ToSql + Sync> {
    match *ty {
        PgType::BOOL => Box::new(row.get::<_, bool>(idx)),
        PgType::INT2 => Box::new(row.get::<_, i16>(idx)),
        PgType::INT4 => Box::new(row.get::<_, i32>(idx)),
        PgType::INT8 => Box::new(row.get::<_, i64>(idx)),
        PgType::FLOAT4 => Box::new(row.get::<_, f32>(idx)),
        PgType::FLOAT8 => Box::new(row.get::<_, f64>(idx)),
        PgType::VARCHAR | PgType::TEXT => Box::new(row.get::<_, String>(idx)),
        // PgType::UUID => Box::new(row.get::<_, Uuid>(idx)),
        // PgType::TIMESTAMP => Box::new(row.get::<_, NaiveDateTime>(idx)),
        // PgType::TIMESTAMPTZ => Box::new(row.get::<_, DateTime<Utc>>(idx)),
        // PgType::DATE => Box::new(row.get::<_, NaiveDate>(idx)),
        // PgType::BYTEA => Box::new(row.get::<_, Vec<u8>>(idx)),
        // Add more types as needed
        _ => panic!("Unsupported type: {:?}", ty),
    }
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
