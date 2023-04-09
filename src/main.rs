use std::{collections::HashMap, sync::Arc, time::Duration};

use clap::Parser;
use clokwerk::{AsyncScheduler, TimeUnits};
use log::{debug, info, warn};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

mod command;
mod errors;
mod storage;

#[derive(Parser, Debug)]
struct Args {
    // Port to run the server
    #[arg(short, long, default_value_t = 7777)]
    port: u16,

    // Number of shards
    #[arg(short, long, default_value_t = 8)]
    shards: usize,

    // Enable the background job to save the data to disk
    #[arg(short, long, default_value_t = false)]
    enable_save_job: bool,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Args::parse();

    let database = storage::create_sharded_database(args.shards);

    if args.enable_save_job {
        enable_save_job(database.clone());
    }

    let listener = start_server(&args).await;
    handle_connections(listener, database).await;
}

fn enable_save_job(database: Arc<Vec<Mutex<HashMap<String, String>>>>) {
    let mut scheduler = AsyncScheduler::new();
    // TODO Allow this to be configurable
    scheduler.every(1.hours()).run(move || {
        // Clones the database arc, and moves the cloned arc to the
        // async block, this way the arc cloned each time in the async
        // block is a clone of the first clone and the original
        // database isn't dropped.
        let database = database.clone();
        async move {
            storage::save(database).await;
        }
    });

    // Spawns a thread on background to dump the database to a file
    // from time to time.
    tokio::spawn(async move {
        loop {
            scheduler.run_pending().await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
}

async fn start_server(args: &Args) -> TcpListener {
    info!("Ssache is starting");

    let port = args.port;
    let listener = match TcpListener::bind(format!("127.0.0.1:{port}")).await {
        Ok(listener) => listener,
        Err(e) => panic!("Unable to start ssache on port {port}. Error = {:?}", e),
    };

    info!("Ssache is ready to accept connections on port {port}");

    listener
}

async fn handle_connections(
    listener: TcpListener,
    database: Arc<Vec<Mutex<HashMap<String, String>>>>,
) {
    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                let database_clone = database.clone();
                tokio::spawn(async move {
                    process_connection_loop(database_clone, &mut stream).await;
                });
            }
            Err(e) => warn!("Error listening to socket, {}", e),
        }
    }
}

/// Generates an infinte loop with the connection to handle the
/// requests. The loop is only broken if the request is an empty
/// stream.
async fn process_connection_loop(
    database: Arc<Vec<Mutex<HashMap<String, String>>>>,
    stream: &mut TcpStream,
) {
    loop {
        let database_clone = database.clone();
        match handle_request(stream, database_clone).await {
            Ok(_) => continue,
            Err(e) => {
                match e {
                    errors::SsacheErrorKind::NoDataReceived => break,
                    _ => warn!("Error executing stream"),
                };
            }
        }
    }
}

const CRLF: &str = "\r\n";

async fn handle_request(
    mut stream: &mut TcpStream,
    database: Arc<Vec<Mutex<HashMap<String, String>>>>,
) -> Result<(), errors::SsacheErrorKind> {
    let buf_reader = BufReader::new(&mut stream);
    let command_line = parse_command_line_from_stream(buf_reader).await;
    if command_line.is_err() {
        debug!("No data received");
        return Err(command_line.err().unwrap());
    }

    let command = command::parse_command(command_line.unwrap());

    if let Err(e) = command {
        return match e {
            errors::SsacheErrorKind::NotEnoughParameters { message } => {
                stream.write_all(message.as_bytes()).await.unwrap();
                Err(errors::SsacheErrorKind::NotEnoughParameters { message })
            }
            _ => return Err(e),
        };
    }

    let command = command.unwrap();
    match command {
        command::Command::Get { key } => {
            let response = storage::get(database, key).await;
            stream.write_all(response.as_bytes()).await.unwrap();
            Ok(())
        }
        command::Command::Set { key, value } => {
            let response = storage::set(database, key, value).await;
            stream.write_all(response.as_bytes()).await.unwrap();
            Ok(())
        }
        command::Command::Expire {
            key: _key,
            time: _time,
        } => {
            debug!("WIP");
            let response = format!("+OK{CRLF}");
            stream.write_all(response.as_bytes()).await.unwrap();
            Ok(())
        }
        command::Command::Save => {
            let response = storage::save(database).await;
            stream.write_all(response.as_bytes()).await.unwrap();
            Ok(())
        }
        command::Command::Load => {
            let response = storage::load(database).await;
            stream.write_all(response.as_bytes()).await.unwrap();
            Ok(())
        }
        command::Command::Quit => {
            let response = format!("+OK{CRLF}");
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();
            Ok(())
        }
        command::Command::Ping { message } => {
            let size = message.len();
            let response = if size == 0 {
                format!("+PONG{CRLF}")
            } else {
                format!("${size}{CRLF}+{message}{CRLF}")
            };
            stream.write_all(response.as_bytes()).await.unwrap();
            Ok(())
        }
        command::Command::Unknown => {
            debug!("Unknown command");
            let response = format!("-ERROR unknown command{CRLF}");
            stream.write_all(response.as_bytes()).await.unwrap();
            Ok(())
        }
    }
}

async fn parse_command_line_from_stream(
    mut buf_reader: BufReader<&mut &mut TcpStream>,
) -> Result<Vec<String>, errors::SsacheErrorKind> {
    let mut command_line = String::new();
    let result = buf_reader.read_line(&mut command_line).await;
    if result.is_err() {
        return Err(errors::SsacheErrorKind::NoDataReceived);
    }
    let command_line = command_line.split_whitespace();
    let command_line: Vec<String> = command_line
        .into_iter()
        .map(|slice| slice.to_string())
        .collect();
    if command_line.get(0).is_none() {
        return Err(errors::SsacheErrorKind::NoDataReceived);
    }

    Ok(command_line)
}
