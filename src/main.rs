use std::process::exit;
use std::sync::mpsc;
use std::{sync::Arc, time::Duration};

use clap::Parser;
use clokwerk::{AsyncScheduler, TimeUnits};
use log::{debug, info, trace, warn};
use storage::ShardedStorage;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

mod command;
mod errors;
mod storage;

#[derive(Parser, Debug)]
struct Args {
    /// Port to run the server
    #[arg(short, long, default_value_t = 7777)]
    port: u16,

    /// Number of shards
    #[arg(short, long, default_value_t = 8)]
    shards: usize,

    /// Enable the scheduled background job to save the data to disk
    #[arg(short, long, default_value_t = false)]
    enable_scheduled_save: bool,

    /// The interval of the scheduled save job in minutes
    #[arg(long, default_value_t = 60)]
    save_job_interval: u32,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Args::parse();

    let storage = Arc::new(ShardedStorage::new(args.shards));

    if args.enable_scheduled_save {
        enable_scheduled_save_job(storage.clone(), &args);
    }

    tokio::spawn(async move {
        let (tx, rx) = mpsc::channel();

        ctrlc::set_handler(move || tx.send(()).expect("Could not send signal on channel."))
            .expect("Error setting Ctrl-C handler");

        rx.recv().expect("Could not receive from channel.");
        exit(0);
    });

    enable_expiration_job(storage.clone());

    let listener = start_server(&args).await;
    handle_connections(listener, storage).await;
}

fn enable_expiration_job(storage: Arc<ShardedStorage>) {
    let mut scheduler = AsyncScheduler::new();
    scheduler.every(1.seconds()).run(move || {
        let storage = storage.clone();
        async move {
            trace!("Checking for expired keys");
            storage.check_expirations().await;
            storage.remove_expiration().await;
        }
    });

    tokio::spawn(async move {
        loop {
            scheduler.run_pending().await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
}

fn enable_scheduled_save_job(storage: Arc<ShardedStorage>, args: &Args) {
    let mut scheduler = AsyncScheduler::new();
    scheduler
        .every(args.save_job_interval.minutes())
        .run(move || {
            // Clones the storage arc, and moves the cloned arc to the
            // async block, this way the arc cloned each time in the async
            // block is a clone of the first clone and the original
            // storage isn't dropped.
            let storage = storage.clone();
            async move {
                // Ignores if there are any errors
                let _ = storage.save().await;
            }
        });

    // Spawns a thread on background to dump the in-memory storage to
    // a file from time to time.
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

async fn handle_connections(listener: TcpListener, storage: Arc<ShardedStorage>) {
    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                let storage_clone = storage.clone();
                tokio::spawn(async move {
                    process_connection_loop(storage_clone, &mut stream).await;
                });
            }
            Err(e) => warn!("Error listening to socket, {}", e),
        }
    }
}

/// Generates an infinte loop with the connection to handle the
/// requests. The loop is only broken if the request is an empty
/// stream.
async fn process_connection_loop(storage: Arc<ShardedStorage>, stream: &mut TcpStream) {
    loop {
        let storage_clone = storage.clone();
        match handle_request(stream, storage_clone).await {
            Ok(_) => continue,
            Err(e) => {
                match e {
                    errors::SsacheError::NoDataReceived => break,
                    _ => warn!("Error executing stream"),
                };
            }
        }
    }
}

const CRLF: &str = "\r\n";

async fn handle_request(
    mut stream: &mut TcpStream,
    storage: Arc<ShardedStorage>,
) -> Result<(), errors::SsacheError> {
    let buf_reader = BufReader::new(&mut stream);
    let command_line = parse_command_line_from_stream(buf_reader).await?;

    let command = command::parse_command(command_line);

    if let Err(e) = command {
        return match e {
            errors::SsacheError::NotEnoughParameters { message } => {
                stream.write_all(message.as_bytes()).await.unwrap();
                Err(errors::SsacheError::NotEnoughParameters { message })
            }
            _ => return Err(e),
        };
    }

    let command = command.unwrap();
    match command {
        command::Command::Get { key } => {
            let response = match storage.get(key).await {
                Some(value) => {
                    let size = value.len();
                    format!("${size}{CRLF}+{}{CRLF}", value)
                }
                None => {
                    format!("$-1{CRLF}")
                }
            };
            stream.write_all(response.as_bytes()).await.unwrap();
            Ok(())
        }
        command::Command::Set { key, value } => {
            storage.set(key, value).await;
            let response = format!("+OK{CRLF}");
            stream.write_all(response.as_bytes()).await.unwrap();
            Ok(())
        }
        command::Command::Expire { key, time } => {
            storage.set_expiration(key, time).await;
            let response = format!("+OK{CRLF}");
            stream.write_all(response.as_bytes()).await.unwrap();
            Ok(())
        }
        command::Command::Incr { key } => {
            let response = match storage.incr(key).await {
                Ok(value) => format!(":{value}{CRLF}"),
                Err(e) => {
                    match e.kind() {
                        std::num::IntErrorKind::Empty => {
                            format!("-ERROR the value is empty, impossible to convert to a number{CRLF}")
                        }
                        std::num::IntErrorKind::InvalidDigit => {
                            format!("-ERROR the value is not a valid number{CRLF}")
                        }
                        std::num::IntErrorKind::NegOverflow => {
                            format!("-ERROR negative overflow{CRLF}")
                        }
                        std::num::IntErrorKind::PosOverflow => {
                            format!("-ERROR positive overflow{CRLF}")
                        }
                        &_ => {
                            debug!("unkwon error incrementing key {}", e);
                            format!("-ERROR unknown error {CRLF}")
                        }
                    }
                }
            };
            stream.write_all(response.as_bytes()).await.unwrap();
            Ok(())
        }
        command::Command::Decr { key } => {
            let response = match storage.decr(key).await {
                Ok(value) => format!(":{value}{CRLF}"),
                Err(e) => {
                    match e.kind() {
                        std::num::IntErrorKind::Empty => {
                            format!("-ERROR the value is empty, impossible to convert to a number{CRLF}")
                        }
                        std::num::IntErrorKind::InvalidDigit => {
                            format!("-ERROR the value is not a valid number{CRLF}")
                        }
                        std::num::IntErrorKind::NegOverflow => {
                            format!("-ERROR negative overflow{CRLF}")
                        }
                        std::num::IntErrorKind::PosOverflow => {
                            format!("-ERROR positive overflow{CRLF}")
                        }
                        &_ => {
                            debug!("unkwon error incrementing key {}", e);
                            format!("-ERROR unknown error {CRLF}")
                        }
                    }
                }
            };
            stream.write_all(response.as_bytes()).await.unwrap();
            Ok(())
        }
        command::Command::Save => {
            let response = match storage.save().await {
                Ok(()) => format!("+OK{CRLF}"),
                Err(e) => match e {
                    errors::SaveError::CreatingDump => {
                        format!("-ERROR Unable to create dump file{CRLF}")
                    }
                    errors::SaveError::SerializingIntoBinary => {
                        format!("-ERROR Unable to serialize data into binary format{CRLF}")
                    }
                    errors::SaveError::WritingDump => {
                        format!("-ERROR Unable to write the data to the dump file{CRLF}")
                    }
                },
            };
            stream.write_all(response.as_bytes()).await.unwrap();
            Ok(())
        }
        command::Command::Load => {
            let response = match storage.load().await {
                Ok(()) => format!("+OK{CRLF}"),
                Err(e) => match e {
                    errors::LoadError::DeserializingData => {
                        format!("-ERROR Unable to deserialize data into hashmap format{CRLF}")
                    }
                    errors::LoadError::ReadingDump => {
                        format!("-ERROR Unable to read dump file{CRLF}")
                    }
                },
            };
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
) -> Result<Vec<String>, errors::SsacheError> {
    let mut command_line = String::new();
    let result = buf_reader.read_line(&mut command_line).await;
    if result.is_err() {
        return Err(errors::SsacheError::NoDataReceived);
    }
    let command_line = command_line.split_whitespace();
    let command_line: Vec<String> = command_line
        .into_iter()
        .map(|slice| slice.to_string())
        .collect();
    if command_line.get(0).is_none() {
        return Err(errors::SsacheError::NoDataReceived);
    }

    Ok(command_line)
}
