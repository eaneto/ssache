use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
};

use clap::Parser;
use log::{debug, info, warn};
use ssache::ThreadPool;

mod command;

#[derive(Parser, Debug)]
struct Args {
    // Port to run the server
    #[arg(short, long, default_value_t = 7777)]
    port: u16,

    // Size of the thread pool to process requests
    #[arg(short, long, default_value_t = 8)]
    thread_pool_size: usize,
}

fn main() {
    env_logger::init();
    let args = Args::parse();
    let listener = start_server(&args);
    handle_connections(listener, &args);
}

fn start_server(args: &Args) -> TcpListener {
    info!("Ssache is starting");

    let port = args.port;
    let listener = match TcpListener::bind(format!("127.0.0.1:{port}")) {
        Ok(listener) => listener,
        Err(e) => panic!("Unable to start ssache on port {port}. Error = {:?}", e),
    };

    info!("Ssache is ready to accept connections on port {port}");

    listener
}

fn handle_connections(listener: TcpListener, args: &Args) {
    // TODO Change value to Bytes
    // TODO Save changes on disk once an hour
    let database: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));

    let pool = match ThreadPool::new(args.thread_pool_size) {
        Ok(pool) => pool,
        Err(_) => panic!("Invalid number of threads for the thread pool."),
    };

    // TODO Keep connection open with client.
    for stream in listener.incoming() {
        let stream = match stream {
            Ok(stream) => stream,
            Err(_) => continue,
        };

        let database_clone = database.clone();
        pool.execute(move || {
            if let Err(_) = handle_request(stream, database_clone) {
                warn!("Error executing tcp stream");
            };
        });
    }
}

const CRLF: &str = "\r\n";

#[derive(Debug, Clone)]
struct NoDataReceivedError;

// TODO Add integration tests
fn handle_request(
    mut stream: TcpStream,
    database: Arc<Mutex<HashMap<String, String>>>,
) -> Result<(), command::NotEnoughParametersError> {
    let buf_reader = BufReader::new(&mut stream);
    let command_line = parse_command_line_from_stream(buf_reader);
    // If no data is received in the command line then there's no need
    // to return an error to the client.
    if let Err(_) = command_line {
        return Ok(());
    }

    let command = command::parse_command(command_line.unwrap());

    if let Err(e) = command {
        stream.write_all(e.message.as_bytes()).unwrap();
        return Err(e);
    }

    let command = command.unwrap();

    let mut database = database.lock().unwrap();
    match command {
        command::Command::Get { key } => match database.get(&key) {
            Some(value) => {
                debug!("found {:?} for {:?}", value, key);
                let size = value.len();
                let response = format!("${size}{CRLF}+{value}{CRLF}");
                stream.write_all(response.as_bytes()).unwrap();
                Ok(())
            }
            None => {
                debug!("value not found for {:?}", key);
                let response = format!("$-1{CRLF}");
                stream.write_all(response.as_bytes()).unwrap();
                Ok(())
            }
        },
        command::Command::Set { key, value } => {
            database.insert(key, value);
            debug!("value successfully set");
            let response = format!("+OK{CRLF}");
            stream.write_all(response.as_bytes()).unwrap();
            Ok(())
        }
        command::Command::Quit => {
            let response = format!("+OK{CRLF}");
            stream.write_all(response.as_bytes()).unwrap();
            Ok(())
        }
        command::Command::Ping { message } => {
            let size = message.len();
            let response = if size == 0 {
                format!("+PONG{CRLF}")
            } else {
                format!("${size}{CRLF}+{message}{CRLF}")
            };
            stream.write_all(response.as_bytes()).unwrap();
            Ok(())
        }
        command::Command::Unknown => {
            debug!("Unknown command");
            let response = format!("-ERROR unknown command{CRLF}");
            stream.write_all(response.as_bytes()).unwrap();
            Ok(())
        }
    }
}

fn parse_command_line_from_stream(
    buf_reader: BufReader<&mut TcpStream>,
) -> Result<Vec<String>, NoDataReceivedError> {
    let command_line = buf_reader.lines().next();
    if command_line.is_none() {
        return Err(NoDataReceivedError);
    }
    let command_line = command_line.unwrap();
    if let Err(_e) = command_line {
        return Err(NoDataReceivedError);
    }

    let command_line = command_line.unwrap();
    let command_line = command_line.split_whitespace();
    let command_line: Vec<String> = command_line
        .into_iter()
        .map(|slice| slice.to_string())
        .collect();
    if command_line.get(0).is_none() {
        return Err(NoDataReceivedError);
    }

    Ok(command_line)
}
