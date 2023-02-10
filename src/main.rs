use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
};

use log::{debug, info, warn};
use ssache::ThreadPool;

enum Command {
    // GET key
    Get { key: String },
    // SET key value
    Set { key: String, value: String },
    Quit,
    // PING message
    Ping { message: String },
    Unknown,
}

const CRLF: &str = "\r\n";

fn main() {
    env_logger::init();
    let listener = start_server();
    handle_connections(listener);
}

fn start_server() -> TcpListener {
    info!("Ssache is starting");

    // TODO Get port from command line
    let listener = match TcpListener::bind("127.0.0.1:7777") {
        Ok(listener) => listener,
        Err(e) => panic!("Unable to start ssache on port 7777. Error = {:?}", e),
    };

    info!("Ssache is ready to accept connections");

    listener
}

fn handle_connections(listener: TcpListener) {
    // TODO Change value to Bytes
    // TODO Save changes on disk once an hour
    let database: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));

    // TODO Get thread pool size from command line
    let pool = match ThreadPool::new(8) {
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

#[derive(Debug, Clone)]
struct NotEnoughParametersError {
    message: String,
}

#[derive(Debug, Clone)]
struct NoDataReceivedError;

// TODO Add integration tests
fn handle_request(
    mut stream: TcpStream,
    database: Arc<Mutex<HashMap<String, String>>>,
) -> Result<(), NotEnoughParametersError> {
    let buf_reader = BufReader::new(&mut stream);
    let command_line = parse_command_line_from_stream(buf_reader);
    // If no data is received in the command line then there's no need
    // to return an error to the client.
    if let Err(_) = command_line {
        return Ok(());
    }

    let command = parse_command(command_line.unwrap());

    if let Err(e) = command {
        stream.write_all(e.message.as_bytes()).unwrap();
        return Err(e);
    }

    let command = command.unwrap();

    let mut database = database.lock().unwrap();
    match command {
        Command::Get { key } => match database.get(&key) {
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
        Command::Set { key, value } => {
            database.insert(key, value);
            debug!("value successfully set");
            let response = format!("+OK{CRLF}");
            stream.write_all(response.as_bytes()).unwrap();
            Ok(())
        }
        Command::Quit => {
            let response = format!("+OK{CRLF}");
            stream.write_all(response.as_bytes()).unwrap();
            Ok(())
        }
        Command::Ping { message } => {
            let size = message.len();
            let response = if size == 0 {
                format!("+PONG{CRLF}")
            } else {
                format!("${size}{CRLF}+{message}{CRLF}")
            };
            stream.write_all(response.as_bytes()).unwrap();
            Ok(())
        }
        Command::Unknown => {
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

fn parse_command(command_line: Vec<String>) -> Result<Command, NotEnoughParametersError> {
    let command = command_line.get(0).unwrap();
    if command.eq(&String::from("GET")) {
        if let Some(key) = command_line.get(1) {
            Ok(Command::Get {
                key: key.to_string(),
            })
        } else {
            debug!("not enough parameters for GET command");
            let message = format!("-ERROR not enough parameters for GET{CRLF}");
            Err(NotEnoughParametersError { message })
        }
    } else if command.eq(&String::from("SET")) {
        if let (Some(key), Some(_)) = (command_line.get(1), command_line.get(2)) {
            Ok(Command::Set {
                key: key.to_string(),
                value: command_line[2..].join(" "),
            })
        } else {
            debug!("not enough parameters for SET command");
            let message = format!("-ERROR not enough parameters for SET{CRLF}");
            Err(NotEnoughParametersError { message })
        }
    } else if command.eq(&String::from("QUIT")) {
        Ok(Command::Quit)
    } else if command.eq(&String::from("PING")) {
        let value = command_line[1..].join(" ");
        Ok(Command::Ping { message: value })
    } else {
        Ok(Command::Unknown)
    }
}
