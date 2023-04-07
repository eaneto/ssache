use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    fs::{self, File},
    hash::{Hash, Hasher},
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    time::Duration,
};

use clap::Parser;
use clokwerk::{AsyncScheduler, TimeUnits};
use log::{debug, error, info, warn};
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

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Args::parse();

    let database = create_sharded_database(args.thread_pool_size);

    let mut scheduler = AsyncScheduler::new();

    let database_clone = database.clone();
    // TODO Allow this to be configurable
    scheduler.every(1.hours()).run(move || {
        // Clones the database arc, and moves the cloned arc to the
        // async block, this way the arc cloned each time in the async
        // block is a clone of the first clone and the original
        // database isn't dropped.
        let database = database_clone.clone();
        async move {
            save(database);
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

    let pool = match ThreadPool::new(args.thread_pool_size) {
        Ok(pool) => pool,
        Err(_) => panic!("Invalid number of threads for the thread pool."),
    };

    let listener = start_server(&args);
    handle_connections(listener, database, &pool);
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

fn handle_connections(
    listener: TcpListener,
    database: Arc<Vec<Mutex<HashMap<String, String>>>>,
    pool: &ThreadPool,
) {
    // TODO Keep connection open with client.
    for stream in listener.incoming() {
        let stream = match stream {
            Ok(stream) => stream,
            Err(_) => continue,
        };

        let database_clone = database.clone();
        let result = pool.execute(move || {
            if handle_request(stream, database_clone).is_err() {
                warn!("Error executing tcp stream");
            };
        });

        if result.is_err() {
            error!("Error sending stream to thread pool executor")
        }
    }
}

fn create_sharded_database(num_shards: usize) -> Arc<Vec<Mutex<HashMap<String, String>>>> {
    let mut db = Vec::with_capacity(num_shards);
    for _ in 0..num_shards {
        db.push(Mutex::new(HashMap::new()));
    }
    Arc::new(db)
}

const CRLF: &str = "\r\n";

#[derive(Debug, Clone)]
struct NoDataReceivedError;

fn handle_request(
    mut stream: TcpStream,
    database: Arc<Vec<Mutex<HashMap<String, String>>>>,
) -> Result<(), command::NotEnoughParametersError> {
    let buf_reader = BufReader::new(&mut stream);
    let command_line = parse_command_line_from_stream(buf_reader);
    // If no data is received in the command line then there's no need
    // to return an error to the client.
    if command_line.is_err() {
        debug!("No data received");
        return Ok(());
    }

    let command = command::parse_command(command_line.unwrap());

    if let Err(e) = command {
        stream.write_all(e.message.as_bytes()).unwrap();
        return Err(e);
    }

    let command = command.unwrap();
    match command {
        command::Command::Get { key } => {
            let shard_key = get_shard_key(&key, database.len());
            // TODO Remove locks for reading.
            let shard = database[shard_key].lock().unwrap();

            return match shard.get(&key) {
                Some(value) => {
                    debug!("found {:?} for {:?} on shard {:?}", value, key, shard_key);
                    let size = value.len();
                    let response = format!("${size}{CRLF}+{value}{CRLF}");
                    stream.write_all(response.as_bytes()).unwrap();
                    Ok(())
                }
                None => {
                    debug!("value not found for {:?} on shard {:?}", key, shard_key);
                    let response = format!("$-1{CRLF}");
                    stream.write_all(response.as_bytes()).unwrap();
                    Ok(())
                }
            };
        }
        command::Command::Set { key, value } => {
            let shard_key = get_shard_key(&key, database.len());
            let mut shard = database[shard_key].lock().unwrap();
            shard.insert(key, value);
            debug!("value successfully set on shard {:?}", shard_key);
            let response = format!("+OK{CRLF}");
            stream.write_all(response.as_bytes()).unwrap();
            Ok(())
        }
        command::Command::Expire {
            key: _key,
            time: _time,
        } => {
            debug!("WIP");
            let response = format!("+OK{CRLF}");
            stream.write_all(response.as_bytes()).unwrap();
            Ok(())
        }
        command::Command::Save => {
            let response = save(database);
            stream.write_all(response.as_bytes()).unwrap();
            Ok(())
        }
        command::Command::Load => {
            let response = match fs::read("dump.ssch") {
                Ok(file_content) => {
                    match bincode::deserialize::<HashMap<String, String>>(&file_content) {
                        Ok(dump) => {
                            for (key, value) in dump {
                                let shard_key = get_shard_key(&key, database.len());
                                let mut shard = database[shard_key].lock().unwrap();
                                shard.insert(key, value);
                            }
                            format!("+OK{CRLF}")
                        }
                        Err(e) => {
                            debug!("Error deserializing database into hashmap format {:?}", e);
                            format!("-ERROR Unable to deserialize data into hashmap format{CRLF}")
                        }
                    }
                }
                Err(e) => {
                    debug!("Error reading dump file {:?}", e);
                    format!("-ERROR Unable to read dump file{CRLF}")
                }
            };
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

/// Hashes the key to define the shard key and locate the value on the
/// database.
fn get_shard_key(key: &String, database_size: usize) -> usize {
    // TODO Replace with a specific hashing algorithm.
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    let hash = hasher.finish();
    hash as usize % database_size
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

/// Dumps the database into a single file with the data from all shards.
fn save(database: Arc<Vec<Mutex<HashMap<String, String>>>>) -> String {
    debug!("Initiating save process");
    // FIXME Terrible solution, duplicates all data already in
    // memory.  I think the best way to solve this without
    // memory duplication is to save only the reference to the
    // keys and values on the joined_database map, then when
    // the file is being created it's only necessary to follow
    // the reference.
    let mut joined_database: HashMap<String, String> = HashMap::new();
    for i in 0..database.len() {
        debug!("Initiating save process for shard {i}");
        database[i].lock().unwrap().iter().for_each(|(key, value)| {
            joined_database.insert(key.clone(), value.clone());
        });
    }

    match File::create("dump.ssch") {
        Ok(mut file) => match bincode::serialize(&joined_database) {
            Ok(serialized_database) => match file.write_all(&serialized_database) {
                Ok(()) => format!("+OK{CRLF}"),
                Err(e) => {
                    debug!("Error writing the dump to the file {:?}", e);
                    format!("-ERROR Unable to write the data to the dump file{CRLF}")
                }
            },
            Err(e) => {
                debug!("Error serializing database into binary format {:?}", e);
                format!("-ERROR Unable to serialize data into binary format{CRLF}")
            }
        },
        Err(e) => {
            debug!("Error creating dump file {:?}", e);
            format!("-ERROR Unable to create dump file{CRLF}")
        }
    }
}
