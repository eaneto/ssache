use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    fs::{self, File},
    hash::{Hash, Hasher},
    io::Write,
    sync::Arc,
};

use log::debug;
use tokio::sync::Mutex;

const CRLF: &str = "\r\n";

pub fn create_sharded_database(num_shards: usize) -> Arc<Vec<Mutex<HashMap<String, String>>>> {
    let mut db = Vec::with_capacity(num_shards);
    for _ in 0..num_shards {
        db.push(Mutex::new(HashMap::new()));
    }
    Arc::new(db)
}

pub async fn get(database: Arc<Vec<Mutex<HashMap<String, String>>>>, key: String) -> String {
    let shard_key = get_shard_key(&key, database.len());
    // TODO Remove locks for reading.
    let shard = database[shard_key].lock().await;

    match shard.get(&key) {
        Some(value) => {
            debug!("found {:?} for {:?} on shard {:?}", value, key, shard_key);
            let size = value.len();
            format!("${size}{CRLF}+{value}{CRLF}")
        }
        None => {
            debug!("value not found for {:?} on shard {:?}", key, shard_key);
            format!("$-1{CRLF}")
        }
    }
}

pub async fn set(
    database: Arc<Vec<Mutex<HashMap<String, String>>>>,
    key: String,
    value: String,
) -> String {
    let shard_key = get_shard_key(&key, database.len());
    let mut shard = database[shard_key].lock().await;
    shard.insert(key, value);
    debug!("value successfully set on shard {:?}", shard_key);
    format!("+OK{CRLF}")
}

/// Dumps the database into a single file with the data from all shards.
pub async fn save(database: Arc<Vec<Mutex<HashMap<String, String>>>>) -> String {
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
        database[i].lock().await.iter().for_each(|(key, value)| {
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

pub async fn load(database: Arc<Vec<Mutex<HashMap<String, String>>>>) -> String {
    match fs::read("dump.ssch") {
        Ok(file_content) => match bincode::deserialize::<HashMap<String, String>>(&file_content) {
            Ok(dump) => {
                for (key, value) in dump {
                    let shard_key = get_shard_key(&key, database.len());
                    let mut shard = database[shard_key].lock().await;
                    shard.insert(key, value);
                }
                format!("+OK{CRLF}")
            }
            Err(e) => {
                debug!("Error deserializing database into hashmap format {:?}", e);
                format!("-ERROR Unable to deserialize data into hashmap format{CRLF}")
            }
        },
        Err(e) => {
            debug!("Error reading dump file {:?}", e);
            format!("-ERROR Unable to read dump file{CRLF}")
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
