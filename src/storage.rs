use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    fs::{self, File},
    hash::{Hash, Hasher},
    io::Write,
    time::Instant,
};

use log::debug;
use tokio::sync::Mutex;

const CRLF: &str = "\r\n";

struct Entry {
    value: String,
    _created_at: Instant,
}

pub struct ShardedStorage {
    shards: Vec<Mutex<HashMap<String, Entry>>>,
}

impl ShardedStorage {
    pub fn new(num_shards: usize) -> ShardedStorage {
        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(Mutex::new(HashMap::new()));
        }
        ShardedStorage { shards }
    }

    pub async fn get(&self, key: String) -> String {
        let shard_key = self.get_shard_key(&key);
        // TODO Remove locks for reading.
        let shard = self.shards[shard_key].lock().await;

        match shard.get(&key) {
            Some(entry) => {
                debug!(
                    "found {:?} for {:?} on shard {:?}",
                    entry.value, key, shard_key
                );
                let size = entry.value.len();
                format!("${size}{CRLF}+{}{CRLF}", entry.value)
            }
            None => {
                debug!("value not found for {:?} on shard {:?}", key, shard_key);
                format!("$-1{CRLF}")
            }
        }
    }

    pub async fn set(&self, key: String, value: String) -> String {
        let shard_key = self.get_shard_key(&key);
        let mut shard = self.shards[shard_key].lock().await;
        shard.insert(
            key,
            Entry {
                value,
                _created_at: Instant::now(),
            },
        );
        debug!("value successfully set on shard {:?}", shard_key);
        format!("+OK{CRLF}")
    }

    /// Dumps the in-memory storage into a single file with the data
    /// from all shards.
    pub async fn save(&self) -> String {
        debug!("Initiating save process");
        // FIXME Terrible solution, duplicates all data already in
        // memory.  I think the best way to solve this without memory
        // duplication is to save only the reference to the keys and
        // values on the joined_shards map, then when the file is
        // being created it's only necessary to follow the reference.
        let mut joined_shards: HashMap<String, String> = HashMap::new();
        for i in 0..self.shards.len() {
            debug!("Initiating save process for shard {i}");
            self.shards[i].lock().await.iter().for_each(|(key, entry)| {
                joined_shards.insert(key.clone(), entry.value.clone());
            });
        }

        match File::create("dump.ssch") {
            Ok(mut file) => match bincode::serialize(&joined_shards) {
                Ok(serialized_storage) => match file.write_all(&serialized_storage) {
                    Ok(()) => format!("+OK{CRLF}"),
                    Err(e) => {
                        debug!("Error writing the dump to the file {:?}", e);
                        format!("-ERROR Unable to write the data to the dump file{CRLF}")
                    }
                },
                Err(e) => {
                    debug!("Error serializing storage into binary format {:?}", e);
                    format!("-ERROR Unable to serialize data into binary format{CRLF}")
                }
            },
            Err(e) => {
                debug!("Error creating dump file {:?}", e);
                format!("-ERROR Unable to create dump file{CRLF}")
            }
        }
    }

    pub async fn load(&self) -> String {
        match fs::read("dump.ssch") {
            Ok(file_content) => {
                match bincode::deserialize::<HashMap<String, String>>(&file_content) {
                    Ok(dump) => {
                        for (key, value) in dump {
                            let shard_key = self.get_shard_key(&key);
                            let mut shard = self.shards[shard_key].lock().await;
                            shard.insert(
                                key,
                                Entry {
                                    value,
                                    _created_at: Instant::now(),
                                },
                            );
                        }
                        format!("+OK{CRLF}")
                    }
                    Err(e) => {
                        debug!(
                            "Error deserializing dump content into hashmap format {:?}",
                            e
                        );
                        format!("-ERROR Unable to deserialize data into hashmap format{CRLF}")
                    }
                }
            }
            Err(e) => {
                debug!("Error reading dump file {:?}", e);
                format!("-ERROR Unable to read dump file{CRLF}")
            }
        }
    }

    /// Hashes the key to define the shard key and locate the value on the
    /// storage.
    fn get_shard_key(&self, key: &String) -> usize {
        // TODO Replace with a specific hashing algorithm.
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        hash as usize % self.shards.len()
    }
}
