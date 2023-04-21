use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    fs::{self, File},
    hash::{Hash, Hasher},
    io::Write,
    time::{Duration, Instant},
};

use log::debug;
use tokio::sync::{Mutex, RwLock};

use crate::errors::{LoadErrorKind, SaveErrorKind};

struct Entry {
    value: String,
    created_at: Instant,
}

pub struct ShardedStorage {
    shards: Vec<RwLock<HashMap<String, Entry>>>,
    expirations: Mutex<HashMap<String, Instant>>,
    expired_keys: Mutex<Vec<String>>,
}

impl ShardedStorage {
    pub fn new(num_shards: usize) -> ShardedStorage {
        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(RwLock::new(HashMap::new()));
        }
        ShardedStorage {
            shards,
            expirations: Mutex::new(HashMap::new()),
            expired_keys: Mutex::new(Vec::new()),
        }
    }

    pub async fn get(&self, key: String) -> Option<String> {
        let shard_key = self.get_shard_key(&key);
        let shard = self.shards[shard_key].read().await;

        match shard.get(&key) {
            Some(entry) => {
                debug!(
                    "found {:?} for {:?} on shard {:?}",
                    entry.value, key, shard_key
                );
                Some(entry.value.clone())
            }
            None => {
                debug!("value not found for {:?} on shard {:?}", key, shard_key);
                None
            }
        }
    }

    pub async fn set(&self, key: String, value: String) {
        let shard_key = self.get_shard_key(&key);
        let mut shard = self.shards[shard_key].write().await;
        shard.insert(
            key,
            Entry {
                value,
                created_at: Instant::now(),
            },
        );
        debug!("value successfully set on shard {:?}", shard_key);
    }

    /// Dumps the in-memory storage into a single file with the data
    /// from all shards.
    pub async fn save(&self) -> Result<(), SaveErrorKind> {
        debug!("Initiating save process");
        // FIXME Terrible solution, duplicates all data already in
        // memory.  I think the best way to solve this without memory
        // duplication is to save only the reference to the keys and
        // values on the joined_shards map, then when the file is
        // being created it's only necessary to follow the reference.
        let mut joined_shards: HashMap<String, String> = HashMap::new();
        for i in 0..self.shards.len() {
            debug!("Initiating save process for shard {i}");
            self.shards[i].read().await.iter().for_each(|(key, entry)| {
                joined_shards.insert(key.clone(), entry.value.clone());
            });
        }

        match File::create("dump.ssch") {
            Ok(mut file) => match bincode::serialize(&joined_shards) {
                Ok(serialized_storage) => match file.write_all(&serialized_storage) {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        debug!("Error writing the dump to the file {:?}", e);
                        Err(SaveErrorKind::UnableToWriteToDump)
                    }
                },
                Err(e) => {
                    debug!("Error serializing storage into binary format {:?}", e);
                    Err(SaveErrorKind::UnableToSerializeIntoBinary)
                }
            },
            Err(e) => {
                debug!("Error creating dump file {:?}", e);
                Err(SaveErrorKind::UnableToCreateDump)
            }
        }
    }

    pub async fn load(&self) -> Result<(), LoadErrorKind> {
        match fs::read("dump.ssch") {
            Ok(file_content) => {
                match bincode::deserialize::<HashMap<String, String>>(&file_content) {
                    Ok(dump) => {
                        for (key, value) in dump {
                            let shard_key = self.get_shard_key(&key);
                            let mut shard = self.shards[shard_key].write().await;
                            shard.insert(
                                key,
                                Entry {
                                    value,
                                    created_at: Instant::now(),
                                },
                            );
                        }
                        Ok(())
                    }
                    Err(e) => {
                        debug!(
                            "Error deserializing dump content into hashmap format {:?}",
                            e
                        );
                        Err(LoadErrorKind::UnableToDeserializaData)
                    }
                }
            }
            Err(e) => {
                debug!("Error reading dump file {:?}", e);
                Err(LoadErrorKind::UnableToReadDump)
            }
        }
    }

    pub async fn set_expiration(&self, key: String, ttl: Duration) {
        let shard = self.shards[self.get_shard_key(&key)].read().await;
        let entry = shard.get(&key);
        if entry.is_some() {
            let entry = entry.unwrap();
            let expiration_time = entry.created_at + ttl;
            self.expirations.lock().await.insert(key, expiration_time);
        }
    }

    /// Checks for expired keys, removes them from the shard and saves
    /// the removed keys.
    pub async fn check_expirations(&self) {
        let mut expirations = self.expirations.lock().await;
        let mut expired_keys = self.expired_keys.lock().await;
        for (key, expiration_time) in expirations.iter_mut() {
            let shard_key = self.get_shard_key(key);
            let mut shard = self.shards[shard_key].write().await;
            if shard.get(key).is_none() {
                debug!("Key '{}' already deleted on shard {}", key, shard_key);
            } else {
                let now = Instant::now();
                if now >= *expiration_time {
                    debug!("Removing '{}' from shard {}", key, shard_key);
                    shard.remove(key);
                    expired_keys.push(key.clone());
                }
            }
        }
    }

    /// Removes already expired keys from the expirations.
    pub async fn remove_expiration(&self) {
        let mut expirations = self.expirations.lock().await;
        let mut expired_keys = self.expired_keys.lock().await;
        for expired_key in expired_keys.iter_mut() {
            expirations.remove(expired_key);
        }
        expired_keys.clear();
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
