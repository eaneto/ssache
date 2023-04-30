use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    fs::{self, File},
    hash::{Hash, Hasher},
    io::Write,
    num::ParseIntError,
    time::{Duration, Instant},
};

use log::debug;
use tokio::sync::{Mutex, RwLock};

use crate::errors::{LoadError, SaveError};

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
    pub async fn save(&self) -> Result<(), SaveError> {
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
                        Err(SaveError::WritingDump)
                    }
                },
                Err(e) => {
                    debug!("Error serializing storage into binary format {:?}", e);
                    Err(SaveError::SerializingIntoBinary)
                }
            },
            Err(e) => {
                debug!("Error creating dump file {:?}", e);
                Err(SaveError::CreatingDump)
            }
        }
    }

    pub async fn load(&self) -> Result<(), LoadError> {
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
                        Err(LoadError::DeserializingData)
                    }
                }
            }
            Err(e) => {
                debug!("Error reading dump file {:?}", e);
                Err(LoadError::ReadingDump)
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

    pub async fn incr(&self, key: String) -> Result<i64, ParseIntError> {
        let mut shard = self.shards[self.get_shard_key(&key)].write().await;
        let entry = shard.entry(key).or_insert(Entry {
            value: (-1).to_string(),
            created_at: Instant::now(),
        });
        let value = entry.value.parse::<i64>()? + 1;
        entry.value = value.to_string();
        Ok(value)
    }

    pub async fn decr(&self, key: String) -> Result<i64, ParseIntError> {
        let mut shard = self.shards[self.get_shard_key(&key)].write().await;
        let entry = shard.entry(key).or_insert(Entry {
            value: 1.to_string(),
            created_at: Instant::now(),
        });
        let value = entry.value.parse::<i64>()? - 1;
        entry.value = value.to_string();
        Ok(value)
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

#[cfg(test)]
mod tests {
    use std::{fs::remove_file, path::Path, thread};

    use super::*;

    #[tokio::test]
    async fn create_storage_with_one_shard() {
        let storage = ShardedStorage::new(1);

        assert_eq!(storage.shards.len(), 1);
        assert_eq!(storage.expirations.lock().await.is_empty(), true);
        assert_eq!(storage.expired_keys.lock().await.is_empty(), true);
    }

    #[tokio::test]
    async fn create_storage_with_ten_shards() {
        let storage = ShardedStorage::new(10);

        assert_eq!(storage.shards.len(), 10);
        assert_eq!(storage.expirations.lock().await.is_empty(), true);
        assert_eq!(storage.expired_keys.lock().await.is_empty(), true);
    }

    #[tokio::test]
    async fn get_unset_key() {
        let storage = ShardedStorage::new(3);

        let result = storage.get("key".to_string()).await;

        assert_eq!(result.is_none(), true);
    }

    #[tokio::test]
    async fn set_value_to_key() {
        let storage = ShardedStorage::new(3);

        storage.set("key".to_string(), "value".to_string()).await;
        let result = storage.get("key".to_string()).await;

        assert_eq!(result.is_some(), true);
        assert_eq!(result.unwrap(), "value");
    }

    #[tokio::test]
    async fn set_different_value_to_same_key() {
        let storage = ShardedStorage::new(3);

        storage.set("key".to_string(), "value".to_string()).await;
        storage
            .set("key".to_string(), "different value".to_string())
            .await;
        let result = storage.get("key".to_string()).await;

        assert_eq!(result.is_some(), true);
        assert_eq!(result.unwrap(), "different value");
    }

    #[tokio::test]
    async fn set_value_with_spaces_to_key() {
        let storage = ShardedStorage::new(3);

        storage
            .set("key".to_string(), "value with spaces".to_string())
            .await;
        let result = storage.get("key".to_string()).await;

        assert_eq!(result.is_some(), true);
        assert_eq!(result.unwrap(), "value with spaces");
    }

    #[tokio::test]
    async fn incr_unset_key() {
        let storage = ShardedStorage::new(3);

        let result = storage.incr("key".to_string()).await;

        assert_eq!(result.is_ok(), true);
        assert_eq!(result.unwrap(), 0);
    }

    #[tokio::test]
    async fn incr_set_key() {
        let storage = ShardedStorage::new(3);

        storage.set("key".to_string(), "9".to_string()).await;
        let result = storage.incr("key".to_string()).await;

        assert_eq!(result.is_ok(), true);
        assert_eq!(result.unwrap(), 10);
    }

    #[tokio::test]
    async fn decr_unset_key() {
        let storage = ShardedStorage::new(3);

        let result = storage.decr("key".to_string()).await;

        assert_eq!(result.is_ok(), true);
        assert_eq!(result.unwrap(), 0);
    }

    #[tokio::test]
    async fn decr_set_key() {
        let storage = ShardedStorage::new(3);

        storage.set("key".to_string(), "17".to_string()).await;
        let result = storage.decr("key".to_string()).await;

        assert_eq!(result.is_ok(), true);
        assert_eq!(result.unwrap(), 16);
    }

    #[tokio::test]
    async fn set_expiration_to_unkown_key() {
        let storage = ShardedStorage::new(3);

        storage
            .set_expiration("key".to_string(), Duration::from_millis(10))
            .await;

        assert_eq!(storage.expirations.lock().await.is_empty(), true);
        assert_eq!(storage.expired_keys.lock().await.is_empty(), true);
    }

    #[tokio::test]
    async fn set_expiration_to_key() {
        let storage = ShardedStorage::new(3);

        storage.set("key".to_string(), "value".to_string()).await;

        storage
            .set_expiration("key".to_string(), Duration::from_millis(10))
            .await;

        let expirations = storage.expirations.lock().await;
        assert_eq!(expirations.is_empty(), false);
        assert_eq!(expirations.contains_key("key"), true);
        assert_eq!(storage.expired_keys.lock().await.is_empty(), true);
    }

    #[tokio::test]
    async fn set_expiration_to_key_and_check_expirations() {
        let storage = ShardedStorage::new(3);

        storage.set("key".to_string(), "value".to_string()).await;

        storage
            .set_expiration("key".to_string(), Duration::from_millis(10))
            .await;

        thread::sleep(Duration::from_millis(10));

        storage.check_expirations().await;

        let result = storage.get("key".to_string()).await;
        assert_eq!(result.is_none(), true);

        let expirations = storage.expirations.lock().await;
        assert_eq!(expirations.is_empty(), false);
        assert_eq!(expirations.contains_key("key"), true);

        let expired_keys = storage.expired_keys.lock().await;
        assert_eq!(expired_keys.is_empty(), false);
        assert_eq!(expired_keys.contains(&"key".to_string()), true);
    }

    #[tokio::test]
    async fn set_expiration_to_key_check_expirations_and_remove_expired_keys() {
        let storage = ShardedStorage::new(3);

        storage.set("key".to_string(), "value".to_string()).await;

        storage
            .set_expiration("key".to_string(), Duration::from_millis(10))
            .await;

        thread::sleep(Duration::from_millis(10));

        storage.check_expirations().await;
        storage.remove_expiration().await;

        assert_eq!(storage.expirations.lock().await.is_empty(), true);
        assert_eq!(storage.expired_keys.lock().await.is_empty(), true);
    }

    #[tokio::test]
    async fn save_dump_with_multiple_keys_and_load_to_new_storage_with_different_number_of_shards()
    {
        let storage = ShardedStorage::new(3);

        storage.set("key-1".to_string(), "value".to_string()).await;
        storage.set("key-2".to_string(), "value".to_string()).await;
        storage.set("key-3".to_string(), "value".to_string()).await;

        let result = storage.get("key-1".to_string()).await;
        assert_eq!(result.is_none(), false);

        let result = storage.save().await;
        assert_eq!(result.is_ok(), true);

        let storage = ShardedStorage::new(7);
        let result = storage.load().await;
        assert_eq!(result.is_ok(), true);

        let result = storage.get("key-1".to_string()).await;
        assert_eq!(result.is_none(), false);
        let result = storage.get("key-2".to_string()).await;
        assert_eq!(result.is_none(), false);
        let result = storage.get("key-3".to_string()).await;
        assert_eq!(result.is_none(), false);

        remove_file(Path::new("dump.ssch")).unwrap();
    }
}
