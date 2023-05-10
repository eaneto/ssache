use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    fs::{self, File},
    hash::{Hash, Hasher},
    io::Write,
    num::ParseIntError,
    time::{Duration, Instant},
};

use log::{debug, error, trace};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{Mutex, RwLock},
};

use crate::{
    errors::{LoadError, SaveError},
    CRLF,
};

struct Entry {
    value: String,
    created_at: Instant,
}

type ShardedLog = Vec<RwLock<Vec<(String, String)>>>;

pub struct ShardedStorage {
    num_shards: usize,
    shards: Vec<RwLock<HashMap<String, Entry>>>,
    expirations: Mutex<HashMap<String, Instant>>,
    expired_keys: Mutex<Vec<String>>,
    log: HashMap<String, ShardedLog>,
    log_offset: HashMap<String, Vec<Mutex<u32>>>,
    replicas: Vec<String>,
}

impl ShardedStorage {
    pub fn new(num_shards: usize, replicas: Vec<String>) -> ShardedStorage {
        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(RwLock::new(HashMap::new()));
        }
        let mut log = HashMap::new();
        let mut log_offset = HashMap::new();
        for replica in replicas.clone() {
            let mut replica_log = Vec::with_capacity(num_shards);
            let mut replica_offset = Vec::with_capacity(num_shards);
            for _ in 0..num_shards {
                replica_log.push(RwLock::new(Vec::new()));
                replica_offset.push(Mutex::new(0))
            }
            log.insert(replica.clone(), replica_log);
            log_offset.insert(replica.clone(), replica_offset);
        }
        ShardedStorage {
            num_shards,
            shards,
            expirations: Mutex::new(HashMap::new()),
            expired_keys: Mutex::new(Vec::new()),
            log,
            log_offset,
            replicas,
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
        self.write_operation_on_log(shard_key, &key, &value).await;
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
        // If the duration is set to 0 ignore the expiration.
        if entry.is_some() && ttl != Duration::from_millis(0) {
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
        let shard_key = self.get_shard_key(&key);
        let mut shard = self.shards[shard_key].write().await;
        let entry = shard.entry(key.clone()).or_insert(Entry {
            value: (-1).to_string(),
            created_at: Instant::now(),
        });
        let value = entry.value.parse::<i64>()? + 1;
        entry.value = value.to_string();
        self.write_operation_on_log(shard_key, &key, &entry.value)
            .await;
        Ok(value)
    }

    pub async fn decr(&self, key: String) -> Result<i64, ParseIntError> {
        let shard_key = self.get_shard_key(&key);
        let mut shard = self.shards[self.get_shard_key(&key)].write().await;
        let entry = shard.entry(key.clone()).or_insert(Entry {
            value: 1.to_string(),
            created_at: Instant::now(),
        });
        let value = entry.value.parse::<i64>()? - 1;
        entry.value = value.to_string();
        self.write_operation_on_log(shard_key, &key, &entry.value)
            .await;
        Ok(value)
    }

    async fn write_operation_on_log(&self, shard_key: usize, key: &String, value: &String) {
        for replica in &self.replicas {
            self.log.get(replica).unwrap()[shard_key]
                .write()
                .await
                .push((key.to_string(), value.to_string()));
        }
    }

    /// Broadcast the operation log to all registered replicas.
    pub async fn broadcast_to_replicas(&self) {
        for replica in &self.replicas {
            debug!("Broadcasting to {replica}");
            for i in 0..self.num_shards {
                let log = self.log.get(replica).unwrap()[i].read().await;
                let mut log_offset = self.log_offset.get(replica).unwrap()[i].lock().await;
                self.replicate_shard(&log, &mut log_offset, replica).await;
            }
        }

        self.clean_log().await;
    }

    async fn replicate_shard(
        &self,
        log: &[(String, String)],
        log_offset: &mut u32,
        replica: &String,
    ) {
        trace!("Current log offset {log_offset}");

        let mut stream = match TcpStream::connect(&replica).await {
            Ok(stream) => {
                trace!("Successfully connected to replica {replica}");
                stream
            }
            Err(e) => {
                error!("Error connecting to replica {replica} {e}");
                return;
            }
        };

        let batch_size = 100;
        let mut replicated_operations_by_shard = 0;
        for offset in *log_offset..(*log_offset + batch_size) {
            trace!("Replicating log offset {offset}");
            let operation = match log.get(offset as usize) {
                Some(operation) => operation,
                None => {
                    trace!("Operation not found on offset {offset}");
                    break;
                }
            };
            self.replicate_operation(operation, &mut stream, &mut replicated_operations_by_shard)
                .await;
        }

        // Updates the log offset for the partition after
        // sending all possible messages.
        *log_offset += replicated_operations_by_shard;
    }

    async fn replicate_operation(
        &self,
        operation: &(String, String),
        stream: &mut TcpStream,
        replicated_operations_by_shard: &mut u32,
    ) {
        trace!("Sending operation {} {}", operation.0, operation.1);

        let command = format!("SET {} {}{CRLF}", operation.0, operation.1);

        match stream.write_all(command.as_bytes()).await {
            Ok(_) => *replicated_operations_by_shard += 1,
            Err(e) => {
                // Ignore error and proceed with replication
                error!(
                    "Error sending operation({} {}) to replica {e}",
                    operation.0, operation.1
                )
            }
        }

        let mut buf = [0u8; 5];
        match stream.read_exact(&mut buf).await {
            Ok(_) => {
                let response = String::from_utf8_lossy(&buf);
                if response == format!("+OK{CRLF}") {
                    trace!("Successfully processed operation");
                } else {
                    error!("Error replicating operation {response}");
                }
            }
            Err(e) => error!("Error receiving replica response {e}"),
        }
    }

    async fn clean_log(&self) {
        for replica in &self.replicas {
            for i in 0..self.num_shards {
                let mut log = self.log.get(replica).unwrap()[i].write().await;
                let mut log_offset = self.log_offset.get(replica).unwrap()[i].lock().await;
                log.drain(0..((*log_offset) as usize));
                *log_offset = 0;
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

#[cfg(test)]
mod tests {
    use std::{fs::remove_file, path::Path, thread};

    use super::*;

    #[tokio::test]
    async fn create_storage_with_one_shard_and_no_replicas() {
        let storage = ShardedStorage::new(1, Vec::new());

        assert_eq!(storage.shards.len(), 1);
        assert!(storage.log.is_empty());
        assert!(storage.log_offset.is_empty());
        assert!(storage.replicas.is_empty());
        assert!(storage.expirations.lock().await.is_empty());
        assert!(storage.expired_keys.lock().await.is_empty());
    }

    #[tokio::test]
    async fn create_storage_with_ten_shards_and_no_replicas() {
        let storage = ShardedStorage::new(10, Vec::new());

        assert_eq!(storage.shards.len(), 10);
        assert!(storage.log.is_empty());
        assert!(storage.log_offset.is_empty());
        assert!(storage.replicas.is_empty());
        assert!(storage.expirations.lock().await.is_empty());
        assert!(storage.expired_keys.lock().await.is_empty());
    }

    #[tokio::test]
    async fn create_storage_with_ten_shards_and_two_replicas() {
        let replicas = vec!["127.0.0.1:7778".to_string(), "127.0.0.1:7779".to_string()];

        let storage = ShardedStorage::new(10, replicas.clone());

        assert_eq!(storage.shards.len(), 10);
        for replica in replicas {
            assert!(storage.log.contains_key(&replica));
            assert!(storage.log_offset.contains_key(&replica));
        }
        assert_eq!(storage.replicas.len(), 2);
        assert!(storage.expirations.lock().await.is_empty());
        assert!(storage.expired_keys.lock().await.is_empty());
    }

    #[tokio::test]
    async fn get_unset_key() {
        let storage = ShardedStorage::new(3, Vec::new());

        let result = storage.get("key".to_string()).await;

        assert_eq!(result.is_none(), true);
    }

    #[tokio::test]
    async fn set_value_to_key() {
        let storage = ShardedStorage::new(3, Vec::new());

        storage.set("key".to_string(), "value".to_string()).await;
        let result = storage.get("key".to_string()).await;

        assert_eq!(result.is_some(), true);
        assert_eq!(result.unwrap(), "value");
    }

    #[tokio::test]
    async fn set_different_value_to_same_key() {
        let storage = ShardedStorage::new(3, Vec::new());

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
        let storage = ShardedStorage::new(3, Vec::new());

        storage
            .set("key".to_string(), "value with spaces".to_string())
            .await;
        let result = storage.get("key".to_string()).await;

        assert_eq!(result.is_some(), true);
        assert_eq!(result.unwrap(), "value with spaces");
    }

    #[tokio::test]
    async fn incr_unset_key() {
        let storage = ShardedStorage::new(3, Vec::new());

        let result = storage.incr("key".to_string()).await;

        assert_eq!(result.is_ok(), true);
        assert_eq!(result.unwrap(), 0);
    }

    #[tokio::test]
    async fn incr_set_key() {
        let storage = ShardedStorage::new(3, Vec::new());

        storage.set("key".to_string(), "9".to_string()).await;
        let result = storage.incr("key".to_string()).await;

        assert_eq!(result.is_ok(), true);
        assert_eq!(result.unwrap(), 10);
    }

    #[tokio::test]
    async fn decr_unset_key() {
        let storage = ShardedStorage::new(3, Vec::new());

        let result = storage.decr("key".to_string()).await;

        assert_eq!(result.is_ok(), true);
        assert_eq!(result.unwrap(), 0);
    }

    #[tokio::test]
    async fn decr_set_key() {
        let storage = ShardedStorage::new(3, Vec::new());

        storage.set("key".to_string(), "17".to_string()).await;
        let result = storage.decr("key".to_string()).await;

        assert_eq!(result.is_ok(), true);
        assert_eq!(result.unwrap(), 16);
    }

    #[tokio::test]
    async fn set_expiration_to_unkown_key() {
        let storage = ShardedStorage::new(3, Vec::new());

        storage
            .set_expiration("key".to_string(), Duration::from_millis(10))
            .await;

        assert_eq!(storage.expirations.lock().await.is_empty(), true);
        assert_eq!(storage.expired_keys.lock().await.is_empty(), true);
    }

    #[tokio::test]
    async fn set_expiration_to_key() {
        let storage = ShardedStorage::new(3, Vec::new());

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
    async fn set_expiration_zero_to_key() {
        let storage = ShardedStorage::new(3, Vec::new());

        storage.set("key".to_string(), "value".to_string()).await;

        storage
            .set_expiration("key".to_string(), Duration::from_millis(0))
            .await;

        assert_eq!(storage.expirations.lock().await.is_empty(), true);
        assert_eq!(storage.expired_keys.lock().await.is_empty(), true);
    }

    #[tokio::test]
    async fn set_expiration_to_key_and_check_expirations() {
        let storage = ShardedStorage::new(3, Vec::new());

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
        let storage = ShardedStorage::new(3, Vec::new());

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
        let storage = ShardedStorage::new(3, Vec::new());

        storage.set("key-1".to_string(), "value".to_string()).await;
        storage.set("key-2".to_string(), "value".to_string()).await;
        storage.set("key-3".to_string(), "value".to_string()).await;

        let result = storage.get("key-1".to_string()).await;
        assert_eq!(result.is_none(), false);

        let result = storage.save().await;
        assert_eq!(result.is_ok(), true);

        let storage = ShardedStorage::new(7, Vec::new());
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
