use std::{
    sync::{mpsc, Arc, Mutex},
    thread,
};

use log::debug;

type Job = Box<dyn FnOnce() + Send + 'static>;

struct Worker {
    id: usize,
    thread: thread::JoinHandle<()>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Job>>>) -> Worker {
        let thread = thread::Builder::new()
            .name(format!("ssache-worker-{}", id))
            .spawn(move || loop {
                let job = receiver.lock().unwrap().recv().unwrap();
                debug!("Worker {id} got a job; executing.");
                job();
            })
            .unwrap();
        Worker { id, thread }
    }
}

#[derive(Debug, PartialEq)]
pub struct PoolCreationError;

#[derive(Debug, PartialEq)]
pub struct PoolExecutionError;

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Job>,
}

impl ThreadPool {
    /// Create a new ThreadPool.
    ///
    /// The size is the number of threads in the pool. If the size is
    /// zero a [`PoolCreationError`] is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use ssache::ThreadPool;
    /// let pool = ThreadPool::new(0);
    ///
    /// assert_eq!(pool.is_ok(), false);
    /// ```
    pub fn new(size: usize) -> Result<ThreadPool, PoolCreationError> {
        if size == 0 {
            return Err(PoolCreationError);
        }

        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(size);

        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&receiver)))
        }

        Ok(ThreadPool { workers, sender })
    }

    pub fn execute<F>(&self, f: F) -> Result<(), PoolExecutionError>
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);
        return match self.sender.send(job) {
            Ok(_) => Ok(()),
            Err(_) => Err(PoolExecutionError),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_worker() {
        let (_, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));

        let worker = Worker::new(1, Arc::clone(&receiver));
        assert_eq!(worker.id, 1);
    }

    #[test]
    fn build_thread_pool_with_zero_threads() {
        let result = ThreadPool::new(0);
        assert_eq!(result.err(), Some(PoolCreationError));
    }

    #[test]
    fn build_thread_pool_with_two_threads() {
        let result = ThreadPool::new(2);
        assert_eq!(result.is_ok(), true);
        if let Ok(pool) = result {
            assert_eq!(pool.workers.len(), 2);
        }
    }
}
