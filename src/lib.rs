use std::{
    sync::{mpsc, Arc, Mutex},
    thread,
};

use log::debug;

type Job = Box<dyn FnOnce() + Send + 'static>;

#[derive(Debug, PartialEq)]
pub struct PoolCreationError;

#[derive(Debug, PartialEq)]
pub struct PoolExecutionError;

pub struct ThreadPool {
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

        for id in 0..size {
            Self::prepare_receiver_for_execution(id, Arc::clone(&receiver))
        }

        Ok(ThreadPool { sender })
    }

    pub fn execute<F>(&self, f: F) -> Result<(), PoolExecutionError>
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);
        match self.sender.send(job) {
            Ok(_) => Ok(()),
            Err(_) => Err(PoolExecutionError),
        }
    }

    /// Prepares the receiver to get and execute a job on a worker thread.
    fn prepare_receiver_for_execution(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Job>>>) {
        thread::Builder::new()
            .name(format!("ssache-worker-{}", id))
            .spawn(move || loop {
                let job = receiver.lock().unwrap().recv().unwrap();
                debug!("Worker {id} got a job; executing.");
                job();
            })
            .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_thread_pool_with_zero_threads() {
        let result = ThreadPool::new(0);
        assert_eq!(result.err(), Some(PoolCreationError));
    }

    #[test]
    fn build_thread_pool_with_two_threads() {
        let result = ThreadPool::new(2);
        assert_eq!(result.is_ok(), true);
    }
}
