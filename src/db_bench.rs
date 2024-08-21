mod db_bench;

use std::sync::Arc;
use std::time::Duration;
use bytes::{Bytes, BytesMut};
use leaky_bucket::RateLimiter;
use rand::RngCore;
use rand_xorshift::XorShiftRng;
use crate::config::WriteOptions;
use crate::db::Db;

pub trait KeyGenerator : Send {
    fn next_key(&self) -> Bytes;
}

// TODO: implement other distributions

pub struct RandomKeyGenerator {
    key_bytes: usize,
    rng: XorShiftRng,
}

impl RandomKeyGenerator {
    pub fn new(key_bytes: usize) -> Self {
        Self{
            key_bytes,
            rng: rand_xorshift::XorShiftRng::from_entropy()
        }
    }
}

impl KeyGenerator for RandomKeyGenerator {
    fn next_key(&mut self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(self.key_bytes);
        self.rng.fill_bytes(bytes.as_mut());
        bytes.freeze()
    }
}

pub struct DbBench {
    key_gen_supplier: Box<dyn Fn() -> Box<dyn KeyGenerator>>,
    val_size: usize,
    write_options: WriteOptions,
    write_rate: Option<u32>,
    read_rate: Option<u32>,
    write_tasks: u32,
    read_tasks: u32,
    num_keys: Option<u64>,
    duration: Option<Duration>,
    db: Arc<Db>,
}

impl DbBench {
    pub fn new() -> Self {

    }

    pub async fn run(&self) {
        let rate_limiter = self.write_rate.map(|r| Arc::new(
            RateLimiter::builder()
                .initial(r as usize)
                .max(r as usize)
                .interval(Duration::from_millis(1))
                .refill((r / 1000) as usize)
                .build()
        ));
        let mut write_tasks = Vec::new();
        for _ in 0..self.write_tasks {
            let write_task = WriteTask::new(
                (*self.key_gen_supplier)(),
                self.val_size,
                self.write_options.clone(),
                self.num_keys.clone(),
                self.duration.clone(),
                rate_limiter.clone(),
                self.db.clone()
            );
            write_tasks.push(
                tokio::spawn(async move { write_task.run().await })
            );
        }
        for write_task in write_tasks {
            write_task.await.unwrap();
        }
    }
}

struct WriteTask {
    key_generator: Box<dyn KeyGenerator>,
    val_size: usize,
    write_options: WriteOptions,
    num_keys: Option<u64>,
    duration: Option<Duration>,
    rate_limiter: Option<Arc<RateLimiter>>,
    db: Arc<Db>,
}

impl WriteTask {
    fn new(
        key_generator: Box<dyn KeyGenerator>,
        val_size: usize,
        write_options: WriteOptions,
        num_keys: Option<u64>,
        duration: Option<Duration>,
        rate_limiter: Option<Arc<RateLimiter>>,
        db: Arc<Db>,
    ) -> Self {
        Self {
            key_generator,
            val_size,
            write_options,
            num_keys,
            duration,
            rate_limiter,
            db,
        }
    }

    async fn run(&self) {
        let start = std::time::Instant::now();
        let mut keys_written = 0u64;
        let write_batch = 4;
        let mut val_rng = rand_xorshift::XorShiftRng::from_entropy();
        loop {
            if start.elapsed() >= self.duration.unwrap_or(Duration::MAX)  {
                break;
            }
            if keys_written >= self.num_keys.unwrap_or(u64::MAX) {
                break;
            }
            if let Some(rate_limiter) = &self.rate_limiter {
                rate_limiter.acquire(write_batch).await;
            }
            for _ in 0..write_batch {
                let key = self.key_generator.next_key();
                let mut value = Vec::with_capacity(self.val_size);
                val_rng.fill_bytes(value.as_mut_slice());
                self.db.put_with_options(key.as_ref(), value.as_ref(), &self.write_options).await;
            }
            keys_written += write_batch as u64;
        }
    }
}