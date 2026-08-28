use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};

use serde::Serialize;

use crate::error::KernelError;

#[derive(Debug)]
struct Counters {
    active: usize,
    waiting_tool: usize,
}

#[derive(Debug)]
struct Worker {
    id: String,
    generation: AtomicU64,
    capacity: usize,
    counters: Mutex<Counters>,
    healthy: AtomicBool,
    draining: AtomicBool,
    latency_ewma_micros: AtomicU64,
    error_ewma_ppm: AtomicU64,
}

#[derive(Clone, Debug, Serialize)]
pub struct WorkerSnapshot {
    pub index: usize,
    pub id: String,
    pub generation: u64,
    pub capacity: usize,
    pub active: usize,
    pub waiting_tool: usize,
    pub healthy: bool,
    pub draining: bool,
    pub score: u64,
}

pub struct Scheduler {
    workers: Vec<Arc<Worker>>,
    cursor: AtomicUsize,
}

pub struct WorkerLease {
    worker: Arc<Worker>,
    index: usize,
    active: bool,
}

impl Scheduler {
    pub fn new(worker_count: usize, slots_per_worker: usize) -> Self {
        assert!(worker_count > 0, "worker_count must be positive");
        assert!(slots_per_worker > 0, "slots_per_worker must be positive");

        let workers = (0..worker_count)
            .map(|index| {
                Arc::new(Worker {
                    id: format!("runtime-{index:02}"),
                    generation: AtomicU64::new(1),
                    capacity: slots_per_worker,
                    counters: Mutex::new(Counters {
                        active: 0,
                        waiting_tool: 0,
                    }),
                    healthy: AtomicBool::new(true),
                    draining: AtomicBool::new(false),
                    latency_ewma_micros: AtomicU64::new(0),
                    error_ewma_ppm: AtomicU64::new(0),
                })
            })
            .collect();

        Self {
            workers,
            cursor: AtomicUsize::new(0),
        }
    }

    pub fn acquire(&self, preferred: Option<usize>) -> Result<WorkerLease, KernelError> {
        if let Some(index) = preferred
            && let Some(lease) = self.try_acquire(index)
        {
            return Ok(lease);
        }

        let available: Vec<usize> = self
            .workers
            .iter()
            .enumerate()
            .filter_map(|(index, worker)| self.is_available(worker).then_some(index))
            .collect();

        if available.is_empty() {
            return Err(KernelError::NoCapacity);
        }

        let cursor = self.cursor.fetch_add(1, Ordering::Relaxed);
        let first = available[cursor % available.len()];
        let second = available[(cursor.wrapping_mul(7).wrapping_add(3)) % available.len()];
        let selected = if self.score(first) <= self.score(second) {
            first
        } else {
            second
        };
        let alternate = if selected == first { second } else { first };
        for index in std::iter::once(selected)
            .chain(std::iter::once(alternate))
            .chain(available)
        {
            if let Some(lease) = self.try_acquire(index) {
                return Ok(lease);
            }
        }
        Err(KernelError::NoCapacity)
    }

    pub fn resume(&self, index: usize, generation: u64) -> Result<WorkerLease, KernelError> {
        let worker = self
            .workers
            .get(index)
            .ok_or(KernelError::ContinuationLost)?;

        if worker.generation.load(Ordering::Acquire) != generation
            || !worker.healthy.load(Ordering::Acquire)
        {
            return Err(KernelError::ContinuationLost);
        }

        let mut counters = worker.counters.lock().expect("worker counters poisoned");
        if counters.waiting_tool == 0 {
            return Err(KernelError::ContinuationLost);
        }
        counters.waiting_tool -= 1;
        counters.active += 1;
        drop(counters);

        Ok(WorkerLease {
            worker: Arc::clone(worker),
            index,
            active: true,
        })
    }

    pub fn snapshots(&self) -> Vec<WorkerSnapshot> {
        self.workers
            .iter()
            .enumerate()
            .map(|(index, worker)| {
                let counters = worker.counters.lock().expect("worker counters poisoned");
                WorkerSnapshot {
                    index,
                    id: worker.id.clone(),
                    generation: worker.generation.load(Ordering::Relaxed),
                    capacity: worker.capacity,
                    active: counters.active,
                    waiting_tool: counters.waiting_tool,
                    healthy: worker.healthy.load(Ordering::Relaxed),
                    draining: worker.draining.load(Ordering::Relaxed),
                    score: score_worker(worker, &counters),
                }
            })
            .collect()
    }

    pub fn ready(&self) -> bool {
        self.workers.iter().any(|worker| self.is_available(worker))
    }

    pub fn expire_waiting(&self, index: usize, generation: u64) {
        let Some(worker) = self.workers.get(index) else {
            return;
        };
        if worker.generation.load(Ordering::Acquire) != generation {
            return;
        }
        let mut counters = worker.counters.lock().expect("worker counters poisoned");
        if counters.waiting_tool > 0 {
            counters.waiting_tool -= 1;
        }
    }

    fn try_acquire(&self, index: usize) -> Option<WorkerLease> {
        let worker = self.workers.get(index)?;
        if !worker.healthy.load(Ordering::Acquire) || worker.draining.load(Ordering::Acquire) {
            return None;
        }

        let mut counters = worker.counters.lock().expect("worker counters poisoned");
        if counters.active + counters.waiting_tool >= worker.capacity {
            return None;
        }
        counters.active += 1;
        drop(counters);

        Some(WorkerLease {
            worker: Arc::clone(worker),
            index,
            active: true,
        })
    }

    fn is_available(&self, worker: &Worker) -> bool {
        if !worker.healthy.load(Ordering::Acquire) || worker.draining.load(Ordering::Acquire) {
            return false;
        }
        let counters = worker.counters.lock().expect("worker counters poisoned");
        counters.active + counters.waiting_tool < worker.capacity
    }

    fn score(&self, index: usize) -> u64 {
        let worker = &self.workers[index];
        let counters = worker.counters.lock().expect("worker counters poisoned");
        score_worker(worker, &counters)
    }
}

fn score_worker(worker: &Worker, counters: &Counters) -> u64 {
    let utilization =
        ((counters.active + counters.waiting_tool) as u64 * 1_000_000) / worker.capacity as u64;
    let latency = worker
        .latency_ewma_micros
        .load(Ordering::Relaxed)
        .min(1_000_000);
    let errors = worker.error_ewma_ppm.load(Ordering::Relaxed).min(1_000_000);
    35 * utilization + 15 * latency + 15 * errors
}

impl WorkerLease {
    pub fn index(&self) -> usize {
        self.index
    }

    pub fn id(&self) -> &str {
        &self.worker.id
    }

    pub fn generation(&self) -> u64 {
        self.worker.generation.load(Ordering::Acquire)
    }

    pub fn park_waiting(mut self) {
        let mut counters = self
            .worker
            .counters
            .lock()
            .expect("worker counters poisoned");
        debug_assert!(counters.active > 0);
        counters.active -= 1;
        counters.waiting_tool += 1;
        self.active = false;
    }
}

impl Drop for WorkerLease {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut counters = self
            .worker
            .counters
            .lock()
            .expect("worker counters poisoned");
        debug_assert!(counters.active > 0);
        counters.active -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::Scheduler;

    #[test]
    fn waiting_tool_reserves_capacity_and_can_resume() {
        let scheduler = Scheduler::new(1, 1);
        let lease = scheduler.acquire(None).expect("acquire");
        let generation = lease.generation();
        lease.park_waiting();

        assert!(scheduler.acquire(None).is_err());
        let resumed = scheduler.resume(0, generation).expect("resume");
        drop(resumed);
        assert!(scheduler.acquire(None).is_ok());
    }
}
