use std::collections::VecDeque;

use ctru_sys::{
    LightEvent, LightEvent_Init, LightEvent_Signal, LightEvent_Wait, LightSemaphore,
    LightSemaphore_Acquire, LightSemaphore_Init, LightSemaphore_Release, RESET_ONESHOT,
    svcCreateSemaphore, svcReleaseSemaphore, svcWaitSynchronization,
};
use ds_ipc::ds_try;

use crate::err::BunnyResult;

/// libctru-based semaphore
pub struct Semaphore {
    semaphore: LightSemaphore,
}

impl Semaphore {
    /// creates a libctru semaphore with [max] permits.
    pub fn new(max: i16) -> Semaphore {
        let mut semaphore = LightSemaphore {
            current_count: 0,
            num_threads_acq: 0,
            max_count: 0,
        };
        unsafe { LightSemaphore_Init(&mut semaphore, 0, max) };
        Semaphore { semaphore }
    }

    /// acquire [permits], blocking until enough are added to the semaphore
    pub fn acquire_permits<'a>(&'a self, permits: i32) -> SemaphorePermit<'a> {
        unsafe {
            LightSemaphore_Acquire(
                &self.semaphore as *const LightSemaphore as *mut LightSemaphore,
                permits,
            );
        }
        SemaphorePermit {
            semaphore: self,
            permits,
        }
    }

    /// add [permits] to the semaphore
    pub fn add_permits(&self, permits: i32) {
        unsafe {
            LightSemaphore_Release(
                &self.semaphore as *const LightSemaphore as *mut LightSemaphore,
                permits,
            );
        }
    }
}

/// note! this does not release the permit on drop, you must do that explicitly with release()
pub struct SemaphorePermit<'a> {
    semaphore: &'a Semaphore,
    permits: i32,
}

impl<'a> SemaphorePermit<'a> {
    pub fn release(self) {
        self.semaphore.add_permits(self.permits);
    }
}

/// libctru-based notification/signal
pub struct WaitSignal {
    event: LightEvent,
}

impl Default for WaitSignal {
    fn default() -> Self {
        Self::new()
    }
}

impl WaitSignal {
    pub fn new() -> WaitSignal {
        let mut light_event = LightEvent { state: 0, lock: 0 };
        unsafe { LightEvent_Init(&mut light_event, RESET_ONESHOT) };
        WaitSignal { event: light_event }
    }

    /// waits until event is signaled
    pub fn wait(&self) {
        unsafe {
            LightEvent_Wait(&self.event as *const LightEvent as *mut LightEvent);
        }
    }

    /// signals a single waiter that's waiting on this signal
    pub fn signal(&self) {
        unsafe {
            LightEvent_Signal(&self.event as *const LightEvent as *mut LightEvent);
        }
    }
}

/// os-semaphore based queue
pub struct SyncQueue<T> {
    pub(crate) semaphore_handle: u32,
    pub(crate) vals: parking_lot::Mutex<VecDeque<T>>,
}

impl<T> Default for SyncQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> SyncQueue<T> {
    pub fn new() -> Self {
        let mut semaphore = 0;
        unsafe { svcCreateSemaphore(&mut semaphore, 0, 255) };
        SyncQueue {
            semaphore_handle: semaphore,
            vals: parking_lot::Mutex::new(VecDeque::new()),
        }
    }

    pub fn add(&self, val: T) {
        self.vals.lock().push_back(val);
        let mut out = 0;
        unsafe { svcReleaseSemaphore(&mut out, self.semaphore_handle, 1) };
    }

    pub fn remove(&self) -> Option<T> {
        self.vals.lock().pop_back()
    }

    pub fn wait(&self, timeout: i64) -> BunnyResult<()> {
        ds_try!(unsafe { svcWaitSynchronization(self.semaphore_handle, timeout) });
        Ok(())
    }
}
