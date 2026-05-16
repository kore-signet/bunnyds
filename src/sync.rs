use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering},
    },
    task::Poll,
};

use ctru_sys::{
    ARBITRATION_SIGNAL, ARBITRATION_WAIT_IF_LESS_THAN, svcArbitrateAddressNoTimeout,
    svcCreateAddressArbiter,
};
use ds_ipc::{DSResult, ds_try};
use smallvec::SmallVec;

use crate::executor::{self, TaskToken};

static mut ADDRESS_ARBITER: u32 = 0;

pub fn init() -> DSResult<()> {
    ds_try!(unsafe { svcCreateAddressArbiter(&raw mut ADDRESS_ARBITER) });
    Ok(())
}

pub fn oneshot<T: Send>() -> (OneshotSender<T>, OneshotReceiver<T>) {
    let state = Arc::new(OneshotState {
        value: UnsafeCell::new(None),
        state: AtomicBool::new(false),
        rx_task: OnceLock::new(),
    });

    (
        OneshotSender {
            state: Arc::clone(&state),
        },
        OneshotReceiver { state },
    )
}

pub(crate) struct OneshotState<T: Send> {
    value: UnsafeCell<Option<T>>,
    state: AtomicBool,
    rx_task: OnceLock<TaskToken>,
}

impl<T: Send> OneshotState<T> {
    fn send(&self, value: T) -> bool {
        if self.state.load(Ordering::Acquire) {
            false
        } else {
            *unsafe { &mut *self.value.get() } = Some(value);
            self.state.store(true, Ordering::Release);
            if let Some(task) = self.rx_task.get() {
                executor::WAKE_QUEUE.get().unwrap().add(*task); // we might be in the same thread as the executor, so can't rely on IPC calls ):
            }

            true
        }
    }
}

unsafe impl<T: Send> Send for OneshotState<T> {}
unsafe impl<T: Send> Sync for OneshotState<T> {}

pub struct OneshotSender<T: Send> {
    state: Arc<OneshotState<T>>,
}

impl<T: Send> OneshotSender<T> {
    pub fn send(self, value: T) -> bool {
        self.state.send(value)
    }
}

impl<T: Send> Drop for OneshotSender<T> {
    fn drop(&mut self) {
        self.state.state.store(true, Ordering::Release);
        if let Some(task) = self.state.rx_task.get() {
            executor::WAKE_QUEUE.get().unwrap().add(*task); // we might be in the same thread as the executor, so can't rely on IPC calls ):
        }
    }

    // true
}

pub struct OneshotReceiver<T: Send> {
    state: Arc<OneshotState<T>>,
}

impl<T: Send> Future for OneshotReceiver<T> {
    type Output = Option<T>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let _ = self.state.rx_task.set(TaskToken::from_waker(cx.waker()));

        if self.state.state.load(Ordering::Acquire) {
            Poll::Ready((unsafe { &mut *self.state.value.get() }).take())
        } else {
            Poll::Pending
        }
    }
}

pub struct ArbitratedValue(pub AtomicI32);

impl ArbitratedValue {
    pub fn wait_less_than(&self, value: i32) -> DSResult<()> {
        ds_try!(unsafe {
            svcArbitrateAddressNoTimeout(
                ADDRESS_ARBITER,
                self.0.as_ptr() as u32,
                ARBITRATION_WAIT_IF_LESS_THAN,
                value,
            )
        });
        Ok(())
    }

    pub fn signal_one(&self) -> DSResult<()> {
        ds_try!(unsafe {
            svcArbitrateAddressNoTimeout(
                ADDRESS_ARBITER,
                self.0.as_ptr() as u32,
                ARBITRATION_SIGNAL,
                1,
            )
        });
        Ok(())
    }
}

// heavily based on libctru's LightLock
struct DsLock {
    arb: ArbitratedValue,
    waiters: parking_lot::Mutex<SmallVec<[TaskToken; 4]>>, // this should be uncontested always, i think?
}

impl DsLock {
    // returns whether locking succeeded. registers thread as waiting!
    fn try_lock_and_register(&self, token: TaskToken) -> bool {
        let state = self.arb.0.load(Ordering::Acquire);
        if state > 0 {
            self.arb.0.store(-state, Ordering::Release);
            true
        } else {
            self.arb.0.store(state - 1, Ordering::Release);
            self.waiters.lock().push(token);
            false
        }
    }

    // trys to lock, without registering thread as waiting. returns true if is now locked
    fn try_lock_release_waiter(&self) -> bool {
        let lock_state = self.arb.0.load(Ordering::Acquire);
        if lock_state > 0 {
            self.arb.0.store(-(lock_state - 1), Ordering::Release); // mark that there's one less thread waiting, and mark state as locked
            true
        } else {
            false
        }
    }

    unsafe fn unlock(&self) {
        let lock_state = self.arb.0.load(Ordering::Acquire);
        if lock_state < 0 {
            self.arb.0.store(-lock_state, Ordering::Release);
            let mut waiters = self.waiters.lock();
            if waiters.is_empty() {
                return;
            }

            let task = waiters.remove(0);
            drop(waiters);

            if task == TaskToken(u32::MAX) {
                let _ = self.arb.signal_one();
            } else {
                executor::WAKE_QUEUE.get().unwrap().add(task);
            }
        }
    }
}

pub struct DSMutex<T: Send> {
    state: DsLock,
    data: UnsafeCell<T>,
}

unsafe impl<T: Send> Send for DSMutex<T> {}
unsafe impl<T: Send> Sync for DSMutex<T> {}

pub struct DSMutexGuard<'a, T: Send>(&'a DSMutex<T>);

impl<'a, T: Send> Deref for DSMutexGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.0.data.get() }
    }
}

impl<'a, T: Send> DerefMut for DSMutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.0.data.get() }
    }
}

impl<'a, T: Send> Drop for DSMutexGuard<'a, T> {
    fn drop(&mut self) {
        unsafe { self.0.state.unlock() };
    }
}

impl<T: Send> DSMutex<T> {
    pub fn new(value: T) -> DSMutex<T> {
        DSMutex {
            state: DsLock {
                arb: ArbitratedValue(AtomicI32::new(1)),
                waiters: parking_lot::Mutex::new(SmallVec::new()),
            },
            data: UnsafeCell::new(value),
        }
    }

    pub fn lock_sync<'a>(&'a self) -> DSMutexGuard<'a, T> {
        // we use u32::MAX to indicate that this is a thread and not an async task
        if self.state.try_lock_and_register(TaskToken(u32::MAX)) {
            return DSMutexGuard(self);
        }

        loop {
            self.state.arb.wait_less_than(0);
            if self.state.try_lock_release_waiter() {
                return DSMutexGuard(self);
            }
        }
    }

    pub fn lock(&self) -> LockFut<'_, T> {
        LockFut {
            registered: false,
            lock: self,
        }
    }
}

pub struct LockFut<'a, T: Send> {
    lock: &'a DSMutex<T>,
    registered: bool,
}

impl<'a, T: Send> Future for LockFut<'a, T> {
    type Output = DSMutexGuard<'a, T>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        if self.registered {
            if self.lock.state.try_lock_release_waiter() {
                Poll::Ready(DSMutexGuard(self.lock))
            } else {
                Poll::Pending
            }
        } else {
            if self
                .lock
                .state
                .try_lock_and_register(TaskToken::from_waker(cx.waker()))
            {
                Poll::Ready(DSMutexGuard(self.lock))
            } else {
                self.registered = true;
                Poll::Pending
            }
        }
    }
}

const NOTIFICATION_READY: u32 = u32::MAX - 1;
const NOTIFICATION_UNINIT: u32 = u32::MAX;

pub fn notification() -> (NotificationSender, NotificationWaiter) {
    let state = Arc::new(AtomicU32::new(NOTIFICATION_UNINIT));
    (
        NotificationSender {
            state: Arc::clone(&state),
        },
        NotificationWaiter { state },
    )
}

pub struct NotificationWaiter {
    state: Arc<AtomicU32>,
}

pub struct NotificationSender {
    state: Arc<AtomicU32>,
}

impl NotificationSender {
    pub unsafe fn from_state(state: Arc<AtomicU32>) -> Self {
        NotificationSender { state }
    }

    pub fn into_state(self) -> Arc<AtomicU32> {
        self.state
    }

    pub fn notify(self) {
        let state = self.state.load(Ordering::Acquire);
        if state == NOTIFICATION_UNINIT || state == NOTIFICATION_READY {
        } else {
            let task = TaskToken(state);
            self.state.store(NOTIFICATION_READY, Ordering::Release);
            executor::WAKE_QUEUE.get().unwrap().add(task);
        }
    }
}

impl Future for NotificationWaiter {
    type Output = ();

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let state = self.state.load(Ordering::Acquire);
        if state == NOTIFICATION_READY {
            Poll::Ready(())
        } else if state == NOTIFICATION_UNINIT {
            self.state
                .store(TaskToken::from_waker(cx.waker()).0, Ordering::Release);
            Poll::Pending
        } else {
            Poll::Pending
        }
    }
}
