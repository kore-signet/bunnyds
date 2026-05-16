// heavily edited from : https://github.com/rust3ds/ctru-rs/blob/3dedcd9809b7f1ff34f190ef948dc35cac4bd05b/ctru-rs/src/thread.rs

use core::panic;
use std::{any::Any, cell::UnsafeCell, sync::Arc};

pub type ThreadResult<T> = std::result::Result<T, Box<dyn Any + Send + 'static>>;

struct Packet<T>(Arc<UnsafeCell<Option<ThreadResult<T>>>>);

impl<T> Clone for Packet<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

unsafe impl<T: Send> Send for Packet<T> {}
unsafe impl<T: Sync> Sync for Packet<T> {}

pub struct CtruThreadBuilder {
    pub stack_size: usize,
    pub priority: Option<i32>,
    pub affinity: Option<i32>,
}

impl CtruThreadBuilder {
    pub fn spawn_thread<F, T>(self, f: F) -> RawJoin<T>
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static,
    {
        let CtruThreadBuilder {
            stack_size,
            priority,
            affinity,
        } = self;

        let priority = priority.unwrap_or_else(|| unsafe {
            let mut priority = 0;
            ctru_sys::svcGetThreadPriority(&mut priority, 0xFFFF8000);
            priority
        });

        let affinity = affinity.unwrap_or_else(|| unsafe { ctru_sys::svcGetProcessorID() });

        let my_packet: Packet<T> = Packet(Arc::new(UnsafeCell::new(None)));
        let their_packet = my_packet.clone();

        let main = move || unsafe {
            let try_result = std::panic::catch_unwind(panic::AssertUnwindSafe(f));
            *their_packet.0.get() = Some(try_result);
        };

        RawJoin {
            native: Some(unsafe { RawThread::new(stack_size, priority, affinity, Box::new(main)) }),
            packet: my_packet,
        }
    }
}

pub struct RawThread {
    handle: ctru_sys::Thread,
}

unsafe impl Send for RawThread {}
unsafe impl Sync for RawThread {}

impl RawThread {
    pub unsafe fn new<'a>(
        stack: usize,
        priority: i32,
        affinity: i32,
        p: Box<dyn FnOnce() + 'a>,
    ) -> RawThread {
        extern "C" fn thread_func(start: *mut core::ffi::c_void) {
            unsafe { RawThread::_start_thread(start as *mut u8) }
        }

        let p = Box::new(p);
        let handle = unsafe {
            ctru_sys::threadCreate(
                Some(thread_func),
                &*p as *const _ as *mut _,
                stack.try_into().unwrap(),
                priority,
                affinity,
                false,
            )
        };

        if handle.is_null() {
            panic!("failed to spawn thread");
        } else {
            std::mem::forget(p);
            RawThread { handle }
        }
    }

    unsafe fn _start_thread(main: *mut u8) {
        (unsafe { Box::from_raw(main as *mut Box<dyn FnOnce()>) })()
    }

    pub fn join(self) {
        unsafe {
            let _ret = ctru_sys::threadJoin(self.handle, u64::MAX);
            ctru_sys::threadFree(self.handle);
            core::mem::forget(self);
        }
    }
}

impl Drop for RawThread {
    fn drop(&mut self) {
        unsafe { ctru_sys::threadDetach(self.handle) }
    }
}

pub struct RawJoin<T> {
    native: Option<RawThread>,
    packet: Packet<T>,
}

impl<T> RawJoin<T> {
    pub fn join(mut self) -> ThreadResult<T> {
        self.native.take().unwrap().join();
        unsafe { (*self.packet.0.get()).take().unwrap() }
    }
}
