pub use ctru_sys::Handle as OSHandle;

// pub mod arm_intrinsics;
pub mod ctru_thread;
pub mod ctru_utils;
pub mod err;
pub mod executor;
pub mod fs;
pub mod net;
pub mod reactor;
pub use err::{BunnyError, BunnyResult};

pub use executor::{spawn, spawn_blocking};
pub use reactor::sleep;

pub use ds_ipc::ds_try;

use crate::{executor::Executor, reactor::Reactor};
pub mod net_sync;
pub mod sync;

pub mod tunables {
    pub mod net {
        pub const SOCKET_WORKER_STACK_SIZE: usize = 1024 * 1024;
        pub const SOCKET_POLL_TIMEOUT: i32 = 100;
    }

    pub mod reactor {
        pub const THREAD_STACK_SIZE: usize = 1024 * 1024;
    }

    pub mod executor {
        pub const THREAD_STACK_SIZE: usize = 1024 * 1024;
        pub const EXECUTOR_THREAD_PRIORITY: i32 = 0x18;
    }
}

pub struct RuntimeConfiguration {
    pub fs_workers: usize,
    pub enable_soc: bool,
}

impl RuntimeConfiguration {
    pub fn run(self) -> Runtime {
        crate::sync::init().unwrap();

        let executor = Executor::new();
        let reactor = Reactor::new(&executor.server.client());

        let executor_thread = executor.run_thread();
        let reactor_thread = reactor.run_thread();

        if self.enable_soc {
            crate::net::init().unwrap();
        }

        crate::fs::init_sdmc(self.fs_workers);

        Runtime {
            reactor_thread,
            executor_thread,
        }
    }
}

#[allow(dead_code)]
pub struct Runtime {
    reactor_thread: std::thread::JoinHandle<()>,
    executor_thread: std::thread::JoinHandle<()>,
    // log_writer: Option<std::thread::JoinHandle<()>>
}

impl Runtime {
    pub fn block_on<T: Send + 'static>(self, fut: impl Future<Output = T> + Send + 'static) -> T {
        executor::spawn_and_block(fut)
    }
}
