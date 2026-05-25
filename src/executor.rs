use std::{
    sync::{Arc, Mutex, OnceLock},
    task::{Context, Poll, Waker},
};

use ctru_sys::{CUR_THREAD_HANDLE, svcWaitSynchronizationN};
use ds_ipc::*;
use futures::future::BoxFuture;
use litemap::LiteMap;
use tracing::{info, trace};

use crate::{
    ctru_thread::CtruThreadBuilder,
    ctru_utils::{SyncQueue, WaitSignal},
    err::BunnyResult,
    executor::waker::make_waker,
    tunables,
};

/// A token ID'ing one of our tasks.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Ord, Default)]
#[repr(transparent)]
pub struct TaskToken(pub u32);

impl TaskToken {
    pub fn from_waker(waker: &Waker) -> Self {
        TaskToken(waker.data() as u32)
    }
}

impl_ipc_args_for_newty!(TaskToken, u32);

pub type ExecutorPort = IPCClientPort<ExecutorCmd, ExecutorReply>;

pub struct ExecutorSession(pub IPCClientSession<ExecutorCmd, ExecutorReply>);

impl ExecutorSession {
    pub fn wake(&self, task: TaskToken) -> BunnyResult<()> {
        // self.0.request(&ExecutorCmd::WakeTask(task))?;
        WAKE_QUEUE.get().unwrap().add(task);
        Ok(())
    }
}

#[derive(IPCMessage, Clone, Copy, Debug)]
#[repr(u32)]
pub enum ExecutorCmd {
    WakeTask(#[normal] TaskToken) = 0xA,
}

#[derive(IPCMessage)]
#[repr(u32)]
pub enum ExecutorReply {
    Ok = 0xA,
}

pub static EXECUTOR_PORT: OnceLock<ExecutorPort> = OnceLock::new();

mod waker {
    use std::task::{RawWaker, RawWakerVTable, Waker};

    use crate::executor::{TaskToken, WAKE_QUEUE};

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_waker, wake, wake_by_ref, drop);

    /// waker design
    /// 3ds pointers are 32bit, which is also what we use for task ids!
    /// since that's the only data we need for waking up a task,
    /// we can just use the pointer as a built-in data token ;)
    /// (this is a hack. but it's fun!)

    unsafe fn clone_waker(data: *const ()) -> RawWaker {
        RawWaker::new(data, &VTABLE)
    }

    unsafe fn wake(data: *const ()) {
        // let message = ExecutorCmd::WakeTask(TaskToken(data as u32)); // cast pointer value as task token

        // let client = EXECUTOR_PORT.get().unwrap();
        // client.make_session().unwrap().request(&message).unwrap();
        WAKE_QUEUE.get().unwrap().add(TaskToken(data as u32));
    }

    unsafe fn wake_by_ref(data: *const ()) {
        unsafe { wake(data) }
    }

    // todo: impl popping of tasks
    unsafe fn drop(_data: *const ()) {}

    pub fn make_waker(task: TaskToken) -> Waker {
        unsafe { Waker::from_raw(RawWaker::new(task.0 as *const (), &VTABLE)) }
    }
}

/// task spawn queue
pub static TASK_QUEUE: OnceLock<SyncQueue<BoxFuture<'static, ()>>> = OnceLock::new();
/// needed for waking tasks from other tasks (since same-thread IPC calls block forever)
pub static WAKE_QUEUE: OnceLock<SyncQueue<TaskToken>> = OnceLock::new();

pub struct Executor {
    pub(crate) tasks: LiteMap<TaskToken, BoxFuture<'static, ()>>,
    // pub(crate) server: IPCServer<ExecutorCmd, ExecutorReply>,
    pub(crate) task_semaphore: u32,
    pub(crate) wake_semaphore: u32,
}

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

impl Executor {
    pub fn new() -> Executor {
        let (mut server, port) = IPCServer::new().unwrap();
        let _ = EXECUTOR_PORT.set(port);

        let task_semaphore = TASK_QUEUE.get_or_init(SyncQueue::new).semaphore_handle;
        let wake_semaphore = WAKE_QUEUE.get_or_init(SyncQueue::new).semaphore_handle;

        // server.add_handle_to_list(task_semaphore);
        // server.add_handle_to_list(wake_semaphore);

        Executor {
            tasks: LiteMap::new(),
            // server,
            wake_semaphore,
            task_semaphore,
        }
    }

    // pub fn spawn(&mut self, task: impl Future<Output = ()> + Send + 'static) -> Option<TaskToken> {
    //     let task_key: TaskToken = self
    //         .tasks
    //         .last_entry()
    //         .map_or(TaskToken(0), |v| TaskToken(v.key().0 + 1));
    //     let mut fut = Box::pin(task);

    //     // initial poll to let it register a waker and whatnot
    //     let task_waker = make_waker(task_key);
    //     let mut ctx = Context::from_waker(&task_waker);

    //     if let Poll::Ready(()) = fut.as_mut().poll(&mut ctx) {
    //         return None;
    //     }

    //     self.tasks.insert(task_key, fut);
    //     Some(task_key)
    // }

    pub fn run(self) {
        let Executor {
            tasks,
            // server,
            wake_semaphore,
            task_semaphore,
        } = self;
        ExecutorHandler { task_counter: 0, wake_semaphore, task_semaphore, tasks }.run();
        // server.run(ExecutorHandler {
        //     task_counter: 0,
        //     tasks,
        //     wake_semaphore,
        //     task_semaphore,
        // });
    }

    pub fn run_thread(self) -> std::thread::JoinHandle<()> {
        std::thread::Builder::new()
            .stack_size(tunables::executor::THREAD_STACK_SIZE)
            .spawn(move || {
                unsafe {
                    ctru_sys::svcSetThreadPriority(
                        CUR_THREAD_HANDLE,
                        tunables::executor::EXECUTOR_THREAD_PRIORITY,
                    )
                };
                self.run()
            })
            .unwrap()
    }
}

struct ExecutorHandler {
    pub task_counter: u32,
    pub tasks: LiteMap<TaskToken, BoxFuture<'static, ()>>,
    pub task_semaphore: u32,
    pub wake_semaphore: u32,
}

impl ExecutorHandler {
    fn run(mut self) {
        let handles = [self.task_semaphore, self.wake_semaphore];
        loop {
            let mut idx = 0;
            unsafe { svcWaitSynchronizationN(&mut idx, handles.as_ptr(), 2, false, i64::MAX) };
            if idx == 1 {
                while let Some(task) = WAKE_QUEUE.get().unwrap().remove() {
                    self.wake(task);
                }
            } else if idx == 0 {
                while let Some(task) = TASK_QUEUE.get().unwrap().remove() {
                    self.spawn(task);
                }
            }
        }
    }

    fn spawn(&mut self, mut fut: BoxFuture<'static, ()>) -> Option<TaskToken> {
        let task_key: TaskToken = TaskToken(self.task_counter);
        self.task_counter += 1;
        info!("spawning task {task_key:?}");
        // initial poll to let it register a waker and whatnot
        let task_waker = make_waker(task_key);
        let mut ctx = Context::from_waker(&task_waker);

        if let Poll::Ready(()) = fut.as_mut().poll(&mut ctx) {
            return None;
        }

        self.tasks.insert(task_key, fut);
        Some(task_key)
    }

    #[tracing::instrument(skip(self))]
    fn wake(&mut self, task: TaskToken) {
        let task_waker = make_waker(task);
        let mut ctx = Context::from_waker(&task_waker);

        let Some(task_fut) = self.tasks.get_mut(&task) else {
            return;
        };

        match task_fut.as_mut().poll(&mut ctx) {
            Poll::Ready(()) => {
                trace!("task done, resolving");
                self.tasks.remove(&task);
            }
            Poll::Pending => {
                trace!("task not done ):");
            }
        }
    }
}

// impl IPCServerHandler<ExecutorCmd, ExecutorReply> for ExecutorHandler {
//     fn handle_request(
//         &mut self,
//         request: ExecutorCmd,
//         _server: &mut IPCServer<ExecutorCmd, ExecutorReply>,
//     ) -> ExecutorReply {
//         match request {
//             ExecutorCmd::WakeTask(task) => {
//                 self.wake(task);
//                 ExecutorReply::Ok
//             }
//         }
//     }

//     fn handle_additional_oshandle(
//         &mut self,
//         handle: ctru_sys::Handle,
//         server: &mut IPCServer<ExecutorCmd, ExecutorReply>,
//     ) {
//         server.add_handle_to_list(handle);


//     }
// }

// this impl is heavily, heavily based on https://jacko.io/async_tasks.html#joinhandle

enum JoinState<T> {
    Unawaited,
    Awaited(Waker),
    Ready(T),
    Done,
}

/// A JoinHandle for an asynchronous task.
pub struct JoinHandle<T> {
    state: Arc<Mutex<JoinState<T>>>,
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut guard = self.state.lock().unwrap();

        match std::mem::replace(&mut *guard, JoinState::Done) {
            JoinState::Ready(val) => Poll::Ready(val),
            JoinState::Unawaited | JoinState::Awaited(_) => {
                *guard = JoinState::Awaited(cx.waker().clone());
                Poll::Pending
            }
            JoinState::Done => panic!("polled ready future ):"),
        }
    }
}

async fn wrap_with_join_state<F: Future>(
    future: F,
    join_state: Arc<Mutex<JoinState<F::Output>>>,
    signal: Option<Arc<WaitSignal>>,
) {
    let value = future.await;
    let mut guard = join_state.lock().unwrap();
    *guard = JoinState::Ready(value);
    if let JoinState::Awaited(waker) = &*guard {
        WAKE_QUEUE.get().unwrap().add(TaskToken::from_waker(waker));
    }
    if let Some(signal) = signal {
        signal.signal();
    }
}

/// Spawns an async task into the executor.
pub fn spawn<F, T>(future: F) -> JoinHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let join_state = Arc::new(Mutex::new(JoinState::Unawaited));
    let join_handle = JoinHandle {
        state: Arc::clone(&join_state),
    };
    let task = Box::pin(wrap_with_join_state(future, join_state, None));
    TASK_QUEUE.get().unwrap().add(task);
    info!("task spawned");
    join_handle
}

/// Spawns an async task into the executor, blocking until it completes.
pub fn spawn_and_block<F, T>(future: F) -> T
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let join_state = Arc::new(Mutex::new(JoinState::Unawaited));

    let signal = Arc::new(WaitSignal::new());

    let task = Box::pin(wrap_with_join_state(
        future,
        Arc::clone(&join_state),
        Some(Arc::clone(&signal)),
    ));
    TASK_QUEUE.get().unwrap().add(task);
    signal.wait();

    let mut guard = join_state.lock().unwrap();

    match std::mem::replace(&mut *guard, JoinState::Done) {
        JoinState::Ready(val) => val,
        _ => panic!("got ready signal, but future wasn't actually ready"),
    }
}

/// Spawns a blocking task and return an awaitable handle to its results.
/// CAREFUL! This spawns a *normal* 3ds thread, which can easily hog the entire cpu.
pub fn spawn_blocking<T: Send + 'static>(
    priority: Option<i32>,
    affinity: Option<i32>,
    func: impl FnOnce() -> T + Send + 'static,
) -> impl Future<Output = Option<T>> + Send {
    let (oneshot_tx, oneshot_rx) = crate::sync::oneshot();
    let _thread = CtruThreadBuilder {
        stack_size: 1024 * 1024,
        priority,
        affinity,
    }
    .spawn_thread(move || {
        let res = func();
        oneshot_tx.send(res);
    });
    // std::thread::spawn(move || {
    // let res = func();
    // oneshot_tx.send(res);
    // });
    oneshot_rx
}
