use std::{
    sync::OnceLock,
    task::Waker,
    time::{Duration, Instant},
};

use super::OSHandle;

use crate::{
    executor::{ExecutorPort, ExecutorSession, TaskToken, WAKE_QUEUE},
    tunables,
};

use ds_ipc::*;
use litemap::LiteMap;

pub static REACTOR_PORT: OnceLock<IPCClientPort<ReactorCmd, ReactorReply>> = OnceLock::new();

// todo: pretty sure this can just be merged directly into the executor
pub struct Reactor {
    pub(crate) server: IPCServer<ReactorCmd, ReactorReply>,
    // pub(crate) executor_session: ExecutorSession,
}

impl Reactor {
    pub fn new() -> Reactor {
        Reactor {
            server: IPCServer::new().unwrap().0,
            // executor_session: ExecutorSession(executor_port.make_session().unwrap()),
        }
    }

    pub fn run(self) {
        let Reactor {
            server,
            // executor_session,
        } = self;
        let handler = ReactorHandler {
            // executor_session,
            wait_tokens: LiteMap::new(),
        };
        let _ = REACTOR_PORT.set(server.client());
        server.run(handler);
    }

    pub fn run_thread(self) -> std::thread::JoinHandle<()> {
        std::thread::Builder::new()
            .stack_size(tunables::reactor::THREAD_STACK_SIZE)
            .spawn(move || self.run())
            .unwrap()
    }
}

struct ReactorHandler {
    wait_tokens: LiteMap<OSHandle, Vec<TaskToken>>,
    // executor_session: ExecutorSession,
}

impl IPCServerHandler<ReactorCmd, ReactorReply> for ReactorHandler {
    fn handle_request(
        &mut self,
        request: ReactorCmd,
        server: &mut IPCServer<ReactorCmd, ReactorReply>,
    ) -> ReactorReply {
        match request {
            ReactorCmd::AddHandle(task, handle) => {
                self.wait_tokens.entry(handle).or_default().push(task);
                server.add_handle_to_list(handle);
                ReactorReply::Ok
            }
            ReactorCmd::PopHandle => todo!(),
        }
    }

    fn handle_additional_oshandle(
        &mut self,
        handle: OSHandle,
        _server: &mut IPCServer<ReactorCmd, ReactorReply>,
    ) {
        for token in self.wait_tokens.remove(&handle).unwrap() {
            WAKE_QUEUE.get().unwrap().add(token);
            // self.executor_session.wake(token);
        }
    }
}

#[derive(IPCMessage, Debug)]
#[repr(u32)]
pub enum ReactorCmd {
    AddHandle(#[normal] TaskToken, #[move_handle] OSHandle) = 0xA,
    PopHandle = 0xB,
}

impl ReactorCmd {
    /// only valid for wakers from this executor! else uh. bad time?
    pub fn add_handle_for_waker(waker: &Waker, handle: OSHandle) -> Self {
        ReactorCmd::AddHandle(TaskToken(waker.data() as u32), handle)
    }
}

#[derive(IPCMessage)]
#[repr(u32)]
pub enum ReactorReply {
    Ok = 0xA,
}

pub fn sleep(time: Duration) -> futures::Sleep {
    futures::Sleep {
        deadline: Instant::now() + time,
        timer: None,
    }
}

pub mod futures {
    use std::{task::Poll, time::Instant};

    use ctru_sys::{RESET_ONESHOT, svcCreateTimer, svcSetTimer};

    use crate::reactor::{REACTOR_PORT, ReactorCmd};

    pub struct Sleep {
        pub(super) deadline: Instant,
        pub(super) timer: Option<u32>,
    }

    impl Future for Sleep {
        type Output = ();

        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            let cur = Instant::now();
            if cur >= self.deadline {
                Poll::Ready(())
            } else if self.timer.is_none() {
                let mut timer = 0;
                unsafe { svcCreateTimer(&mut timer, RESET_ONESHOT) };
                unsafe { svcSetTimer(timer, (self.deadline - cur).as_nanos() as i64, 0) };

                REACTOR_PORT
                    .get()
                    .unwrap()
                    .make_session()
                    .unwrap()
                    .request(&ReactorCmd::add_handle_for_waker(cx.waker(), timer))
                    .unwrap();

                self.timer = Some(timer);

                Poll::Pending
            } else {
                Poll::Pending
            }
        }
    }
}
