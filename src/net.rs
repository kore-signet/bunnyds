use std::{net::ToSocketAddrs, sync::Arc, task::Poll};

use tracing::trace;
use futures::FutureExt;

use crate::{
    executor::{EXECUTOR_PORT, ExecutorPort, ExecutorSession, TaskToken},
    net_sync::{PollFlags, TcpListener, TcpSocket},
    ctru_utils::SyncQueue,
    tunables,
};

pub use crate::net_sync::{NetError, NetResult, init};

#[derive(Clone, Copy, Debug)]
pub enum PollInterest {
    Read,
    Write,
}

#[derive(Clone)]
pub struct AsyncTcpSocket {
    interests_queue: Arc<SyncQueue<(TaskToken, PollInterest)>>,
    inner: Arc<TcpSocket>,
}

impl AsyncTcpSocket {
    pub fn wrap(socket: TcpSocket) -> AsyncTcpSocket {
        socket.set_nonblock().unwrap();
        AsyncTcpSocket {
            interests_queue: Arc::new(SyncQueue::new()),
            inner: Arc::new(socket),
        }
    }

    pub fn recv<'a>(&'a self, data: &'a mut [u8]) -> Recv<'a> {
        Recv { socket: self, data }
    }

    pub fn send<'a>(&'a self, data: &'a [u8]) -> SendFuture<'a> {
        SendFuture { socket: self, data }
    }

    pub fn connect(
        addr: impl ToSocketAddrs,
    ) -> impl Future<Output = NetResult<AsyncTcpSocket>> + Send {
        let (oneshot_tx, oneshot_rx) = crate::sync::oneshot();
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        std::thread::spawn(move || oneshot_tx.send(TcpSocket::connect(addr)));

        oneshot_rx.map(|v| {
            v.unwrap().map(|v| {
                let v = AsyncTcpSocket::wrap(v);
                let _socket_task = v.run(EXECUTOR_PORT.get().unwrap());
                v
            })
        })
    }

    pub fn bind(addr: impl ToSocketAddrs) -> NetResult<AsyncTcpListener> {
        TcpSocket::bind(addr).map(AsyncTcpListener::wrap)
    }

    pub fn run(&self, executor: &ExecutorPort) -> std::thread::JoinHandle<()> {
        let interests_queue = Arc::clone(&self.interests_queue);
        let executor = ExecutorSession(executor.make_session().unwrap());
        let socket = self.inner.clone();

        std::thread::Builder::new()
            .stack_size(tunables::net::SOCKET_WORKER_STACK_SIZE) // might even be okay to make this smaller
            .spawn(move || {
                let mut read_interests = Vec::new();
                let mut write_interests = Vec::new();

                loop {
                    if read_interests.is_empty() && write_interests.is_empty() {
                        trace!("{:?} => no interest, waiting...", socket.fd());
                        interests_queue.wait(i64::MAX).unwrap();
                    }

                    for (task, interest) in interests_queue.vals.lock().drain(..) {
                        trace!(
                            task = ?task,
                            interest = ?interest,
                            "{:?} => registering task",
                            socket.fd(),
                        );

                        match interest {
                            PollInterest::Read => read_interests.push(task),
                            PollInterest::Write => write_interests.push(task),
                        }
                    }

                    let has_write = !write_interests.is_empty();
                    let timeout = if has_write {
                        -1
                    } else {
                        tunables::net::SOCKET_POLL_TIMEOUT
                    };
                    let flags = if has_write {
                        PollFlags::POLLIN | PollFlags::POLLOUT
                    } else {
                        PollFlags::POLLIN
                    };

                    trace!(
                        flags = ?flags,
                        "{:?} => polling",
                        socket.fd(),
                    );

                    let poll_out = socket.poll(flags, timeout).unwrap();

                    trace!(res = ?poll_out, "{:?} => poll resolved", socket.fd());

                    if poll_out.contains(PollFlags::POLLOUT) {
                        for waker in write_interests.drain(..) {
                            executor.wake(waker).unwrap();
                        }
                    }

                    if poll_out.contains(PollFlags::POLLIN) {
                        for waker in read_interests.drain(..) {
                            executor.wake(waker).unwrap();
                        }
                    }
                }
            })
            .unwrap()
    }
}

pub struct AsyncTcpListener {
    socket: Arc<TcpListener>,
}

impl AsyncTcpListener {
    pub fn wrap(socket: TcpListener) -> AsyncTcpListener {
        AsyncTcpListener {
            socket: Arc::new(socket),
        }
    }

    pub fn accept(&self) -> impl Future<Output = NetResult<AsyncTcpSocket>> + Send {
        let (oneshot_tx, oneshot_rx) = crate::sync::oneshot();
        let socket = Arc::clone(&self.socket);
        std::thread::spawn(move || oneshot_tx.send(socket.accept()));

        oneshot_rx.map(|v| {
            v.unwrap().map(|v| {
                let v = AsyncTcpSocket::wrap(v);
                let _socket_task = v.run(EXECUTOR_PORT.get().unwrap());
                v
            })
        })
    }
}

pub struct Recv<'a> {
    socket: &'a AsyncTcpSocket,
    data: &'a mut [u8],
}

impl<'a> Future for Recv<'a> {
    type Output = NetResult<usize>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.socket.inner.recv(self.data) {
            Ok(res) => Poll::Ready(Ok(res)),
            Err(NetError::Libc(libc::EAGAIN)) => {
                self.socket
                    .interests_queue
                    .add((TaskToken::from_waker(cx.waker()), PollInterest::Read));
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

pub struct SendFuture<'a> {
    socket: &'a AsyncTcpSocket,
    data: &'a [u8],
}

impl<'a> Future for SendFuture<'a> {
    type Output = NetResult<()>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        match self.socket.inner.send(self.data) {
            Ok(_) => Poll::Ready(Ok(())),
            Err(NetError::Libc(libc::EAGAIN)) => {
                self.socket
                    .interests_queue
                    .add((TaskToken::from_waker(cx.waker()), PollInterest::Write));
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}
