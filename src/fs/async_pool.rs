use std::{
    mem::ManuallyDrop,
    sync::{
        OnceLock,
        atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering},
    },
};

use ds_ipc::*;
use tracing::info;

use crate::{
    BunnyError, BunnyResult,
    ctru_utils::SyncQueue,
    executor::{EXECUTOR_PORT, ExecutorSession, TaskToken},
    fs::sync_impl::FileHandle,
};

pub(crate) static ASYNC_FS_POOL: OnceLock<SyncQueue<AsyncFsMsg>> = OnceLock::new();

pub fn init_pool(workers: usize) {
    ASYNC_FS_POOL.set(SyncQueue::new()).unwrap();

    let port = unsafe {
        EXECUTOR_PORT
            .get()
            .expect("executor not initialized")
            .duplicate()
    };
    for id in 0..workers {
        let session = port.make_session().unwrap();
        std::thread::spawn(move || {
            IoWorker {
                id,
                executor: ExecutorSession(session),
            }
            .run()
        });
    }
}

pub(crate) struct IoWorker {
    id: usize,
    executor: ExecutorSession,
}

impl IoWorker {
    pub(crate) fn run(self) {
        let task_queue = ASYNC_FS_POOL.get().unwrap();

        loop {
            task_queue.wait(i64::MAX).unwrap();
            let Some(task) = task_queue.remove() else {
                continue;
            };

            match task {
                AsyncFsMsg::Write(op) => {
                    info!("io.worker.{} write fd:{:x}", self.id, op.file);

                    let mut op = op.view();
                    let res: BunnyResult<usize> =
                        op.file
                            .write(op.offset as u64, op.data, super::WriteOptions::empty());
                    op.resolve(res);
                    // drop(guard);
                    self.executor.wake(op.task).unwrap();
                }
                AsyncFsMsg::Read(mut op) => {
                    let mut op = op.view_mut();
                    let res = op.file.read(op.offset as u64, op.data);
                    op.resolve(res);
                    self.executor.wake(op.task).unwrap();
                }
                AsyncFsMsg::Flush(op) => {
                    info!("io.worker.{} flush fd:{:x}", self.id, op.file);

                    let mut op = op.view();
                    let res = op.file.flush();
                    op.resolve(res);
                    self.executor.wake(op.task).unwrap();
                }
                AsyncFsMsg::Close(op) => {
                    info!("io.worker.{} close fd:{:x}", self.id, op.file);

                    let op = op.view();
                    let res = unsafe { op.file.duplicate().close() };
                    op.resolve(res);
                    self.executor.wake(op.task).unwrap();
                }
            }
        }
    }
}

/// An async-io file!
pub struct AsyncFile {
    pub(crate) handle: FileHandle,
    cursor: u32,
}

impl AsyncFile {
    pub fn wrap(file: FileHandle) -> AsyncFile {
        AsyncFile {
            handle: file,
            cursor: 0,
        }
    }

    /// Reads bytes from [offset] in file.
    pub fn read_at<'a>(&'a mut self, offset: u32, buf: &'a mut [u8]) -> io_futures::Read<'a> {
        io_futures::Read::new(&self.handle, offset, buf)
    }

    /// Writes bytes from [offset] in file
    pub fn write_at<'a>(&'a mut self, offset: u32, buf: &'a [u8]) -> io_futures::Write<'a> {
        io_futures::Write::new(&self.handle, offset, buf)
    }

    /// Flushes all data to file
    pub fn flush<'a>(&'a mut self) -> io_futures::Flush<'a> {
        io_futures::Flush::new(&self.handle)
    }

    /// this is blocking, because async overhead would probably be worse ? review this in future
    pub fn get_size(&mut self) -> BunnyResult<u64> {
        self.handle.get_size()
    }

    pub unsafe fn raw_handle(&mut self) -> &mut FileHandle {
        &mut self.handle
    }
}

impl embedded_io_async::ErrorType for AsyncFile {
    type Error = BunnyError;
}

impl embedded_io_async::Read for AsyncFile {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        match self.read_at(self.cursor, buf).await {
            Ok(v) => {
                self.cursor += v as u32;
                Ok(v)
            }
            Err(e) => Err(e),
        }
    }
}

impl embedded_io_async::Write for AsyncFile {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        match self.write_at(self.cursor, buf).await {
            Ok(bytes) => {
                self.cursor += bytes as u32;
                Ok(bytes)
            }
            Err(e) => Err(e),
        }
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        self.flush().await
    }
}

impl std::fmt::Debug for AsyncFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("AsyncFile")
            .field(&format_args!("{:#x}", self.handle.inner.session))
            .finish()
    }
}

#[derive(IPCMessage, Debug)]
#[repr(u32)]
pub(crate) enum AsyncFsMsg {
    Write(#[flatten] FileIoOperation) = 0xA,
    Read(#[flatten] FileIoOperation) = 0xB,
    Flush(#[flatten] FileControlOperation) = 0xC,
    Close(#[flatten] FileControlOperation) = 0xD,
}

#[derive(IPCMessage)]
#[repr(u32)]
pub(crate) enum AsyncFsReply {
    Ok = 0xA,
}

#[derive(IPCSerializable, Debug)]
pub(crate) struct FileIoOperation {
    #[normal]
    pub file: u32,
    #[normal]
    pub task: TaskToken,
    #[normal]
    pub state: u32, // atomici32 ptr (result code),
    #[normal]
    pub result: u32, // atomicu32 ptr (u32::MAX if not done, else bytes read)
    #[normal]
    pub offset: u32,
    #[normal]
    pub len: u32,
    #[normal]
    pub data_ptr: u32,
}

impl FileIoOperation {
    fn view<'a>(&'a self) -> FileIoOperationView<'a> {
        FileIoOperationView {
            file: ManuallyDrop::new(unsafe { FileHandle::from_raw(self.file) }),
            task: self.task,
            state: unsafe { AtomicI32::from_ptr(self.state as *mut i32) },
            result: unsafe { AtomicU32::from_ptr(self.result as *mut u32) },
            offset: self.offset,
            data: unsafe {
                std::slice::from_raw_parts(self.data_ptr as *const u8, self.len as usize)
            },
        }
    }

    fn view_mut<'a>(&'a mut self) -> FileIoOperationViewMut<'a> {
        FileIoOperationViewMut {
            file: ManuallyDrop::new(unsafe { FileHandle::from_raw(self.file) }),
            task: self.task,
            state: unsafe { AtomicI32::from_ptr(self.state as *mut i32) },
            result: unsafe { AtomicU32::from_ptr(self.result as *mut u32) },
            offset: self.offset,
            data: unsafe {
                std::slice::from_raw_parts_mut(self.data_ptr as *mut u8, self.len as usize)
            },
        }
    }
}

struct FileIoOperationView<'a> {
    file: ManuallyDrop<FileHandle>,
    task: TaskToken,
    state: &'a AtomicI32,
    result: &'a AtomicU32,
    offset: u32,
    data: &'a [u8],
}

impl<'a> FileIoOperationView<'a> {
    fn resolve(&self, res: BunnyResult<usize>) {
        match res {
            Ok(v) => {
                self.state.store(0, Ordering::Release);
                self.result.store(v as u32, Ordering::Release);
            }
            Err(BunnyError::Ctru(ctru::Error::Os(os_err))) => {
                self.state.store(os_err, Ordering::Release);
                self.result.store(0_u32, Ordering::Release);
            }
            Err(_) => todo!(),
        }
    }
}

struct FileIoOperationViewMut<'a> {
    file: ManuallyDrop<FileHandle>,
    task: TaskToken,
    state: &'a AtomicI32,
    result: &'a AtomicU32,
    offset: u32,
    data: &'a mut [u8],
}

impl<'a> FileIoOperationViewMut<'a> {
    fn resolve(&self, res: BunnyResult<usize>) {
        match res {
            Ok(v) => {
                self.state.store(0, Ordering::Release);
                self.result.store(v as u32, Ordering::Release);
            }
            Err(BunnyError::Ctru(ctru::Error::Os(os_err))) => {
                self.state.store(os_err, Ordering::Release);
                self.result.store(0_u32, Ordering::Release);
            }
            Err(_) => todo!(),
        }
    }
}

#[derive(IPCSerializable, Debug)]
pub(crate) struct FileControlOperation {
    #[normal]
    pub file: u32,
    #[normal]
    pub task: TaskToken,
    #[normal]
    pub state: u32, // atomici32 ptr (res code)
    #[normal]
    pub done: u32, // atomic bool ptr
}

impl FileControlOperation {
    fn view<'a>(&'a self) -> FileControlOpView<'a> {
        FileControlOpView {
            file: ManuallyDrop::new(unsafe { FileHandle::from_raw(self.file) }),
            task: self.task,
            state: unsafe { AtomicI32::from_ptr(self.state as *mut i32) },
            done: unsafe { AtomicBool::from_ptr(self.done as *mut bool) },
        }
    }
}

struct FileControlOpView<'a> {
    file: ManuallyDrop<FileHandle>,
    task: TaskToken,
    state: &'a AtomicI32,
    done: &'a AtomicBool,
}

impl<'a> FileControlOpView<'a> {
    fn resolve(&self, res: BunnyResult<()>) {
        match res {
            Ok(_) => {
                self.state.store(0, Ordering::Release);
                self.done.store(true, Ordering::Release);
            }
            Err(BunnyError::Ctru(ctru::Error::Os(os_err))) => {
                self.state.store(os_err, Ordering::Release);
                self.done.store(true, Ordering::Release);
            }
            Err(_) => todo!(),
        }
    }
}

pub mod io_futures {
    use std::{
        sync::atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering},
        task::Poll,
    };

    use ds_ipc::IPCClientSession;

    use crate::{
        BunnyResult,
        executor::TaskToken,
        fs::{
            async_pool::{ASYNC_FS_POOL, AsyncFsMsg, AsyncFsReply, FileIoOperation},
            sync_impl::FileHandle,
        },
    };

    fn state_resolve<T>(state: &AtomicI32, res: T) -> Poll<BunnyResult<T>> {
        let state = state.load(Ordering::Acquire);
        if ctru_sys::R_FAILED(state) || ctru_sys::R_SUMMARY(state) != ctru_sys::RS_SUCCESS {
            Poll::Ready(Err(ctru::Error::Os(state).into()))
        } else {
            Poll::Ready(Ok(res))
        }
    }

    pub struct Read<'a> {
        file: &'a FileHandle,
        offset: u32,
        data: &'a mut [u8],
        state: AtomicI32,
        bytes_read: AtomicU32,
        registered: bool,
    }

    impl<'a> Read<'a> {
        pub(crate) fn new(file: &'a FileHandle, offset: u32, buf: &'a mut [u8]) -> Self {
            Read {
                file,
                offset,
                data: buf,
                state: AtomicI32::new(0),
                bytes_read: AtomicU32::new(u32::MAX),
                registered: false,
            }
        }
    }

    impl<'a> Future for Read<'a> {
        type Output = BunnyResult<usize>;

        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            let bytes_read = self.bytes_read.load(Ordering::Acquire);
            if bytes_read == u32::MAX && !self.registered {
                ASYNC_FS_POOL
                    .get()
                    .expect("fs not initialized")
                    .add(AsyncFsMsg::Read(FileIoOperation {
                        file: self.file.inner.session,
                        task: TaskToken::from_waker(cx.waker()),
                        state: self.state.as_ptr() as u32,
                        result: self.bytes_read.as_ptr() as u32,
                        offset: self.offset,
                        len: self.data.len() as u32,
                        data_ptr: self.data.as_ptr() as u32,
                    }));
                self.registered = true;
                Poll::Pending
            } else if self.registered && bytes_read != u32::MAX {
                state_resolve(&self.state, bytes_read as usize)
            } else {
                Poll::Pending
            }
        }
    }

    pub struct Write<'a> {
        file: &'a FileHandle,
        offset: u32,
        data: &'a [u8],
        state: AtomicI32,
        bytes_written: AtomicU32,
        registered: bool,
    }

    impl<'a> Write<'a> {
        pub(crate) fn new(file: &'a FileHandle, offset: u32, buf: &'a [u8]) -> Self {
            Write {
                file,
                offset,
                data: buf,
                state: AtomicI32::new(0),
                bytes_written: AtomicU32::new(u32::MAX),
                registered: false,
            }
        }
    }

    impl<'a> Future for Write<'a> {
        type Output = BunnyResult<usize>;

        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            let bytes_read = self.bytes_written.load(Ordering::Acquire);
            if bytes_read == u32::MAX && !self.registered {
                ASYNC_FS_POOL
                    .get()
                    .expect("fs not initialized")
                    .add(AsyncFsMsg::Write(FileIoOperation {
                        file: self.file.inner.session,
                        task: TaskToken::from_waker(cx.waker()),
                        state: self.state.as_ptr() as u32,
                        result: self.bytes_written.as_ptr() as u32,
                        offset: self.offset,
                        len: self.data.len() as u32,
                        data_ptr: self.data.as_ptr() as u32,
                    }));

                self.registered = true;
                Poll::Pending
            } else if self.registered && bytes_read != u32::MAX {
                state_resolve(&self.state, bytes_read as usize)
            } else {
                Poll::Pending
            }
        }
    }

    pub struct Flush<'a> {
        file: &'a FileHandle,
        registered: bool,
        res: AtomicI32,
        done: AtomicBool,
    }

    impl<'a> Flush<'a> {
        pub(crate) fn new(file: &'a FileHandle) -> Self {
            Flush {
                file,
                registered: false,
                res: AtomicI32::new(0),
                done: AtomicBool::new(false),
            }
        }
    }

    impl<'a> Future for Flush<'a> {
        type Output = BunnyResult<()>;

        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> Poll<Self::Output> {
            if !self.registered {
                ASYNC_FS_POOL
                    .get()
                    .expect("fs not initialized")
                    .add(AsyncFsMsg::Flush(super::FileControlOperation {
                        file: self.file.inner.session,
                        task: TaskToken::from_waker(cx.waker()),
                        state: self.res.as_ptr() as u32,
                        done: self.done.as_ptr() as u32,
                    }));

                self.registered = true;
                Poll::Pending
            } else if self.done.load(Ordering::Acquire) {
                state_resolve(&self.res, ())
            } else {
                Poll::Pending
            }
        }
    }

    pub struct Close {
        file: FileHandle,
        registered: bool,
        res: AtomicI32,
        done: AtomicBool,
        client: IPCClientSession<AsyncFsMsg, AsyncFsReply>,
    }

    impl Future for Close {
        type Output = BunnyResult<()>;

        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> Poll<Self::Output> {
            if !self.registered {
                if let Err(e) =
                    self.client
                        .request(&AsyncFsMsg::Close(super::FileControlOperation {
                            file: self.file.inner.session,
                            task: TaskToken::from_waker(cx.waker()),
                            state: self.res.as_ptr() as u32,
                            done: self.done.as_ptr() as u32,
                        }))
                {
                    Poll::Ready(Err(e.into()))
                } else {
                    self.registered = true;
                    Poll::Pending
                }
            } else if self.done.load(Ordering::Acquire) {
                state_resolve(&self.res, ())
            } else {
                Poll::Pending
            }
        }
    }
}
