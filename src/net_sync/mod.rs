use std::{
    io,
    net::{SocketAddr, SocketAddrV4, ToSocketAddrs},
    sync::OnceLock,
};

use ctru_sys::{srvGetServiceHandle, svcCreateMemoryBlock};
use ds_ipc::*;
use libc::{AF_INET, SOCK_STREAM, memalign, sockaddr};
use socket2::{SockAddr, SockAddrStorage};

mod err;

pub use err::net_try;
pub use err::{NetError, NetResult};
use tracing::info;

// const SOCKADDR_STORAGE_LEN: usize = std::mem::size_of::<sockaddr_storage>();

fn sockaddr_as_bytes(addr: SocketAddrV4) -> [u8; 8] {
    let addr = SockAddr::from(addr);

    let mut tmp_addr = [0u8; 8];
    if addr.family() as i32 != AF_INET {
        panic!("only AF_INET addrs supported")
    };

    tmp_addr[0] = 8;
    tmp_addr[1] = addr.family() as u8;

    let mut addr_storage = addr.as_storage();
    let addr_storage_view = unsafe { addr_storage.view_as::<sockaddr>() };

    unsafe {
        std::ptr::copy_nonoverlapping(
            addr_storage_view.sa_data.as_mut_ptr(),
            tmp_addr.as_mut_ptr().add(2),
            6,
        )
    };

    tmp_addr
}

fn read_sockaddr(tmpaddr: &[u8]) -> SockAddr {
    let mut addr = SockAddrStorage::zeroed();
    let addr_storage = unsafe { addr.view_as::<sockaddr>() };

    addr_storage.sa_family = tmpaddr[1] as u16;
    let mut user_addrlen = tmpaddr[0] as usize;
    if addr_storage.sa_family == AF_INET as u16 {
        user_addrlen += 8;
    }

    unsafe {
        std::ptr::copy_nonoverlapping(
            tmpaddr.as_ptr().add(2),
            addr_storage.sa_data.as_mut_ptr(),
            user_addrlen - 2,
        )
    };

    unsafe { SockAddr::new(addr, user_addrlen as u32) }
}

#[derive(IPCMessage)]
#[repr(u32)]
pub(crate)  enum SocketMessage<'a> {
    Initialize {
        #[normal]
        mem_size: u32,
        #[cur_process]
        process_handle: u32,
        #[share_handle]
        memory_handle: u32,
    } = 0x1,
    Socket {
        #[normal]
        domain: i32,
        #[normal]
        ty: i32,
        #[normal]
        protocol: i32,
        #[cur_process]
        process_handle: u32,
    } = 0x2,
    Listen {
        #[normal]
        socket: u32,
        #[normal]
        max_conns: u32,
        #[cur_process]
        process_handle: u32,
    } = 0x3,
    Accept {
        #[normal]
        socket: u32,
        #[normal]
        max_addrlen: u32,
        #[cur_process]
        process_handle: u32,
        #[thread_static_buf]
        output_addr: &'a mut [u8],
    } = 0x4,
    Bind {
        #[normal]
        socket: u32,
        #[normal]
        socket_len: u32,
        #[cur_process]
        process_handle: u32,
        #[static_buf(0)]
        addr: &'a [u8],
    } = 0x5,
    Connect {
        #[normal]
        socket: u32,
        #[normal]
        socket_len: u32,
        #[cur_process]
        process_handle: u32,
        #[static_buf(0)]
        addr: &'a [u8],
    } = 0x6,
    RecvFrom {
        #[normal]
        socket: u32,
        #[normal]
        len: u32,
        #[normal]
        flags: u32,
        #[normal]
        addr_len: u32,
        #[cur_process]
        process_handle: u32,
        #[thread_static_buf]
        output: &'a mut [u8],
        #[thread_static_buf]
        src_addr: &'a mut [u8],
    } = 0x8,
    SendTo {
        #[normal]
        socket: u32,
        #[normal]
        len: u32,
        #[normal]
        flags: u32,
        #[normal]
        addr_len: u32,
        #[cur_process]
        process_handle: u32,
        #[static_buf(2)]
        data: &'a [u8],
        #[static_buf(1)]
        addr: &'a [u8],
    } = 0xA,
    Close {
        #[normal]
        socket: u32,
        #[cur_process]
        process_handle: u32,
    } = 0xB,
    Shutdown {
        #[normal]
        socket: u32,
        #[normal]
        how: i32,
        #[cur_process]
        process_handle: u32,
    } = 0xC,
    Poll {
        #[normal]
        fds_len: u32,
        #[normal]
        timeout: i32,
        #[cur_process]
        process_handle: u32,
        #[static_buf(10)]
        fds_in: &'a [PollFd],
        #[thread_static_buf]
        fds_out: &'a mut [PollFd],
    } = 0x14,
    Fcntl {
        #[normal]
        socket: u32,
        #[normal]
        cmd: i32,
        #[normal]
        arg: u32,
        #[cur_process]
        process_handle: u32,
    } = 0x13,
    ShutdownSockets = 0x19,
}

#[derive(IPCMessage)]
#[repr(u32)]
pub(crate)  enum SocketReply {
    Init(#[normal] i32) = 0x1,
    Socket(#[normal] i32, #[normal] u32) = 0x2,
    Listen(#[normal] i32, #[normal] i32) = 0x3,
    Accept(#[normal] i32, #[normal] i32) = 0x4,
    Bind(#[normal] i32, #[normal] i32) = 0x5,
    Connect(#[normal] i32, #[normal] i32) = 0x6,
    RecvFrom(#[normal] i32, #[normal] i32, #[normal] u32) = 0x8,
    SendTo(#[normal] i32, #[normal] i32) = 0xA,
    Close(#[normal] i32, #[normal] i32) = 0xB,
    Shutdown(#[normal] i32, #[normal] i32) = 0xC,
    Fcntl(#[normal] i32, #[normal] i32) = 0x13,
    Poll(#[normal] i32, #[normal] i32) = 0x14,
    ShutdownSockets(#[normal] i32) = 0x19,
}

pub static SOCKET_SERVICES: OnceLock<SocketService> = OnceLock::new();

pub fn init() -> NetResult<()> {
    if SOCKET_SERVICES.get().is_some() {
        Ok(())
    } else {
        SocketService::init()
    }
}

pub struct SocketService {
    memory_handle: u32,
    inner: IPCClientSession<SocketMessage<'static>, SocketReply>,
}

impl SocketService {
    fn init() -> NetResult<()> {
        let num_bytes = 0x100000;
        let socket_memory = unsafe { memalign(0x1000, num_bytes) } as *mut u32;
        let mut shared_mem = 0;
        ds_try!(unsafe {
            svcCreateMemoryBlock(
                &mut shared_mem,
                socket_memory as u32,
                num_bytes as u32,
                0,
                3,
            )
        });

        let mut service_handle = 0;
        ds_try!(unsafe { srvGetServiceHandle(&mut service_handle, c"soc:U".as_ptr()) });

        let handle = unsafe { IPCClientSession::from_raw(service_handle) };

        let SocketReply::Init(res_code) = handle.request(&SocketMessage::Initialize {
            mem_size: num_bytes as u32,
            process_handle: 0,
            memory_handle: shared_mem,
        })?
        else {
            panic!()
        };
        ds_try!(res_code);

        let _ = SOCKET_SERVICES.set(SocketService {
            memory_handle: shared_mem,
            inner: handle,
        });

        Ok(())
    }

    pub fn create_socket(&self) -> NetResult<SocketHandle> {
        let SocketReply::Socket(res, desc) = self.inner.request(&SocketMessage::Socket {
            domain: AF_INET,
            ty: SOCK_STREAM,
            protocol: 0,
            process_handle: 0,
        })?
        else {
            panic!()
        };

        ds_try!(res);

        Ok(SocketHandle(desc))
    }

    pub fn connect(&self, socket: &SocketHandle, addr: SocketAddrV4) -> NetResult<()> {
        let addr = sockaddr_as_bytes(addr);

        let SocketReply::Connect(res_code, posix_code) =
            self.inner.request(&SocketMessage::Connect {
                socket: socket.0,
                socket_len: addr.len() as u32,
                process_handle: 0,
                addr: &addr,
            })?
        else {
            panic!()
        };
        ds_try!(res_code);
        net_try(posix_code)?;

        Ok(())
    }

    pub fn bind(&self, socket: &SocketHandle, addr: SocketAddrV4) -> NetResult<()> {
        let addr = sockaddr_as_bytes(addr);

        let SocketReply::Bind(res_code, posix_code) = self.inner.request(&SocketMessage::Bind {
            socket: socket.0,
            socket_len: addr.len() as u32,
            process_handle: 0,
            addr: &addr,
        })?
        else {
            panic!()
        };

        ds_try!(res_code);
        net_try(posix_code)?;

        Ok(())
    }

    pub fn listen(&self, socket: &SocketHandle) -> NetResult<()> {
        let SocketReply::Listen(res_code, posix_code) =
            self.inner.request(&SocketMessage::Listen {
                socket: socket.0,
                max_conns: 8,
                process_handle: 0,
            })?
        else {
            panic!()
        };
        ds_try!(res_code);
        net_try(posix_code)?;
        Ok(())
    }

    pub fn accept(&self, socket: &SocketHandle) -> NetResult<(SocketHandle, SocketAddrV4)> {
        let mut tmpaddr = [0u8; 0x1C];

        let SocketReply::Accept(res_code, posix_code) =
            self.inner.request(&SocketMessage::Accept {
                socket: socket.0,
                max_addrlen: tmpaddr.len() as u32,
                process_handle: 0,
                output_addr: &mut tmpaddr,
            })?
        else {
            panic!()
        };

        dbg!(res_code);
        dbg!(posix_code);
        if res_code == 0 {
            net_try(posix_code)?;
        }

        let fd = posix_code as u32;

        Ok((
            SocketHandle(fd),
            read_sockaddr(&tmpaddr).as_socket_ipv4().unwrap(),
        ))
    }

    pub fn send_to(&self, socket: &SocketHandle, addr: SocketAddrV4, data: &[u8]) -> NetResult<()> {
        assert!(
            data.len() < 0x2000,
            "socket writes for more than 0x2000 bytes not yet supported ):"
        );

        let addr = sockaddr_as_bytes(addr);

        let SocketReply::SendTo(res_code, posix_code) =
            self.inner.request(&SocketMessage::SendTo {
                socket: socket.0,
                len: data.len() as u32,
                flags: 0,
                addr_len: addr.len() as u32,
                process_handle: 0,
                data,
                addr: &addr,
            })?
        else {
            panic!()
        };

        ds_try!(res_code);
        net_try(posix_code)?;

        Ok(())
    }

    pub fn recvfrom(&self, socket: &SocketHandle, data: &mut [u8]) -> NetResult<(SockAddr, u32)> {
        assert!(
            data.len() < 0x2000,
            "socket recvs for more than 0x2000 bytes not yet supported ):"
        );
        // let addr = sockaddr_as_bytes(addr);
        const SOCKADDR_STORAGE_LEN: usize = 0x1C;
        let mut tmpaddr = [0u8; 0x1C];

        let SocketReply::RecvFrom(res_code, posix_code, data_rxed) =
            self.inner.request(&SocketMessage::RecvFrom {
                socket: socket.0,
                len: data.len() as u32,
                flags: 0,
                addr_len: SOCKADDR_STORAGE_LEN as u32,
                process_handle: 0,
                output: data,
                src_addr: &mut tmpaddr,
            })?
        else {
            panic!()
        };

        ds_try!(res_code);
        net_try(posix_code)?;

        Ok((
            unsafe { SockAddr::new(SockAddrStorage::zeroed(), 0) },
            data_rxed,
        ))
    }

    pub fn poll(&self, poll_fds: &[PollFd], out: &mut [PollFd], timeout: i32) -> NetResult<()> {
        assert_eq!(poll_fds.len(), out.len());
        let SocketReply::Poll(res_code, posix_code) = self.inner.request(&SocketMessage::Poll {
            process_handle: 0,
            fds_len: poll_fds.len() as u32,
            fds_in: poll_fds,
            fds_out: out,
            timeout,
        })?
        else {
            panic!()
        };
        ds_try!(res_code);
        net_try(posix_code)?;
        Ok(())
    }

    pub fn set_nonblock(&self, socket: &SocketHandle) -> NetResult<()> {
        let SocketReply::Fcntl(res_code, posix_code) =
            self.inner.request(&SocketMessage::Fcntl {
                socket: socket.0,
                cmd: 0x4,
                arg: 0x4,
                process_handle: 0,
            })?
        else {
            panic!()
        };
        ds_try!(res_code);
        net_try(posix_code)?;
        Ok(())
    }

    pub fn close(&self, socket: &SocketHandle) -> NetResult<()> {
        let SocketReply::Close(res_code, posix_code) =
            self.inner.request(&SocketMessage::Close {
                socket: socket.0,
                process_handle: 0,
            })?
        else {
            panic!()
        };
        ds_try!(res_code);
        net_try(posix_code)?;
        Ok(())
    }

    pub fn shutdown(&self, socket: &SocketHandle, read: bool, write: bool) -> NetResult<()> {
        let how = if read && write {
            2
        } else if write && !read {
            1
        } else {
            0
        };
        let SocketReply::Shutdown(res_code, posix_code) =
            self.inner.request(&SocketMessage::Shutdown {
                socket: socket.0,
                how,
                process_handle: 0,
            })?
        else {
            panic!()
        };

        ds_try!(res_code);
        net_try(posix_code)?;

        Ok(())
    }

    pub fn shutdown_service(self) -> NetResult<()> {
        self.shutdown_service_inner()
    }

    fn shutdown_service_inner(&self) -> NetResult<()> {
        let SocketReply::ShutdownSockets(res_code) =
            self.inner.request(&SocketMessage::ShutdownSockets)?
        else {
            panic!()
        };
        ds_try!(res_code);
        Ok(())
    }
}

impl Drop for SocketService {
    fn drop(&mut self) {
        let _ = self.shutdown_service_inner();
    }
}

#[repr(C)]
#[derive(Clone, Copy, Default, Debug)]
pub struct PollFd {
    pub fd: u32,
    pub poll_in: PollFlags,
    pub poll_out: PollFlags,
}

impl PollFd {
    pub fn new(fd: u32, poll_in: PollFlags) -> PollFd {
        PollFd {
            fd,
            poll_in,
            poll_out: PollFlags::empty(),
        }
    }
}

bitflags::bitflags! {
    #[repr(transparent)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct PollFlags: u32 {
        const POLLIN = 0x01;
        const POLLPRI = 0x02;
        const POLLWRNORM = 0x08;
        const POLLWRBAND = 0x10;
        const POLLNVAL = 0x20;
        const POLLOUT = 0x08;

        const _ = !0;
    }
}

#[derive(Debug)]
pub struct SocketHandle(pub(crate) u32);

impl Drop for SocketHandle {
    fn drop(&mut self) {
        info!("closing socket: {:?}", self);
        SOCKET_SERVICES.get().unwrap().close(self);
    }
}

pub struct TcpSocket {
    addr: SocketAddrV4,
    handle: SocketHandle,
}

impl TcpSocket {
    unsafe fn from_fd(fd: SocketHandle, addr: SocketAddrV4) -> TcpSocket {
        TcpSocket { addr, handle: fd }
    }

    pub fn fd(&self) -> &SocketHandle {
        &self.handle
    }

    pub fn addr(&self) -> &SocketAddrV4 {
        &self.addr
    }

    pub fn connect(addr: impl ToSocketAddrs) -> NetResult<TcpSocket> {
        let soc = SOCKET_SERVICES.get().unwrap();

        let fd = soc.create_socket()?;
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        let SocketAddr::V4(addr) = addr else {
            panic!("only ipv4 supported")
        };
        soc.connect(&fd, addr)?;

        Ok(TcpSocket { addr, handle: fd })
    }

    pub fn bind(addr: impl ToSocketAddrs) -> NetResult<TcpListener> {
        let soc = SOCKET_SERVICES.get().unwrap();

        let fd = soc.create_socket()?;
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        let SocketAddr::V4(addr) = addr else {
            panic!("only ipv4 supported")
        };
        soc.bind(&fd, addr)?;
        soc.listen(&fd)?;

        Ok(TcpListener {
            inner: TcpSocket { addr, handle: fd },
        })
    }

    pub fn recv(&self, rd_buf: &mut [u8]) -> NetResult<usize> {
        let (_, bytes_read) = SOCKET_SERVICES
            .get()
            .unwrap()
            .recvfrom(&self.handle, rd_buf)?;

        Ok(bytes_read as usize)
    }

    pub fn send(&self, data: &[u8]) -> NetResult<()> {
        SOCKET_SERVICES
            .get()
            .unwrap()
            .send_to(&self.handle, self.addr, data)
    }

    pub fn poll(&self, events: PollFlags, timeout: i32) -> NetResult<PollFlags> {
        let in_fds = [PollFd::new(self.handle.0, events)];
        let mut out_fds = [PollFd::default()];

        SOCKET_SERVICES
            .get()
            .unwrap()
            .poll(&in_fds, &mut out_fds, timeout)?;

        Ok(out_fds[0].poll_out)
    }

    pub fn set_nonblock(&self) -> NetResult<()> {
        SOCKET_SERVICES.get().unwrap().set_nonblock(&self.handle)?;
        Ok(())
    }
}

impl std::io::Write for TcpSocket {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let buf = if buf.len() >= 0x2000 {
            &buf[..0x2000 - 1]
        } else {
            buf
        };

        self.send(buf)
            .map_err(|e| io::Error::other(e.to_string()))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct TcpListener {
    inner: TcpSocket,
}

impl TcpListener {
    pub fn accept(&self) -> NetResult<TcpSocket> {
        let (fd, addr) = SOCKET_SERVICES.get().unwrap().accept(&self.inner.handle)?;
        Ok(unsafe { TcpSocket::from_fd(fd, addr) })
    }
}
