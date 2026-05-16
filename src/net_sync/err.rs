use std::ffi::CStr;

const ERROR_MAP: [i32; 77] = [
    0, // 0
    libc::E2BIG,
    libc::EACCES,
    libc::EADDRINUSE,
    libc::EADDRNOTAVAIL,
    libc::EAFNOSUPPORT, // 5
    libc::EAGAIN,
    libc::EALREADY,
    libc::EBADF,
    libc::EBADMSG,
    libc::EBUSY, // 10
    libc::ECANCELED,
    libc::ECHILD,
    libc::ECONNABORTED,
    libc::ECONNREFUSED,
    libc::ECONNRESET, // 15
    libc::EDEADLK,
    libc::EDESTADDRREQ,
    libc::EDOM,
    libc::EDQUOT,
    libc::EEXIST, // 20
    libc::EFAULT,
    libc::EFBIG,
    libc::EHOSTUNREACH,
    libc::EIDRM,
    libc::EILSEQ, // 25
    libc::EINPROGRESS,
    libc::EINTR,
    libc::EINVAL,
    libc::EIO,
    libc::EISCONN, // 30
    libc::EISDIR,
    libc::ELOOP,
    libc::EMFILE,
    libc::EMLINK,
    libc::EMSGSIZE, // 35
    libc::EMULTIHOP,
    libc::ENAMETOOLONG,
    libc::ENETDOWN,
    libc::ENETRESET,
    libc::ENETUNREACH, // 40
    libc::ENFILE,
    libc::ENOBUFS,
    libc::ENODATA,
    libc::ENODEV,
    libc::ENOENT, // 45
    libc::ENOEXEC,
    libc::ENOLCK,
    libc::ENOLINK,
    libc::ENOMEM,
    libc::ENOMSG, // 50
    libc::ENOPROTOOPT,
    libc::ENOSPC,
    libc::ENOSR,
    libc::ENOSTR,
    libc::ENOSYS, // 55
    libc::ENOTCONN,
    libc::ENOTDIR,
    libc::ENOTEMPTY,
    libc::ENOTSOCK,
    libc::ENOTSUP, // 60
    libc::ENOTTY,
    libc::ENXIO,
    libc::EOPNOTSUPP,
    libc::EOVERFLOW,
    libc::EPERM, // 65
    libc::EPIPE,
    libc::EPROTO,
    libc::EPROTONOSUPPORT,
    libc::EPROTOTYPE,
    libc::ERANGE, // 70
    libc::EROFS,
    libc::ESPIPE,
    libc::ESRCH,
    libc::ESTALE,
    libc::ETIME, // 75
    libc::ETIMEDOUT,
];

pub fn net_try(ret_val: i32) -> NetResult<()> {
    if ret_val >= 0 {
        Ok(())
    } else {
        let ret_val = (-ret_val) as u32;
        let Some(ret) = ERROR_MAP.get(ret_val as usize) else {
            todo!("unknown net errors not yet impl'd")
        };
        Err(NetError::Libc(*ret))
    }
}

#[derive(Debug)]
pub enum NetError {
    Ctru(ctru::error::Error),
    Libc(i32),
}

impl From<ctru::error::Error> for NetError {
    fn from(value: ctru::error::Error) -> Self {
        NetError::Ctru(value)
    }
}

impl std::fmt::Display for NetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub type NetResult<T> = Result<T, NetError>;

impl From<NetError> for ctru::error::Error {
    fn from(value: NetError) -> Self {
        match value {
            NetError::Ctru(error) => error,
            NetError::Libc(libc_no) => {
                let c = unsafe { CStr::from_ptr(libc::strerror(libc_no)) };
                ctru::Error::Libc(c.to_string_lossy().into())
            }
        }
    }
}
