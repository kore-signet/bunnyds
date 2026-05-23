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

pub fn net_try(ret_val: i32) -> BunnyResult<()> {
    if ret_val >= 0 {
        Ok(())
    } else {
        let ret_val = (-ret_val) as u32;
        let Some(ret) = ERROR_MAP.get(ret_val as usize) else {
            todo!("unknown net errors not yet impl'd")
        };
        Err(BunnyError::Libc(*ret))
    }
}

#[derive(Debug)]
pub enum BunnyError {
    Ctru(ctru::error::Error),
    Libc(i32),
    Other(&'static str),
}

impl From<ctru::error::Error> for BunnyError {
    fn from(value: ctru::error::Error) -> Self {
        BunnyError::Ctru(value)
    }
}

pub type BunnyResult<T> = Result<T, BunnyError>;

impl From<BunnyError> for ctru::error::Error {
    fn from(value: BunnyError) -> Self {
        match value {
            BunnyError::Ctru(error) => error,
            BunnyError::Libc(libc_no) => {
                let c = unsafe { CStr::from_ptr(libc::strerror(libc_no)) };
                ctru::Error::Libc(c.to_string_lossy().into())
            }
            BunnyError::Other(v) => ctru::error::Error::Other(v.into()),
        }
    }
}

impl std::fmt::Display for BunnyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BunnyError::Ctru(error) => write!(f, "{error}"),
            BunnyError::Libc(libc_no) => {
                let c = unsafe { CStr::from_ptr(libc::strerror(*libc_no)) };
                write!(f, "libc: {}", c.to_string_lossy())
            }
            BunnyError::Other(v) => write!(f, "{v}"),
        }
    }
}

impl std::error::Error for BunnyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }

    fn description(&self) -> &str {
        "description() is deprecated; use Display"
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        self.source()
    }
}

impl embedded_io_async::Error for BunnyError {
    fn kind(&self) -> embedded_io_async::ErrorKind {
        embedded_io_async::ErrorKind::Other // todo: improve this
    }
}
