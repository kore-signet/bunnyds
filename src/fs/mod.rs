pub mod async_pool;
pub mod sync_impl;
mod types;
use std::sync::OnceLock;

use crate::BunnyResult;
use ctru_sys::fsGetSessionHandle;
pub use types::*;
use widestring::U16CStr;

use crate::fs::{async_pool::AsyncFile, sync_impl::Filesystem};

static SDMC_ARCHIVE_HANDLE: OnceLock<ArchiveHandle> = OnceLock::new();
static FS_SESSION: OnceLock<Filesystem> = OnceLock::new();

/// Initializes the SDMC filesystem, alongside an async pool.
pub fn init_sdmc(archive_fs_workers: usize) -> ArchiveHandle {
    let fs =
        FS_SESSION.get_or_init(|| unsafe { Filesystem::from_raw(fsGetSessionHandle().read()) });
    async_pool::init_pool(archive_fs_workers);
    *SDMC_ARCHIVE_HANDLE.get_or_init(|| fs.open_archive(ArchiveId::SDMC, DSPath::Empty).unwrap())
}

/// Opens a file on the SDMC filesystem using the async fs pool.
pub fn open(path: &U16CStr, flags: OpenFlags) -> BunnyResult<AsyncFile> {
    let fs = FS_SESSION.get().unwrap();
    let archive = SDMC_ARCHIVE_HANDLE.get().unwrap();
    let file = fs.open_file(archive, path, flags)?;
    Ok(AsyncFile::wrap(file))
}
