use ds_ipc::*;

use crate::{
    err::BunnyResult,
    fs::{
        ArchiveHandle, ArchiveId, DSPath, FileAttributes, OpenFlags, SerializableDSPath,
        WriteOptions,
    },
};

#[derive(IPCMessage)]
#[repr(u32)]
pub(crate) enum FsUserMessage<'a> {
    Initialize = 0x801,
    OpenArchive(#[normal] ArchiveId, #[flatten] SerializableDSPath<'a, 0>) = 0x80c,
    OpenFile {
        #[normal]
        transaction: u32,
        #[normal]
        archive: ArchiveHandle,
        #[flatten]
        path: SerializableDSPath<'a, 0>,
        #[normal]
        flags: OpenFlags,
        #[normal]
        attributes: FileAttributes,
    } = 0x802,
}

#[derive(IPCMessage)]
#[repr(u32)]
pub(crate) enum FsUserReply {
    Initialize(#[normal] i32) = 0x801,
    OpenArchive(#[normal] i32, #[normal] ArchiveHandle) = 0x80c,
    OpenFile(#[normal] i32, #[move_handle] u32) = 0x802,
}

pub struct Filesystem {
    inner: IPCClientSession<FsUserMessage<'static>, FsUserReply>,
}

impl Filesystem {
    pub unsafe fn from_raw(handle: u32) -> Filesystem {
        Filesystem {
            inner: unsafe { IPCClientSession::from_raw(handle) },
        }
    }

    pub fn open_archive<'a>(
        &self,
        archive: ArchiveId,
        path: impl Into<DSPath<'a>>,
    ) -> BunnyResult<ArchiveHandle> {
        let FsUserReply::OpenArchive(res_code, handle) = self
            .inner
            .request(&FsUserMessage::OpenArchive(archive, path.into().as_ser()))?
        else {
            panic!()
        };
        ds_try!(res_code);
        Ok(handle)
    }

    pub fn open_file<'a>(
        &self,
        archive: &ArchiveHandle,
        path: impl Into<DSPath<'a>>,
        flags: OpenFlags,
    ) -> BunnyResult<FileHandle> {
        let FsUserReply::OpenFile(res_code, handle) =
            self.inner.request(&FsUserMessage::OpenFile {
                transaction: 0,
                archive: *archive,
                path: path.into().as_ser(),
                flags,
                attributes: FileAttributes::empty(),
            })?
        else {
            panic!()
        };
        ds_try!(res_code);
        Ok(unsafe {
            FileHandle {
                inner: IPCClientSession::from_raw(handle),
            }
        })
    }
}

#[derive(IPCMessage)]
#[repr(u32)]
pub enum FileHandleMessage<'a> {
    Read {
        #[normal]
        offset: u64,
        #[normal]
        size: u32,
        #[map_buf(write)]
        data: &'a mut [u8],
    } = 0x802,
    Write {
        #[normal]
        offset: u64,
        #[normal]
        size: u32,
        #[normal]
        options: WriteOptions,
        #[map_buf(read)]
        data: &'a [u8],
    } = 0x803,
    GetSize = 0x804,
    Close = 0x808,
    Flush = 0x809,
}

#[derive(IPCMessage)]
#[repr(u32)]
pub(crate) enum FileHandleReply {
    Read(#[normal] i32, #[normal] u32) = 0x802,
    Write(#[normal] i32, #[normal] u32) = 0x803,
    GetSize(#[normal] i32, #[normal] u64) = 0x804,
    Close(#[normal] i32) = 0x808,
    Flush(#[normal] i32) = 0x809,
}

pub struct FileHandle {
    pub(crate) inner: IPCClientSession<FileHandleMessage<'static>, FileHandleReply>,
}

impl FileHandle {
    pub fn write(&mut self, offset: u64, data: &[u8], options: WriteOptions) -> BunnyResult<usize> {
        let FileHandleReply::Write(res_code, bytes_written) =
            self.inner.request(&FileHandleMessage::Write {
                offset,
                size: data.len() as u32,
                options,
                data,
            })?
        else {
            panic!()
        };
        ds_try!(res_code);

        Ok(bytes_written as usize)
    }

    pub fn read(&mut self, offset: u64, rd_buf: &mut [u8]) -> BunnyResult<usize> {
        let FileHandleReply::Read(res_code, bytes_read) =
            self.inner.request(&FileHandleMessage::Read {
                offset,
                size: rd_buf.len() as u32,
                data: rd_buf,
            })?
        else {
            panic!()
        };

        ds_try!(res_code);

        Ok(bytes_read as usize)
    }

    pub fn flush(&mut self) -> BunnyResult<()> {
        let FileHandleReply::Flush(res_code) = self.inner.request(&FileHandleMessage::Flush)?
        else {
            panic!()
        };
        ds_try!(res_code);
        Ok(())
    }

    pub fn close(self) -> BunnyResult<()> {
        let FileHandleReply::Close(res_code) = self.inner.request(&FileHandleMessage::Close)?
        else {
            panic!()
        };
        ds_try!(res_code);
        Ok(())
    }

    pub fn get_size(&mut self) -> BunnyResult<u64> {
        let FileHandleReply::GetSize(res_code, size) =
            self.inner.request(&FileHandleMessage::GetSize)?
        else {
            panic!()
        };
        ds_try!(res_code);
        Ok(size)
    }

    pub unsafe fn from_raw(handle: u32) -> FileHandle {
        unsafe {
            FileHandle {
                inner: IPCClientSession::from_raw(handle),
            }
        }
    }

    pub unsafe fn duplicate(&self) -> FileHandle {
        FileHandle {
            inner: unsafe { IPCClientSession::from_raw(self.inner.session) },
        }
    }
}
