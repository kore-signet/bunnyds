use std::ffi::CStr;

use bitflags::bitflags;
use ds_ipc::*;
use strum::{EnumDiscriminants, IntoDiscriminant};
use widestring::U16CStr;

#[derive(EnumDiscriminants)]
#[strum_discriminants(name(DSPathType))]
#[strum_discriminants(derive(strum::FromRepr))]
#[repr(u32)]
pub enum DSPath<'a> {
    Invalid = 0x0,
    Empty = 0x1,
    Binary(&'a [u8]) = 0x2,
    Ascii(&'a CStr) = 0x3,
    Utf16(&'a U16CStr) = 0x4,
}

impl<'a> DSPath<'a> {
    pub fn as_ser<const SLOT: u8>(&'a self) -> SerializableDSPath<'a, SLOT> {
        SerializableDSPath {
            ty: self.kind(),
            size: self.size() as u32,
            data: self.as_bytes(),
        }
    }

    pub fn kind(&self) -> DSPathType {
        self.discriminant()
    }

    pub fn size(&self) -> usize {
        match self {
            DSPath::Invalid => 0,
            DSPath::Empty => 0,
            DSPath::Binary(items) => items.len(),
            DSPath::Ascii(items) => items.to_bytes_with_nul().len(),
            DSPath::Utf16(items) => items.as_slice_with_nul().len() * 2,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        match self {
            DSPath::Invalid => &[],
            DSPath::Empty => &[],
            DSPath::Binary(items) => items,
            DSPath::Ascii(items) => items.to_bytes_with_nul(),
            DSPath::Utf16(items) => bytemuck::cast_slice(items.as_slice_with_nul()),
        }
    }
}

impl<'a> From<&'a U16CStr> for DSPath<'a> {
    fn from(value: &'a U16CStr) -> Self {
        DSPath::Utf16(value)
    }
}

impl_ipc_args_for_enum!(DSPathType);

#[derive(IPCSerializable)]
pub struct SerializableDSPath<'a, const SLOT: u8> {
    #[normal]
    pub ty: DSPathType,
    #[normal]
    pub size: u32,
    #[static_buf(SLOT)]
    pub data: &'a [u8],
}

#[derive(Debug, Copy, Clone, strum::FromRepr)]
#[repr(u32)]
pub enum ArchiveId {
    SelfNCCH = 0x00000003,
    SaveData = 0x00000004,
    ExtSaveData = 0x00000006,
    SharedExtSaveData = 0x00000007,
    SystemSaveData = 0x00000008,
    SDMC = 0x00000009,
    SDMCWriteOnly = 0x0000000A,
}

impl_ipc_args_for_enum!(ArchiveId);

#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct ArchiveHandle(pub u64);

impl_ipc_args_for_newty!(ArchiveHandle, u64);

bitflags! {
    pub struct FileAttributes: u32 {
        const DIR = 1 << 0;
        const HIDDEN = 1 << 8;
        const ARCHIVE = 1 << 16;
        const READ_ONLY = 1 << 24;
    }
}

impl_ipc_args_for_bitflags!(FileAttributes);

bitflags! {
    pub struct OpenFlags: u8 {
        const READ = 1 << 0;
        const WRITE = 1 << 1;
        const CREATE = 1 << 2;
    }
}

impl_ipc_args_for_bitflags!(OpenFlags);

bitflags! {
    #[derive(Debug)]
    pub struct WriteOptions: u32 {
        const FLUSH = 1 << 0;
        const UPDATE_TIMESTAMP = 1 << 8;
    }
}

impl_ipc_args_for_bitflags!(WriteOptions);
