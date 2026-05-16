use std::arch::asm;

use ctru_sys::s32;

// based on https://github.com/rust-embedded/heapless/blob/ca95c4beaf7340d678acefe44817e87547a9b8b4/src/pool/treiber/llsc.rs#L135

// _LOCK_T
// libc
// use libc::_lock_t;

#[inline(always)]
pub unsafe fn __ldrex(addr: *const s32) -> s32 {
    let val: s32;

    unsafe { asm!("ldrex {}, [{}]", out(reg) val, in(reg) addr, options(nostack)) };

    val
}

#[inline(always)]
pub unsafe fn __strex(val: s32, addr: *const s32) -> s32 {
    let outcome;

    unsafe {
        asm!("strex {}, {}, [{}]", out(reg) outcome, in(reg) val, in(reg) addr, options(nostack))
    };

    outcome
}

#[inline(always)]
pub unsafe fn __clrex() {
    unsafe { asm!("clrex", options(nomem, nostack)) }
}
