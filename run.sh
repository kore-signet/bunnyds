#!/bin/bash
cargo 3ds build --release
azahar ./target/armv6k-nintendo-3ds/release/bunnyds.3dsx
# ctremu ./target/armv6k-nintendo-3ds/debug/asyncds.3dsx
