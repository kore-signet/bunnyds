#!/bin/bash
cargo 3ds build
azahar ./target/armv6k-nintendo-3ds/debug/bunnyds.3dsx
# ctremu ./target/armv6k-nintendo-3ds/debug/asyncds.3dsx
