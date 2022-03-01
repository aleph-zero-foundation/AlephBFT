#!/bin/bash

rustup component add llvm-tools-preview

cargo install rustfilt --version 0.2.1
cargo install cargo-binutils --version 0.3.5
