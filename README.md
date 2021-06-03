[![Crate][crate-image]][crate-link]
[![Docs][docs-image]][docs-link]
[![Build Status][build-image]][build-link]
[![Apache 2.0 Licensed][license-image]][license-link]
![Rust Stable][rustc-image]

# Overview

# Detailed documentation

# Implementation status

# Using the crate

# Examples

There is a dummy implementation of an honest committee member that is not
cryptographically secure and serves only as a working example of what has to be
implemented and not how it should be implemented. For detailed information on
what is required from a client using this library see [Documentation](??).
The example may be run using:

    cargo run --example dummy_honest my_id n_members n_finalized

    my_id -- our index, 0-based
    n_members -- size of the committee
    n_finalized -- number of data to be finalized

# Tests

# Resources

# License

# Contributing

[//]: # "badges"
[crate-image]: https://img.shields.io/crates/v/rush.svg
[crate-link]: https://crates.io/crates/rush
[docs-image]: https://docs.rs/rush/badge.svg
[docs-link]: https://docs.rs/rush
[build-image]: https://github.com/Cardinal-Cryptography/rush/workflows/CI/badge.svg
[build-link]: https://github.com/Cardinal-Cryptography/rush/actions?query=workflow%3ACI
[license-image]: https://img.shields.io/badge/license-Apache2.0-blue.svg
[license-link]: https://github.com/Cardinal-Cryptography/rush/blob/main/LICENSE
[rustc-image]: https://img.shields.io/badge/rustc-stable-blue.svg

[docs-image]:
[docs-link]: https://cardinal-cryptography.github.io/rush/index.html
