![CI](https://github.com/Cardinal-Cryptography/rush/workflows/CI/badge.svg)

# rush

rust implementation of Aleph protocol

# examples

There is a dummy implementation of an honest committee member that is not
cryptographically secure and serves only as a working example of what has to be
implemented and not how it should be implemented. For detailed information on
what is required from a client using this library see [Decumentation](??).
The example may be run using:

    cargo run --example honest my_id n_members n_finalized

    my_id -- our index
    n_members -- size of the committee
    n_finalized -- number of data to be finalized
