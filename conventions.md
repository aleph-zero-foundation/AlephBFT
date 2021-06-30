# Conventions

A basic overview of (semi-)arbitrary conventions we follow in this repository.

## Logging

We use all the logging macros, for the purposes outlined below:

1. `error!` -- for errors that are fatal to the program or a significant subsystem (at least leaving them in a broken state).
2. `warn!` -- for errors that can be handled or unexpected states of the program that are not fatal.
3. `info!` -- for changes in the state of the program (e.g. started/stopped a subservice).
4. `debug!` -- for everything lower level than the above, but without many details.
5. `trace!` -- for extremely noisy logging, including writing out details about the state of the program.

A rule of thumb to decide between `debug!` and `trace!` is that the former should never include data more detailed than simple numeric identifiers.
No `warn!` logs should ever show up in a run that does not include Byzantine nodes.

All log calls should include a `target` parameter, with a `AlephBFT` prefix for messages in the core library.
