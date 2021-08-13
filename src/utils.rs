use futures::{future::ready, stream::iter, Future, FutureExt, Stream};
use std::iter::repeat;

pub(crate) fn into_infinite_stream<F: Future>(f: F) -> impl Stream<Item = ()> {
    f.then(|_| ready(iter(repeat(())))).flatten_stream()
}
