use futures::{future::ready, stream::repeat, Future, FutureExt, Stream};

pub(crate) fn into_infinite_stream<F: Future>(f: F) -> impl Stream<Item = ()> {
    f.then(|_| ready(repeat(()))).flatten_stream()
}
