use std::marker::PhantomData;

pub(crate) fn after_iter<'a, I>(
    iter: impl Iterator<Item = I> + 'a,
    action: impl FnOnce(),
) -> impl Iterator<Item = I> {
    struct EndSignal<S, I> {
        signal: S,
        _phantom: PhantomData<I>,
    }
    struct EmptyIterator<S, I> {
        action: Option<S>,
        _marker: PhantomData<I>,
    }
    impl<S: FnOnce(), I> Iterator for EmptyIterator<S, I> {
        type Item = I;

        fn next(&mut self) -> Option<Self::Item> {
            self.action.take().and_then(|a| Some((a)()));
            None
        }
    }

    impl<S: FnOnce(), I> IntoIterator for EndSignal<S, I> {
        type Item = I;

        type IntoIter = EmptyIterator<S, I>;

        fn into_iter(self) -> Self::IntoIter {
            EmptyIterator {
                action: Some(self.signal),
                _marker: PhantomData,
            }
        }
    }
    iter.chain(EndSignal {
        signal: action,
        _phantom: PhantomData,
    })
}
