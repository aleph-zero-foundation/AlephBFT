use futures::prelude::*;
use std::{
	pin::Pin,
	task::{Context, Poll},
};
use tokio::runtime::Runtime;

use crate::{
	dag::Dag,
	traits::Context as ContextT,
};


pub enum Error {}

pub trait Syncer {
	type MIn: Stream<Item = Unit>;
	type MOut: Sink<Unit, Error = Error>;
	type FIn: Stream<Item = Unit>;
	type FOut: Sink<Unit, Error = Error>;

	fn mcast_io(&self) -> (Self::MIn, Self::MOut);
	fn fetch_io(&self) -> (Self::FIn, Self::FOut);
}

pub trait Environment<C: ContextT>: Syncer {
	fn finalize_block(&self, _: C::Hash);
	fn best_block(&self) -> C::Hash;
	// sth needed in the future for randomness
	//fn crypto(&self) -> C;
}

pub struct ConsensusConfig {}

pub struct Consensus<C: ContextT> {
	_conf: ConsensusConfig,
	_env: Box<dyn Environment<C, MOut = (), MIn = (), FOut = (), FIn = ()>>,
	_dag: Dag<C>,
	_creator_sink: CreatorSink,
	_creator_stream: CreatorStream,
	_adder: Adder,
	_extender_sink: ExtenderSink,
	_extender_stream: ExtenderStream,
	_runtime: Runtime,
}

impl<C> Consensus<C> where C:ContextT {
	pub fn new(
		conf: ConsensusConfig,
		env: Box<dyn Environment<C, MOut = (), MIn = (), FOut = (), FIn = ()>>,
	) -> Self {
		let (creator_sink, creator_stream) = new_creator_channel();
		let (extender_sink, extender_stream) = new_extender_channel();
		Consensus {
			_conf: conf,
			_env: env,
			_dag: Dag::<C>::new(),
			_creator_sink: creator_sink,
			_creator_stream: creator_stream,
			_adder: Adder {},
			_extender_sink: extender_sink,
			_extender_stream: extender_stream,
			_runtime: Runtime::new().unwrap(),
		}
	}
}

impl<C> Future for Consensus<C> where C:ContextT {
	type Output = Result<(), Error>;

	fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
		Poll::Pending
	}
}

pub struct Unit {}



fn new_creator_channel() -> (CreatorSink, CreatorStream) {
	(CreatorSink {}, CreatorStream {})
}

struct CreatorSink {}

impl Sink<Unit> for CreatorSink {
	type Error = Error;

	fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		Poll::Ready(Ok(()))
	}

	fn start_send(self: Pin<&mut Self>, _: Unit) -> Result<(), Self::Error> {
		Ok(())
	}

	fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		Poll::Ready(Ok(()))
	}

	fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		Poll::Ready(Ok(()))
	}
}

struct CreatorStream {}

impl Stream for CreatorStream {
	type Item = Unit;

	fn poll_next(self: Pin<&mut Self>, __: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		Poll::Ready(None)
	}
}

struct Adder {}

impl Future for Adder {
	type Output = Result<(), Error>;

	fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
		Poll::Pending
	}
}

fn new_extender_channel() -> (ExtenderSink, ExtenderStream) {
	(ExtenderSink {}, ExtenderStream {})
}

struct ExtenderSink {}

impl Sink<Unit> for ExtenderSink {
	type Error = Error;

	fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		Poll::Ready(Ok(()))
	}

	fn start_send(self: Pin<&mut Self>, _: Unit) -> Result<(), Self::Error> {
		Ok(())
	}

	fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		Poll::Ready(Ok(()))
	}

	fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		Poll::Ready(Ok(()))
	}
}

struct ExtenderStream {}

impl Stream for ExtenderStream {
	type Item = Vec<Unit>;

	fn poll_next(self: Pin<&mut Self>, __: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		Poll::Ready(None)
	}
}
