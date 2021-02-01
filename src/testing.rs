pub mod environment {
	use crate::{skeleton::Message, traits::HashT};
	use derive_more::Display;
	use futures::{Sink, Stream};
	use serde::{Deserialize, Serialize};
	use std::pin::Pin;
	use std::task::{Context, Poll};
	use tokio::sync::broadcast::*;

	type Error = ();

	#[derive(Hash, Debug, Display, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
	pub struct Id(pub u32);

	#[derive(
		Hash,
		Debug,
		Default,
		Display,
		Clone,
		Copy,
		PartialEq,
		Eq,
		Ord,
		PartialOrd,
		Serialize,
		Deserialize,
	)]
	pub struct Hash(pub u32);

	type Out = Box<dyn Sink<Message<Hash>, Error = Error> + Send + Unpin>;
	type In = Box<dyn Stream<Item = Message<Hash>> + Send + Unpin>;

	pub(crate) struct Environment {
		id: Id,
		network: Network,
	}
	impl crate::traits::Environment for Environment {
		type NodeId = Id;
		type Hash = Hash;
		type InstanceId = u32;
		type Crypto = ();
		type Error = Error;
		type Out = Out;
		type In = In;

		fn finalize_block(&self, _: Self::Hash) {}
		fn best_block(&self) -> Self::Hash {
			Default::default()
		}
		fn crypto(&self) -> Self::Crypto {}
		fn hash(_data: &[u8]) -> Self::Hash {
			Default::default()
		}
		fn consensus_data(&self) -> (Self::Out, Self::In) {
			self.network.consensus_data()
		}
	}

	#[derive(Clone)]
	pub(crate) struct Network {
		sender: Sender<Message<Hash>>,
	}

	impl Network {
		pub(crate) fn new(capacity: usize) -> Self {
			let (sender, _) = channel(capacity);

			Network { sender }
		}
		pub(crate) fn consensus_data(&self) -> (Out, In) {
			let sink = Box::new(BSink(self.sender.clone()));
			let stream = Box::new(BStream(self.sender.subscribe()));

			(sink, stream)
		}
	}

	struct BSink(Sender<Message<Hash>>);

	impl Sink<Message<Hash>> for BSink {
		type Error = Error;
		fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
			Poll::Ready(Ok(()))
		}

		fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
			Poll::Ready(Ok(()))
		}

		fn poll_close(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
			Poll::Ready(Ok(()))
		}

		fn start_send(self: Pin<&mut Self>, m: Message<Hash>) -> Result<(), Self::Error> {
			self.0.send(m).map(|_r| ()).map_err(|_e| return ())
		}
	}

	struct BStream(Receiver<Message<Hash>>);

	impl BStream {}

	impl Stream for BStream {
		type Item = Message<Hash>;
		fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
			let mut f = Box::pin(self.0.recv());
			match futures::Future::poll(Pin::as_mut(&mut f), cx) {
				// here we may add custom logic for dropping/changing messages
				Poll::Ready(Ok(m)) => Poll::Ready(Some(m)),
				Poll::Ready(Err(_)) => Poll::Ready(None),
				Poll::Pending => Poll::Pending,
			}
		}
	}
}

mod tests {
	use super::environment::*;
	use crate::skeleton::Message;
	use futures::sink::SinkExt;
	use futures::stream::StreamExt;

	#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
	async fn comm() {
		let n_members = 2;
		let rounds = 1;
		let capacity = n_members * rounds;
		let n = Network::new(capacity);
		let (mut out0, mut in0) = n.consensus_data();
		let (mut out1, mut in1) = n.consensus_data();

		let h0 = tokio::spawn(async move {
			assert_eq!(
				in0.next().await.unwrap(),
				Message::FetchRequest(vec![], 0.into())
			);
			assert_eq!(
				in0.next().await.unwrap(),
				Message::FetchRequest(vec![], 1.into())
			);
		});

		let h1 = tokio::spawn(async move {
			assert_eq!(
				in1.next().await.unwrap(),
				Message::FetchRequest(vec![], 0.into())
			);
			assert_eq!(
				in1.next().await.unwrap(),
				Message::FetchRequest(vec![], 1.into())
			);
		});

		assert!(out0
			.send(Message::FetchRequest(vec![], 0.into()))
			.await
			.is_ok());
		assert!(out1
			.send(Message::FetchRequest(vec![], 1.into()))
			.await
			.is_ok());
		assert!(h0.await.is_ok());
		assert!(h1.await.is_ok());
	}
}
