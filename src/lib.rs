/*!
Plumbing is a library that manages pipelining requests through an
asynchronous request/reply system, such as an HTTP `Keep-Alive` connection
or [Redis] interactions via the [Redis Protocol].

The core of `plumbing` is the [`Pipeline`] struct, which manages a single
request / response connection. This connection consists of a pair of
[`Sink`] and [`Stream`], and should be set up such that each request sent
through the [`Sink`] will eventually result in a response being sent back
through the [`Stream`]. One example of how to create such a pair is:

- Open a TCP connection in tokio, get a `TcpStream`.
- Use `TcpStream::into_split` to split the stream into a reader and a writer
- Use `tokio_util::codec` to wrap these streams in an `Encoder` and `Decoder`
  for your protocol. This `Encoder` and `Decoder` serve as the `Sink` and
  `Stream` for the `Pipeline`.

Requests submitted to the [`Pipeline`] will return a [`Resolver`], which is
a `Future` that will resolve to the response for that request. Any number
of Resolvers can simultaneously exist, and the responses will be delivered
to each one in order, as they arrive through the underlying `Stream`.

Pipelines are backpressure sensitive and don't do their own buffering, so
submitting new requests will block if the underlying stream stops accepting
them. Similarly, each [`Resolver`] must be polled to retrieve their responses;
subsequent Resolvers will block until prior Resolvers have received responses
(or been dropped). Depending on your system, this means you may need to take
care that both the send or flush end is polled concurrently with the receiving
end.

`plumbing` is currently `#![no_std]`; it only requires `alloc` in order to
function.

# Example

This example uses a tokio task to create a fake, single-key database,
and then uses plumbing to manage some simple writes and reads to it.

```
mod fake_db {
    use futures::{channel::mpsc, stream::StreamExt, SinkExt};
    use tokio::task;

    #[derive(Debug)]
    pub struct FakeDb {
        counter: i32,
    }

    #[derive(Debug)]
    pub enum Request {
        Incr(i32),
        Decr(i32),
        Set(i32),
        Get,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Response {
        Ok,
        Value(i32),
    }

    pub fn create_db() -> (mpsc::Sender<Request>, mpsc::Receiver<Response>) {
        let (send_req, mut recv_req) = mpsc::channel(0);
        let (mut send_resp, recv_resp) = mpsc::channel(0);

        let _task = task::spawn(async move {
            let mut database = FakeDb { counter: 0 };

            while let Some(request) = recv_req.next().await {
                match request {
                    Request::Incr(count) => {
                        database.counter += count;
                        send_resp.send(Response::Ok).await.unwrap();
                    }
                    Request::Decr(count) => {
                        database.counter -= count;
                        send_resp.send(Response::Ok).await.unwrap();
                    }
                    Request::Set(value) => {
                        database.counter = value;
                        send_resp.send(Response::Ok).await.unwrap();
                    }
                    Request::Get => {
                        let response = Response::Value(database.counter);
                        send_resp.send(response).await.unwrap();
                    }
                }
            }
        });

        (send_req, recv_resp)
    }
}

use fake_db::{Request, Response, create_db};
use futures::{
    future,
    sink::SinkExt,
    FutureExt,
};
use plumbing::Pipeline;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (send, recv) = create_db();
    // Because the send channel can only handle 1 item at a time, we want
    // to buffer requests
    let send = send.buffer(20);

    let mut pipeline = Pipeline::new(send, recv);

    // Basic interaction
    let fut = pipeline.submit(Request::Set(10)).await?;

    // If we're buffering requests or responses, we may need to make sure
    // they both are being awaited concurrently. We need to await the flush
    // end, to make sure our pipelined requests are being pushed into the
    // service, as well as the response end, to make sure that responses
    // are being pulled. These need to be concurrent, because the service
    // might block reading requests until responses are sent.
    let (_, response) = future::join(pipeline.flush(), fut).await;
    assert_eq!(response.unwrap(), Response::Ok);

    let fut = pipeline.submit(Request::Get).await?;

    // We can use the flush_and helper to ensure that flush is awaited
    // concurrently but lazily with the response.
    let response = pipeline.flush_and(fut).await?;
    assert_eq!(response.unwrap(), Response::Value(10));

    // pipeline several requests together
    let write1 = pipeline.submit(Request::Incr(20)).await?;
    let write2 = pipeline.submit(Request::Decr(5)).await?;
    let read = pipeline.submit(Request::Get).await?;

    // We need to make sure all of these are polled
    let (_, _, _, response) = future::join4(pipeline.flush(), write1, write2, read).await;
    assert_eq!(response.unwrap(), Response::Value(25));

    // Alternatively, if we drop the futures returned by submit, the responses
    // associated with them will be silently discarded. We can use this to
    // keep only the responses we're interested in.
    let _ = pipeline.submit(Request::Set(0)).await?;
    let _ = pipeline.submit(Request::Incr(12)).await?;
    let _ = pipeline.submit(Request::Decr(2)).await?;
    let read1 = pipeline.submit(Request::Get).await?;

    let _ = pipeline.submit(Request::Decr(2)).await?;
    let _ = pipeline.submit(Request::Decr(2)).await?;
    let read2 = pipeline.submit(Request::Get).await?;

    // flush_and will only await the flush while the future is pending. This
    // will resolve as soon as `read1` resolves, regardless of whether the
    // flush completed.
    let resp1 = pipeline.flush_and(read1).await?;
    assert_eq!(resp1.unwrap(), Response::Value(10));

    let resp2 = pipeline.flush_and(read2).await?;
    assert_eq!(resp2.unwrap(), Response::Value(6));

    Ok(())
}
```

[Redis]: https://redis.io/
[Redis Protocol]: https://redis.io/topics/protocol
*/

#![no_std]

use core::{
    future::Future,
    mem,
    pin::Pin,
    task::{Context, Poll},
};

use foreback::FutureExt as _;
use futures::{
    channel::oneshot,
    future::{self, FusedFuture},
    ready,
    stream::Skip,
    FutureExt, Sink, SinkExt, Stream, StreamExt,
};

type ChainSend<St> = oneshot::Sender<ResolverChainItem<St>>;

/// ChainRecv is a helper wrapper around a Receiver of ResolverChainItem. It's
/// Future designed to handle the logic for those items; it aggregates the
/// skips and updates the receiver, until the actual stream item arrives, which
/// it then resolves to.
#[derive(Debug)]
struct ChainRecv<St> {
    recv: oneshot::Receiver<ResolverChainItem<St>>,
    skip: usize,
}

impl<St> ChainRecv<St> {
    fn new(recv: oneshot::Receiver<ResolverChainItem<St>>) -> Self {
        Self { recv, skip: 0 }
    }
}

impl<St> Future for ChainRecv<St> {
    type Output = Option<(St, usize)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            break match ready!(this.recv.poll_unpin(cx)) {
                Ok(ResolverChainItem::Reconnect(recv)) => {
                    this.recv = recv.recv;
                    this.skip += recv.skip;
                    continue;
                }
                Ok(ResolverChainItem::Stream { stream, skip }) => {
                    Poll::Ready(Some((stream, this.skip + skip)))
                }
                Err(..) => Poll::Ready(None),
            };
        }
    }
}

#[derive(Debug)]
enum ResolverChainItem<St> {
    Reconnect(ChainRecv<St>),
    Stream { stream: St, skip: usize },
}

#[derive(Debug)]
enum ResolverState<St> {
    // See Resolver::poll for a description of these states
    Chain {
        recv: ChainRecv<St>,
        send: ChainSend<St>,
    },
    Stream {
        skip: usize,
        stream: St,
        send: ChainSend<St>,
    },
    Disconnect,
    Dead,
}

impl<St> ResolverState<St> {
    /// Replace the state with Dead and return the previous state
    #[inline]
    fn take(&mut self) -> Self {
        mem::replace(self, ResolverState::Dead)
    }
}

/// A [`Future`] associated with a request submitted through a [`Pipeline`].
///
/// When you successfully submit a request to a [`Pipeline`], it returns a
/// `Resolver` that can be awaited to retrieve the response for that request.
/// Because responses are retrieved lazily and in order, *each* `Resolver` must
/// be awaited in order to receive the responses; later Resolvers will block
/// indefinitely until earlier Resolvers have returned their responses (or
/// been dropped).
///
/// If the [`Stream`] used by the [`Pipeline`] to retrieve responses closes
/// prematurely, the stream will be dropped and all remaining (and new)
/// Resolvers will return `None`.
///
/// If a `Resolver` is dropped, the response associated with it will simply
/// be discarded.
#[derive(Debug)]
#[must_use = "Resolvers do nothing unless polled"]
pub struct Resolver<St: Stream + Unpin> {
    state: ResolverState<St>,
}

impl<St: Stream + Unpin> Future for Resolver<St> {
    type Output = Option<St::Item>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            match this.state {
                // We should only be in the dead state transiently between transitions,
                // or after we returned Poll::Ready
                ResolverState::Dead => panic!("Can't re-poll a completed future"),

                // Disconnect means that the stream was already known to be closed before
                // this Resolver was even constructed, so we should just immediately
                // resolve with None.
                ResolverState::Disconnect => break Poll::Ready(None),

                // The chain state means a previous receiver in the chain has the stream
                // right now, and we're waiting for it to eventually come to use. Additionally,
                // We may receive "reconnect" messages, which indicate that our previous
                // resolver is aborting and is sending ITS previous resolver to ensure the
                // chain isn't broken (along with a skip count, indicating the total number
                // of items from the stream that are associated with aborted resolvers and
                // need to be discarded).
                ResolverState::Chain { ref mut recv, .. } => match ready!(recv.poll_unpin(cx)) {
                    // Our channel was closed without a send, indicating the stream returned
                    // None at some point. Clear our state to propagate the None to future
                    // Resolvers, then return it.
                    None => {
                        this.state = ResolverState::Dead;
                        break Poll::Ready(None);
                    }

                    // The previous Resolver has finished, which means it's our turn to drink
                    // from the stream. It's sent us the stream, along with a skip count in
                    // the event it aborted early.
                    Some((stream, skip)) => match this.state.take() {
                        ResolverState::Chain { send, .. } => {
                            this.state = ResolverState::Stream { stream, send, skip };
                        }
                        _ => unreachable!(),
                    },
                },

                // We are the current holder of the stream, so we're waiting for our element.
                // If we have a skip, that means previous Resolvers aborted without resolving,
                // which means that we need to take and discard that many elements before
                // claiming our own.
                ResolverState::Stream {
                    ref mut stream,
                    ref mut skip,
                    ..
                } => match ready!(stream.poll_next_unpin(cx)) {
                    // Stream ended. Clear the state and return the None.
                    // Clearing the state will close the send channel, which
                    // will in activate the next Resolver in the chain and
                    // so on.
                    None => {
                        this.state = ResolverState::Dead;
                        break Poll::Ready(None);
                    }

                    // We got an item, but we still have skips, which means
                    // it's an item associated with a previous aborted Resolver.
                    // Update skip and retry the loop.
                    Some(..) if *skip > 0 => *skip -= 1,

                    // We got our item! Send the stream down the line, then
                    // clear our own state and return it
                    Some(item) => match this.state.take() {
                        ResolverState::Stream { stream, send, .. } => {
                            // If the send channel is closed, that means that
                            // it was part of the pipeline, which was dropped.
                            // we can therefore silently let this send fail.
                            let _ = send.send(ResolverChainItem::Stream { stream, skip: 0 });
                            break Poll::Ready(Some(item));
                        }
                        _ => unreachable!(),
                    },
                },
            }
        }
    }
}

impl<St: Stream + Unpin> FusedFuture for Resolver<St> {
    fn is_terminated(&self) -> bool {
        matches!(self.state, ResolverState::Dead)
    }
}

impl<St: Stream + Unpin> Drop for Resolver<St> {
    fn drop(&mut self) {
        // When a resolver is dropped, we need to make sure the chain is
        // unbroken, so we forward our state via a ResolverChainItem to the
        // next Resolver in the chain, along with an incremented skip so that
        // it knows to skip our response.

        match self.state.take() {
            ResolverState::Chain { mut recv, send } => {
                recv.skip += 1;
                let _ = send.send(ResolverChainItem::Reconnect(recv));
            }

            ResolverState::Stream { stream, send, skip } => {
                let _ = send.send(ResolverChainItem::Stream {
                    stream,
                    skip: skip + 1,
                });
            }

            ResolverState::Disconnect | ResolverState::Dead => {}
        };
    }
}

/// PipelineState manages the receiving end of the Pipeline. This is the end
/// that is connected to new Resolvers.
#[derive(Debug)]
enum PipelineState<St: Stream + Unpin> {
    Chain(ChainRecv<St>),
    Stream { stream: St, skip: usize },
    Disconnect,
}

impl<St: Stream + Unpin> PipelineState<St> {
    /// Drive the open end of the chain. Poll the receiver to wait for the
    /// stream again, then poll the stream to retrieve and discard any items
    /// that need to be skipped. This is used to make sure we don't deadlock
    /// when submitting new requests, in the event that the tail Resolvers
    /// were dropped. This returns Ready when there's no more work to be done,
    /// but will be unreadied if more resolvers are created.
    fn poll_pump(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        loop {
            match *self {
                // Chain state: we are waiting for the stream to be delivered from
                // a prior Resolver
                PipelineState::Chain(ref mut recv) => {
                    *self = match ready!(recv.poll_unpin(cx)) {
                        Some((stream, skip)) => PipelineState::Stream { stream, skip },
                        None => PipelineState::Disconnect,
                    }
                }
                // Stream state, skip==0: There's no work left to do until a
                // Resolver takes ownership of the stream to pull a response out
                // of it
                PipelineState::Stream { skip: 0, .. } => break Poll::Ready(()),

                // Stream state, with skips: Poll and discard responses from the
                // stream
                PipelineState::Stream {
                    ref mut stream,
                    ref mut skip,
                } => match ready!(stream.poll_next_unpin(cx)) {
                    Some(..) => *skip -= 1,
                    None => *self = PipelineState::Disconnect,
                },
                PipelineState::Disconnect => break Poll::Ready(()),
            }
        }
    }

    fn pump(&mut self) -> impl Future<Output = ()> + '_ {
        future::poll_fn(move |cx| self.poll_pump(cx))
    }
}

/// A `Pipeline` manages sending requests through a stream and retrieving their
/// matching responses.
///
/// A pipeline manages request/response flow through a [`Sink`] and associated
/// [`Stream`]. The two halves should be set up such that:
///
/// - Items sent into the [`Sink`] are submitted to some underlying system as
/// requests.
/// - That system replies to each request with a response, in order, via the
/// [`Stream`].
///
/// This could be HTTP requests sent through a `Keep-Alive` connection, Redis
/// interactions through the [Redis Protocol], or anything else.
///
/// The `Pipeline` provides a [`submit`][Pipeline::submit] method, which
/// submits a request. Pipelines are backpressure sensitive, so this method
/// will block until the underlying [`Sink`] can accept it. Pipelines do not do
/// any extra buffering, so if you're using it to enqueue several requests at
/// once, be sure that the [`Sink`] has been set up with its own buffering.
///
/// The [`submit`][Pipeline::submit] method will return a [`Resolver`]
/// associated with that request. This `Resolver` is a future which will, when
/// awaited, resolve to the response associated with the Request. These
/// resolvers must be awaited or dropped in order to consume responses from the
/// underlying stream; be sure to set up concurrency or buffering to ensure
/// your request submissions don't get stuck because the system is waiting for
/// a response to be collected.
///
/// [Redis Protocol]: https://redis.io/topics/protocol
#[derive(Debug)]
pub struct Pipeline<Si, St: Stream + Unpin> {
    sink: Si,
    state: PipelineState<St>,
}

impl<Si: Unpin, St: Unpin + Stream> Pipeline<Si, St> {
    /// Construct a new `Pipeline` with associated channels for requests and
    /// responses. In order for the Pipeline's logic to function correctly,
    /// this pair must be set up such that each request submitted through
    /// `requests` eventually results in a `response` being sent back through
    /// `responses`, in order.
    pub fn new<T>(requests: Si, responses: St) -> Self
    where
        Si: Sink<T>,
    {
        Self {
            sink: requests,
            state: PipelineState::Stream {
                stream: responses,
                skip: 0,
            },
        }
    }

    /// Submit a request to this `Pipeline`, blocking until it can be sent to
    /// the underlying `Sink`. Returns a [`Resolver`] that can be used to
    /// await the response, or an error if the `Sink` returned an error.
    pub async fn submit<T>(&mut self, item: T) -> Result<Resolver<St>, Si::Error>
    where
        Si: Sink<T>,
    {
        // Make sure to poll our recv end in case any trailing resolvers were
        // dropped
        self.sink
            .feed(item)
            .with_background(self.state.pump().fuse())
            .await?;

        // Open a new channel; this is to where the resolver will forward the
        // stream
        let (send, recv) = oneshot::channel();
        let recv = ChainRecv::new(recv);

        // Swap the new recv end for the one we've stored; that previously
        // stored one will be given to the new Resolver.
        let state = match mem::replace(&mut self.state, PipelineState::Chain(recv)) {
            PipelineState::Chain(recv) => ResolverState::Chain { recv, send },
            PipelineState::Stream { stream, skip } => ResolverState::Stream { stream, skip, send },
            PipelineState::Disconnect => ResolverState::Disconnect,
        };

        Ok(Resolver { state })
    }

    /// Submit a request to this `Pipeline`. Same as [`submit`][Pipeline::submit],
    /// but this takes `self` by move and returns it as part of the result, which
    /// can make it easier to construct chained futures (for instance, via
    /// [`.then`][FutureExt::then]).
    pub async fn submit_owned<T>(mut self, item: T) -> (Self, Result<Resolver<St>, Si::Error>)
    where
        Si: Sink<T>,
    {
        let res = self.submit(item).await;
        (self, res)
    }

    /// Flush the underlying `Sink`, blocking until it's finished. Note that,
    /// depending on your request/response system, you may also need to be sure
    /// that any incomplete Resolvers are also being awaited so that the
    /// responses can be drained; this method only handles flushing the
    /// requests side. The [`flush_and`][Pipeline::flush_and] method is a
    /// helper for this.
    pub async fn flush<T>(&mut self) -> Result<(), Si::Error>
    where
        Si: Sink<T>,
    {
        // Make sure to poll our recv end in case any trailing resolvers were
        // dropped
        self.sink
            .flush()
            .with_background(self.state.pump().fuse())
            .await
    }

    /// Flush the underlying sink while awaiting polling the given future. If
    /// the flush encounters an error, that error will be returned. If the
    /// future completes before the flush is finished, the result of the future
    /// will be returned immediately. If the flush finishes, this will continue
    /// blocking until the given future is complete.
    ///
    /// This is a helper function designed to help ensure the requests are
    /// pushed concurrently with reading responses with Resolvers. It tries
    /// to perform a lazy amount of flushing work; it only polls flush while
    /// the given future hasn't resolved.
    pub async fn flush_and<T, F>(&mut self, fut: F) -> Result<F::Output, Si::Error>
    where
        Si: Sink<T>,
        F: Future,
    {
        fut.with_try_background(self.flush().fuse()).await
    }
}

impl<Si, St: Stream + Unpin> Pipeline<Si, St> {
    /// Finish the pipeline. Wait for all the Resolvers to complete (or abort),
    /// then return the original sink & stream. If the stream completed during
    /// the resolvers, return None instead of the stream.
    ///
    /// Note that this method will *not* do any additional request flushing, so
    /// be sure that all of the remaining resolvers are able to complete.
    ///
    /// This function returns a `Skip<St>` so that any responses associated
    /// with aborted Resolvers will be skipped.
    // TODO: Make a flushing version of finish. The only issue is how to handle
    // errors.
    pub async fn finish(self) -> (Si, Option<Skip<St>>) {
        let stream = match self.state {
            PipelineState::Chain(recv) => recv.await.map(|(stream, skip)| stream.skip(skip)),
            PipelineState::Stream { stream, skip } => Some(stream.skip(skip)),
            PipelineState::Disconnect => None,
        };

        (self.sink, stream)
    }
}
