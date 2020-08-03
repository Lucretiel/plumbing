use future::FusedFuture;
use futures::{channel::oneshot, future, ready, Sink, Stream, StreamExt};
use std::{
    future::Future,
    mem,
    pin::Pin,
    task::{Context, Poll},
};

type ChainSend<St> = oneshot::Sender<ResolverChainItem<St>>;
type ChainRecv<St> = oneshot::Receiver<ResolverChainItem<St>>;

#[derive(Debug)]
enum ResolverChainItem<St> {
    Reconnect { recv: ChainRecv<St>, skip: u32 },
    Stream { stream: St, skip: u32 },
}

#[derive(Debug)]
enum ResolverState<St> {
    Chain {
        recv: ChainRecv<St>,
        send: ChainSend<St>,
    },
    Stream {
        stream: St,
        send: ChainSend<St>,
    },

    // We should only be in the dead state transiently between transitions,
    // or after we returned Poll::Ready
    Dead,
}

impl<St> ResolverState<St> {
    // These are force-inlined because in context generally most of these branches
    // will be replaced

    /// Replace the state with Dead and return the previous state
    #[inline]
    fn take(&mut self) -> Self {
        mem::replace(self, ResolverState::Dead)
    }
}

/// Resolvers form a chain. When requesting an item from a resolver, it first
/// has to wait for the Stream to
#[derive(Debug)]
pub struct Resolver<St: Stream + Unpin> {
    state: ResolverState<St>,
    skip: u32,
}

impl<St: Stream + Unpin> Future for Resolver<St> {
    type Output = Option<St::Item>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // Some design notes for this function:
        // - We try to be as conservative as possible with writes; this means
        //   we don't modify the state until we *know* it's changing. We don't
        //   .take() the state, process it, then restore it. This allows us
        //   to ensure the state remains consistent even through ready! calls
        //   and possible panics.
        // - We could loop here until we receive a Pending from one of our
        //   inner polls or until we're ready, but we don't want to starve
        //   the event loop, so we do at most one state update, then auto
        //   awaken the context if we can continue to make progress.

        match this.state {
            ResolverState::Dead => panic!("Can't re-poll a completed future"),

            // The chain state means a previous receiver in the chain has the stream
            // right now, and we're waiting for it to eventually come to use. Additionally,
            // We may receive "reconnect" messages, which indicate that our previous
            // resolver is aborting and is sending ITS previous resolver to ensure the
            // chain isn't broken (along with a skip count, indicating the total number
            // of items from the stream that are associated with aborted resolvers and
            // need to be discarded).
            ResolverState::Chain { ref mut recv, .. } => {
                let mut pinned = Pin::new(recv);
                match ready!(pinned.as_mut().poll(cx)) {
                    // Our channel was closed without a send, indicating the stream returned
                    // None at some point. Clear our state to propagate the None to future
                    // Resolvers, then return it.
                    Err(..) => {
                        this.state = ResolverState::Dead;
                        return Poll::Ready(None);
                    }

                    // The previous Resolver in the chain has aborted, and is sending us *its*
                    // receiver, along with its skip count (including itself)
                    Ok(ResolverChainItem::Reconnect {
                        recv: new_recv,
                        skip,
                    }) => {
                        let recv = pinned.get_mut();
                        *recv = new_recv;
                        this.skip += skip;
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }

                    // The previous Resolver has finished, which means it's our turn to drink
                    // from the stream. It's sent us the stream, along with a skip count in
                    // the event it aborted early.
                    Ok(ResolverChainItem::Stream { stream, skip }) => match this.state.take() {
                        ResolverState::Chain { send, .. } => {
                            this.state = ResolverState::Stream { stream, send };
                            this.skip += skip;
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        }
                        _ => unreachable!(),
                    },
                }
            }

            // We are the current holder of the stream, so we're waiting for our element.
            // If we have a skip, that means previous Resolvers aborted without resolving,
            // which means that we need to take and discard that many elements before
            // claiming our own.
            ResolverState::Stream { ref mut stream, .. } => {
                match ready!(Pin::new(stream).poll_next(cx)) {
                    // Stream ended. Clear the state and return the None.
                    // Clearing the state will close the send channel, which
                    // will in activate the next Resolver in the chain and
                    // so on.
                    None => {
                        this.state = ResolverState::Dead;
                        Poll::Ready(None)
                    }

                    // We got an item, but we still have skips, which means
                    // it's an item associated with a previous aborted Resolver.
                    // Update skip and retry the loop.
                    Some(..) if this.skip > 0 => {
                        this.skip -= 1;
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }

                    // We got our item! Send the stream down the line, then
                    // clear our own state and return it
                    Some(item) => match this.state.take() {
                        ResolverState::Stream { stream, send } => {
                            // If the send channel is closed, that means that
                            // it was part of the pipeline, which was dropped.
                            // we can therefore silently let this send fail.
                            let _ = send.send(ResolverChainItem::Stream { stream, skip: 0 });
                            Poll::Ready(Some(item))
                        }
                        _ => unreachable!(),
                    },
                }
            }
        }
    }
}

impl<St: Stream + Unpin> FusedFuture for Resolver<St> {
    fn is_terminated(&self) -> bool {
        match self.state {
            ResolverState::Dead => true,
            _ => false,
        }
    }
}

impl<St: Stream + Unpin> Drop for Resolver<St> {
    fn drop(&mut self) {
        let (send, item) = match self.state.take() {
            // We're "breaking" the chain, so we have to forward our recv
            // end down to the next Resolver. Increment skip so that the next
            // Resolver knows to skip our element.
            ResolverState::Chain { recv, send } => (
                send,
                ResolverChainItem::Reconnect {
                    recv,
                    skip: self.skip + 1,
                },
            ),

            // We have a stream, but we've decided not to retrieve an element.
            // Forward the stream down the chain, and increment `skip` so that
            // the next Resolver knows to skip our element.
            ResolverState::Stream { stream, send } => (
                send,
                ResolverChainItem::Stream {
                    stream,
                    skip: self.skip + 1,
                },
            ),

            ResolverState::Dead => return,
        };

        let _ = send.send(item);
    }
}

/// A pipeline manages sending requests
#[derive(Debug)]
pub struct Pipeline<Si: Unpin, St: Unpin> {
    sink: Si,

    // This is the "open" end of our stream chain. We use it to construct
    // new resolvers
    recv: oneshot::Receiver<ResolverChainItem<St>>,
}

impl<Si: Unpin, St: Unpin> Pipeline<Si, St> {
    pub fn new<T>(requests: Si, responses: St) -> Self
    where
        Si: Sink<T>,
        St: Stream,
    {
        let (send, recv) = oneshot::channel();

        send.send(ResolverChainItem::Stream {
            stream: responses,
            skip: 0,
        })
        .map_err(|_| ())
        .expect("Unreachable");

        Self {
            sink: requests,
            recv,
        }
    }
    pub async fn submit<T>(&mut self, item: T) -> Result<Resolver<St>, Si::Error>
    where
        Si: Sink<T>,
        St: Stream,
    {
        future::poll_fn(|cx| Pin::new(&mut self.sink).poll_ready(cx)).await?;
        Pin::new(&mut self.sink).start_send(item)?;

        let (send, recv) = oneshot::channel();

        // Swap out the receive end. We now have a receive end connected to the
        // previous Resolver, and a send end connected to this.recv
        let recv = mem::replace(&mut self.recv, recv);

        // Construct a resolver. Our current stream slot will be transferred to
        // the resolver, and our new one will be connected to the resolver's
        // sender.
        let resolver = Resolver {
            state: ResolverState::Chain { recv, send },
            skip: 0,
        };

        Ok(resolver)
    }

    pub async fn submit_owned<T>(mut self, item: T) -> (Self, Result<Resolver<St>, Si::Error>)
    where
        Si: Sink<T>,
        St: Stream,
    {
        let res = self.submit(item).await;
        (self, res)
    }

    pub async fn flush<'a, T>(&mut self) -> Result<(), Si::Error>
    where
        Si: Unpin + Sink<T>,
    {
        future::poll_fn(move |cx| Pin::new(&mut self.sink).poll_flush(cx)).await
    }

    /// Finish the pipeline. Wait for all the Resolvers to complete (or abort),
    /// then return the original sink & stream. If the stream completed during
    /// the resolvers, return None instead.
    pub async fn finish(self) -> (Si, Option<St>)
    where
        St: Stream,
    {
        let mut current_recv = self.recv;
        let mut total_skip = 0;

        let mut stream = loop {
            match current_recv.await {
                // As described above, a closed recv channel means the stream
                // returned None.
                Err(_) => return (self.sink, None),
                Ok(ResolverChainItem::Reconnect { recv, skip }) => {
                    total_skip += skip;
                    current_recv = recv;
                }
                Ok(ResolverChainItem::Stream { stream, skip }) => {
                    total_skip += skip;
                    break stream;
                }
            }
        };

        for _ in 0..total_skip {
            if let None = stream.next().await {
                return (self.sink, None);
            }
        }

        (self.sink, Some(stream))
    }
}
