# plumbing

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

## Example

This example uses a tokio task to create a fake, single-key database,
and then uses plumbing to manage some simple writes and reads to it.

```rust
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
    // they both
    let (_, response) = future::join(pipeline.flush(), fut).await;
    assert_eq!(response.unwrap(), Response::Ok);

    let fut = pipeline.submit(Request::Get).await?;
    let (_, response) = future::join(pipeline.flush(), fut).await;
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

    let (_, resp1, resp2) = future::join3(pipeline.flush(), read1, read2).await;
    assert_eq!(resp1.unwrap(), Response::Value(10));
    assert_eq!(resp2.unwrap(), Response::Value(6));

    Ok(())
```

[redis]: https://redis.io/
[redis protocol]: https://redis.io/topics/protocol
[`pipeline`]: https://docs.rs/plumbing/0.9.0/plumbing/struct.Pipeline.html
[`resolver`]: https://docs.rs/plumbing/0.9.0/plumbing/struct.Resolver.html
[`sink`]: https://docs.rs/futures/0.3.5/futures/sink/trait.Sink.html
[`stream`]: https://docs.rs/futures/0.3.5/futures/stream/trait.Stream.html

License: MPL-2.0
