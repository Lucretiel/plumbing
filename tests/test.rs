use futures::{
    channel::mpsc,
    future::{self, FutureExt},
    StreamExt,
};
use request_stream::Pipeline;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::{self, task};

/// Basic functionality; test that items may be submitted to a pipeline, and
/// responses are received correctly.
#[tokio::test]
async fn basic_test() {
    let (send, recv) = mpsc::unbounded();
    let mut pipeline = Pipeline::new(send, recv);

    let slot1 = pipeline.submit(1i32).await.unwrap();
    let slot2 = pipeline.submit(2).await.unwrap();
    let slot3 = pipeline.submit(3).await.unwrap();

    assert_eq!(slot1.await.unwrap(), 1);
    assert_eq!(slot2.await.unwrap(), 2);
    assert_eq!(slot3.await.unwrap(), 3);
}

#[tokio::test]
async fn basic_test_owned() {
    let (send, recv) = mpsc::unbounded();
    let pipeline = Pipeline::new(send, recv);

    pipeline
        // Submit a 1
        .submit_owned(1i32)
        // Get slot1, submit 2
        .then(|(pipeline, slot1)| {
            pipeline
                .submit_owned(2)
                .map(move |(pipe, slot2)| (pipe, slot1, slot2))
        })
        // Get slot2, submit 3
        .then(|(pipeline, slot1, slot2)| {
            pipeline
                .submit_owned(3)
                .map(move |(_pipe, slot3)| (slot1, slot2, slot3))
        })
        // Await all 3 slots
        .then(|(slot1, slot2, slot3)| future::join3(slot1.unwrap(), slot2.unwrap(), slot3.unwrap()))
        // Check their values
        .map(|(value1, value2, value3)| {
            assert_eq!(value1.unwrap(), 1);
            assert_eq!(value2.unwrap(), 2);
            assert_eq!(value3.unwrap(), 3);
        })
        .await;
}

/// Test that submissions may be dropped, and the responses associated with them
/// will be discarded.
#[tokio::test]
async fn drop_test() {
    let (send, recv) = mpsc::unbounded();
    let mut pipeline = Pipeline::new(send, recv);

    let _ = pipeline.submit(1).await.unwrap();
    let slot2 = pipeline.submit(2).await.unwrap();
    let _ = pipeline.submit(3).await.unwrap();
    let slot4 = pipeline.submit(4).await.unwrap();
    let slot5 = pipeline.submit(5).await.unwrap();

    assert_eq!(slot2.await.unwrap(), 2);
    drop(slot4);
    assert_eq!(slot5.await.unwrap(), 5);
}

/// Test that submissions must be resolved in order; that is, subsequent
/// submissions will block indefinitely until previous ones complete
#[tokio::test]
async fn ordering_test() {
    let (send, recv) = mpsc::unbounded();
    let mut pipeline = Pipeline::new(send, recv);

    let slot1 = pipeline.submit(1i32).await.unwrap();
    let slot2 = pipeline.submit(2).await.unwrap();
    let slot3 = pipeline.submit(3).await.unwrap();

    let finish2 = Arc::new(AtomicBool::new(false));
    let finish2_alt = finish2.clone();
    let finish3 = Arc::new(AtomicBool::new(false));
    let finish3_alt = finish3.clone();

    // Spawn background tasks that will block on slots 2 and 3. They won't
    // be able to make progress until slot1 is resolved.

    let task3 = task::spawn(async move {
        assert_eq!(slot3.await.unwrap(), 3);
        finish3_alt.store(true, Ordering::SeqCst)
    });

    let task2 = task::spawn(async move {
        assert_eq!(slot2.await.unwrap(), 2);
        finish2_alt.store(true, Ordering::SeqCst)
    });

    // Give the tasks a chance to run
    task::yield_now().await;
    task::yield_now().await;

    assert_eq!(finish2.load(Ordering::SeqCst), false);
    assert_eq!(finish3.load(Ordering::SeqCst), false);

    assert_eq!(slot1.await.unwrap(), 1);

    task2.await.unwrap();
    task3.await.unwrap();

    assert_eq!(finish2.load(Ordering::SeqCst), true);
    assert_eq!(finish3.load(Ordering::SeqCst), true);
}

/// Test that, if the stream closes early, all the remaining submissions return
/// None
#[tokio::test]
async fn test_none_propagation() {
    let (send, recv) = mpsc::unbounded();
    let recv = recv.take(2);
    let mut pipeline = Pipeline::new(send, recv);

    let slot1 = pipeline.submit(1i32).await.unwrap();
    let slot2 = pipeline.submit(2).await.unwrap();
    let slot3 = pipeline.submit(3).await.unwrap();
    let slot4 = pipeline.submit(4).await.unwrap();

    assert_eq!(slot1.await, Some(1));
    assert_eq!(slot2.await, Some(2));
    assert_eq!(slot3.await, None);
    assert_eq!(slot4.await, None);

    let (_, recv) = pipeline.finish().await;
    assert!(recv.is_none());
}

/// Test that, when a pipeline is finish()ed, any unconsumed items in the stream
/// are discarded, and the return sink & stream are still connected & in sync.
#[tokio::test]
async fn test_drain() {
    let (send, recv) = mpsc::unbounded();
    let mut pipeline = Pipeline::new(send, recv);

    let _ = pipeline.submit(1).await.unwrap();
    let _ = pipeline.submit(2).await.unwrap();
    let _ = pipeline.submit(3).await.unwrap();
    let _ = pipeline.submit(4).await.unwrap();

    let (send, recv) = pipeline.finish().await;
    let mut recv = recv.unwrap();

    send.unbounded_send(5).unwrap();
    let value = recv.next().await.unwrap();
    assert_eq!(value, 5);
}

#[cfg(test)]
mod fake_db_test {
    /// This module defines a trivial simulated database. This database is
    /// a (sender/receiver) pair connected to a tokio task. The tokio task
    /// receives requests, which may either be writes that update the int
    /// value, or a read that gets it. It responds with Ok to writes.
    ///
    /// These channels are tightly bounded, which allows us to test that
    /// Pipeline can submit into a buffer, and that we can defer a flush and
    /// read a response.
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

        /// Create a new database. The request & response channels are bounded,
        /// so be sure to buffer requests and consume responses
        pub fn create_db() -> (mpsc::Sender<Request>, mpsc::Receiver<Response>) {
            let (send_req, mut recv_req) = mpsc::channel(0);
            let (mut send_resp, recv_resp) = mpsc::channel(0);

            let _task = task::spawn(async move {
                let mut database = FakeDb { counter: 0 };

                while let Some(request) = recv_req.next().await {
                    eprintln!("Received request: {:?}", request);
                    match request {
                        Request::Incr(count) => {
                            database.counter += count;
                            send_resp.send(Response::Ok).await.unwrap();
                            eprintln!("New value: {}; Responded Ok", database.counter);
                        }
                        Request::Decr(count) => {
                            database.counter -= count;
                            send_resp.send(Response::Ok).await.unwrap();
                            eprintln!("New value: {}; Responded Ok", database.counter);
                        }
                        Request::Set(value) => {
                            database.counter = value;
                            send_resp.send(Response::Ok).await.unwrap();
                            eprintln!("New value: {}; Responded Ok", database.counter);
                        }
                        Request::Get => {
                            let response = Response::Value(database.counter);
                            send_resp.send(response).await.unwrap();
                            eprintln!("Responded with {:?}", response);
                        }
                    }
                }
            });

            (send_req, recv_resp)
        }
    }

    use fake_db::{Request, Response};
    use futures::{
        future::{self},
        sink::SinkExt,
    };
    use request_stream::Pipeline;

    /// Test a typical interaction with the fake_db
    #[tokio::test]
    async fn test_fake_db() {
        let (send, recv) = fake_db::create_db();
        // We want to buffer requests because the database can only handle 1 at a
        // time
        let send = send.buffer(20);
        let mut pipeline = Pipeline::new(send, recv);

        eprintln!("Submitting write 1/4...");
        let _ = pipeline.submit(Request::Set(10)).await.unwrap();

        eprintln!("Submitting write 2/4...");
        let _ = pipeline.submit(Request::Incr(12)).await.unwrap();

        eprintln!("Submitting write 3/4...");
        let _ = pipeline.submit(Request::Decr(8)).await.unwrap();

        eprintln!("Submitting write 4/4...");
        let _ = pipeline.submit(Request::Incr(100)).await.unwrap();

        // TODO: Create a way to peek at the database here, to confirm that we're
        // blocked until a value is received

        eprintln!("Submitting query...");
        let query = pipeline.submit(Request::Get).await.unwrap();

        // Because the items are in a buffer, we need to flush them before
        // the query will be ready. However, we also need to make sure the
        // query is pulling data from the database, so we have to drive both
        // futures at once
        let (query_result, flush_result) = future::join(query, pipeline.flush()).await;
        flush_result.unwrap();

        assert_eq!(query_result.unwrap(), Response::Value(114));
    }
}
