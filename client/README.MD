# Client Library

This shared library allows applications written in Rust to take advantage of the
binary serialization API in the broker. The binary serialization API supports
functioallity that requires high throughput, i.e. publishing, subscribing and
acknowledging messages. There is also an http API that can be used for low
throughput activities like adding and removing topics.

This API has the following advantages:

* Send and receive channels are fully decoupled, so that requests are streamed in
one direction and responses are streamed in the other direction without any
coordination. This is especially beneficial over a network connection with higher
latency, where we can send many requests before receiving any replies.

* Provides both blocking and non-blocking (async) clients without having a
dependency on an async executor (tokio). When you initiate an async request, it
will be streaned to the broker over the network connection. When the matching
response is received from the broker, the future will complete.

* Provides a streaming topic subscriber that has messages pushed to it from
the broker, and makes these available to the application as a mpsc channel.

* Provides a streaming topic producer that receives messages from a mpsc
channel and streams them to the broker.

* Performs binary serialization of messages over the wire. This produces much
smaller message sizes, which minimizes network bandwidth, as well as memory 
and cpu associated with message transmission over the network. It also minimizes
the cpu required to deserialize the messages compared with parsing a text based
representation like Json.

If you are only expecting to process a few thousand messages per second, then
the http interface may be simpler for youe use case, but if you are expecting
to process hundreds of thousands of messages per second, then I strongly 
encourage you to consider writting your application in Rust and using this client
library.

## Blocking Client

This client is very simple to use. It makes one request at a time to the broker,
and blocks the current thrad until a response is received.

Example:

```rust
use std::{
    collections::HashMap, 
    sync::Arc,
};
use pulsar_rust_client::{
    blocking::Client,
    BufferPool,
    TopicId,
};

fn main() {
    let buffer_pool = Arc::new(BufferPool::new());
    let mut client = Client::new(&buffer_pool, "localhost:8001");
    client.connect().unwrap();

    let topic_id: TopicId = 1;
    let mut attributes = HashMap::new();
    attributes.insert(String::from("order_number"), String::from("ABC123"));

    let publish_result = client.publish(topic_id, None, None, attributes).unwrap();

    println!("Published message id {}", publish_result.message_ref.message_id)
}
```

## Non-blocking Client

This client allows you to submit many requests to the broker, and then wait for all the
replies to come back. This allows much greater throughput of requests, especially
when there is high network latency.

To use this API you will need an async executor like tokio. Note that this crate does
not depend on tokio or any other async executor.

Example:

```rust
use std::{
    collections::HashMap, 
    sync::Arc
};
use pulsar_rust_client::{
    TopicId,
    BufferPool,
    non_blocking::Client,
};

#[tokio::main]
async fn main() {
    let buffer_pool = Arc::new(BufferPool::new());
    let mut client = Client::new(&buffer_pool, "localhost:8001");
    client.connect().unwrap();

    let topic_id: TopicId = 1;
    let mut attributes = HashMap::new();
    attributes.insert(String::from("order_number"), String::from("ABC123"));

    let future = client.publish(topic_id, None, None, attributes).unwrap();
    let handle = tokio::spawn(future);
    
    let publish_result = handle.await.unwrap().unwrap();
    println!("Published message id {}", publish_result.message_ref.message_id)
}
```

## Streaming producer

Provides a mpsc channel sender for publishing messages. Any messages posted into the
channel will be streamed over an open network connection to the broker without waiting for
replies. This allows you to publish hundreds of thousand of messages per second.

This client has not been implemnted yet.

## Streaming subscriber

Provides an mpsc channel receiver for subscribing to a topic. The broker will stream
messages over an open network connection to the client. The client will stream flow
control and ack messages back to the broker.

This client has not been implemnted yet.
