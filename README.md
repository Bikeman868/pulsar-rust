# Pulsar Rust

This is a rewrite of the Pulsar application that is maintained by the Apache Foundation. This version of Pulsar is
written in Rust for maximum performance and efficiency.

## About

Apache Pulsar is a suite of applications that provide resilient message pub-sub. If your application publishes a
message to Pulsar and you get a positive reponse, then Pulsar guarantees to durably store the message until it has
been successfully processed by each of the subscribers at least once.

This implementation made some different design decisions than Apache Pulsar, because this version is focussed
on maximizing performance and efficiency - hence Rust. The main differences are:

- In Apache Pulsar you pass the message body to Pulsar. In this implementation you choose how you want to store
message bodies within your application. This broker has a singluar focus on ensuring that each message is processed
by each subscriber at least once as quickly as possible. Having a singluar focus means that we don't have to make
compromises because of conflicting priorities. You can pass a collection of name-value pairs with each message
to allow the message consumer to retrieve the message body.

- Apache Pulsar has bookies that store messages. Since this implementation does not store messages, it doesn't need
bookies. This implementation stores message metadata in memory and writes a transaction log. If the broker is restarted
due to a re-configuration or version upgrade, then it will restore its internal state from the transaction log.

- This implementation deletes information from its internal state and from the transaction log when messages are
successfully processed by all subscribers. This means that the storage backlog concept in Apache Pulsar is not 
applicable. If an application fails to acknowledge one message, then that one message will remain in the
transaction log, this won't cause other messages to be retained.

- This implementation allows multiple messages with the same key to be buffered in the client application for
key-shared subscriptions. This is not permitted by Apache Pulsar.

## Terminology

### Node

A computer that is running the broker application and has a static IP address. For local development, the
IP address can be 127.0.0.1, and this is the default in a debug build.

### Cluster

A collection of nodes that load share for a set of topics. Nodes in the cluster must share a persistence
mechnism (usually a database) and have a static IP address that is part of the configuration stored in the 
database.

### Topic

You can think of a topic as an elastic pipe, where messages that are pushed in at one end are durably stored,
and will eventually come out of the other end in the same order. System designers frequently specify one topic per
message contract, so that all of the messages in the pipe have the same structure and meaning, but this is not
a requirement. You can also have one topic per communication channel between applications with multiple types
of message being sent through the same pipe.

### Partition

Partitions provide for load-balancing accross nodes in the cluster. Every message must have key. The key can
be globally unique, or it can be related to the message data. The key is consistently hashed to identify the
partition within the topic that will process the message.

Each partition is assigned to a specific node at a point in time, but partitions can occasionally move between
nodes to balance load (see ledgers below).

All messages with the same key will hash to the same partition, and therefore be processed on the same node.
This is essential for key-shared subscriptions to work as expected, but can lead to an imbalance between nodes.
The ways to avoid imbalance are to ensure that keys are well distributed, and that you have enough partitions.

For high throughput topics, you should aim to have at least 10 times as many partitions per topic as nodes
in the cluster. Having hundreds, or a few thousand partitions is perfectly fine. If you have a large number of
low throughput topics, then you can have as few as one partition per topic.

If you have a key-shared subscription on the topic, the message key is also used for consumer affinity (see below).

### Publisher

Applications that produce messages are referred to as publishers. A publisher can publish to as many topics as
they like. Publishers must hash the message key to figure out the partition, and send messages to the node 
that currently owns that partition. This process is handled transparently for you if you use the client library.

You can use any hashing algorithm you like to choose the partition, but the has must be consistent for
key-shared subscriptions to work, and you must make allowance for the fact that partitions can be added and
removed at any time.

### Subscription

Subscriptoins can be created for a topic. Pulsar is designed to deliver each message published to the topic to 
every topic subscription at least once.

There are various types of subscription that provide different different behaviors and guarantees. You can
mix subscription types on a topic.

The highest performing subscription type is "shared", in which messages are delivered to any consumer that
has bandwidth to process it, without any guarantees about message ordering or consumer affinity. With this
type of subscription each message will be processed by only one consumer, but messages with the same key could
be delivered to different consumers. This is great for processing events where the each event should only be
processed once by the application, and event processing is not sensitive to ordering.

The "multicast" subscription type is similar to shared, but each message is delivered to every consumer. This
is great for distributing in-memory updates to all running instances of an application, for example 
in-memory cache invalidation.

The lowest performing subscription type is "key-shared", in which messages with the same key are delivered
to the same consumer, and are always delivered in the order that they were published. This subscription type
allows you to simplify your application code, but is not suitable for topics with very high throughput.

### Consumer

An application that consumes and processes messages from a subscription. Each message must be acknowledged
by the consumer. When all of the subscribers on the topic have acknowledged a specific message, all evidence
of this message is deleted from the broker to conseve resources.

If an application is unhealthy or shutting down, then it can also negatively acknoledge a message. This
will push the message back to the the queue for re-delivery. Negative acknowledgemant also happens 
automatically if the consumer does not acknowledge the message within the message processing timeout. This
ensures that every message will eventually be processed if processing applications terminate unexpectedly.

Messages delivered to consumers include a count of how many times the message has been delivered so that
applications can take appropriate action with messages that are being continually retried.

Consumers must request a list of nodes in the cluster, and establish a newwork connection to each node to
receive all of the published messages. This happens automatically if you usethe client library, which also
handles the situation where nodes are added or removed from the cluster. If you have enough instances of
your application running, then you can also choose to have instances of your application connecting to a
subset of the nodes.

### Ledger

Partitions are divided into ledgers to facilitate transfer of partitions between nodes. When a partition is
moved between nodes:

1. The node taking ownership of the partition will create a new ledger and start accepting messages into it,
but will not deliver these messages to consumers yet.

2. The original partition owner will stop accepting new messages from publishers, redirecting them to the new
partition owner.

3. Once all messages in the old ledger have been acknowldged for all subscriptions, and the ledger is empty,
this is communicated to the node that is taking ownership of the partition.

4. The new partition owner starts delivering messages to consmers.

Because this ownership transfer causes a disruption to the flow of messages, it happens infrequently, and only
when a node is overloaded. On a regluar cycle, the nodes in a cluster exchange information about the resource
usage on the node, the message throughput on each partition. This can result in the most heavily trafficked
partition on the most heavily loaded node being transitioned to the least loaded node. This works most 
effectively when there are enough partitions.

When nodes are added or removed from the cluster, this also causes partitions to be transferred between nodes.

## Project Structure

This folder has the following sub-folders. Note sub-folders also contain README files with more detail:

### broker

This folder contains a library module and an executable module. The executable only contains initialization code that
constructs types in the library module, starts them running and waits the a signal to terminate the application.

The broker application is at the core of this service. It accepts requests to publish messages, and ensures that
each message is successfully processed at least once by each subscription.

The broker service provides a REST over HTTP API for ease of use and compatibility.

It also provides a very high performance API that uses Rust specific binary serialization over raw sockets. To
take advantage of this high performance API you should write your application in Rust.

Even the lower performance REST API is several times faster than Apache Pulsar.

### client

This folder contains a Rust crate that lets applications take advantage of binary serialization over raw socket
connections. You can only use this if you import the crate into a Rust application.

### net

This folder contins a Rust crate that is shared between the client and the broker. It defines the wire protocols
and data transfer objects that the client uses to communicate with the broker.

## Performance

I have not undertaken a comprehensive benchmarking exercise, or done any apples for apples comparisons with
Apache Pulsar, but I have been very mindful of performance, and I have keeping an eye on the throughput 
capacity troughout the development, and this is what I observed:

|   | Microsoft Surface 3 | M1 MacBook Pro |
| --- | --- | --- |
| CPU | Intel Core i5 | Apple M1 Pro |
| OS | Windows 10 | macOS Sequoia |
| CPU cores | 4 | 10 |
| CPU clock |2.4GHz | 3.2GHz |
| Memory | 8GB | 16GB |
| JSON + Http API | 2,500 | 30,000 |
| Bin + TCP API | 6,000 | TBM |

Throughput rates in the above table are messages per second that were published by the client, processed by the
broker, delivered to the consumer, and acknowledged back to the broker.

These tests were run with the producer, consumer and broker all running on the same computer. This is more load
on the CPU and memory that you would expect in a real-world deployment where these applications would typically
run on different machines, but removes most of the networking overhead by using localhost.

An interesting side note about network impact on throughout. When usig the JSON over Http 1.1 API, you can only have
one in-flight request per connection. If for example your network has 100ms latency, then you can only make 10 
requests per second per connection, because Http 1.1 waits for the response to a request to complete before a new
request can be started.

When designing the binary serialization API for this application, I made the request and response channels fully
decoupled, so that you can send thousands of requests over a connection before receiving the first response. This
massively increases the maximum throughput per connection on high latency connections, but it's not unlimited
because TCP/IP also has limitations on how many IP packets can be pending ack.
