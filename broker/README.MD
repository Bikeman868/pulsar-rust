# Broker

This executable listens for requests from clients, and provides the following functionallity:
- Allocating unique IDs to each message within each topic partition
- Keeping track of which subscriptions have consumed which messages
- Delivering each message at least once to each subscriber
- Re-delivering messages that are negatively acknowledged
- Re-delivering messages that are not acknowledged in a timely manner
- Durably storing state with ACID guarantees so that messages can not be lost once published
- Balancing partitions across broker instances to eaven out the load
- Moving partitions to new broker instances if a broker instance fails

Additionally for key-shared subscriptions:
- Ensuring messages with the same key are delivered to the same consumer in the order they were published
- Balancing keys across consumers to minimize hot consumers

The broker does not:
- Store message data. It only deals with message IDs and keys, storage and retrievel of message data is a separate concern

Assumes that the application developer:
- Configures topics, where each topic contains an ordered stream of messages and each message has a non-unique key value
- Configures a fixed number of partitions per topic, where message keys are consistenty hashed to a partition
- Runs multiple instances of the broker application, where each broker instance will handle several partitions
- Configures a subscription for each application that consumes the messages in a topic
- Dynamically constructs multiple consumers for each subscription (typically 1 per application instance)
- Dynamically constructs multiple producers for each topic (typically 1 per application instance)

Message publication consists of:
- The application calls the client library with the topic name, message key, custom attributes, and message data
- The client library hashes the key to a partition, maps the partition to the broker instance for that partition
- The client library generates a unique identifer for the message
- The client uploads the data to a storage mechanism using the generated identifier
- The client calls the broker and asks it to publish the message, including the generated identifier as message attribute
- The broker adds the message to the undelivered message queue of each subscriber

Message consumption consists of:
- The application constructs a consumer from the client library with the topic name and subscription name
- The client library establishes a persistent connection to each broker in the cluster registering a consumer for the topic and subscription
- The broker pushes new message ids to the consumers until the consumer asks it to pause
- The client library presents messsage metadata to the application 
- Message metadata consists of the topic name, partitionid, messageid, custom attributes, retry count, and age of message
- For messages that the application wants to process, it calls the client library to get the message data
- The client library uses generatd identifier in the message attributes to retrieve the message data from a storage medium
- The client returns the message metadata and message data to the application for processing
- The application calls methods in the client library to ack or negatively ack the message
- The client library calls the broker to pass on the ack/nack response from the application
- Acked messages are removed from the queue for the subscription
- Nacked and unacked messages are redelivered with an incremented retry count
