# Curl

## Getting information about the cluster configuration

curl "http://localhost:8000/v1/admin/nodes"

curl "http://localhost:8000/v1/admin/topics"

curl "http://localhost:8000/v1/admin/node/1"

curl "http://localhost:8000/v1/admin/topic/1"

curl "http://localhost:8000/v1/admin/topic/1/partitions"

curl "http://localhost:8000/v1/admin/topic/1/partition/1"

curl "http://localhost:8000/v1/admin/topic/1/subscriptions"

curl "http://localhost:8000/v1/admin/topic/1/subscription/1"

## Getting information about message processing

curl "http://localhost:8000/v1/admin/topic/1/partition/1/ledgers"

curl "http://localhost:8000/v1/admin/topic/1/partition/1/ledger/1"

curl "http://localhost:8000/v1/admin/topic/1/partition/1/ledger/1/messageids"

curl "http://localhost:8000/v1/admin/topic/1/partition/1/ledger/1/message/1"

curl "http://localhost:8000/v1/admin/topic/1/subscription/1/messageids"

curl "http://localhost:8000/v1/admin/topic/1/subscription/1/message/1"

## Querying the transaction log

Note that any of these URLs can be copied into a browser address bar.

curl "http://localhost:8000/v1/logs?limit=100"

curl "http://localhost:8000/v1/logs/topic/1?limit=10"

curl "http://localhost:8000/v1/logs/topic/1/partition/1"

curl "http://localhost:8000/v1/logs/topic/1/partition/1/ledger/1?detailed=true"

curl "http://localhost:8000/v1/logs/topic/1/partition/1/ledger/1/message/1?detailed=true&exact=true"

curl "http://localhost:8000/v1/logs?detailed=true" -H "Accept: text/plain"

curl "http://localhost:8000/v1/logs?detailed=true" -H "Accept: text/html"

curl "http://localhost:8000/v1/logs?detailed=true" -H "Accept: text/plain"

curl "http://localhost:8000/v1/logs?detailed=true" -H "Accept: text/html"

## Getting information about the internal state of the broker

Note that these endpoints are not versioned because they are not part of the API. They are
just for debugging issues with you application

curl "http://localhost:8000/stats"

curl "http://localhost:8000/stats/topic/1"

curl "http://localhost:8000/stats/topic/1/partition/1"

curl "http://localhost:8000/stats/topic/1/partition/1/ledger/1"

## Publishing messages

curl "http://localhost:8000/v1/pub/ping"

curl "http://localhost:8000/v1/pub/partitions/topic-1"

curl "http://localhost:8000/v1/pub/message" -X POST -H "Content-Type: application/json" --data "{""topic_id"":1, ""partition_id"":1, ""attributes"":{""order"":""abc123""}}"

## Subscribing to messages

curl "http://localhost:8000/v1/sub/consumer" -X POST -H "Content-Type: application/json" --data "{ ""topic_id"": 1, ""subscription_id"": 1 }"

curl "http://localhost:8000/v1/sub/topic/1/subscription/1/consumer/1" -X DELETE

curl "http://localhost:8000/v1/sub/ping"

curl "http://localhost:8000/v1/sub/nodes"

curl "http://localhost:8000/v1/sub/topic/1/subscription/1/consumer/1/message"
curl "http://localhost:8000/v1/sub/topic/1/subscription/1/consumer/2/message"
curl "http://localhost:8000/v1/sub/topic/1/subscription/2/consumer/1/message"
curl "http://localhost:8000/v1/sub/topic/1/subscription/2/consumer/2/message"

curl "http://localhost:8000/v1/sub/ack" -X POST -H "Content-Type: application/json" --data "{ ""subscription_id"": 1, ""consumer_id"": 1, ""message_ack_key"": ""1:1:1:1""}"

curl "http://localhost:8000/v1/sub/nack" -X POST -H "Content-Type: application/json" --data "{ ""subscription_id"": 1, ""consumer_id"": 1, ""message_ack_key"": ""1:1:1:1""}"
