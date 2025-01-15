# Curl

## Getting information about the cluster configuration

curl http://localhost:8000/v1/admin/nodes -i

curl http://localhost:8000/v1/admin/topics -i

curl http://localhost:8000/v1/admin/node/1 -i

curl http://localhost:8000/v1/admin/topic/1 -i

curl http://localhost:8000/v1/admin/topic/1/partitions -i

curl http://localhost:8000/v1/admin/topic/1/partition/1 -i

curl http://localhost:8000/v1/admin/topic/1/subscriptions -i

curl http://localhost:8000/v1/admin/topic/1/subscription/1 -i

## Getting information about message processing

curl http://localhost:8000/v1/admin/topic/1/partition/1/ledgers -i

curl http://localhost:8000/v1/admin/topic/1/partition/1/ledger/1 -i

curl http://localhost:8000/v1/admin/topic/1/partition/1/ledger/1/messageids -i

curl http://localhost:8000/v1/admin/topic/1/partition/1/ledger/1/message/1 -i

curl http://localhost:8000/v1/admin/topic/1/subscription/1/messageids -i

curl http://localhost:8000/v1/admin/topic/1/subscription/1/message/1 -i

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

curl http://localhost:8000/stats

curl http://localhost:8000/stats/topic/1

curl http://localhost:8000/stats/topic/1/partition/1

curl http://localhost:8000/stats/topic/1/partition/1/ledger/1

## Publishing messages

curl http://localhost:8000/v1/pub/ping -i

curl http://localhost:8000/v1/pub/partitions/topic-1 -i

curl http://localhost:8000/v1/pub/message -X POST -H "Content-Type: application/json" -i \
  --data '{"topic_id":1, "partition_id":1, "key":"lululemon:abc-123456-778645", "timestamp":1733752486522, "attributes":{"retailer":"lululemon", "order_number":"abc-123456-778645", "event-type":"order-placed", "event-id":"611ab308-72e0-42bd-bb9e-65a4447d8eb2", "span":"3c03bf82-0c9b-4a04-871d-bbcf15b15e03", "parent-span":"4765e479-aaf8-4901-b1ef-be6cdbdff384", "root-span":"1830291d-3362-4857-a140-b6fd67fa33f0" }}'

curl http://localhost:8000/v1/pub/message -X POST -H "Content-Type: application/json" \
  --data '{"topic_id":1, "partition_id":1, "key":"abc123", "attributes":{"order":"abc123"}}'

## Subscribing to messages

curl http://localhost:8000/v1/sub/consumer -X POST -H "Content-Type: application/json" --data '{ "topic_id":1, "subscription_id":1, "max_messages": 3 }'

curl http://localhost:8000/v1/sub/topic/1/subscription/1/consumer/1 -X DELETE

curl http://localhost:8000/v1/sub/ping

curl http://localhost:8000/v1/sub/nodes

curl http://localhost:8000/v1/sub/topic/1/subscription/1/consumer/1/message
curl http://localhost:8000/v1/sub/topic/1/subscription/1/consumer/2/message
curl http://localhost:8000/v1/sub/topic/1/subscription/2/consumer/1/message
curl http://localhost:8000/v1/sub/topic/1/subscription/2/consumer/2/message

curl http://localhost:8000/v1/sub/ack -X POST -H "Content-Type: application/json" --data '{"subscription_id":1, "consumer_id":1, "message_ack_key":"1:1:1:1"}'

curl http://localhost:8000/v1/sub/nack -X POST -H "Content-Type: application/json" --data '{"subscription_id":1, "consumer_id":1, "message_ack_key":"1:1:1:1"}'
