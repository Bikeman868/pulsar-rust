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

curl "http://localhost:8000/v1/logs/topic/1/partition/1/ledger/1"

curl "http://localhost:8000/v1/logs/topic/1/partition/1/ledger/1/message/1?detailed=true"

curl "http://localhost:8000/v1/logs?detailed=true" -H "Accept: text/plain"

curl "http://localhost:8000/v1/logs?detailed=true" -H "Accept: text/html"

## Publishing messages

curl http://localhost:8000/v1/pub/ping -i

curl http://localhost:8000/v1/pub/partitions/my-topic -i

curl http://localhost:8000/v1/pub/message -X POST -H "Content-Type: application/json" -i \
  --data '{"topic_id":1, "partition_id":1, "key":"lululemon:abc-123456-778645", "timestamp":1733752486522, "attributes":{"retailer":"lululemon", "order_number":"abc-123456-778645", "event-type":"order-placed", "event-id":"611ab308-72e0-42bd-bb9e-65a4447d8eb2", "span":"3c03bf82-0c9b-4a04-871d-bbcf15b15e03", "parent-span":"4765e479-aaf8-4901-b1ef-be6cdbdff384", "root-span":"1830291d-3362-4857-a140-b6fd67fa33f0" }}'

curl http://localhost:8000/v1/pub/message -X POST -H "Content-Type: application/json" -i \
  --data '{"topic_id":1, "partition_id":1, "attributes":{"message-url":"http://s3-us-east-1.amazonaws.com/messages/order-placed/4765e479-aaf8-4901-b1ef-be6cdbdff384"}}'

## Subscribing to messages

curl http://localhost:8000/v1/sub/ping -i

curl http://localhost:8000/v1/sub/nodes -i

curl http://localhost:8000/v1/sub/topic/1/messages?consumer_id=0553b3e9-8c2c-4a5f-a9a5-4d6eede1e47f&limit=10 -i

curl http://localhost:8000/v1/sub/ack -X POST -H "Content-Type: application/json" -i --data '{"topic_id":1, "partition_id":1, "ledger_id":1, "subscription_id":1, "consumer_id":"0553b3e9-8c2c-4a5f-a9a5-4d6eede1e47f", "message_ids":[1,2,3] }'

curl http://localhost:8000/v1/sub/nack -X POST -H "Content-Type: application/json" -i --data '{"topic_id":1, "partition_id":1, "ledger_id":1, "subscription_id":1, "consumer_id":"0553b3e9-8c2c-4a5f-a9a5-4d6eede1e47f", "message_ids":[4,5] }'
