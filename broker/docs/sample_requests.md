# Curl

## Getting information about the cluster configuration

curl http://localhost:8000/v1/admin/nodes -i

curl http://localhost:8000/v1/admin/topics -i

curl http://localhost:8000/v1/admin/node/1 -i

curl http://localhost:8000/v1/admin/topic/1 -i

curl http://localhost:8000/v1/admin/topic/1/partitions -i

curl http://localhost:8000/v1/admin/topic/1/partition/1 -i

curl http://localhost:8000/v1/admin/topic/1/partition/1/ledgers -i

curl http://localhost:8000/v1/admin/topic/1/partition/1/ledger/1 -i

## Publishing messages

curl http://localhost:8000/v1/pub/ping -i

curl http://localhost:8000/v1/pub/partitions/my-topic -i

curl http://localhost:8000/v1/pub/message -X POST -H "Content-Type: application/json" -i \
  --data '{"topic_id":1, "partition_id":1, "key":"lululemon:abc-123456-778645", "timestamp":1733752486522, "attributes":{"retailer":"lululemon", "order_number":"abc-123456-778645", "event-type":"order-placed", "event-id":"611ab308-72e0-42bd-bb9e-65a4447d8eb2", "span":"3c03bf82-0c9b-4a04-871d-bbcf15b15e03", "parent-span":"4765e479-aaf8-4901-b1ef-be6cdbdff384", "root-span":"1830291d-3362-4857-a140-b6fd67fa33f0" }}'

curl http://localhost:8000/v1/pub/message -X POST -H "Content-Type: application/json" -i \
  --data '{"topic_id":1, "partition_id":1, "attributes":{"message-url":"http://s3-us-east-1.amazonaws.com/messages/order-placed/4765e479-aaf8-4901-b1ef-be6cdbdff384"}}'

