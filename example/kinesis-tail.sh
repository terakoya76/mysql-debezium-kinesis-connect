#!/bin/bash
ENDPOINT="https://localhost:4568"
SHARDID="shardId-000000000000"
STREAM_NAME="kinesis.inventory.customers"
ITER=$(aws kinesis get-shard-iterator \
    --stream-name $STREAM_NAME \
    --shard-id $SHARDID \
    --shard-iterator-type LATEST \
    --endpoint-url $ENDPOINT \
    --no-verify-ssl \
    2>/dev/null \
    | jq -r .ShardIterator)

while :
do
    LEN=$(aws kinesis get-records \
        --shard-iterator $ITER \
        --endpoint-url $ENDPOINT \
        --no-verify-ssl \
        2>/dev/null \
        | jq '.Records | length')
    if [ $LEN -gt 0 ]; then
        echo $LEN
    fi
    ITER=$(aws kinesis get-records \
        --shard-iterator $ITER \
        --endpoint-url $ENDPOINT \
        --no-verify-ssl \
        2>/dev/null \
        | jq -r .NextShardIterator)
    sleep 0.1
done
