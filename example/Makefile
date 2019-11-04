mysql.setup:
	populator

dynamo.setup:
	aws dynamodb create-table \
		--table-name offsets \
		--key-schema \
			AttributeName=instance_id,KeyType=HASH \
		--attribute-definitions \
			AttributeName=instance_id,AttributeType=S \
		--billing-mode PAY_PER_REQUEST \
		--region ap-northeast-1 \
		--endpoint-url http://localhost:8000 \
		--no-verify-ssl
	aws dynamodb create-table \
		--table-name histories \
		--key-schema \
			AttributeName=instance_id,KeyType=HASH \
		--attribute-definitions \
			AttributeName=instance_id,AttributeType=S \
		--billing-mode PAY_PER_REQUEST \
		--region ap-northeast-1 \
		--endpoint-url http://localhost:8000 \
		--no-verify-ssl

kinesis.setup:
	aws kinesis create-stream \
		--stream-name kinesis.inventory.customers \
		--shard-count 1 \
		--endpoint-url https://localhost:4568 \
		--no-verify-ssl \
        2>/dev/null

dynamo.desc:
	aws dynamodb query \
		--table-name offsets \
		--key-condition-expression 'instance_id = :1' \
		--expression-attribute-values '{":1": {"S": "hoge"}}' \
		--endpoint-url http://localhost:8000 \
		--no-verify-ssl
	aws dynamodb query \
		--table-name histories \
		--key-condition-expression 'instance_id = :1' \
		--expression-attribute-values '{":1": {"S": "hoge"}}' \
		--endpoint-url http://localhost:8000 \
		--no-verify-ssl

kinesis.desc:
	aws kinesis list-streams --endpoint-url https://localhost:4568 --no-verify-ssl 2>/dev/null
	aws kinesis describe-stream --stream-name kinesis.inventory.customers --endpoint-url https://localhost:4568 --no-verify-ssl 2>/dev/null

kinesis.tail:
	./kinesis-tail.sh
