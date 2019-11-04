# doc: https://docs.aws.amazon.com/ja_jp/amazondynamodb/latest/developerguide/DynamoDBLocal.UsageNotes.html
FROM amazon/dynamodb-local:latest

RUN mkdir /home/dynamodblocal/data

# ENTRYPOINT が ["java"] になってる
CMD ["-jar", "DynamoDBLocal.jar", "-sharedDb", "-dbPath", "/home/dynamodblocal/data", "-optimizeDbBeforeStartup"]
