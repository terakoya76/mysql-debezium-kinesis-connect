package com.terakoya76.debeziumsample.storage;

import java.util.List;
import java.io.IOException;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.errors.ConnectException;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;

import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;

import com.terakoya76.debeziumsample.model.History;

public final class DynamoDBDatabaseHistory extends AbstractDatabaseHistory {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBDatabaseHistory.class);

    private final DocumentWriter writer = DocumentWriter.defaultWriter();
    private final DocumentReader reader = DocumentReader.defaultReader();

    static AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder
            .standard()
            .withEndpointConfiguration(
                new EndpointConfiguration("http://localhost:8000", "us-west-2"))
            .build();
    static DynamoDBMapper mapper = new DynamoDBMapper(amazonDynamoDB);

    @Override
    protected void storeRecord(HistoryRecord record) {
        try {
            String document = writer.write(record.document());
            String instanceId = "hoge";
            History history = new History();
            history.setInstanceId(instanceId);
            history.setDocument(document);
            mapper.save(history);
        } catch (IOException e) {
            logger.error("Failed to convert record to string: {}", record, e);
        }
    }

    @Override
    protected synchronized void recoverRecords(Consumer<HistoryRecord> records) {
        History partitionKey = new History();

        String instanceId = "hoge";
        partitionKey.setInstanceId(instanceId);
        DynamoDBQueryExpression<History> queryExpression = new DynamoDBQueryExpression<History>()
            .withHashKeyValues(partitionKey);

        List<History> histories = mapper.query(History.class, queryExpression);

        histories.forEach(history -> {
            try {
                HistoryRecord record = new HistoryRecord(reader.read(history.getDocument()));
                records.accept(record);
            } catch (IOException e) {
                logger.error("Failed to add recover records from history ", e);
            }
        });
    }

    @Override
    public boolean exists() {
        History partitionKey = new History();

        String instanceId = "hoge";
        partitionKey.setInstanceId(instanceId);
        DynamoDBQueryExpression<History> queryExpression = new DynamoDBQueryExpression<History>()
            .withHashKeyValues(partitionKey);

        List<History> histories = mapper.query(History.class, queryExpression);
        LOGGER.info("{}", histories.size());
        return histories.size() > 0;
    }

    @Override
    public String toString() {
        return "dynamodb";
    }
}
