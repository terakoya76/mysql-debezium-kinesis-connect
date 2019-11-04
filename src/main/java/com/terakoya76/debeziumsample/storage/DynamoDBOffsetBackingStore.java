package com.terakoya76.debeziumsample.storage;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.terakoya76.debeziumsample.model.Offset;

/**
 * Implementation of OffsetBackingStore that doesn't actually persist any data. To ensure this
 * behaves similarly to a real backing store, operations are executed asynchronously on a
 * background thread.
 */
public class DynamoDBOffsetBackingStore extends MemoryOffsetBackingStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBOffsetBackingStore.class);

    protected ExecutorService executor;

    static AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder
            .standard()
            .withEndpointConfiguration(
                new EndpointConfiguration("http://localhost:8000", "us-west-2"))
            .build();
    static DynamoDBMapper mapper = new DynamoDBMapper(amazonDynamoDB);

    public DynamoDBOffsetBackingStore() {
    }

    @Override
    public synchronized void start() {
        super.start();
        load();
    }

    @Override
    protected void save() {
        for (Map.Entry<ByteBuffer, ByteBuffer> mapEntry : data.entrySet()) {
            ByteBuffer key = (mapEntry.getKey() != null) ? mapEntry.getKey() : null;
            ByteBuffer value = (mapEntry.getValue() != null) ? mapEntry.getValue() : null;

            String instanceId = "hoge";
            Offset offset = new Offset();
            offset.setInstanceId(instanceId);
            offset.setKey(bb2str(key, StandardCharsets.UTF_8));
            offset.setValue(bb2str(value, StandardCharsets.UTF_8));
            mapper.save(offset);
        }
    }

    private void load() {
        data = new HashMap<>();

        String instanceId = "hoge";
        Offset offset = mapper.load(Offset.class, instanceId);

        if (offset != null) {
            ByteBuffer key = str2bb(offset.getKey(), StandardCharsets.UTF_8);
            ByteBuffer value = str2bb(offset.getValue(), StandardCharsets.UTF_8);
            data.put(key, value);
        }
    }

    private ByteBuffer str2bb(String msg, Charset charset){
        return ByteBuffer.wrap(msg.getBytes(charset));
    }

    private String bb2str(ByteBuffer buffer, Charset charset){
        byte[] bytes;
        if(buffer.hasArray()) {
            bytes = buffer.array();
        } else {
            bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
        }
        return new String(bytes, charset);
    }
}
