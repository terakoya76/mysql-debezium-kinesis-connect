package com.terakoya76.debeziumsample;

import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.SpringApplication;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.util.Clock;

import com.terakoya76.debeziumsample.storage.DynamoDBDatabaseHistory;

@SpringBootApplication
public class DebeziumSampleApplication implements ApplicationRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumSampleApplication.class);

    private static final String APP_NAME = "kinesis";
    private static final String KINESIS_REGION_CONF_NAME = "kinesis.region";

    private EmbeddedEngine engine;

    private final Configuration config;

    private final JsonConverter valueConverter;
    /*
     * for KPL
    private final KinesisProducer producer;
    */
    private final AmazonKinesis kinesisClient;
    private final ExecutorService callbackThreadPool = Executors.newCachedThreadPool();

    // KinesisProducer.addUserRecord is asynchronous. A callback can be used to receive the results.
    private final FutureCallback<UserRecordResult> callback = new FutureCallback<UserRecordResult>() {
        @Override
        public void onFailure(Throwable t) {
            // If we see any failures, we will LOGGER them.
            if (t instanceof UserRecordFailedException) {
                Attempt last = Iterables.getLast(
                        ((UserRecordFailedException) t).getResult().getAttempts());
                LOGGER.error(String.format(
                        "Record failed to put - %s : %s",
                        last.getErrorCode(), last.getErrorMessage()));
            }
            LOGGER.error("Exception during put", t);
        }

        @Override
        public void onSuccess(UserRecordResult result) {
            LOGGER.info("Successed: {}", result);
        }
    };

	public static void main(String[] args) {
		SpringApplication.run(DebeziumSampleApplication.class, args);
	}

    public DebeziumSampleApplication() {
         config = Configuration.create()
                .with(EmbeddedEngine.CONNECTOR_CLASS, "io.debezium.connector.mysql.MySqlConnector")
                .with(EmbeddedEngine.ENGINE_NAME, "kinesis")
                .with(MySqlConnectorConfig.SERVER_NAME, "kinesis")
                .with(MySqlConnectorConfig.SERVER_ID, 8192)
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "debezium")
                .with(MySqlConnectorConfig.PASSWORD, "dbz")
                .with(MySqlConnectorConfig.DATABASE_WHITELIST, "inventory")
                .with(MySqlConnectorConfig.TABLE_WHITELIST, "inventory.customers")
                .with(EmbeddedEngine.OFFSET_STORAGE,
                    "com.terakoya76.debeziumsample.storage.DynamoDBOffsetBackingStore")
                .with(MySqlConnectorConfig.DATABASE_HISTORY,
                    DynamoDBDatabaseHistory.class.getName())
                .with("database.history.dynamo.endpoint", "http://localhost:8000")
                .with("database.history.dynamo.region", "ap-northeast-1")
                .with("database.history.dynamo.instance_id", "hoge")
                .with("schemas.enable", false)
                .build();

        valueConverter = new JsonConverter();
        valueConverter.configure(config.asMap(), false);

        /*
         * for KPL
        producer = KinesisProducer();
        */

        kinesisClient = AmazonKinesis();
    }

    public KinesisProducer KinesisProducer() {
        KinesisProducerConfiguration config = new KinesisProducerConfiguration();

        // You can also load config from file. A sample properties file is
        // included in the project folder.
        // KinesisProducerConfiguration config =
        //     KinesisProducerConfiguration.fromPropertiesFile("default_config.properties");

        config.setRegion("ap-northeast-1");
        config.setCredentialsProvider(new ProfileCredentialsProvider("default"));
        config.setMaxConnections(1);
        config.setRequestTimeout(60000);
        config.setRecordMaxBufferedTime(2000);
        config.setKinesisEndpoint("localhost");
        config.setKinesisPort(4568);
        config.setVerifyCertificate(false);

        KinesisProducer producer = new KinesisProducer(config);
        return producer;
    }

    public AmazonKinesis AmazonKinesis() {
        AmazonKinesis amazonKinesis = AmazonKinesisClientBuilder
                .standard()
                .withCredentials(new ProfileCredentialsProvider("default"))
                .withEndpointConfiguration(
                    new EndpointConfiguration("http://localhost:4567", "ap-northeast-1"))
                .build();
        return amazonKinesis;
    }

    @Override
    public void run(ApplicationArguments arg0) throws Exception {
        engine = EmbeddedEngine.create()
                .using(config)
                .using(this.getClass().getClassLoader())
                .using(Clock.SYSTEM)
                .notifying(this::sendRecord)
                .build();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Requesting embedded engine to shut down");
            engine.stop();
        }));

        // the submitted task keeps running, only no more new ones can be added
        executor.shutdown();

        awaitTermination(executor);

        cleanUp();

        LOGGER.info("Engine terminated");
    }

    private void awaitTermination(ExecutorService executor) {
        try {
            while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                LOGGER.info("Waiting another 10 seconds for the embedded engine to complete");
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void cleanUp() {
        /*
         * for KPL
        producer.flushSync();
        producer.destroy();
        */

        kinesisClient.shutdown();
    }

    private void sendRecord(SourceRecord record) {
        // We are interested only in data events not schema change events
        if (record.topic().equals(APP_NAME)) {
            return;
        }

        Schema schema = null;

        if ( null == record.keySchema() ) {
            LOGGER.error("The keySchema is missing. Something is wrong.");
            return;
        }

        // For deletes, the value node is null
        if ( null != record.valueSchema() ) {
            schema = SchemaBuilder.struct()
                    .field("key", record.keySchema())
                    .field("value", record.valueSchema())
                    .build();
        } else {
            schema = SchemaBuilder.struct()
                    .field("key", record.keySchema())
                    .build();
        }

        Struct message = new Struct(schema);
        message.put("key", record.key());

        if ( null != record.value() )
            message.put("value", record.value());

        String partitionKey = String.valueOf(record.key() != null ? record.key().hashCode() : -1);
        final byte[] payload = valueConverter.fromConnectData("dummy", schema, message);

        /*
         * for KPL
        ListenableFuture<UserRecordResult> f =
                producer.addUserRecord(record.topic(), partitionKey, ByteBuffer.wrap(payload));
        Futures.addCallback(f, callback, callbackThreadPool);
        */

        PutRecordRequest putRecord = new PutRecordRequest();
        putRecord.setStreamName(streamNameMapper(record.topic()));
        putRecord.setPartitionKey(partitionKey);
        putRecord.setData(ByteBuffer.wrap(payload));

        kinesisClient.putRecord(putRecord);
    }

    private String streamNameMapper(String topic) {
        return topic;
    }
}
