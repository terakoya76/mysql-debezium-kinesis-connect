package com.terakoya76.debeziumsample.model;

import java.nio.ByteBuffer;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAutoGeneratedKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName = "offsets")
public class Offset {
    private String instanceId;
    private String key;
    private String value;

    @DynamoDBHashKey(attributeName = "instance_id")
    public String getInstanceId() {
        return instanceId;
    }

    @DynamoDBAttribute(attributeName = "key")
    public String getKey() {
        return key;
    }

    @DynamoDBAttribute(attributeName = "value")
    public String getValue() {
        return value;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
