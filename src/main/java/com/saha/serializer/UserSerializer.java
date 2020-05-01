package com.saha.serializer;

import com.saha.model.User;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class UserSerializer implements Serializer<User> {

    private final RawAvroSerializer rawAvroSerializer = new RawAvroSerializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, User data) {
        return rawAvroSerializer.serialize(data);
    }

    @Override
    public void close() {

    }
}
