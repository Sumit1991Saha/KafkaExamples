package com.saha.deserializer;

import com.saha.model.User;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class UserDeserializer implements Deserializer<User> {

    private final RawAvroDeserializer rawAvroDeserializer = new RawAvroDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public User deserialize(String topic, byte[] data) {
        return rawAvroDeserializer.deserialize(data, new User());
    }

    @Override
    public void close() {

    }
}
