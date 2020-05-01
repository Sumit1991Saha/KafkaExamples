package com.saha.deserializer;

import com.saha.model.Order;
import com.saha.model.User;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class OrderDeserializer implements Deserializer<Order> {

    private final RawAvroDeserializer rawAvroDeserializer = new RawAvroDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Order deserialize(String topic, byte[] data) {
        return rawAvroDeserializer.deserialize(data, new Order());
    }

    @Override
    public void close() {

    }
}
