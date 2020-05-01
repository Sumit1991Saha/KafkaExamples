package com.saha.serde;

import com.saha.deserializer.OrderDeserializer;
import com.saha.model.Order;
import com.saha.serializer.OrderSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class OrderSerde implements Serde<Order> {

    private OrderSerializer orderSerializer = new OrderSerializer();
    private OrderDeserializer orderDeserializer = new OrderDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<Order> serializer() {
        return orderSerializer;
    }

    @Override
    public Deserializer<Order> deserializer() {
        return orderDeserializer;
    }
}
