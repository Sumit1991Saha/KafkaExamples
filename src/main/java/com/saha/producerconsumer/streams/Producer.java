package com.saha.producerconsumer.streams;

import com.saha.model.Order;
import com.saha.serializer.OrderSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {

    private static String TOPIC = "order-topic";

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        KafkaProducer<String, Order> producer = new KafkaProducer<>(props, new StringSerializer(), new OrderSerializer());

        produceMessage(producer, "1",
                Order.newBuilder()
                        .setUserId("")
                        .setNoOfItems(5)
                        .setTotalAmount(5)
                        .build());

        produceMessage(producer, "2",
                Order.newBuilder()
                        .setUserId("123")
                        .setNoOfItems(1001)
                        .setTotalAmount(100)
                        .build());

        produceMessage(producer, "3",
                Order.newBuilder()
                        .setUserId("ghi")
                        .setNoOfItems(1)
                        .setTotalAmount(10001)
                        .build());

        produceMessage(producer, "4",
                Order.newBuilder()
                        .setUserId("abc")
                        .setNoOfItems(10)
                        .setTotalAmount(100)
                        .build());

        produceMessage(producer, "5",
                Order.newBuilder()
                        .setUserId("JKl")
                        .setNoOfItems(1)
                        .setTotalAmount(1)
                        .build());

        producer.close();
    }

    private static void produceMessage(KafkaProducer<String, Order> producer, String key, Order value)
            throws InterruptedException {
        ProducerRecord<String, Order> producerRecord = new ProducerRecord<>(TOPIC, key, value);
        producer.send(producerRecord);
        Thread.sleep(2000);
    }
}
