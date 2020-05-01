package com.saha.producerconsumer.simple;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class ConsumerApp {

    public static void main(String[] args) {

        Properties properties = new Properties();
        // Need to provide at least 1 broker details as consumer will fetch other broker details from the metadata that zookepeer would send
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test");
        // Kafka message/record is processed by only one consumer process per consumer group.
        // So if you want multiple consumers to process the message/record you can use different groups for the consumers.

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        Collection<String> topics = Collections.singletonList("simple-topic");
        consumer.subscribe(topics);
        try {
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println( String.format("Topic: %s, partition: %d, offset: %d, key: %s value: %s",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
