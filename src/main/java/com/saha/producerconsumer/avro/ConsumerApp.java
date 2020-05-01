package com.saha.producerconsumer.avro;

import com.saha.deserializer.UserDeserializer;
import com.saha.model.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class ConsumerApp {

    public static void main(String[] args) {

        Properties properties = new Properties();
        // Need to provide at least 1 broker details as consumer will fetch other broker details from the metadata that zookepeer would send
        properties.put("bootstrap.servers", "localhost:9092");
        //properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //properties.put("value.deserializer", "com.saha.deserializer.UserDeserializer");
        properties.put("group.id", "test");

        //KafkaConsumer<String, User> consumer = new KafkaConsumer<>(properties);
        KafkaConsumer<String, User> consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new UserDeserializer());

        Collection<String> topics = Collections.singletonList("simple-topic");
        consumer.subscribe(topics);
        try {
            while(true) {
                ConsumerRecords<String, User> records = consumer.poll(Duration.ofMillis(10));
                for (ConsumerRecord<String, User> record : records) {
                    System.out.println( String.format("key: %s, User Id : %s, User Name: %s",
                            record.key(), record.value().getUserId(), record.value().getUserName()));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
