package com.saha.producerconsumer.simple;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerApp {

    public static void main(String[] args) {
        Properties properties = new Properties();
        // Need to provide at least 1 broker details as producer will fetch other broker details from the metadata that zookepeer would send
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        ProducerRecord<String, String> producerRecord;
        try {
            for (int i = 0; i < 10; ++i ) {
                producerRecord =
                        new ProducerRecord<>("simple-topic", Integer.toString(i), "MyMessage: " + i);
                producer.send(producerRecord);
                Thread.sleep(1000);
                // To view the messages produced by producer on console :-
                // bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic simple-topic --from-beginning
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
