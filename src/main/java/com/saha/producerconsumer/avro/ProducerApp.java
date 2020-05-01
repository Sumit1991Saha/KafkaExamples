package com.saha.producerconsumer.avro;

import com.saha.model.User;
import com.saha.serializer.UserSerializer;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class ProducerApp {

    public static void main(String[] args) {
        Properties properties = new Properties();
        // Need to provide at least 1 broker details as producer will fetch other broker details from the metadata that zookepeer would send
        properties.put("bootstrap.servers", "localhost:9092");
        //properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //properties.put("value.serializer", "com.saha.serializer.UserSerializer");

        KafkaProducer<String, User> producer = new KafkaProducer<>(properties, new StringSerializer(), new UserSerializer());
        //KafkaProducer<String, User> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, User> producerRecord;
        User user;
        try {
            for (int i = 0; i < 10; ++i ) {
                user = User.newBuilder()
                        .setUserId(i + 1)
                        .setUserName("Sumit")
                        .setDateOfBirth(10000)
                        .build();
                producerRecord =
                        new ProducerRecord<>("simple-topic", Integer.toString(i), user);
                producer.send(producerRecord);
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
