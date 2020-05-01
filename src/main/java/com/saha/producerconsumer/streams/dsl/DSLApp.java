package com.saha.producerconsumer.streams.dsl;

import com.saha.model.Order;
import com.saha.serde.OrderSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DSLApp {

    private static String INPUT_TOPIC = "order-topic";
    private static String OUTPUT_TOPIC = "order-topic-filtered";

    private static Logger LOG = LoggerFactory.getLogger(DSLApp.class);

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dsl-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, OrderSerde.class);
        //props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.serdeFrom(new OrderSerializer(), new OrderDeserializer());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, Order> stream = streamsBuilder.stream(INPUT_TOPIC);
        stream.peek(DSLApp::printOnEnter)
                .filter((transactionId, order) -> !order.getUserId().toString().equals(""))
                .filter((transactionId, order) -> order.getNoOfItems() < 1000)
                .filter((transactionId, order) -> order.getTotalAmount() < 10000)
                .mapValues((order) -> {
                    order.setUserId(String.valueOf(order.getUserId()).toUpperCase());
                    return order;
                })
                .peek(DSLApp::printOnExit)
                .to(OUTPUT_TOPIC);

        Topology topology = streamsBuilder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
    }

    private static void printOnEnter(String transactionId, Order order) {
        LOG.info("\n*******************************************");
        LOG.info("ENTERING stream transaction with ID < " + transactionId + " >, " +
                "of user < " + order.getUserId() + " >, total amount < " + order.getTotalAmount() +
                " > and nb of items < " + order.getNoOfItems() + " >");
    }

    private static void printOnExit(String transactionId, Order order) {
        LOG.info("EXITING from stream transaction with ID < " + transactionId + " >, " +
                "of user < " + order.getUserId() + " >, total amount < " + order.getTotalAmount() +
                " > and number of items < " + order.getNoOfItems() + " >");
    }
}
