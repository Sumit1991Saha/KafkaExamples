package com.saha.producerconsumer.streams.processor;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProcessorApp {

    private static Logger LOG = LoggerFactory.getLogger(ProcessorApp.class);

    public static void main(String[] args) {

        TopologyProvider topologyProvider = new TopologyProvider();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        LOG.info("Starting KafkaStreaming");
        //Use the topologyBuilder and streamingConfig to start the kafka streams process
        KafkaStreams streaming = new KafkaStreams(topologyProvider.get(), props);
        streaming.start();
        LOG.info("Now started");
    }
}
