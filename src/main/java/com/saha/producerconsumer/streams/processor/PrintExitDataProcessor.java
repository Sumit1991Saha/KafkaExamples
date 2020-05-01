package com.saha.producerconsumer.streams.processor;

import com.saha.model.Order;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrintExitDataProcessor extends AbstractProcessor<String, Order> {

    private static Logger LOG = LoggerFactory.getLogger(PrintExitDataProcessor.class);

    @Override
    public void process(String key, Order value) {
        LOG.info("EXITING from stream transaction with ID < " + key + " >, " +
                "of user < " + value.getUserId() + " >, total amount < " + value.getTotalAmount() +
                " > and number of items < " + value.getNoOfItems() + " >");
        this.context().forward(key, value);
        this.context().commit();
    }
}
