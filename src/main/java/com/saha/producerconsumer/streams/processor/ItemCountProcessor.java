package com.saha.producerconsumer.streams.processor;

import com.saha.model.Order;
import com.saha.producerconsumer.streams.dsl.DSLApp;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ItemCountProcessor extends AbstractProcessor<String, Order> {

    private static Logger LOG = LoggerFactory.getLogger(ItemCountProcessor.class);

    @Override
    public void process(String key, Order value) {
        if (value.getNoOfItems() < 1000) {
            LOG.info("Forwarding to TotalAmountProcessor");
            this.context().forward(key, value);
            this.context().commit();
        } else {
            LOG.info("User " + value.getUserId() +" has crossed max items");
        }
    }
}
