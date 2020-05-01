package com.saha.producerconsumer.streams.processor;

import com.saha.model.Order;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserIDProcessor extends AbstractProcessor<String, Order> {

    private static Logger LOG = LoggerFactory.getLogger(UserIDProcessor.class);

    @Override
    public void process(String key, Order value) {
        if (!value.getUserId().toString().equals("")) {
            value.setUserId(value.getUserId().toString().toUpperCase());
            LOG.info("Forwarding to ItemCountProcessor");
            this.context().forward(key, value);
            this.context().commit();
        } else {
            LOG.info("Invalid User");
        }
    }
}
