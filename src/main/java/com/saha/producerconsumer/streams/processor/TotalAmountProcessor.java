package com.saha.producerconsumer.streams.processor;

import com.saha.model.Order;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TotalAmountProcessor extends AbstractProcessor<String, Order> {

    private static Logger LOG = LoggerFactory.getLogger(TotalAmountProcessor.class);

    @Override
    public void process(String key, Order value) {
        if (value.getTotalAmount() < 10000) {
            LOG.info("Forwarding to PrintExitDataProcessor");
            this.context().forward(key, value);
            this.context().commit();
        } else {
            LOG.info("User " + value.getUserId() +" has crossed max allowed amount");
        }
    }
}
