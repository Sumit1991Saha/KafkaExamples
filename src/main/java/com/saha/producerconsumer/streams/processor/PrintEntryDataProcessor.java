package com.saha.producerconsumer.streams.processor;

import com.saha.model.Order;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrintEntryDataProcessor extends AbstractProcessor<String, Order> {

    private static Logger LOG = LoggerFactory.getLogger(PrintEntryDataProcessor.class);

    @Override
    public void process(String key, Order value) {
        LOG.info("\n*******************************************");
        LOG.info("ENTERING stream transaction with ID < " + key + " >, " +
                "of user < " + value.getUserId() + " >, total amount < " + value.getTotalAmount() +
                " > and nb of items < " + value.getNoOfItems() + " >");
        LOG.info("Forwarding to UserIDProcessor");
        this.context().forward(key, value);
        this.context().commit();
    }
}
