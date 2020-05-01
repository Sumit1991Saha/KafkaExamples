package com.saha.producerconsumer.streams.processor;

import com.saha.model.Order;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;

public class ValidOrderCountProcessor extends AbstractProcessor<String, Order> implements Punctuator {

    private static Logger LOG = LoggerFactory.getLogger(ValidOrderCountProcessor.class);

    private KeyValueStore<String, Order> summaryStore;
    private Punctuator punctuator;

    protected static final String STATE_STORE_NAME = "valid-order-Store";

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        super.init(context);
        context.schedule(Duration.ofMillis(10000), PunctuationType.WALL_CLOCK_TIME, punctuator);
        summaryStore = (KeyValueStore<String, Order>) context.getStateStore(STATE_STORE_NAME);
        Objects.requireNonNull(summaryStore, "State store can't be null");
    }

    @Override
    public void process(String key, Order value) {
        String userId = value.getUserId().toString();
        Order orderData = summaryStore.get(userId);
        if (orderData == null) {
            summaryStore.put(userId, value);
        }
        this.context().commit();
    }

    @Override
    public void punctuate(long timestamp) {
        KeyValueIterator<String, Order> iterator = summaryStore.all();
        long count = 0;
        while (iterator.hasNext()) {
            count++;
            iterator.next();
        }
        LOG.info("Total valid orders are :- " + count);
        this.context().forward(timestamp, count);
        this.context().commit();
    }
}
