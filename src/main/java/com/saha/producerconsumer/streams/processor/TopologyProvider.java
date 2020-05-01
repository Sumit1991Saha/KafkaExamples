package com.saha.producerconsumer.streams.processor;

import com.saha.model.Order;
import com.saha.serde.OrderSerde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class TopologyProvider {

    private static String ORDER_TOPIC = "order-topic";
    private static String VALID_ORDER_TOPIC = "order-topic-filtered";
    private static String VALID_ORDER_COUNT_TOPIC = "valid-order-count-topic";

    private static final String SOURCE1 = "SOURCE1";
    private static final String SOURCE2 = "SOURCE2";
    private static final String SINK1 = "SINK1";
    private static final String SINK2 = "SINK2";
    private static final String PRINT_ENTRY_DATA = "PRINT-ENTRY-DATA";
    private static final String PRINT_EXIT_DATA = "PRINT-EXIT-DATA";
    private static final String USER_ID = "USER-ID";
    private static final String ITEM_COUNT = "ITEM-COUNT";
    private static final String TOTAL_AMOUNT = "TOTAL-AMOUNT";
    private static final String ORDER_COUNT = "ORDER-COUNT";

    public Topology get() {
        Topology topology = new Topology();

        Serializer<String> stringSerializer = new StringSerializer();
        Serializer<Long> longSerializer = new LongSerializer();
        Deserializer<String> stringDeserializer = new StringDeserializer();
        Serde<Order> orderSerde = new OrderSerde();

        topology.addSource(SOURCE1, stringDeserializer, orderSerde.deserializer(), ORDER_TOPIC)
                .addProcessor(PRINT_ENTRY_DATA, PrintEntryDataProcessor::new, SOURCE1)
                .addProcessor(USER_ID, UserIDProcessor::new, PRINT_ENTRY_DATA)
                .addProcessor(ITEM_COUNT, ItemCountProcessor::new, USER_ID)
                .addProcessor(TOTAL_AMOUNT, TotalAmountProcessor::new, ITEM_COUNT)
                .addProcessor(PRINT_EXIT_DATA, PrintExitDataProcessor::new, TOTAL_AMOUNT)
                .addSink(SINK1, VALID_ORDER_TOPIC, stringSerializer, orderSerde.serializer(), PRINT_EXIT_DATA);

        // health rule store
        /*StoreBuilder<KeyValueStore<String, Order>> validOrderStore =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(ValidOrderCountProcessor.STATE_STORE_NAME),
                        Serdes.String(), orderSerde);

        topology.addSource(SOURCE2, stringDeserializer, orderSerde.deserializer(), VALID_ORDER_TOPIC)
                .addProcessor(ORDER_COUNT, ValidOrderCountProcessor::new, SOURCE2)
                .addStateStore(validOrderStore, ORDER_COUNT)
                .addSink(SINK2, VALID_ORDER_COUNT_TOPIC, longSerializer, longSerializer, ORDER_COUNT);*/

        return topology;
    }
}
