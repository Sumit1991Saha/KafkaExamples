package com.saha.deserializer;

import org.apache.avro.generic.GenericRecord;

import java.util.Map;

public interface AvroDeserializer {
    /**
     * Deserialize a byte array into an avro record.
     *
     * @param bytes  - Input byte array
     * @param <T>    - Type of avro record
     * @param record - An empty avro record of same type
     * @return - Avro record filled with data
     */
    <T extends GenericRecord> T deserialize(byte[] bytes, T record);

    /**
     * Configures this avro serializer with a property bag
     *
     * @param props
     */
    void configure(Map<String, Object> props);
}
