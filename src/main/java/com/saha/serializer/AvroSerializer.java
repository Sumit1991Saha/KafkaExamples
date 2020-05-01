package com.saha.serializer;

import org.apache.avro.generic.GenericRecord;

import java.io.OutputStream;
import java.util.Map;


public interface AvroSerializer {
    /**
     * Serializes a avro record into byte array
     *
     * @param record - Avro record
     * @param <T>    - Type of avro record
     * @return - serialized byte array
     */
    <T extends GenericRecord> byte[] serialize(T record);

    /**
     * Serializes a avro record into byte array and write it to output stream
     * @param outputStream - Output stream to write data to
     * @param record - Avro record
     * @param <T>    - Type of avro record
     */
    <T extends GenericRecord> void serialize(OutputStream outputStream, T record);

    /**
     * Configures this avro serializer with a property bag
     *
     * @param props
     */
    void configure(Map<String, Object> props);
}

