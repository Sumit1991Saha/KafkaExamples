package com.saha.deserializer;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.util.Map;

public class RawAvroDeserializer implements AvroDeserializer {

    @Override
    public <T extends GenericRecord> T deserialize(byte[] bytes, T record) {
        if (null == bytes) {
            return null;
        }
        try {
            DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(record.getSchema());
            datumReader.read(record, DecoderFactory.get().binaryDecoder(bytes, null));
            return record;
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public void configure(Map<String, Object> props) {
        //No-op
    }
}
