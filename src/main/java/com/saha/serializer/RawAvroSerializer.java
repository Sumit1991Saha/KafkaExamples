package com.saha.serializer;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

public class RawAvroSerializer implements AvroSerializer {
    @Override
    public <T extends GenericRecord> byte[] serialize(T record) {
        if (null == record) {
            return null;
        }
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        serialize(byteArrayOutputStream, record);
        return byteArrayOutputStream.toByteArray();
    }

    @Override
    public <T extends GenericRecord> void serialize(OutputStream outputStream, T record) {
        if (null == record) {
            return;
        }
        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(record.getSchema());
        try {
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(outputStream, null);
            datumWriter.write(record, encoder);
            encoder.flush();
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public void configure(Map<String, Object> props) {
        //No-op
    }

}
