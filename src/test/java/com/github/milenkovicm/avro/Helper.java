package com.github.milenkovicm.avro;

import io.netty.buffer.ByteBuf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import com.github.milenkovicm.avro.io.ByteBufDecoder;
import com.github.milenkovicm.avro.io.ByteBufEncoder;
import com.marko.test.event.E_ARRAY;
import com.marko.test.event.E_BOOLEAN;
import com.marko.test.event.E_BYTES;
import com.marko.test.event.E_COMPOSITE;
import com.marko.test.event.E_DOUBLE;
import com.marko.test.event.E_ENUM;
import com.marko.test.event.E_FLOAT;
import com.marko.test.event.E_INT;
import com.marko.test.event.E_LONG;
import com.marko.test.event.E_MAP;
import com.marko.test.event.E_NULL;
import com.marko.test.event.E_STRING;
import com.marko.test.event.E_SUIT;

public class Helper {

    public static byte[] avroGenericBinaryEncoder(final GenericRecord record) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        final DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(record.getSchema());

        try {
            writer.write(record, encoder);
            encoder.flush();
            out.close();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    public static byte[] avroGenericByteBufEncoder(final GenericRecord record) {
        final ByteBufEncoder encoder = new ByteBufEncoder();
        final DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(record.getSchema());

        try {
            writer.write(record, encoder);
            return encoder.getBytes();

        } catch (final Exception e) {
            throw new RuntimeException(e);
        } finally {
            encoder.release();
        }
    }

    public static GenericRecord avroGenericByteBufDecoder(final ByteBuf buffer, final GenericRecord record) {
        final ByteBufDecoder decoder = new ByteBufDecoder(buffer);
        final GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(record.getSchema());
        try {
            return reader.read(null, decoder);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static GenericRecord defaultGeneric() {
        final GenericRecord record = new GenericData.Record(E_COMPOSITE.SCHEMA$);
        record.put("f_string", "this is a stringa");
        record.put("f_int", 12456);
        record.put("f_long", (long) 3);
        record.put("f_float", (float) 12.0);
        record.put("f_double", 25668.0);
        record.put("f_boolean", true);
        record.put("f_bytes", ByteBuffer.wrap(new byte[] { 2, 3, 4, 5, 6, 7, 8 }));

        return record;
    }

    public static GenericRecord genericInt(final int value) {
        final GenericRecord record = new GenericData.Record(E_INT.SCHEMA$);
        record.put("f_value", value);
        return record;
    }

    public static GenericRecord genericLong(final long value) {
        final GenericRecord record = new GenericData.Record(E_LONG.SCHEMA$);
        record.put("f_value", value);
        return record;
    }

    public static GenericRecord genericFloat(final float value) {
        final GenericRecord record = new GenericData.Record(E_FLOAT.SCHEMA$);
        record.put("f_value", value);
        return record;
    }

    public static GenericRecord genericDouble(final double value) {
        final GenericRecord record = new GenericData.Record(E_DOUBLE.SCHEMA$);
        record.put("f_value", value);
        return record;
    }

    public static GenericRecord genericBoolean(final boolean value) {
        final GenericRecord record = new GenericData.Record(E_BOOLEAN.SCHEMA$);
        record.put("f_value", value);
        return record;
    }

    public static GenericRecord genericBytes(final byte[] value) {
        final GenericRecord record = new GenericData.Record(E_BYTES.SCHEMA$);
        record.put("f_value", ByteBuffer.wrap(value));
        return record;
    }

    public static GenericRecord genericString(final String value) {
        final GenericRecord record = new GenericData.Record(E_STRING.SCHEMA$);
        record.put("f_value", value);
        return record;
    }

    public static GenericRecord genericNull(final String value) {
        final GenericRecord record = new GenericData.Record(E_NULL.SCHEMA$);
        record.put("f_value", value);
        return record;
    }

    public static GenericRecord genericMap(final Map<String, Long> value) {
        final GenericRecord record = new GenericData.Record(E_MAP.SCHEMA$);
        record.put("f_value", value);
        return record;
    }

    public static GenericRecord genericList(final List<Long> value) {
        final GenericRecord record = new GenericData.Record(E_ARRAY.SCHEMA$);
        record.put("f_value", value);
        return record;
    }

    public static GenericRecord genericEnum(final E_SUIT value) {
        final GenericRecord record = new GenericData.Record(E_ENUM.SCHEMA$);
        record.put("f_value", value);
        return record;
    }
}
