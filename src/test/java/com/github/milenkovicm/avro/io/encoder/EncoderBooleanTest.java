package com.github.milenkovicm.avro.io.encoder;

import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import com.github.milenkovicm.avro.Helper;

public class EncoderBooleanTest {

    @Test
    public void test_plus_value() {
        final GenericRecord record = Helper.genericBoolean(false);
        Assert.assertArrayEquals("should be equal " + record, Helper.avroGenericBinaryEncoder(record), Helper.avroGenericByteBufEncoder(record));
    }

    @Test
    public void test_minus_value() {
        final GenericRecord record = Helper.genericBoolean(false);
        Assert.assertArrayEquals("should be equal -" + record, Helper.avroGenericBinaryEncoder(record), Helper.avroGenericByteBufEncoder(record));
    }
}
