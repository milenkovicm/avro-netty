package com.github.milenkovicm.avro.io.encoder;

import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import com.github.milenkovicm.avro.Helper;

public class EncoderCompositeTest {

    @Test
    public void test_record() {
        final GenericRecord record = Helper.defaultGeneric();
        Assert.assertArrayEquals("should be equal", Helper.avroGenericBinaryEncoder(record), Helper.avroGenericByteBufEncoder(record));
    }
}
