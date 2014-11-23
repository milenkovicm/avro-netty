package com.github.milenkovicm.avro.io.encoder;

import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import com.github.milenkovicm.avro.Helper;
import com.marko.test.event.E_SUIT;

public class EncodeEnumTest {
    @Test
    public void test_value() {
        final GenericRecord record = Helper.genericEnum(E_SUIT.HEARTS);
        Assert.assertArrayEquals("should be equal " + record, Helper.avroGenericBinaryEncoder(record), Helper.avroGenericByteBufEncoder(record));
    }
}
