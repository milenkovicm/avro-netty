package com.github.milenkovicm.avro.io.encoder;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import com.github.milenkovicm.avro.Helper;

public class EncodeArrayTest {
    @Test
    public void test_value() {
        final List<Long> value = new ArrayList<Long>();
        value.add( Long.valueOf(1));
        value.add( Long.valueOf(2));
        value.add( Long.valueOf(3));
        value.add( Long.valueOf(4));
        final GenericRecord record = Helper.genericList(value);
        Assert.assertArrayEquals("should be equal " + record, Helper.avroGenericBinaryEncoder(record), Helper.avroGenericByteBufEncoder(record));
    }
}
