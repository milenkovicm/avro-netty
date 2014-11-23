package com.github.milenkovicm.avro.io.encoder;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import com.github.milenkovicm.avro.Helper;

public class EncodeMapTest {
    @Test
    public void test_value() {
        final Map<String, Long> value = new HashMap<String, Long>();
        value.put("aaaaa", Long.valueOf(1));
        value.put("bbbbb", Long.valueOf(2));
        value.put("ccccc", Long.valueOf(3));
        value.put("ddddd", Long.valueOf(4));
        final GenericRecord record = Helper.genericMap(value);
        Assert.assertArrayEquals("should be equal " + record, Helper.avroGenericBinaryEncoder(record), Helper.avroGenericByteBufEncoder(record));
    }
}
