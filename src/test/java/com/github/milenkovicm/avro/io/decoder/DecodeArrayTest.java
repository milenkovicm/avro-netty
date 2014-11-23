package com.github.milenkovicm.avro.io.decoder;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import com.github.milenkovicm.avro.Helper;

public class DecodeArrayTest extends AbstractDecoderTest {
    @Test
    public void test_value() {
        final List<Long> value = new ArrayList<Long>();
        value.add(Long.valueOf(1));
        value.add(Long.valueOf(2));
        value.add(Long.valueOf(3));
        value.add(Long.valueOf(4));
        final GenericRecord record = Helper.genericList(value);
        this.test(record);
    }
}
