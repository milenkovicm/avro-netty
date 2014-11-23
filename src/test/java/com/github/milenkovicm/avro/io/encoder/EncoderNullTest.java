package com.github.milenkovicm.avro.io.encoder;

import java.util.Arrays;
import java.util.Collection;

import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.github.milenkovicm.avro.Helper;

@RunWith(Parameterized.class)
public class EncoderNullTest {
    final String value;

    public EncoderNullTest(final String value) {
        this.value = value;
    }

    @Parameterized.Parameters(name = "value={0}")
    public static Collection<?> numners() {
        return Arrays.asList(new Object[][] { { null }, { "bla" } });
    }

    @Test
    public void test_value() {
        final GenericRecord record = Helper.genericNull(this.value);
        Assert.assertArrayEquals("should be equal " + record, Helper.avroGenericBinaryEncoder(record), Helper.avroGenericByteBufEncoder(record));
    }
}
