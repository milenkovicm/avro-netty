package com.github.milenkovicm.avro.io.encoder;

import java.util.Collection;

import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.github.milenkovicm.avro.DataHelper;
import com.github.milenkovicm.avro.Helper;

@RunWith(Parameterized.class)
public class EncoderIntTest {
    final int value;

    public EncoderIntTest(final int value) {
        this.value = value;
    }

    @Parameterized.Parameters(name = "value={0}")
    public static Collection<?> data() {
        return DataHelper.integers();
    }

    @Test
    public void test_plus_value() {
        final GenericRecord record = Helper.genericInt(this.value);
        Assert.assertArrayEquals("should be equal " + record, Helper.avroGenericBinaryEncoder(record), Helper.avroGenericByteBufEncoder(record));
    }

    @Test
    public void test_minus_value() {
        final GenericRecord record = Helper.genericInt(-this.value);
        Assert.assertArrayEquals("should be equal -" + record, Helper.avroGenericBinaryEncoder(record), Helper.avroGenericByteBufEncoder(record));
    }

}
