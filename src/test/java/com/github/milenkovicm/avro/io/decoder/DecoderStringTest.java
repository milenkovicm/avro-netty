package com.github.milenkovicm.avro.io.decoder;

import io.netty.buffer.ByteBuf;

import java.util.Collection;

import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.github.milenkovicm.avro.DataHelper;
import com.github.milenkovicm.avro.Helper;

@RunWith(Parameterized.class)
public class DecoderStringTest extends AbstractDecoderTest {
    final String value;

    public DecoderStringTest(final String value) {
        this.value = value;
    }

    @Parameterized.Parameters(name = "value={0}")
    public static Collection<?> data() {
        return DataHelper.strings();
    }

    @Test
    public void test_plus_value() {

        final GenericRecord originalValue = Helper.genericString(this.value);
        final ByteBuf buffer = this.encode(originalValue);

        Assert.assertEquals(originalValue.get("f_value"), Helper.avroGenericByteBufDecoder(buffer, originalValue.getSchema()).get("f_value")
                .toString());
    }
}
