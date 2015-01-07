package com.github.milenkovicm.avro.io.decoder;

import io.netty.buffer.ByteBuf;

import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import com.github.milenkovicm.avro.Helper;
import com.github.milenkovicm.avro.test.event.E_SUIT;

public class DecodeEnumTest extends AbstractDecoderTest {
    @Test
    public void test_value() {
        final GenericRecord originalValue = Helper.genericEnum(E_SUIT.DIAMONDS);
        final ByteBuf buffer = this.encode(originalValue);
        Assert.assertEquals(originalValue.get("f_value").toString(),
                Helper.avroGenericByteBufDecoder(buffer, originalValue.getSchema()).get("f_value")
                .toString());
    }
}
