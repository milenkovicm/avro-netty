package com.github.milenkovicm.avro.io.decoder;

import io.netty.buffer.ByteBuf;

import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;

import com.github.milenkovicm.avro.AbstractTest;
import com.github.milenkovicm.avro.Helper;

public abstract class AbstractDecoderTest extends AbstractTest {

    public ByteBuf encode(final GenericRecord originalValue) {
        final byte[] blob = Helper.avroGenericBinaryEncoder(originalValue);

        return freeLaterBuffer(blob);
    }

    public void test(final GenericRecord originalValue, final GenericRecord decodedValue) {
        Assert.assertEquals(originalValue.get("f_value"), decodedValue.get("f_value"));
    }

    public void test(final GenericRecord originalValue, final ByteBuf buffer) {
        Assert.assertEquals(originalValue.get("f_value"), Helper.avroGenericByteBufDecoder(buffer, originalValue.getSchema()).get("f_value"));
    }

    public void test(final GenericRecord originalValue) {
        final ByteBuf buffer = this.encode(originalValue);
        Assert.assertEquals(originalValue.get("f_value"), Helper.avroGenericByteBufDecoder(buffer, originalValue.getSchema()).get("f_value"));
    }




}
