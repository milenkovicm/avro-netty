package com.github.milenkovicm.avro.io.decoder;

import io.netty.buffer.ByteBuf;

import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import com.github.milenkovicm.avro.Helper;

public class DecodeCompoisteTest extends AbstractDecoderTest {
    @Test
    public void test_record() {
        final GenericRecord originalValue = Helper.defaultGeneric();
        final ByteBuf buffer = this.encode(originalValue);
        final GenericRecord decodedValue = Helper.avroGenericByteBufDecoder(buffer, originalValue);

        Assert.assertEquals(originalValue.get("f_string"), decodedValue.get("f_string").toString());
        Assert.assertEquals(originalValue.get("f_int"), decodedValue.get("f_int"));
        Assert.assertEquals(originalValue.get("f_long"), decodedValue.get("f_long"));
        Assert.assertEquals(originalValue.get("f_float"), decodedValue.get("f_float"));
        Assert.assertEquals(originalValue.get("f_double"), decodedValue.get("f_double"));
        Assert.assertEquals(originalValue.get("f_boolean"), decodedValue.get("f_boolean"));
        Assert.assertEquals(originalValue.get("f_bytes"), decodedValue.get("f_bytes"));
    }
}
