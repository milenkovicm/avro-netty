package com.github.milenkovicm.avro.io.decoder;

import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import com.github.milenkovicm.avro.Helper;

public class DecodeMapTest extends AbstractDecoderTest {
    @Test
    public void test_value() {
        final Map<String, Long> value = new HashMap<String, Long>();
        value.put("aaaaa", Long.valueOf(1));
        value.put("bbbbb", Long.valueOf(2));
        value.put("ccccc", Long.valueOf(3));
        value.put("ddddd", Long.valueOf(4));

        final GenericRecord originalValue = Helper.genericMap(value);
        final ByteBuf buffer = this.encode(originalValue);

        final Map<CharSequence, Long> result = (Map<CharSequence, Long>) Helper.avroGenericByteBufDecoder(buffer, originalValue).get("f_value");

        for (final CharSequence key : result.keySet()) {
            Assert.assertEquals(value.get(key.toString()), result.get(key));
        }


    }
}