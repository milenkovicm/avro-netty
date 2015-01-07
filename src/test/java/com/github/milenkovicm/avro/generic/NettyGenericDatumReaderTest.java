package com.github.milenkovicm.avro.generic;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import com.github.milenkovicm.avro.Helper;
import com.github.milenkovicm.avro.test.event.E_BYTES;

public class NettyGenericDatumReaderTest {

    final byte[] value = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

    @Test
    public void test_bytebufEncoding() {

        final GenericRecord bytebuf = new GenericData.Record(E_BYTES.SCHEMA$);
        bytebuf.put("f_value", ByteBufAllocator.DEFAULT.buffer(this.value.length).writeBytes(this.value));

        final GenericRecord result = Helper.avroGenericNettyDecoder(Helper.avroGenericNettyEncoder(bytebuf), E_BYTES.SCHEMA$);

        Assert.assertTrue(result.get("f_value") instanceof ByteBuf);
        Assert.assertArrayEquals(this.value, Helper.getBytes((ByteBuf) result.get("f_value")));


    }



}
