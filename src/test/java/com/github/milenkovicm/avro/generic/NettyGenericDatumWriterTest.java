package com.github.milenkovicm.avro.generic;

import io.netty.buffer.ByteBufAllocator;

import java.nio.ByteBuffer;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import com.github.milenkovicm.avro.Helper;
import com.github.milenkovicm.avro.test.event.E_BYTES;

public class NettyGenericDatumWriterTest {
    final byte[] value = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

    @Test
    public void test_bytebufEncoding() {
        final GenericRecord bytebuffer = new GenericData.Record(E_BYTES.SCHEMA$);
        bytebuffer.put("f_value", ByteBuffer.wrap(this.value));

        final GenericRecord bytebuf = new GenericData.Record(E_BYTES.SCHEMA$);
        bytebuf.put("f_value", ByteBufAllocator.DEFAULT.buffer(this.value.length).writeBytes(this.value));

        Assert.assertArrayEquals(Helper.avroGenericBinaryEncoder(bytebuffer), Helper.getBytes(Helper.avroGenericNettyEncoder(bytebuf)));
    }

    @Test
    public void test_bytebufferSupport() {

        final GenericRecord bytebuffer = new GenericData.Record(E_BYTES.SCHEMA$);
        bytebuffer.put("f_value", ByteBuffer.wrap(this.value));

        Assert.assertArrayEquals(Helper.avroGenericBinaryEncoder(bytebuffer), Helper.getBytes(Helper.avroGenericNettyEncoder(bytebuffer)));
    }

    @Test
    public void test_byteBufCompositeObject() {

        final GenericRecord record = Helper.defaultGeneric();
        record.put("f_bytes", ByteBuffer.wrap(this.value));
        final GenericRecord recordByteBuf = Helper.defaultGeneric();
        recordByteBuf.put("f_bytes", ByteBufAllocator.DEFAULT.buffer(this.value.length).writeBytes(this.value));

        Assert.assertArrayEquals("should be equal", Helper.avroGenericBinaryEncoder(record),
                Helper.getBytes(Helper.avroGenericNettyEncoder(recordByteBuf)));
    }


}
