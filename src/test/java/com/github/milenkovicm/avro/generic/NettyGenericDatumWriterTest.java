package com.github.milenkovicm.avro.generic;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.nio.ByteBuffer;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.junit.Assert;
import org.junit.Test;

import com.github.milenkovicm.avro.Helper;
import com.github.milenkovicm.avro.io.ByteBufEncoder;
import com.marko.test.event.E_BYTES;

public class NettyGenericDatumWriterTest {
    final byte[] value = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

    @Test
    public void test_bytebufEncoding() {
        final GenericRecord bytebuffer = new GenericData.Record(E_BYTES.SCHEMA$);
        bytebuffer.put("f_value", ByteBuffer.wrap(this.value));

        final GenericRecord bytebuf = new GenericData.Record(E_BYTES.SCHEMA$);
        bytebuf.put("f_value", ByteBufAllocator.DEFAULT.buffer(this.value.length).writeBytes(this.value));

        Assert.assertArrayEquals(Helper.avroGenericBinaryEncoder(bytebuffer), avroGenericNettyEncoder(bytebuf));
    }

    @Test
    public void test_bytebufferSupport() {

        final GenericRecord bytebuffer = new GenericData.Record(E_BYTES.SCHEMA$);
        bytebuffer.put("f_value", ByteBuffer.wrap(this.value));

        Assert.assertArrayEquals(Helper.avroGenericBinaryEncoder(bytebuffer), avroGenericNettyEncoder(bytebuffer));
    }

    @Test
    public void test_byteBufCompositeObject() {

        final GenericRecord record = Helper.defaultGeneric();
        record.put("f_bytes", ByteBuffer.wrap(this.value));
        final GenericRecord recordByteBuf = Helper.defaultGeneric();
        recordByteBuf.put("f_bytes", ByteBufAllocator.DEFAULT.buffer(this.value.length).writeBytes(this.value));

        Assert.assertArrayEquals("should be equal", Helper.avroGenericBinaryEncoder(record), avroGenericNettyEncoder(recordByteBuf));
    }

    static byte[] avroGenericNettyEncoder(final GenericRecord record) {
        final ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer(64);
        final ByteBufEncoder encoder = new ByteBufEncoder();
        encoder.setBuffer(buffer);
        final DatumWriter<GenericRecord> writer = new NettyGenericDatumWriter<GenericRecord>(record.getSchema());
        try {
            writer.write(record, encoder);
            return Helper.getBytes(encoder.getBuffer());
        } catch (final Exception e) {
            throw new RuntimeException(e);
        } finally {
            buffer.release();
        }
    }

}
