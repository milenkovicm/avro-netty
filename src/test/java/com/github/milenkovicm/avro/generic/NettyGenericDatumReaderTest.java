package com.github.milenkovicm.avro.generic;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import com.github.milenkovicm.avro.Helper;
import com.github.milenkovicm.avro.io.ByteBufDecoder;
import com.github.milenkovicm.avro.test.event.E_BYTES;
import com.github.milenkovicm.avro.test.event.E_STRING;

public class NettyGenericDatumReaderTest {

    final byte[] value = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    final String string = "0123456789";

    @Test
    public void test_bytebufEncoding() {

        final GenericRecord bytebuf = new GenericData.Record(E_BYTES.SCHEMA$);
        bytebuf.put("f_value", ByteBufAllocator.DEFAULT.buffer(this.value.length).writeBytes(this.value));

        final GenericRecord result = Helper.avroGenericNettyDecoder(Helper.avroGenericNettyEncoder(bytebuf), E_BYTES.SCHEMA$);

        Assert.assertTrue(result.get("f_value") instanceof ByteBuf);
        Assert.assertArrayEquals(this.value, Helper.getBytes((ByteBuf) result.get("f_value")));
    }

    @Test
    public void test_bytebufStringEncoding() {

        final GenericRecord stringbuf = new GenericData.Record(E_STRING.SCHEMA$);
        stringbuf.put("f_value", this.string);

        final GenericRecord result = Helper.avroGenericNettyDecoder(Helper.avroGenericNettyEncoder(stringbuf), E_STRING.SCHEMA$);

        Assert.assertTrue(result.get("f_value") instanceof CharSequence);
        Assert.assertEquals(this.string, ((CharSequence) result.get("f_value")).toString());
    }

    @Test
    public void test_reuseReader() throws IOException {

        final GenericRecord bytebuf = new GenericData.Record(E_BYTES.SCHEMA$);
        bytebuf.put("f_value", ByteBufAllocator.DEFAULT.buffer(this.value.length).writeBytes(this.value));

        final ByteBuf buffer = Helper.avroGenericNettyEncoder(bytebuf);

        final ByteBufDecoder decoder = new ByteBufDecoder();
        decoder.setBuffer(buffer);
        final GenericDatumReader<GenericRecord> reader = new NettyGenericDatumReader<GenericRecord>(E_BYTES.SCHEMA$);

        GenericRecord  result = reader.read(null, decoder);
        Assert.assertTrue(result.get("f_value") instanceof ByteBuf);
        Assert.assertArrayEquals(this.value, Helper.getBytes((ByteBuf) result.get("f_value")));

        // we want to read data from the same buffer
        buffer.resetReaderIndex();
        result = reader.read(result, decoder);
        Assert.assertTrue(result.get("f_value") instanceof ByteBuf);
        Assert.assertArrayEquals(this.value, Helper.getBytes((ByteBuf) result.get("f_value")));
    }

}
