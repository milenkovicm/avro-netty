package com.github.milenkovicm.avro.generic;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import com.github.milenkovicm.avro.AbstractTest;
import com.github.milenkovicm.avro.Helper;
import com.github.milenkovicm.avro.io.ByteBufDecoder;
import com.github.milenkovicm.avro.test.event.E_BYTES;
import com.github.milenkovicm.avro.test.event.E_STRING;

public class NettyGenericDatumReaderTest extends AbstractTest {

    final byte[] value = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    final byte[] longerValue = new byte[] { 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 9, 8, 7, 6, 5, 4, 3, 2, 1 };
    final String string = "0123456789";

    @Test
    public void test_bytebufEncoding() {

        final GenericRecord bytebuf = new GenericData.Record(E_BYTES.SCHEMA$);
        bytebuf.put("f_value", freeLaterBuffer(this.value));

        final ByteBuf encoded = Helper.avroGenericNettyEncoder(bytebuf);
        freeLaterBuffer(encoded);

        final GenericRecord result = Helper.avroGenericNettyDecoder(encoded, E_BYTES.SCHEMA$);

        Assert.assertTrue(result.get("f_value") instanceof ByteBuf);
        Assert.assertArrayEquals(this.value, Helper.getBytes((ByteBuf) result.get("f_value")));
        freeLaterBuffer((ByteBuf) result.get("f_value"));
    }

    @Test
    public void test_bytebufStringEncoding() {

        final GenericRecord stringbuf = new GenericData.Record(E_STRING.SCHEMA$);
        stringbuf.put("f_value", this.string);

        final ByteBuf encoded = Helper.avroGenericNettyEncoder(stringbuf);
        freeLaterBuffer(encoded);

        final GenericRecord result = Helper.avroGenericNettyDecoder(encoded, E_STRING.SCHEMA$);

        Assert.assertTrue(result.get("f_value") instanceof CharSequence);
        Assert.assertEquals(this.string, ((CharSequence) result.get("f_value")).toString());
    }

    @Test
    public void test_reuseReader() throws IOException {

        GenericRecord bytebuf = new GenericData.Record(E_BYTES.SCHEMA$);
        bytebuf.put("f_value", freeLaterBuffer(this.value));

        ByteBuf encoded = Helper.avroGenericNettyEncoder(bytebuf);
        freeLaterBuffer(encoded);

        final ByteBufDecoder decoder = new ByteBufDecoder();
        decoder.setBuffer(encoded);

        final GenericDatumReader<GenericRecord> reader = new NettyGenericDatumReader<GenericRecord>(E_BYTES.SCHEMA$);

        GenericRecord  result = reader.read(null, decoder);
        Assert.assertTrue(result.get("f_value") instanceof ByteBuf);
        Assert.assertArrayEquals(this.value, Helper.getBytes((ByteBuf) result.get("f_value")));

        freeLaterBuffer((ByteBuf) result.get("f_value"));

        bytebuf = new GenericData.Record(E_BYTES.SCHEMA$);
        bytebuf.put("f_value", freeLaterBuffer(this.longerValue));

        encoded = Helper.avroGenericNettyEncoder(bytebuf);

        freeLaterBuffer(encoded);
        decoder.setBuffer(encoded);

        result = reader.read(result, decoder);

        Assert.assertTrue(result.get("f_value") instanceof ByteBuf);
        Assert.assertArrayEquals(this.longerValue, Helper.getBytes((ByteBuf) result.get("f_value")));
        freeLaterBuffer((ByteBuf) result.get("f_value"));

    }

    @Test
    public void test_reuseReader_zeroCapacity() throws IOException {

        GenericRecord bytebuf = new GenericData.Record(E_BYTES.SCHEMA$);
        bytebuf.put("f_value", freeLaterBuffer(this.value));

        ByteBuf encoded = Helper.avroGenericNettyEncoder(bytebuf);
        freeLaterBuffer(encoded);

        final ByteBufDecoder decoder = new ByteBufDecoder();
        decoder.setBuffer(encoded);

        final GenericDatumReader<GenericRecord> reader = new NettyGenericDatumReader<GenericRecord>(E_BYTES.SCHEMA$);

        GenericRecord result = reader.read(null, decoder);
        Assert.assertTrue(result.get("f_value") instanceof ByteBuf);
        Assert.assertArrayEquals(this.value, Helper.getBytes((ByteBuf) result.get("f_value")));

        freeLaterBuffer((ByteBuf) result.get("f_value"));

        bytebuf = new GenericData.Record(E_BYTES.SCHEMA$);
        bytebuf.put("f_value", freeLaterBuffer(new byte[0]));

        encoded = Helper.avroGenericNettyEncoder(bytebuf);

        freeLaterBuffer(encoded);
        decoder.setBuffer(encoded);

        result = reader.read(result, decoder);

        Assert.assertTrue(result.get("f_value") instanceof ByteBuf);
        Assert.assertArrayEquals(new byte[0], Helper.getBytes((ByteBuf) result.get("f_value")));
        freeLaterBuffer((ByteBuf) result.get("f_value"));

    }

}
