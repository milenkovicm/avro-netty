package com.github.milenkovicm.avro.io.decoder;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import com.github.milenkovicm.avro.Helper;

public class DecoderBooleanTest extends AbstractDecoderTest {
    @Test
    public void test_positive_value(){
        final GenericRecord original = Helper.genericBoolean(true);
        this.test(original);
    }

    @Test
    public void test_negative_value() {
        final GenericRecord original = Helper.genericBoolean(false);
        this.test(original);
    }

}
