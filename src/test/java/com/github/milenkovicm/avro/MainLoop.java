package com.github.milenkovicm.avro;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
//
// to be used with JIT to check if methos are going to be compiled
//

import com.github.milenkovicm.avro.io.ByteBufDecoder;

// JRE arguments used
// -XX:+UnlockDiagnosticVMOptions -XX:+TraceClassLoading -XX:+LogCompilation -XX:+PrintAssembly
public class MainLoop {

    public static void main(final String[] args) throws IOException {
        final GenericRecord record = Helper.defaultGeneric();
        final ByteBuf buffer = ByteBufAllocator.DEFAULT.directBuffer().writeBytes(Helper.avroGenericByteBufEncoder(record));

        final GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(record.getSchema());
        long hash = 0;

        System.err.print("starting ... ");
        final long time = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < time) {
            final ByteBufDecoder decoder = new ByteBufDecoder();
            decoder.setBuffer(buffer);
            final GenericRecord result = reader.read(null, decoder);
            hash += (int) result.get(1);
            buffer.resetReaderIndex();
        }
        System.err.println("DONE" + hash);
    }

}
