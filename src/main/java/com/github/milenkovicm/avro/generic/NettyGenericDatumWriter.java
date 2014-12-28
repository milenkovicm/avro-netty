package com.github.milenkovicm.avro.generic;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;

import com.github.milenkovicm.avro.io.ByteBufEncoder;

/**
 * Netty implementation of {@code DatumWriter} for generic Java objects.
 *
 * @param <D>
 *        datum type
 */
public class NettyGenericDatumWriter<D> extends GenericDatumWriter<D> {
    /**
     * Default constructor.
     *
     * @param root
     *        schema
     */
    public NettyGenericDatumWriter(final Schema root) {
        this.setSchema(root);
    }

    /**
     * Called to write data.
     *
     * @param datum
     *        object to write
     * @param out
     *        encoder to use
     * @throws IOException
     *         in case of error
     */
    public void write(final D datum, final ByteBufEncoder out) throws IOException {
        this.write(datum, out);
    }

    @Override
    protected void writeBytes(final Object datum, final Encoder out) throws IOException {
        if (datum instanceof ByteBuffer) {
            out.writeBytes((ByteBuffer) datum);
        } else if (datum instanceof ByteBuf && out instanceof ByteBufEncoder) {
            ((ByteBufEncoder) out).writeBytes((ByteBuf) datum);
        } else {
            throw new AvroTypeException("can't write bytes! type: " + datum.getClass().getCanonicalName());
        }
    }
}
