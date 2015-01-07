package com.github.milenkovicm.avro.generic;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.ByteBufResolvingDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.ResolvingDecoder;

/**
 * {code DatumReader} for generic Java objects.
 *
 * @param <D>
 *        datum type
 */
public class NettyGenericDatumReader<D> extends GenericDatumReader<D> {
    /**
     * Default constructor.
     *
     * @param root
     *        schema reader
     */
    public NettyGenericDatumReader(final Schema root) {
        this.setSchema(root);
    }

    /**
     * Constructor given writer's and reader's schema.
     *
     * @param writer
     *        schema writer
     * @param reader
     *        schema reader
     */
    public NettyGenericDatumReader(final Schema writer, final Schema reader) {
        super(writer, reader, GenericData.get());
    }

    @Override
    @SuppressWarnings("unchecked")
    public D read(final D reuse, final Decoder in) throws IOException {
        final ResolvingDecoder resolver = new ByteBufResolvingDecoder(Schema.applyAliases(this.getSchema(), this.getExpected()), this.getExpected(),
                in);

        final D result = (D) this.read(reuse, this.getExpected(), resolver);
        resolver.drain();
        return result;
    }

    @Override
    protected Object readBytes(final Object old, final Decoder in) throws IOException {
        if (in instanceof ByteBufResolvingDecoder) {
            return ((ByteBufResolvingDecoder) in).readBytes(old instanceof ByteBuf ? (ByteBuf) old : null);
        } else {
            return in.readBytes(old instanceof ByteBuffer ? (ByteBuffer) old : null);
        }
    }

}
