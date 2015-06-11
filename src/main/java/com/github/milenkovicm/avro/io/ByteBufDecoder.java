package com.github.milenkovicm.avro.io;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

/**
 * {@code ByteBufDecoder} is an implementation of {@code Decoder} backed by {@code ByteBuf}.
 *
 * @see {@code BinaryDecoder}
 */
public class ByteBufDecoder extends Decoder {

    private ByteBuf buffer;

    @Override
    public void readNull() throws IOException {
        // noop
    }

    @Override
    public boolean readBoolean() throws IOException {
        return this.buffer.readBoolean();
    }

    @Override
    public int readInt() throws IOException {
        int n = 0;
        int b;
        int shift = 0;
        do {
            b = this.buffer.readUnsignedByte();
            if (b >= 0) {
                n |= (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    return (n >>> 1) ^ -(n & 1); // back to two's-complement
                }
            } else {
                throw new EOFException();
            }
            shift += 7;
        } while (shift < 32);

        throw new IOException("Invalid int encoding");
    }

    @Override
    public long readLong() throws IOException {
        long n = 0;
        int b;
        int shift = 0;
        do {
            b = this.buffer.readUnsignedByte();
            if (b >= 0) {
                n |= (b & 0x7FL) << shift;
                if ((b & 0x80) == 0) {
                    return (n >>> 1) ^ -(n & 1); // back to two's-complement
                }
            } else {
                throw new EOFException();
            }
            shift += 7;
        } while (shift < 64);
        throw new IOException("Invalid long encoding");
    }

    @Override
    public float readFloat() throws IOException {
        final int n = (this.buffer.readUnsignedByte() & 0xff) | ((this.buffer.readUnsignedByte() & 0xff) << 8)
                | ((this.buffer.readUnsignedByte() & 0xff) << 16) | ((this.buffer.readUnsignedByte() & 0xff) << 24);
        return Float.intBitsToFloat(n);
    }

    @Override
    public double readDouble() throws IOException {
        final long n = (((long) this.buffer.readUnsignedByte()) & 0xff) | ((((long) this.buffer.readUnsignedByte()) & 0xff) << 8)
                | ((((long) this.buffer.readUnsignedByte()) & 0xff) << 16) | ((((long) this.buffer.readUnsignedByte()) & 0xff) << 24)
                | ((((long) this.buffer.readUnsignedByte()) & 0xff) << 32) | ((((long) this.buffer.readUnsignedByte()) & 0xff) << 40)
                | ((((long) this.buffer.readUnsignedByte()) & 0xff) << 48) | ((((long) this.buffer.readUnsignedByte()) & 0xff) << 56);
        return Double.longBitsToDouble(n);
    }

    @Override
    public Utf8 readString(final Utf8 old) throws IOException {
        final int length = this.readInt();
        final Utf8 result = old != null ? old : new Utf8();
        result.setByteLength(length);
        if (0 != length) {
            this.buffer.readBytes(result.getBytes());
        }
        return result;
    }

    @Override
    public String readString() throws IOException {
        return this.readString(null).toString();
    }

    @Override
    public void skipString() throws IOException {
        this.doSkipBytes(this.readInt());
    }

    @Override
    public ByteBuffer readBytes(final ByteBuffer old) throws IOException {
        final int length = this.readInt();
        ByteBuffer result;

        if (old != null && length <= old.capacity()) {
            result = old;
            result.clear();
        } else {
            // not sure how to deal with this!
            result = ByteBuffer.allocate(length);
        }

        if (length > 0) {
            this.buffer.readBytes(result);
        }
        result.limit(length);
        result.flip();

        return result;
    }

    /**
     * @see {link {@link #readBytes(ByteBuffer)}
     * @param old
     *        {@link ByteBuf} to be reused
     * @return {@link ByteBuf}
     * @throws IOException
     *         in case of error
     */
    public ByteBuf readBytes(final ByteBuf old) throws IOException {
        final int length = this.readInt();

        ByteBuf result;
        if (old != null) {
            result = old;
            result.clear();
            if (result.capacity() != length) {
                result.capacity(length);
            }
        } else {
            result = ByteBufAllocator.DEFAULT.buffer(length);
        }

        if (length > 0) {
            this.buffer.readBytes(result);
        }

        return result;
    }

    @Override
    public void skipBytes() throws IOException {
        this.doSkipBytes(this.readInt());
    }

    @Override
    public void readFixed(final byte[] bytes, final int start, final int length) throws IOException {
        this.buffer.readBytes(bytes, start, length);
    }

    @Override
    public void skipFixed(final int length) throws IOException {
        this.doSkipBytes(length);
    }

    @Override
    public int readEnum() throws IOException {
        return this.readInt();
    }

    @Override
    public long readArrayStart() throws IOException {
        return this.doReadItemCount();
    }

    @Override
    public long arrayNext() throws IOException {
        return this.doReadItemCount();
    }

    @Override
    public long skipArray() throws IOException {
        return this.doSkipItems();
    }

    @Override
    public long readMapStart() throws IOException {
        return this.doReadItemCount();
    }

    @Override
    public long mapNext() throws IOException {
        return this.doReadItemCount();
    }

    @Override
    public long skipMap() throws IOException {
        return this.doSkipItems();
    }

    @Override
    public int readIndex() throws IOException {
        return this.readInt();
    }

    private void doSkipBytes(final int length)  {
        final int readerIndex = this.buffer.readerIndex();
        this.buffer.readerIndex(readerIndex + length);
    }

    private long doReadItemCount() throws IOException {
        long result = this.readLong();
        if (result < 0) {
            this.readLong(); // Consume byte-count if present
            result = -result;
        }
        return result;
    }

    private long doSkipItems() throws IOException {
        int result = this.readInt();
        while (result < 0) {
            final long bytecount = this.readLong();
            this.doSkipBytes((int) bytecount);
            result = this.readInt();
        }
        return result;
    }


    public ByteBuf getBuffer() {
        return this.buffer;
    }

    public void setBuffer(final ByteBuf buffer) {
        this.buffer = buffer;
    }


}
