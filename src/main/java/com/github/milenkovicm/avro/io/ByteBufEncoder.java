package com.github.milenkovicm.avro.io;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.io.BinaryEncoder;

/**
 * {@code ByteBufEncoder} is an {@code Encoder} implementation backed by {@link ByteBuf}.
 *
 * @see {@link BinaryEncoder}
 */
public class ByteBufEncoder extends BinaryEncoder {

    private ByteBuf buffer;

    @Override
    public void flush() throws IOException {
    }

    @Override
    protected void writeZero() throws IOException {
        this.buffer.writeByte(0);
    }

    @Override
    public int bytesBuffered() {
        return 0; // not buffered
    }

    @Override
    public void writeBoolean(final boolean b) throws IOException {
        this.buffer.writeBoolean(b);
    }

    @Override
    public void writeInt(final int n) throws IOException {
        final int val = (n << 1) ^ (n >> 31);
        if ((val & ~0x7F) == 0) {
            this.buffer.writeByte(val);
            return;
        } else if ((val & ~0x3FFF) == 0) {
            this.buffer.writeByte(0x80 | val);
            this.buffer.writeByte(val >>> 7);
            return;
        }

        encodeWriteInt(n, this.buffer);
    }

    @Override
    public void writeLong(final long n) throws IOException {
        final long val = (n << 1) ^ (n >> 63); // move sign to low-order bit
        if ((val & ~0x7FFFFFFFL) == 0) {
            int i = (int) val;
            while ((i & ~0x7F) != 0) {
                this.buffer.writeByte((byte) ((0x80 | i) & 0xFF));
                i >>>= 7;
            }
            this.buffer.writeByte((byte) i);
            return;
        }
        encodeWriteLong(n, this.buffer);
    }

    @Override
    public void writeFloat(final float f) throws IOException {
        encodeWriteFloat(f, this.buffer);
    }

    @Override
    public void writeDouble(final double d) throws IOException {
        encodeWriteDouble(d, this.buffer);
    }

    @Override
    public void writeFixed(final byte[] bytes, final int start, final int len) throws IOException {
        this.buffer.writeBytes(bytes, start, len);
    }

    @Override
    public void writeBytes(final ByteBuffer bytes) throws IOException {
        final int len = bytes.limit() - bytes.position();
        if (0 == len) {
            this.writeZero();
        } else {
            this.writeInt(len);
            this.writeFixed(bytes);
        }
    }

    @Override
    public void writeFixed(final ByteBuffer bytes) throws IOException {
        this.buffer.writeBytes(bytes);
    }

    public ByteBuf getBuffer() {
        return this.buffer;
    }

    public void setBuffer(final ByteBuf buffer) {
        this.buffer = buffer;
    }

    /**
     *
     * @see {@code BinaryData#encodeInt(int, byte[], int)
     *
     * @param n integer value to encode
     * @param buffer destination
     */
    static void encodeWriteInt(int n, final ByteBuf buffer) {
        // move sign to low-order bit, and flip others if negative
        n = (n << 1) ^ (n >> 31);
        if ((n & ~0x7F) != 0) {
            buffer.writeByte((byte) ((n | 0x80) & 0xFF));
            n >>>= 7;
            if (n > 0x7F) {
                buffer.writeByte((byte) ((n | 0x80) & 0xFF));
                n >>>= 7;
                if (n > 0x7F) {
                    buffer.writeByte((byte) ((n | 0x80) & 0xFF));
                    n >>>= 7;
                    if (n > 0x7F) {
                        buffer.writeByte((byte) ((n | 0x80) & 0xFF));
                        n >>>= 7;
                    }
                }
            }
        }
        buffer.writeByte((byte) n);
    }

    /**
     * @see {@code BinaryData#encodeLong(long, byte[], int)
     * @param n long value to encode
     * @param buffer destination
     */
    static void encodeWriteLong(long n, final ByteBuf buffer) {
        // move sign to low-order bit, and flip others if negative
        n = (n << 1) ^ (n >> 63);

        if ((n & ~0x7FL) != 0) {
            buffer.writeByte((byte) ((n | 0x80) & 0xFF));
            n >>>= 7;
            if (n > 0x7F) {
                buffer.writeByte((byte) ((n | 0x80) & 0xFF));
                n >>>= 7;
                if (n > 0x7F) {
                    buffer.writeByte((byte) ((n | 0x80) & 0xFF));
                    n >>>= 7;
                    if (n > 0x7F) {
                        buffer.writeByte((byte) ((n | 0x80) & 0xFF));
                        n >>>= 7;
                        if (n > 0x7F) {
                            buffer.writeByte((byte) ((n | 0x80) & 0xFF));
                            n >>>= 7;
                            if (n > 0x7F) {
                                buffer.writeByte((byte) ((n | 0x80) & 0xFF));
                                n >>>= 7;
                                if (n > 0x7F) {
                                    buffer.writeByte((byte) ((n | 0x80) & 0xFF));
                                    n >>>= 7;
                                    if (n > 0x7F) {
                                        buffer.writeByte((byte) ((n | 0x80) & 0xFF));
                                        n >>>= 7;
                                        if (n > 0x7F) {
                                            buffer.writeByte((byte) ((n | 0x80) & 0xFF));
                                            n >>>= 7;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        buffer.writeByte((byte) n);
    }

    /**
     * @see {@code BinaryData#encodeFloat(float, byte[], int)
     *
     * @param f float value to encode
     * @param buffer destination
     */
    static void encodeWriteFloat(final float f, final ByteBuf buffer) {
        final int bits = Float.floatToRawIntBits(f);
        // hotspot compiler works well with this variant
        buffer.writeByte((byte) (bits & 0xFF));
        buffer.writeByte((byte) ((bits >>> 8) & 0xFF));
        buffer.writeByte((byte) ((bits >>> 16) & 0xFF));
        buffer.writeByte((byte) ((bits >>> 24) & 0xFF));
    }

    /**
     * @see {@code BinaryData#encodeDouble(double, byte[], int)}
     * @param d
     *        double value to encode
     * @param buffer
     *        destination
     */
    static void encodeWriteDouble(final double d, final ByteBuf buffer) {
        final long bits = Double.doubleToRawLongBits(d);

        final int first = (int) (bits & 0xFFFFFFFF);
        final int second = (int) ((bits >>> 32) & 0xFFFFFFFF);
        final int writerIndex = buffer.writerIndex();
        // the compiler seems to execute this order the best, likely due to
        // register allocation -- the lifetime of constants is minimized.

        buffer.setByte(writerIndex, (byte) (first & 0xFF));
        buffer.setByte(writerIndex + 4, (byte) (second & 0xFF));
        buffer.setByte(writerIndex + 5, (byte) ((second >>> 8) & 0xFF));
        buffer.setByte(writerIndex + 1, (byte) ((first >>> 8) & 0xFF));
        buffer.setByte(writerIndex + 2, (byte) ((first >>> 16) & 0xFF));
        buffer.setByte(writerIndex + 6, (byte) ((second >>> 16) & 0xFF));
        buffer.setByte(writerIndex + 7, (byte) ((second >>> 24) & 0xFF));
        buffer.setByte(writerIndex + 3, (byte) ((first >>> 24) & 0xFF));
        buffer.writerIndex(writerIndex + 8);
    }
}
