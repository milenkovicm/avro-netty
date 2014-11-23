package com.github.milenkovicm.avro.io;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.io.BinaryEncoder;

/**
 * {@code ByteBuf} {@code Encoder} implementation
 */
public class ByteBufEncoder extends BinaryEncoder {

    private static int BUFF_SIZE = 1024;
    private final ByteBuf buffer;

    public ByteBufEncoder() {
        this.buffer = ByteBufAllocator.DEFAULT.buffer(BUFF_SIZE);
    }

    public ByteBufEncoder(final ByteBuf buffer) {
        this.buffer = buffer;
    }

    @Override
    public void flush() throws IOException {
    }


    @Override
    protected void writeZero() throws IOException {
        this.buffer.writeByte(0);
    }

    @Override
    public int bytesBuffered() {
        // not buffered
        return 0;
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

        ByteBufEncoderHelper.encodeWriteInt(n, this.buffer);
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
        ByteBufEncoderHelper.encodeWriteLong(n, this.buffer);

    }

    @Override
    public void writeFloat(final float f) throws IOException {
        ByteBufEncoderHelper.encodeWriteFloat(f, this.buffer);
    }

    @Override
    public void writeDouble(final double d) throws IOException {
        ByteBufEncoderHelper.encodeWriteDouble(d, this.buffer);
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

    public byte[] getBytes() {
        if (this.buffer.isDirect()) {
            final byte[] blob = new byte[this.buffer.readableBytes()];
            this.buffer.getBytes(0, blob);
            return blob;
        } else {
            return this.buffer.array();
        }
    }

    /** Writes a fixed from a ByteBuffer. */
    @Override
    public void writeFixed(final ByteBuffer bytes) throws IOException {
        this.buffer.writeBytes(bytes);
    }

    public ByteBuf unwrap() {
        return this.buffer;
    }

    public void release() {
        this.buffer.release();
    }
}
