package com.github.milenkovicm.avro.io;

import io.netty.buffer.ByteBuf;

final class ByteBufEncoderHelper {

    private ByteBufEncoderHelper() {
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
        buffer.writeByte((byte) ((bits) & 0xFF));
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

        buffer.setByte(writerIndex, (byte) ((first) & 0xFF));
        buffer.setByte(writerIndex + 4, (byte) ((second) & 0xFF));
        buffer.setByte(writerIndex + 5, (byte) ((second >>> 8) & 0xFF));
        buffer.setByte(writerIndex + 1, (byte) ((first >>> 8) & 0xFF));
        buffer.setByte(writerIndex + 2, (byte) ((first >>> 16) & 0xFF));
        buffer.setByte(writerIndex + 6, (byte) ((second >>> 16) & 0xFF));
        buffer.setByte(writerIndex + 7, (byte) ((second >>> 24) & 0xFF));
        buffer.setByte(writerIndex + 3, (byte) ((first >>> 24) & 0xFF));
        buffer.writerIndex(writerIndex + 8);
    }

}
