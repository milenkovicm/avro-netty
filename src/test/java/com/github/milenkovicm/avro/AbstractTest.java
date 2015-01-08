package com.github.milenkovicm.avro;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.util.ArrayDeque;
import java.util.Queue;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Abstract test.
 *
 */
public class AbstractTest {

    public static final ByteBufAllocator DEFAULT_ALLOCATOR = UnpooledByteBufAllocator.DEFAULT;
    private static final Queue<ByteBuf> freeLaterQueue = new ArrayDeque<>();

    /**
     * Runs before test class.
     */
    @BeforeClass
    public static void beforeClass() {

    }

    /**
     * Runs after test class.
     */
    @AfterClass
    public static void afterClass() {

    }

    /**
     * Creates and references buffer to be dereferenced when test finishes.
     *
     * @param initialCapacity
     *            buffer capacity
     * @return allocated byte buffer
     */
    public static ByteBuf freeLaterBuffer(final int initialCapacity) {
        return freeLaterBuffer(DEFAULT_ALLOCATOR, initialCapacity);
    }

    /**
     * Creates and references buffer to be dereferenced when test finishes.
     *
     * @param buffer
     *            to copy
     * @return allocated byte buffer
     */
    public static ByteBuf freeLaterBuffer(final byte[] buffer) {
        return freeLaterBuffer(DEFAULT_ALLOCATOR, buffer.length).writeBytes(buffer);
    }

    /**
     * Creates and references buffer to be dereferenced when test finishes.
     *
     * @param allocator
     *            allocator to use
     * @param initialCapacity
     *            buffer capacity
     * @return allocated buffer
     */
    public static ByteBuf freeLaterBuffer(final ByteBufAllocator allocator, final int initialCapacity) {
        final ByteBuf buffer = allocator.buffer(initialCapacity);
        freeLaterQueue.add(buffer);
        return buffer;
    }

    /**
     * Creates and references buffer to be dereferenced when test finishes.
     *
     * @param buffer
     *            buffer to copy
     * @return allocated buffer
     */
    public static ByteBuf freeLaterBuffer(final ByteBuf buffer) {
        freeLaterQueue.add(buffer);
        return buffer;
    }

    /**
     * Executed after each test.
     */
    @After
    public void after() {
        for (;;) {
            final ByteBuf buf = freeLaterQueue.poll();
            if (buf == null) {
                break;
            }
            if (buf.refCnt() > 0) {
                buf.release(buf.refCnt());
            }
        }
    }

}
