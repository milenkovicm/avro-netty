package com.github.milenkovicm.avro;

import java.util.Arrays;
import java.util.Collection;

public class DataHelper {
    public static Collection<?> integers() {
        return Arrays.asList(new Object[][] { { 2 }, { 6 }, { 19 }, { 22 }, { 23 }, { 236 }, { 2395 }, { 2346793 }, { 123546879 },
                { Integer.MAX_VALUE } });
    }

    public static Collection<?> longs() {
        return Arrays.asList(new Object[][] { { 2 }, { 6 }, { 19 }, { 22 }, { 23 }, { 236 }, { 2395 }, { 2346793 }, { 123546879 },
                { Integer.MAX_VALUE }, { Long.MAX_VALUE } });
    }

    public static Collection<?> floats() {
        return Arrays.asList(new Object[][] { { (float) 2.0 }, { (float) 6 }, { (float) 19 }, { (float) 22 }, { (float) 23 }, { (float) 236 },
                { (float) 2395 }, { (float) 2346793 }, { 123546879 }, { (float) Integer.MAX_VALUE }, { (float) Float.MAX_VALUE },
                { (float) 24342.234 }, { (float) 12.123 }, { (float) 743.324 } });
    }

    public static Collection<?> doubles() {
        return Arrays.asList(new Object[][] { { (double) 2.0 }, { (double) 6 }, { (double) 19 }, { (double) 22 }, { (double) 23 }, { (double) 236 },
                { (double) 2395 }, { (double) 2346793 }, { 123546879 }, { (double) Integer.MAX_VALUE }, { (double) Float.MAX_VALUE },
                { (double) Double.MAX_VALUE },
                { (double) 24342.234 }, { (double) 12.123 }, { (double) 743.324 } });
    }

    public static Collection<?> strings() {
        return Arrays.asList(new Object[][] { { "Test 1" }, { "Test fdsaqwe qwe" }, { "Test циспсашпсад" }, { "асдфлкјасдфл лкдфсј алкдјф " } });
    }

    public static Collection<?> bytes() {
        return Arrays.asList(new Object[][] { { new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 } }, { new byte[] {} },
                { new byte[] { 1, 2, 3, 4, 5 } } });
    }
}
