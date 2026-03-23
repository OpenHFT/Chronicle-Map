/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package eg;

import net.openhft.chronicle.core.values.ByteValue;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.values.Array;
import net.openhft.chronicle.values.Values;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class OffHeapByteArrayExampleTest {

    public static final char EXPECTED = 'b';
    private static ChronicleMap<LongValue, ByteArray> chm;

    @BeforeAll
    public static void beforeClass() {
        chm = ChronicleMapBuilder
                .of(LongValue.class, ByteArray.class)
                .entries(1000)
                .create();
    }

    @AfterAll
    public static void afterClass() {
        if (chm != null)
            chm.close();
    }

    @Test
    public void test() {

        // this objects will be reused
        ByteValue byteValue = Values.newHeapInstance(ByteValue.class);
        ByteArray value = Values.newHeapInstance(ByteArray.class);
        LongValue key = Values.newHeapInstance(LongValue.class);

        key.setValue(1);

        // this is kind of like byteValue[1] = 'b'
        byteValue.setValue((byte) EXPECTED);
        value.setByteValueAt(1, byteValue);

        chm.put(key, value);

        // clear the value to prove it works
        byteValue.setValue((byte) 0);
        value.setByteValueAt(1, byteValue);

        chm.getUsing(key, value);

        assertEquals(0, value.getByteValueAt(2).getValue());
        assertEquals(0, value.getByteValueAt(3).getValue());

        byte actual = value.getByteValueAt(1).getValue();
        assertEquals(EXPECTED, actual);

    }

    interface ByteArray {
        @Array(length = 7)
        void setByteValueAt(int index, ByteValue value);

        ByteValue getByteValueAt(int index);
    }
}
