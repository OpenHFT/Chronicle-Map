/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.hash.Data;
import org.junit.jupiter.api.Test;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.*;

class ByteBufferDataAccessTest {

    @Test
    void getUsingTest() {
        ByteBufferDataAccess bbDataAccess = new ByteBufferDataAccess();
        ByteBuffer bb1 = ByteBuffer.allocate(10);
        for (int i = 0; i < 10; i++) {
            bb1.put((byte) i);
        }
        Buffer bb1View = bb1;
        bb1View.position(3);
        bb1View.limit(5);
        Data<ByteBuffer> data1 = bbDataAccess.getData(bb1);
        ByteBuffer bb2 = ByteBuffer.allocate(2);
        data1.getUsing(bb2);
        assertEquals(bb2.get(0), 3);
        assertEquals(bb2.get(1), 4);
    }

    @Test
    void shouldKeepOriginalOrder() {
        ByteBufferDataAccess da = new ByteBufferDataAccess();
        ByteBuffer bb = ByteBuffer.allocateDirect(Long.BYTES);
        ByteOrder originalOrder = bb.order();

        bb.putLong(1L);
        Data<ByteBuffer> data = da.getData(bb);

        assertSame(originalOrder, data.get().order());
        assertEquals(1L, data.get().getLong(0));
    }
}
