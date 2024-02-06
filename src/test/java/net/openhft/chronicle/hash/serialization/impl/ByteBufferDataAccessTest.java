/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.hash.Data;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ByteBufferDataAccessTest {

    @Test
    public void getUsingTest() {
        ByteBufferDataAccess bbDataAccess = new ByteBufferDataAccess();
        ByteBuffer bb1 = ByteBuffer.allocate(10);
        for (int i = 0; i < 10; i++) {
            bb1.put((byte) i);
        }
        bb1.position(3).limit(5);
        Data<ByteBuffer> data1 = bbDataAccess.getData(bb1);
        ByteBuffer bb2 = ByteBuffer.allocate(2);
        data1.getUsing(bb2);
        assertEquals(bb2.get(0), 3);
        assertEquals(bb2.get(1), 4);
    }

    @Test
    public void shouldKeepOriginalOrder() {
        ByteBufferDataAccess da = new ByteBufferDataAccess();
        ByteBuffer bb = ByteBuffer.allocateDirect(Long.BYTES);
        ByteOrder originalOrder = bb.order();

        bb.putLong(1L);
        Data<ByteBuffer> data = da.getData(bb);

        assertEquals(originalOrder, data.get().order());
        assertEquals(1L, data.get().getLong(0));
    }
}
