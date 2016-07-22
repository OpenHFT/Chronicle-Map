/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.hash.Data;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

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
        Assert.assertEquals(bb2.get(0), 3);
        Assert.assertEquals(bb2.get(1), 4);
    }
}
