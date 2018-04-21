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

package eg;

import net.openhft.chronicle.core.values.ByteValue;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.values.Array;
import net.openhft.chronicle.values.Values;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class OffHeapByteArrayExampleTest {

    public static final char EXPECTED = 'b';
    private static ChronicleMap<LongValue, ByteArray> chm;

    @BeforeClass
    public static void beforeClass() {
        chm = ChronicleMapBuilder
                .of(LongValue.class, ByteArray.class)
                .entries(1000)
                .create();
    }

    @AfterClass
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

        Assert.assertEquals(0, value.getByteValueAt(2).getValue());
        Assert.assertEquals(0, value.getByteValueAt(3).getValue());

        byte actual = value.getByteValueAt(1).getValue();
        Assert.assertEquals(EXPECTED, actual);

    }

    interface ByteArray {
        @Array(length = 7)
        void setByteValueAt(int index, ByteValue value);

        ByteValue getByteValueAt(int index);
    }
}
