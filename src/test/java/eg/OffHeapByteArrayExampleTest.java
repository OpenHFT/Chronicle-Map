/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package eg;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.model.constraints.MaxSize;
import net.openhft.lang.values.ByteValue;
import net.openhft.lang.values.LongValue;
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
        ByteValue byteValue = DataValueClasses.newDirectInstance(ByteValue.class);
        ByteArray value = chm.newValueInstance();
        LongValue key = chm.newKeyInstance();

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
        void setByteValueAt(@MaxSize(7) int index, ByteValue value);

        ByteValue getByteValueAt(int index);
    }
}
