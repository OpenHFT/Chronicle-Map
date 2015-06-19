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

package net.openhft.chronicle.map;

import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.values.IntValue;
import net.openhft.lang.values.LongValue;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DataKeyValueTest {

    @Test
    public void dataKeyValueTest() {
        ChronicleMap<IntValue, LongValue> map =
                ChronicleMapBuilder.of(IntValue.class, LongValue.class)
                        .entries(1000).create();
        IntValue heapKey = DataValueClasses.newInstance(IntValue.class);
        IntValue directKey = DataValueClasses.newDirectInstance(IntValue.class);
        LongValue heapValue = DataValueClasses.newInstance(LongValue.class);
        LongValue directValue = DataValueClasses.newDirectInstance(LongValue.class);

        heapKey.setValue(1);
        heapValue.setValue(1);
        map.put(heapKey, heapValue);
        assertEquals(1, map.get(heapKey).getValue());
        assertEquals(1, map.getUsing(heapKey, directValue).getValue());

        ((Byteable) directValue).bytes(null, 0L);
        directKey.setValue(1);
        map.getUsing(directKey, directValue).addValue(1);
        assertEquals(2, map.getUsing(directKey, heapValue).getValue());

        directKey.setValue(2);
        heapValue.setValue(3);
        map.put(directKey, heapValue);
        assertEquals(3, map.get(directKey).getValue());
    }
}
