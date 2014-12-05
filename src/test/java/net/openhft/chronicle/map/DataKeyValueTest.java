/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.values.IntValue;
import net.openhft.lang.values.LongValue;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DataKeyValueTest {

    @Test
    public void dataKeyValueTest() {
        ChronicleMap<IntValue, LongValue> map =
                OnHeapUpdatableChronicleMapBuilder.of(IntValue.class, LongValue.class)
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
