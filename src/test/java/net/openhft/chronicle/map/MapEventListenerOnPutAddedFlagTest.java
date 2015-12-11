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

import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

public class MapEventListenerOnPutAddedFlagTest {

    class ExpectedAddFlagChecker<K, V> extends MapEventListener<K, V> {
        boolean expected;

        public void setExpected(boolean expected) {
            this.expected = expected;
        }

        @Override
        public void onPut(Object key, Object newValue, @Nullable Object replacedValue,
                          boolean replicationEvent, boolean added, boolean hasValueChanged,
                          byte identifier, byte replacedIdentifier, long timeStamp,
                          long replacedTimeStamp) {
            Assert.assertEquals(expected, added);
        }
    }

    @Test
    public void testAddedFlagVanilla() {
        ExpectedAddFlagChecker<Integer, CharSequence> checker = new ExpectedAddFlagChecker<>();
        test(ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .eventListener(checker)
                .entries(1).create(), checker);
    }

    @Test
    public void testAddedFlagReplicated() {
        ExpectedAddFlagChecker<Integer, CharSequence> checker = new ExpectedAddFlagChecker<>();
        test(ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .eventListener(checker)
                .entries(1).replication((byte) 1).create(), checker);
    }

    private static void test(ChronicleMap<Integer, CharSequence> map,
                             ExpectedAddFlagChecker checker) {
        checker.setExpected(true);
        map.acquireUsing(1, new StringBuilder());

        checker.setExpected(false);
        map.put(1, "2");

        checker.setExpected(false);
        map.update(1, "2");
        map.update(1, "3");

        map.remove(1);
        checker.setExpected(true);
        map.update(1, "3");

        map.remove(1);
        map.update(1, "4");

        map.remove(1);
        map.put(1, "5");
    }
}
