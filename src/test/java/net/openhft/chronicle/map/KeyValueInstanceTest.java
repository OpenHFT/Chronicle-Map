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

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * @author Rob Austin.
 */
public class KeyValueInstanceTest {

    @Test(expected = IllegalStateException.class)
    public void testLongNewKeyValueInstance() {
        try (ChronicleMap map = ChronicleMapBuilder
                .of(Long.class, Long.class)
                .create()) {

            map.newKeyInstance();
            map.newValueInstance();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testNewKeyValueInstanceWithMapType() {
        try (ChronicleMap map = ChronicleMapBuilder
                .of(Map.class, Map.class)
                .create()) {

            map.newKeyInstance();
            map.newValueInstance();
        }
    }

    @Test
    public void testNewKeyValueInstanceWithHahMapType() {
        try (ChronicleMap map = ChronicleMapBuilder
                .of(HashMap.class, HashMap.class)
                .create()) {

            Object key = map.newKeyInstance();
            Object value = map.newValueInstance();

        }
    }

    @Test
    public void testNewKeyValueInstanceWithListType() {
        try (ChronicleMap map = ChronicleMapBuilder
                .of(HashMap.class, IBean.class)
                .create()) {

            map.newKeyInstance();
            Object value = map.newValueInstance();

            assertTrue(value.getClass().getCanonicalName().endsWith("$$Native"));

        }
    }

    @Test(expected = IllegalStateException.class)
    public void testNewKeyValueInstanceWithByteArray() {
        try (ChronicleMap map = ChronicleMapBuilder
                .of(byte[].class, byte[].class)
                .create()) {

            map.newKeyInstance();
            map.newValueInstance();
        }
    }

    interface IBean {
        long getLong();

        void setLong(long num);

        double getDouble();

        void setDouble(double d);

        int getInt();

        void setInt(int i);
    }
}
