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

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UpdateTest {

    @Test
    public void testUpdateVanilla() {
        test(ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .entries(1).create());
    }

    @Test
    public void testUpdateReplicated() {
        test(ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .entries(1).replication((byte) 1).create());
    }

    public static void test(ChronicleMap<Integer, CharSequence> map) {
        assertEquals(UpdateResult.INSERT, map.update(1, "1"));

        assertEquals(UpdateResult.UPDATE, map.update(1, "2"));
        assertEquals(UpdateResult.UNCHANGED, map.update(1, "2"));

        Assert.assertNotNull(map.remove(1));

        assertEquals(UpdateResult.INSERT, map.update(1, "2"));
    }
}
