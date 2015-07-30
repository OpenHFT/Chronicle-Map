/*
 *      Copyright (C) 2015  higherfrequencytrading.com
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

package net.openhft.chronicle.map;

import org.junit.Assert;
import org.junit.Test;

public class DeletedSearchStateTest {

    @Test
    public void deletedSearchStateTest() {
        ChronicleMap<Integer, Integer> map =
                ChronicleMapBuilder.of(Integer.class, Integer.class).entries(100).create();

        try (ExternalMapQueryContext<Integer, Integer, ?> q = map.queryContext(42)) {
            q.updateLock().lock();
            q.insert(q.absentEntry(), q.wrapValueAsData(1));
            q.remove(q.entry());
            q.insert(q.absentEntry(), q.wrapValueAsData(2));
        }

        Assert.assertEquals((Integer) 2, map.get(42));
    }
}
