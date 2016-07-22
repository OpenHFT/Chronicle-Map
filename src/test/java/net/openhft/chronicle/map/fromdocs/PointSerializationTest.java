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

package net.openhft.chronicle.map.fromdocs;

import net.openhft.chronicle.map.ChronicleMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static net.openhft.chronicle.map.fromdocs.Point.of;

public class PointSerializationTest {

    @Test
    public void pointSerializationTest() {
        try (ChronicleMap<String, List<Point>> objects = ChronicleMap
                .of(String.class, (Class<List<Point>>) (Class) List.class)
                .averageKey("range")
                .valueMarshaller(PointListSizedMarshaller.INSTANCE)
                .averageValue(asList(of(0, 0), of(1, 1)))
                .entries(10)
                .create()) {
            objects.put("range", asList(of(0, 0), of(1, 1)));
            objects.put("square", asList(of(0, 0), of(0, 100), of(100, 100), of(100, 0)));

            Assert.assertEquals(2, objects.get("range").size());
            Assert.assertEquals(4, objects.get("square").size());
        }
    }
}
