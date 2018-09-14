/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
