/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.fromdocs;

import net.openhft.chronicle.map.ChronicleMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static net.openhft.chronicle.map.fromdocs.Point.of;

@SuppressWarnings({"rawtypes", "unchecked"})
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
