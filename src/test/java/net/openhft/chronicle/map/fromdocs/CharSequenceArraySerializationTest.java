/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.fromdocs;

import net.openhft.chronicle.map.ChronicleMap;
import org.junit.Assert;
import org.junit.Test;

public class CharSequenceArraySerializationTest {

    @Test
    public void charSequenceArraySerializationTest() {
        try (ChronicleMap<String, CharSequence[]> map = ChronicleMap
                .of(String.class, CharSequence[].class)
                .averageKey("fruits")
                .valueMarshaller(CharSequenceArrayBytesMarshaller.INSTANCE)
                .averageValue(new CharSequence[]{"banana", "pineapple"})
                .entries(2)
                .create()) {
            map.put("fruits", new CharSequence[]{"banana", "pineapple"});
            map.put("vegetables", new CharSequence[]{"carrot", "potato"});
            Assert.assertEquals(2, map.get("fruits").length);
            Assert.assertEquals(2, map.get("vegetables").length);
        }
    }
}
