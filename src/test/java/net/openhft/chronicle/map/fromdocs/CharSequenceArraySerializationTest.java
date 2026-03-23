/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.fromdocs;

import net.openhft.chronicle.map.ChronicleMap;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

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
            assertEquals(2, map.get("fruits").length);
            assertEquals(2, map.get("vegetables").length);
        }
    }
}
