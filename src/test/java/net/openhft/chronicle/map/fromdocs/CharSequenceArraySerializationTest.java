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
