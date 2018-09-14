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

package net.openhft.chronicle.map;

import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by peter.lawrey on 28/02/14.
 */
public class KeySizesTest {
    @Test
    public void testDifferentKeySizes() throws IOException {

        Map<String, String> map = ChronicleMap.of(String.class, String.class)
                .entries(100).averageKeySize(100).averageValueSize(100).create();

        String k = "";
        for (int i = 0; i < 100; i++) {
            map.put(k, k);
            String k2 = map.get(k);
            assertEquals(k, k2);
            k += "a";
        }
        k = "";
        for (int i = 0; i < 100; i++) {
            String k2 = map.get(k);
            assertEquals(k, k2);
            k += "a";
        }

        ((Closeable) map).close();
    }
}
