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

import java.util.HashMap;

import static org.junit.Assert.assertTrue;

public class ChronicleMapEqualsTest {

    @Test
    public void test() {
        ChronicleMap<String, String> map = ChronicleMap
                .of(String.class, String.class)
                .averageKey("a").averageValue("b")
                .entries(100)
                .create();

        HashMap<String, String> refMap = new HashMap<>();
        refMap.put("a", "b");
        map.putAll(refMap);
        assertTrue(map.equals(refMap));
    }
}
