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

import org.junit.Assert;
import org.junit.Test;

public class Issue60Test {

    @Test
    public void issue60Test() {
        int entries = 200;
        int maxBloatFactor = 10;
        int loop = 1000;

        try (ChronicleMap<String, String> map = ChronicleMap
                .of(String.class, String.class)
                .entries(entries)
                .averageKeySize(12)
                .averageValueSize(12)
                .maxBloatFactor(maxBloatFactor)
                .create()) {

            //System.out.println("begin test " + map.size());
            for (int i = 0; i < loop; i++) {
                map.put("key" + i, "value" + i);
            }
            //System.out.println("map size " + map.size());

            int failedGet = 0;
            for (int i = 0; i < loop; i++) {
                String value = map.get("key" + i);
                if (value == null) {
                    failedGet++;
                }
            }
            Assert.assertEquals(0, failedGet);
            //System.out.println("failedGet " + failedGet);
            //System.out.println("map " + map);
        }
    }
}
