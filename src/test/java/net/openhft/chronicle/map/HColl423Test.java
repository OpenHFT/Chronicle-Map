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

public class HColl423Test {

    @Test
    public void hColl423Test() {
        try {
            ChronicleMap<String, Integer> map = ChronicleMap
                    .of(String.class, Integer.class)
                    .averageKeySize(128)
                    .averageValueSize(100)
                    .entries(2_000_000L)
                    .maxBloatFactor(1_000.0)
                    .create();
            throw new AssertionError("should throw IllegalStateException");
        } catch (IllegalStateException expected) {
            // do nothing
        }
    }
}
