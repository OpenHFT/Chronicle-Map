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

import net.openhft.chronicle.set.Builder;
import org.junit.Test;

import java.io.IOException;

public class SimplePersistedMapOverflowTest {

    @Test
    public void simplePersistedMapOverflowTest() throws IOException {
        try (ChronicleMap<Integer, Integer> map = ChronicleMap
                .of(Integer.class, Integer.class)
                .entries(1_000)
                .createPersistedTo(Builder.getPersistenceFile())) {
            for (int i = 0; i < 2_000; i++) {
                map.put(i, i);
            }
        }
    }
}
