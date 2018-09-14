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

import java.io.File;
import java.io.IOException;

public class NegativeIntegerKeyTest {

    @Test
    public void testNegativeIntegerKey() throws IOException {
        File file = ChronicleMapTest.getPersistenceFile();
        try (ChronicleMap<Integer, Integer> map = ChronicleMap
                .of(Integer.class, Integer.class)
                .entries(1)
                .createPersistedTo(file)) {
            map.put(-1, -1);
        }
        try (ChronicleMap<Integer, Integer> map = ChronicleMap
                .of(Integer.class, Integer.class)
                .entries(1)
                .recoverPersistedTo(file, true)) {
            Assert.assertEquals(Integer.valueOf(-1), map.get(-1));
        }
    }
}