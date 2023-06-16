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

import net.openhft.chronicle.core.OS;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class NoUpperBoundChunksPerEntryTest {

    @Test
    public void noUpperBoundChunksPerEntryTest() {
        Assume.assumeTrue(OS.is64Bit());

        ChronicleMap<Integer, CharSequence> map =
                ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                        .averageValueSize(2).entries(10000L).actualSegments(1).create();
        String ultraLargeValue = "";
        for (int i = 0; i < 100; i++) {
            ultraLargeValue += "Hello";
        }
        map.put(1, ultraLargeValue);
        Assert.assertEquals(ultraLargeValue, map.get(1).toString());
    }
}
