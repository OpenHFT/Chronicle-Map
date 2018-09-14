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

import net.openhft.chronicle.set.ChronicleSet;
import org.junit.Assert;
import org.junit.Test;

public class ChronicleMapNameTest {

    @Test
    public void testChronicleMapName() {
        ChronicleMap<Integer, Integer> map = ChronicleMap
                .of(Integer.class, Integer.class)
                .entries(1)
                .name("foo")
                .create();
        Assert.assertTrue(map.toIdentityString().contains("foo"));
    }

    @Test
    public void testChronicleSetName() {
        ChronicleSet<Integer> set = ChronicleSet
                .of(Integer.class)
                .entries(1)
                .name("foo")
                .create();
        Assert.assertTrue(set.toIdentityString().contains("foo"));
    }
}
