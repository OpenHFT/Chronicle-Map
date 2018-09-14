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

package net.openhft.chronicle.map.externalizable;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ExternalizableTest {
    @Test
    public void externalizable() throws IOException {
        String path = OS.TARGET + "/test-" + System.nanoTime() + ".map";
        new File(path).deleteOnExit();
        try (ChronicleMap<Long, SomeClass> storage = ChronicleMapBuilder
                .of(Long.class, SomeClass.class)
                .averageValueSize(128)
                .entries(128)
                .createPersistedTo(new File(path))) {
            SomeClass value = new SomeClass();
            value.hits.add("one");
            value.hits.add("two");
            storage.put(1L, value);

            SomeClass value2 = storage.get(1L);
            assertEquals(value.hits, value2.hits);
        }
    }
}
