/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map.example;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.values.LongValue;

import java.io.File;
import java.io.IOException;

public class DistributedSequenceMain {

    public static void main(String... ignored) throws IOException {
        Class<LongValue> longValueClass = DataValueClasses.directClassFor(LongValue.class);
        ChronicleMapBuilder.of(String.class, longValueClass)
                        .entries(128)
                        .actualSegments(1).file(new File("/tmp/counters"));
        ChronicleMap<String, LongValue> map =
                ChronicleMapBuilder.of(String.class, longValueClass)
                        .entries(128)
                        .actualSegments(1).create();
        LongValue value = DataValueClasses.newDirectReference(longValueClass);
        map.acquireUsing("sequence", value);

        for (int i = 0; i < 1000000; i++) {
            long nextId = value.addAtomicValue(1);
        }

        map.close();
    }
}
