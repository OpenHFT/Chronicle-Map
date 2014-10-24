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

package net.openhft.chronicle.map;

import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.chronicle.set.ChronicleSetBuilder;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.Set;

public class Issue3 {

    @Test
    public void test() throws IOException {
        Set<Long> set = ChronicleSetBuilder.of(Long.class)
                .actualSegments(1)
                .actualEntriesPerSegment(1000)
                .create();

        Random r = new Random();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 300; j++) {
                set.add(r.nextLong());
            }
            set.clear();
        }
    }
}
