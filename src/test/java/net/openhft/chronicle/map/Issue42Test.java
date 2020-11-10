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
import org.junit.Test;

import java.io.IOException;

public class Issue42Test {

    @Test
    public void crashJVMWindowsTest() throws IOException {

        if (!OS.isWindows())
            return;

        try (final ChronicleMap<CharSequence, CharSequence> map = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .averageKeySize(5.9).averageValueSize(5.9)
                .entries(1000000)
                .minSegments(128).create()) {
            for (int i = 0; i < 1000000; ++i) {
                String s = String.valueOf(i);
                map.put(s, s);
            }

            for (int i = 0; i < 1000000; ++i) {
                String s = String.valueOf(i);
                Assert.assertEquals(s, map.get(s).toString());
            }
        }
    }
}
