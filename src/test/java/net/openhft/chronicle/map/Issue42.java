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

import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;

import static net.openhft.chronicle.map.Alignment.NO_ALIGNMENT;

public class Issue42 {

    private static String OS = System.getProperty("os.name").toLowerCase();

    public static boolean isWindows() {
        return (OS.indexOf("win") >= 0);
    }


    @Test
    public void crashJVMWindowsTest() throws IOException {

        if (!isWindows())
            return;

        final ChronicleMap<CharSequence, CharSequence> map = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .entrySize(18)
                .entries(15000000)
                .minSegments(128).create();

        for (int i = 0; i < 10000000; ++i) {
            String s = String.valueOf(i);
            map.put(s, s);
        }

        for (int i = 0; i < 10000000; ++i) {
            String s = String.valueOf(i);
            Assert.assertEquals(s, map.get(s));
        }

        map.close();
    }
}
