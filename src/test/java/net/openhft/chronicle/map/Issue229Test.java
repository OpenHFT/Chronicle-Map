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
import net.openhft.chronicle.hash.ChronicleHashRecoveryFailedException;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class Issue229Test {

    private File mapFile;

    @Before
    public void setup() {
        mapFile = new File("test_map");
    }

    @After
    public void cleanup() {
        mapFile.delete();
    }

    @Test(expected = ChronicleHashRecoveryFailedException.class)
    public void assureExclusiveAccess() throws IOException {
        Assume.assumeFalse(OS.isWindows());

        try (ChronicleMap<Long, Long> readMap = ChronicleMap
                .of(Long.class, Long.class)
                .entries(10)
                .createPersistedTo(mapFile)) {

// It shall not be possible to recover since the
            // file is open by the readMap
            try (ChronicleMap<Long, Long> recoverMap = ChronicleMap
                    .of(Long.class, Long.class)
                    .entries(10)
                    .recoverPersistedTo(mapFile, true)) {

            }
        }
    }
}