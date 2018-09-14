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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.hash.ChronicleHashBuilderPrivateAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;

public class Issue62ChronicleServer {

    final static String STR =
            //"This is just a long string, which causes sink to fail for some reason.";
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    final static File MAP_FILE_B =
            Paths.get(System.getProperty("java.io.tmpdir"), "map.b").toFile();
    private final static Logger LOGGER = LoggerFactory.getLogger(Issue62ChronicleServer.class);
    private final static File MAP_FILE_A =
            Paths.get(System.getProperty("java.io.tmpdir"), "map.a").toFile();

    static void prepare(File file) {
        if (file.exists())
            file.delete();
        file.deleteOnExit();
    }

    public static void main(String[] args) throws Exception {

        prepare(MAP_FILE_A);

        ChronicleMapBuilder<String, Long> cityPostalCodesMapBuilder =
                ChronicleMapBuilder.of(String.class, Long.class)
                        //.averageKeySize(100)
                        .averageKey(STR)
                        .entries(50_000);

        ((ChronicleHashBuilderPrivateAPI<?, ?>) cityPostalCodesMapBuilder.privateAPI())
                .replication((byte) 1);

        ChronicleMap<String, Long> cityPostalCodes =
                cityPostalCodesMapBuilder.createPersistedTo(MAP_FILE_A);

        for (int i = 0; i < 100; i++) {
            cityPostalCodes.put(STR + i, (long) i);
        }

        LOGGER.info("Map created");
        Jvm.pause(15000);

    }

}
