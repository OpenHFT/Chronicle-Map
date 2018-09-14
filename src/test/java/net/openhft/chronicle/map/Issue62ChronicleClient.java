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

import java.util.Map.Entry;

import static net.openhft.chronicle.map.Issue62ChronicleServer.MAP_FILE_B;
import static net.openhft.chronicle.map.Issue62ChronicleServer.prepare;

public class Issue62ChronicleClient {

    private final static Logger LOGGER = LoggerFactory.getLogger(Issue62ChronicleClient.class);

    public static void main(String[] args) throws Exception {
        prepare(MAP_FILE_B);

        ChronicleMapBuilder<String, Long> cityPostalCodesMapBuilder =
                ChronicleMapBuilder.of(String.class, Long.class)
                        //.averageKeySize(100)
                        .averageKey(Issue62ChronicleServer.STR)
                        .entries(50_000);

        ((ChronicleHashBuilderPrivateAPI<?, ?>) cityPostalCodesMapBuilder.privateAPI())
                .replication((byte) 2);

        try (ChronicleMap<String, Long> map =
                     cityPostalCodesMapBuilder.createPersistedTo(MAP_FILE_B)) {

            LOGGER.info("Starting");
            Jvm.pause(3000);

            for (Entry<String, Long> entry : map.entrySet()) {
                LOGGER.info("{} : {}", entry.getKey(), entry.getValue());
            }
        }
    }

}
