/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
