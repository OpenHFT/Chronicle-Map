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
