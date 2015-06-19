/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * @author Rob Austin.
 */
public class Builder {

    public static final int SIZE = 10_000;
    // added to ensure uniqueness
    static int count;
    static String WIN_OS = "WINDOWS";

    public static File getPersistenceFile() throws IOException {

        final File file = File.createTempFile("chm-test-", "map");

        //Not Guaranteed to work on Windows, since OS file-lock takes precedence
        if (System.getProperty("os.name").indexOf(WIN_OS) > 0) {
            /*Windows will lock a file that are currently in use. You cannot delete it, however,
              using setwritable() and then releasing RandomRW lock adds the file to JVM exit cleanup.
    		  This will only work if the user is an admin on windows.
    		*/
            file.setWritable(true);//just in case relative path was used.
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.close();//allows closing the file access on windows. forcing to close access. Only works for admin-access.
        }

        //file.delete(); //isnt guaranteed on windows.
        file.deleteOnExit();//isnt guaranteed on windows.

        return file;
    }

    public static <T extends ChronicleMap<Integer, Void>> T newMapVoid(
            final byte identifier,
            final int serverPort,
            final InetSocketAddress... endpoints) throws IOException {
        return (T) newTcpSocketShmBuilder(Integer.class, Void.class,
                identifier, serverPort, endpoints).create();
    }

    public static <K, V> ChronicleMapBuilder<K, V> newTcpSocketShmBuilder(
            Class<K> kClass, Class<V> vClass,
            final byte identifier,
            final int serverPort,
            final InetSocketAddress... endpoints) throws IOException {
        TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig.of(serverPort, Arrays.asList(endpoints))
                .heartBeatInterval(1L, TimeUnit.SECONDS).autoReconnectedUponDroppedConnection(true);
        return ChronicleMapBuilder.of(kClass, vClass)
                .entries(SIZE)
                .replication(identifier, tcpConfig);
    }

}
