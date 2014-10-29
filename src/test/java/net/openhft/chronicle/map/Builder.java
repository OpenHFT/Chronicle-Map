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


import net.openhft.chronicle.hash.TcpReplicationConfig;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * @author Rob Austin.
 */
public class Builder {

    // added to ensure uniqueness
    static int count;
    static String WIN_OS = "WINDOWS";

    public static File getPersistenceFile() throws IOException {
        String TMP = System.getProperty("java.io.tmpdir");
        File file = new File(TMP + "/chm-test" + System.nanoTime() + (count++));

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
        TcpReplicationConfig tcpConfig = TcpReplicationConfig.of(serverPort, endpoints)
                .heartBeatInterval(1L, TimeUnit.SECONDS);
        return ChronicleMapBuilder.of(kClass, vClass)
                .replicators(identifier,  tcpConfig);
    }



}
