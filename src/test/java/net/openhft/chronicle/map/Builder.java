/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
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


import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ArrayBlockingQueue;

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


    static <K, V> ChronicleMap<K, V> newShm(
            int size, final ArrayBlockingQueue<byte[]> input,
            final ArrayBlockingQueue<byte[]> output,
            final byte localIdentifier, byte externalIdentifier,
            Class<K> kClass, Class<V> vClass) throws IOException {
        Replicator queue = QueueReplicator.of(localIdentifier, externalIdentifier, input, output);
        return ChronicleMapBuilder.of(kClass, vClass)
                .entries(size)
                .addReplicator(queue)
                .create(getPersistenceFile());
    }

    static MapProvider<ReplicatedChronicleMap<Integer, ?, ?, Integer, ?, ?>> newShmIntInt(
            int size, final ArrayBlockingQueue<byte[]> input,
            final ArrayBlockingQueue<byte[]> output,
            final byte localIdentifier, byte externalIdentifier) throws IOException {

        Replicator queue = QueueReplicator.of(localIdentifier, externalIdentifier, input, output);
        final ReplicatedChronicleMap<Integer, ?, ?, Integer, ?, ?> result =
                (ReplicatedChronicleMap<Integer, ?, ?, Integer, ?, ?>) ChronicleMapBuilder
                        .of(Integer.class, Integer.class)
                        .entries(size)
                        .addReplicator(queue)
                        .create(getPersistenceFile());
        QueueReplicator q = null;
        for (Closeable closeable : result.closeables) {
            if (closeable instanceof QueueReplicator) {
                q = (QueueReplicator) closeable;
                break;
            }
        }
        if (q == null)
            throw new AssertionError();
        final QueueReplicator finalQ = q;
        return new MapProvider<ReplicatedChronicleMap<Integer, ?, ?, Integer, ?, ?>>() {

            @Override
            public ReplicatedChronicleMap<Integer, ?, ?, Integer, ?, ?> getMap() {
                return result;
            }

            @Override
            public boolean isQueueEmpty() {
                return finalQ.isEmpty();
            }

        };
    }

    interface MapProvider<T> {
        T getMap();

        boolean isQueueEmpty();
    }

}
