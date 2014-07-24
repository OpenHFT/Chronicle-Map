/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.collections;

import net.openhft.chronicle.map.ChronicleMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @author Rob Austin.
 */
public class ChronicleMapBuilder0<K, V> extends SharedHashMapBuilder<K, V> implements Cloneable {

    private static final Logger LOG = LoggerFactory.getLogger(ChronicleMapBuilder0.class.getName());

    public ChronicleMapBuilder0(Class<K> kClass, Class<V> vClass) {
        super(kClass, vClass);
    }

    public static <K, V> ChronicleMapBuilder0<K, V> of(Class<K> kClass, Class<V> vClass) {
        return new ChronicleMapBuilder0<K, V>(kClass, vClass);
    }


    /**
     * Not supported yet.
     *
     * @param transactional if the built map should be transactional
     * @return this {@code SharedHashMapBuilder} back
     */
    public ChronicleMapBuilder0<K, V> transactional(boolean transactional) {
        this.transactional = transactional;
        return this;
    }

    public boolean transactional() {
        return transactional;
    }


    public ChronicleMapBuilder0 kClass(Class kClass) {
        this.kClass = kClass;
        return this;
    }

    public ChronicleMapBuilder0 vClass(Class vClass) {
        this.vClass = vClass;
        return this;
    }

    public ChronicleMapBuilder0<K, V> file(File file) {
        this.file = file;
        return this;
    }


    public ChronicleMap<K, V> create() throws IOException {
        super.create();

        if (kClass == null)
            throw new IllegalArgumentException("missing mandatory parameter kClass");

        if (vClass == null)
            throw new IllegalArgumentException("missing mandatory parameter vClass");

        if (file == null)
            throw new IllegalArgumentException("missing mandatory parameter file");


        ChronicleMapBuilder0<K, V> builder = toBuilder();

        if (!canReplicate())
            return new ChronicleMap0<K, V>(builder, file, kClass, vClass);

        if (identifier <= 0)
            throw new IllegalArgumentException("Identifier must be positive, " + identifier + " given");

        final ChronicleReplicatedMap<K, V> result =
                new ChronicleReplicatedMap<K, V>(builder, kClass, vClass);

        if (externalReplicatorBuilder != null)
            externalReplicator = applyExternalReplicator(result, externalReplicatorBuilder, kClass, vClass);

        if (tcpReplicatorBuilder != null)
            applyTcpReplication(result, tcpReplicatorBuilder);

        if (udpReplicatorBuilder != null) {
            if (tcpReplicatorBuilder == null)
                LOG.warn("MISSING TCP REPLICATION : The UdpReplicator only attempts to read data (" +
                        "it does not enforce or guarantee delivery), you should use the UdpReplicator if " +
                        "you have a large number of nodes, and you wish to receive the data before it " +
                        "becomes available on TCP/IP. Since data delivery is not guaranteed, it is " +
                        "recommended that you only use the UDP" +
                        " " +
                        "Replicator " +
                        "in conjunction with a TCP Replicator");
            applyUdpReplication(result, udpReplicatorBuilder);
        }
        return result;
    }

    ChronicleMapBuilder0<K, V> toBuilder() throws IOException {
        ChronicleMapBuilder0<K, V> builder = clone();

        for (int i = 0; i < 10; i++) {
            if (file.exists() && file.length() > 0) {
                readFile(file, builder);
                break;
            }
            if (file.createNewFile() || file.length() == 0) {
                newFile(file);
                break;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        if (builder == null || !file.exists())
            throw new FileNotFoundException("Unable to create " + file);
        return builder;
    }

    @Override
    public ChronicleMapBuilder0<K, V> clone() {
        return (ChronicleMapBuilder0<K, V>) super.clone();
    }

}
