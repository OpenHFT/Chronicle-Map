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
import net.openhft.lang.Maths;
import net.openhft.lang.io.serialization.BytesMarshallableSerializer;
import net.openhft.lang.io.serialization.BytesMarshallerFactory;
import net.openhft.lang.io.serialization.JDKObjectSerializer;
import net.openhft.lang.io.serialization.ObjectSerializer;
import net.openhft.lang.io.serialization.impl.VanillaBytesMarshallerFactory;
import net.openhft.lang.model.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class SharedHashMapBuilder<K, V> implements Cloneable {

    private static final Logger LOG = LoggerFactory.getLogger(net.openhft.collections.SharedHashMapBuilder.class.getName());

    static final int HEADER_SIZE = 128;
    static final int SEGMENT_HEADER = 64;
    private static final byte[] MAGIC = "SharedHM".getBytes();

    public static final short UDP_REPLICATION_MODIFICATION_ITERATOR_ID = 128;
    public static final short JDBC_REPLICATION_MODIFICATION_ITERATOR_ID = 129;
    public static final short FILE_REPLICATION_MODIFICATION_ITERATOR_ID = 130;

    // used when configuring the number of segments.
    private int minSegments = -1;
    private int actualSegments = -1;

    // used when reading the number of entries per
    private int actualEntriesPerSegment = -1;

    private int entrySize = 256;
    private Alignment alignment = Alignment.OF_4_BYTES;
    private long entries = 1 << 20;
    private int replicas = 0;
    boolean transactional = false;
    private long lockTimeOutMS = 20000;
    private int metaDataBytes = 0;
    private SharedMapEventListener eventListener = SharedMapEventListeners.nop();
    private SharedMapErrorListener errorListener = SharedMapErrorListeners.logging();
    private boolean putReturnsNull = false;
    private boolean removeReturnsNull = false;
    private boolean generatedKeyType = false;
    private boolean generatedValueType = false;
    private boolean largeSegments = false;

    // replication
    private boolean canReplicate;
    byte identifier = Byte.MIN_VALUE;
    TcpReplicatorBuilder tcpReplicatorBuilder;
    ExternalReplicatorBuilder externalReplicatorBuilder;

    private TimeProvider timeProvider = TimeProvider.SYSTEM;
    UdpReplicatorBuilder udpReplicatorBuilder;
    private BytesMarshallerFactory bytesMarshallerFactory;
    private ObjectSerializer objectSerializer;
    ExternalReplicator externalReplicator;
    File file;
    Class kClass;
    Class vClass;

    public SharedHashMapBuilder() {
    }

    public SharedHashMapBuilder(Class<K> kClass, Class<V> vClass) {
        this.kClass = kClass;
        this.vClass = vClass;
    }

    public static <K, V> SharedHashMapBuilder<K, V> of(Class<K> kClass, Class<V> vClass) {
        return new SharedHashMapBuilder<K, V>(kClass, vClass);
    }

    @Override
    public SharedHashMapBuilder<K, V> clone() {

        try {
            @SuppressWarnings("unchecked")
            final SharedHashMapBuilder<K, V> result = (SharedHashMapBuilder) super.clone();
            if (tcpReplicatorBuilder() != null)
                result.tcpReplicatorBuilder(tcpReplicatorBuilder().clone());
            if (udpReplicatorBuilder() != null)
                result.udpReplicatorBuilder(udpReplicatorBuilder().clone());
            return result;

        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }


    /**
     * Set minimum number of segments. See concurrencyLevel in {@link java.util.concurrent.ConcurrentHashMap}.
     *
     * @return this builder object back
     */
    public SharedHashMapBuilder minSegments(int minSegments) {
        this.minSegments = minSegments;
        return this;
    }

    public int minSegments() {
        return minSegments < 1 ? tryMinSegments(4, 65536) : minSegments;
    }

    private int tryMinSegments(int min, int max) {
        for (int i = min; i < max; i <<= 1) {
            if (i * i * i >= alignedEntrySize() * 2)
                return i;
        }
        return max;
    }

    /**
     * <p>Note that the actual entrySize will be aligned to 4 (default entry alignment). I. e. if you set
     * entry size to 30, the actual entry size will be 32 (30 aligned to 4 bytes). If you don't want entry
     * size to be aligned, set {@code entryAndValueAlignment(Alignment.NO_ALIGNMENT)}.
     *
     * @param entrySize the size in bytes
     * @return this {@code SharedHashMapBuilder} back
     * @see #entryAndValueAlignment(Alignment)
     * @see #entryAndValueAlignment()
     */
    public SharedHashMapBuilder entrySize(int entrySize) {
        this.entrySize = entrySize;
        return this;
    }

    public int entrySize() {
        return entrySize;
    }

    int alignedEntrySize() {
        return entryAndValueAlignment().alignSize(entrySize());
    }

    /**
     * Specifies alignment of address in memory of entries and independently of address in memory of values
     * within entries. <p/> <p>Useful when values of the map are updated intensively, particularly fields with
     * volatile access, because it doesn't work well if the value crosses cache lines. Also, on some (nowadays
     * rare) architectures any misaligned memory access is more expensive than aligned. <p/> <p>Note that
     * specified {@link #entrySize()} will be aligned according to this alignment. I. e. if you set {@code
     * entrySize(20)} and {@link net.openhft.collections.Alignment#OF_8_BYTES}, actual entry size will be 24
     * (20 aligned to 8 bytes).
     *
     * @return this {@code SharedHashMapBuilder} back
     * @see #entryAndValueAlignment()
     */
    public SharedHashMapBuilder<K, V> entryAndValueAlignment(Alignment alignment) {
        this.alignment = alignment;
        return this;
    }

    /**
     * Returns alignment of addresses in memory of entries and independently of values within entries. <p/>
     * <p>Default is {@link net.openhft.collections.Alignment#OF_4_BYTES}.
     *
     * @see #entryAndValueAlignment(Alignment)
     */
    public Alignment entryAndValueAlignment() {
        return alignment;
    }

    public SharedHashMapBuilder<K, V> entries(long entries) {
        this.entries = entries;
        return this;
    }

    public long entries() {
        return entries;
    }

    public SharedHashMapBuilder<K, V> replicas(int replicas) {
        this.replicas = replicas;
        return this;
    }

    public int replicas() {
        return replicas;
    }

    public SharedHashMapBuilder<K, V> actualEntriesPerSegment(int actualEntriesPerSegment) {
        this.actualEntriesPerSegment = actualEntriesPerSegment;
        return this;
    }

    public int actualEntriesPerSegment() {
        if (actualEntriesPerSegment > 0)
            return actualEntriesPerSegment;
        int as = actualSegments();
        // round up to the next multiple of 64.
        return (int) (Math.max(1, entries * 2L / as) + 63) & ~63;
    }

    public SharedHashMapBuilder<K, V> actualSegments(int actualSegments) {
        this.actualSegments = actualSegments;
        return this;
    }

    public int actualSegments() {
        if (actualSegments > 0)
            return actualSegments;
        if (!largeSegments && entries > (long) minSegments() << 15) {
            long segments = Maths.nextPower2(entries >> 15, 128);
            if (segments < 1 << 20)
                return (int) segments;
        }
        // try to keep it 16-bit sizes segments
        return (int) Maths.nextPower2(Math.max((entries >> 30) + 1, minSegments()), 1);
    }

    /**
     * Not supported yet.
     *
     * @param transactional if the built map should be transactional
     * @return this {@code SharedHashMapBuilder} back
     */
    public SharedHashMapBuilder<K, V> transactional(boolean transactional) {
        this.transactional = transactional;
        return this;
    }

    public boolean transactional() {
        return transactional;
    }


    public SharedHashMapBuilder kClass(Class kClass) {
        this.kClass = kClass;
        return this;
    }

    public SharedHashMapBuilder vClass(Class vClass) {
        this.vClass = vClass;
        return this;
    }

    public SharedHashMapBuilder<K, V> file(File file) {
        this.file = file;
        return this;
    }

    public File file() {
        return this.file;
    }

    public <K> Class<K> kClass() {
        return this.kClass;
    }

    public <V> Class<V> vClass() {
        return this.vClass;
    }


    public ChronicleMap<K, V> create() throws IOException {
        if (kClass == null)
            throw new IllegalArgumentException("missing mandatory parameter kClass");

        if (vClass == null)
            throw new IllegalArgumentException("missing mandatory parameter vClass");

        if (file == null)
            throw new IllegalArgumentException("missing mandatory parameter file");


        SharedHashMapBuilder<K, V> builder = toBuilder();

        if (!canReplicate())
            return new VanillaSharedHashMap<K, V>(builder, file, kClass, vClass);

        if (identifier <= 0)
            throw new IllegalArgumentException("Identifier must be positive, " + identifier + " given");

        final VanillaSharedReplicatedMap<K, V> result =
                new VanillaSharedReplicatedMap<K, V>(builder, kClass, vClass);

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


    /**
     * Its recommended that you use net.openhft.collections.SharedHashMapBuilder#create() instead as this
     * method will be shortly removed
     */
    @Deprecated
    public <K, V> ChronicleMap<K, V> create(File file, Class<K> kClass, Class<V> vClass) throws IOException {
        return file(file).kClass(kClass).vClass(vClass).create();
    }

    SharedHashMapBuilder<K, V> toBuilder() throws IOException {
        SharedHashMapBuilder builder = clone();

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

    static void readFile(File file, SharedHashMapBuilder builder) throws IOException {
        ByteBuffer bb = ByteBuffer.allocateDirect(HEADER_SIZE).order(ByteOrder.nativeOrder());
        FileInputStream fis = new FileInputStream(file);
        fis.getChannel().read(bb);
        fis.close();
        bb.flip();
        if (bb.remaining() < 22) throw new IOException("File too small, corrupted? " + file);
        byte[] bytes = new byte[8];
        bb.get(bytes);
        if (!Arrays.equals(bytes, MAGIC))
            throw new IOException("Unknown magic number, was " + new String(bytes, "ISO-8859-1"));
        builder.actualSegments(bb.getInt());
        builder.actualEntriesPerSegment(bb.getInt());
        builder.entrySize(bb.getInt());
        builder.entryAndValueAlignment(Alignment.fromOrdinal(bb.get()));
        builder.replicas(bb.getInt());
        builder.transactional(bb.get() == 'Y');
        builder.metaDataBytes(bb.get() & 0xFF);
        if (builder.actualSegments() <= 0 || builder.actualEntriesPerSegment() <= 0 || builder.entrySize() <= 0)
            throw new IOException("Corrupt header for " + file);
    }

    void newFile(File file) throws IOException {
        ByteBuffer bb = ByteBuffer.allocateDirect(HEADER_SIZE).order(ByteOrder.nativeOrder());
        bb.put(MAGIC);
        bb.putInt(actualSegments());
        bb.putInt(actualEntriesPerSegment());
        bb.putInt(entrySize());
        bb.put((byte) entryAndValueAlignment().ordinal());
        bb.putInt(replicas());
        bb.put((byte) (transactional ? 'Y' : 'N'));
        bb.put((byte) metaDataBytes);
        bb.flip();
        FileOutputStream fos = new FileOutputStream(file);
        fos.getChannel().write(bb);
        fos.close();
    }

    public SharedHashMapBuilder lockTimeOutMS(long lockTimeOutMS) {
        this.lockTimeOutMS = lockTimeOutMS;
        return this;
    }

    public long lockTimeOutMS() {
        return lockTimeOutMS;
    }

    public SharedHashMapBuilder errorListener(SharedMapErrorListener errorListener) {
        this.errorListener = errorListener;
        return this;
    }

    public SharedMapErrorListener errorListener() {
        return errorListener;
    }

    /**
     * Map.put() returns the previous value, functionality which is rarely used but fairly cheap for HashMap.
     * In the case, for an off heap collection, it has to create a new object (or return a recycled one)
     * Either way it's expensive for something you probably don't use.
     *
     * @param putReturnsNull false if you want SharedHashMap.put() to not return the object that was replaced
     *                       but instead return null
     * @return an instance of the map builder
     */
    public SharedHashMapBuilder putReturnsNull(boolean putReturnsNull) {
        this.putReturnsNull = putReturnsNull;
        return this;
    }

    /**
     * Map.put() returns the previous value, functionality which is rarely used but fairly cheap for HashMap.
     * In the case, for an off heap collection, it has to create a new object (or return a recycled one)
     * Either way it's expensive for something you probably don't use.
     *
     * @return true if SharedHashMap.put() is not going to return the object that was replaced but instead
     * return null
     */
    public boolean putReturnsNull() {
        return putReturnsNull;
    }

    /**
     * Map.remove()  returns the previous value, functionality which is rarely used but fairly cheap for
     * HashMap. In the case, for an off heap collection, it has to create a new object (or return a recycled
     * one) Either way it's expensive for something you probably don't use.
     *
     * @param removeReturnsNull false if you want SharedHashMap.remove() to not return the object that was
     *                          removed but instead return null
     * @return an instance of the map builder
     */
    public SharedHashMapBuilder removeReturnsNull(boolean removeReturnsNull) {
        this.removeReturnsNull = removeReturnsNull;
        return this;
    }


    /**
     * Map.remove() returns the previous value, functionality which is rarely used but fairly cheap for
     * HashMap. In the case, for an off heap collection, it has to create a new object (or return a recycled
     * one) Either way it's expensive for something you probably don't use.
     *
     * @return true if SharedHashMap.remove() is not going to return the object that was removed but instead
     * return null
     */
    public boolean removeReturnsNull() {
        return removeReturnsNull;
    }

    public boolean generatedKeyType() {
        return generatedKeyType;
    }

    public SharedHashMapBuilder generatedKeyType(boolean generatedKeyType) {
        this.generatedKeyType = generatedKeyType;
        return this;
    }

    public boolean generatedValueType() {
        return generatedValueType;
    }

    public SharedHashMapBuilder generatedValueType(boolean generatedValueType) {
        this.generatedValueType = generatedValueType;
        return this;
    }

    public boolean largeSegments() {
        return entries > 1L << (20 + 15) || largeSegments;
    }

    public SharedHashMapBuilder largeSegments(boolean largeSegments) {
        this.largeSegments = largeSegments;
        return this;
    }


    public SharedHashMapBuilder metaDataBytes(int metaDataBytes) {
        if ((metaDataBytes & 0xFF) != metaDataBytes)
            throw new IllegalArgumentException("MetaDataBytes must be [0..255] was " + metaDataBytes);
        this.metaDataBytes = metaDataBytes;
        return this;
    }

    public int metaDataBytes() {
        return metaDataBytes;
    }

    public SharedHashMapBuilder eventListener(SharedMapEventListener eventListener) {
        this.eventListener = eventListener;
        return this;
    }

    public SharedMapEventListener eventListener() {
        return eventListener;
    }

    @Override
    public String toString() {
        return "SharedHashMapBuilder{" +
                "actualSegments=" + actualSegments() +
                ", minSegments=" + minSegments() +
                ", actualEntriesPerSegment=" + actualEntriesPerSegment() +
                ", entrySize=" + entrySize() +
                ", entryAndValueAlignment=" + entryAndValueAlignment() +
                ", entries=" + entries() +
                ", replicas=" + replicas() +
                ", transactional=" + transactional() +
                ", lockTimeOutMS=" + lockTimeOutMS() +
                ", metaDataBytes=" + metaDataBytes() +
                ", eventListener=" + eventListener() +
                ", errorListener=" + errorListener() +
                ", putReturnsNull=" + putReturnsNull() +
                ", removeReturnsNull=" + removeReturnsNull() +
                ", generatedKeyType=" + generatedKeyType() +
                ", generatedValueType=" + generatedValueType() +
                ", largeSegments=" + largeSegments() +
                ", canReplicate=" + canReplicate() +
                ", identifier=" + identifierToString() +
                ", tcpReplicatorBuilder=" + tcpReplicatorBuilder() +
                ", udpReplicatorBuilder=" + udpReplicatorBuilder() +
                ", timeProvider=" + timeProvider() +
                ", bytesMarshallerfactory=" + bytesMarshallerFactory() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SharedHashMapBuilder that = (SharedHashMapBuilder) o;

        if (actualEntriesPerSegment != that.actualEntriesPerSegment) return false;
        if (actualSegments != that.actualSegments) return false;
        if (canReplicate != that.canReplicate) return false;
        if (entries != that.entries) return false;
        if (entrySize != that.entrySize) return false;
        if (generatedKeyType != that.generatedKeyType) return false;
        if (generatedValueType != that.generatedValueType) return false;
        if (identifier != that.identifier) return false;
        if (largeSegments != that.largeSegments) return false;
        if (lockTimeOutMS != that.lockTimeOutMS) return false;
        if (metaDataBytes != that.metaDataBytes) return false;
        if (minSegments != that.minSegments) return false;
        if (putReturnsNull != that.putReturnsNull) return false;
        if (removeReturnsNull != that.removeReturnsNull) return false;
        if (replicas != that.replicas) return false;
        if (transactional != that.transactional) return false;

        if (alignment != that.alignment) return false;
        if (errorListener != null ? !errorListener.equals(that.errorListener) : that.errorListener != null)
            return false;
        if (eventListener != null ? !eventListener.equals(that.eventListener) : that.eventListener != null)
            return false;
        if (tcpReplicatorBuilder != null ? !tcpReplicatorBuilder.equals(that.tcpReplicatorBuilder) : that.tcpReplicatorBuilder != null)
            return false;
        if (timeProvider != null ? !timeProvider.equals(that.timeProvider) : that.timeProvider != null)
            return false;
        if (udpReplicatorBuilder != null ? !udpReplicatorBuilder.equals(that.udpReplicatorBuilder) : that.udpReplicatorBuilder != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = minSegments;
        result = 31 * result + actualSegments;
        result = 31 * result + actualEntriesPerSegment;
        result = 31 * result + entrySize;
        result = 31 * result + (alignment != null ? alignment.hashCode() : 0);
        result = 31 * result + (int) (entries ^ (entries >>> 32));
        result = 31 * result + replicas;
        result = 31 * result + (transactional ? 1 : 0);
        result = 31 * result + (int) (lockTimeOutMS ^ (lockTimeOutMS >>> 32));
        result = 31 * result + metaDataBytes;
        result = 31 * result + (eventListener != null ? eventListener.hashCode() : 0);
        result = 31 * result + (errorListener != null ? errorListener.hashCode() : 0);
        result = 31 * result + (putReturnsNull ? 1 : 0);
        result = 31 * result + (removeReturnsNull ? 1 : 0);
        result = 31 * result + (generatedKeyType ? 1 : 0);
        result = 31 * result + (generatedValueType ? 1 : 0);
        result = 31 * result + (largeSegments ? 1 : 0);
        result = 31 * result + (canReplicate ? 1 : 0);
        result = 31 * result + (int) identifier;
        result = 31 * result + (tcpReplicatorBuilder != null ? tcpReplicatorBuilder.hashCode() : 0);
        result = 31 * result + (timeProvider != null ? timeProvider.hashCode() : 0);
        result = 31 * result + (udpReplicatorBuilder != null ? udpReplicatorBuilder.hashCode() : 0);
        return result;
    }

    public boolean canReplicate() {
        return canReplicate || tcpReplicatorBuilder != null || udpReplicatorBuilder != null;
    }

    public SharedHashMapBuilder canReplicate(boolean canReplicate) {
        this.canReplicate = canReplicate;
        return this;
    }


    <K, V> ExternalReplicator.AbstractExternalReplicator applyExternalReplicator
            (VanillaSharedReplicatedMap<K, V> map,
             ExternalReplicatorBuilder builder,
             Class<K> kClass,
             Class<V> vClass) {
        try {

            final ExternalReplicator.AbstractExternalReplicator externalReplicator;

            final short id;

            if (builder instanceof ExternalJDBCReplicatorBuilder) {
                externalReplicator = new ExternalJDBCReplicator<K, V>(kClass, vClass,
                        (ExternalJDBCReplicatorBuilder) builder, map);
                id = JDBC_REPLICATION_MODIFICATION_ITERATOR_ID;
            } else if (builder instanceof ExternalFileReplicatorBuilder) {
                externalReplicator = new ExternalFileReplicator<K, V>(kClass, vClass,
                        (ExternalFileReplicatorBuilder) builder, map);
                id = FILE_REPLICATION_MODIFICATION_ITERATOR_ID;
            } else
                throw new IllegalStateException("builder of type " + builder.getClass() + " is not supported.");


            final Replica.ModificationIterator modIterator
                    = map.acquireModificationIterator(id, externalReplicator);

            externalReplicator.setModificationIterator(modIterator);

            map.addCloseable(externalReplicator);
            return externalReplicator;
        } catch (Exception e) {
            LOG.error("", e);
        }

        return null;
    }


    <K, V> void applyUdpReplication(VanillaSharedReplicatedMap<K, V> result,
                                    UdpReplicatorBuilder udpReplicatorBuilder)
            throws IOException {

        final InetAddress address = udpReplicatorBuilder.address();

        if (address == null) {
            throw new IllegalArgumentException("address can not be null");
        }

        if (address.isMulticastAddress() && udpReplicatorBuilder.networkInterface() == null) {
            throw new IllegalArgumentException("MISSING: NetworkInterface, " +
                    "When using a multicast addresses, please provided a " +
                    "networkInterface");
        }

        final UdpReplicator udpReplicator =
                new UdpReplicator(result, udpReplicatorBuilder.clone(), entrySize(), result.identifier(), UDP_REPLICATION_MODIFICATION_ITERATOR_ID);


        result.addCloseable(udpReplicator);
    }


    <K, V> void applyTcpReplication(@NotNull VanillaSharedReplicatedMap<K, V> result,
                                    @NotNull TcpReplicatorBuilder tcpReplicatorBuilder)
            throws IOException {

        result.addCloseable(new TcpReplicator(result, result, tcpReplicatorBuilder, entrySize()));
    }

    public SharedHashMapBuilder timeProvider(TimeProvider timeProvider) {
        this.timeProvider = timeProvider;
        return this;
    }

    public TimeProvider timeProvider() {
        return timeProvider;
    }

    public byte identifier() {

        if (identifier == Byte.MIN_VALUE)
            throw new IllegalStateException("identifier is not set.");

        return identifier;
    }

    private String identifierToString() {
        return identifier == Byte.MIN_VALUE ? "identifier is not set" : (identifier + "");
    }

    public SharedHashMapBuilder identifier(byte identifier) {
        this.identifier = identifier;
        return this;
    }

    public SharedHashMapBuilder tcpReplicatorBuilder(TcpReplicatorBuilder tcpReplicatorBuilder) {
        this.tcpReplicatorBuilder = tcpReplicatorBuilder;
        return this;
    }

    public TcpReplicatorBuilder tcpReplicatorBuilder() {
        return tcpReplicatorBuilder;
    }


    public UdpReplicatorBuilder udpReplicatorBuilder() {
        return udpReplicatorBuilder;
    }


    public SharedHashMapBuilder udpReplicatorBuilder(UdpReplicatorBuilder udpReplicatorBuilder) {
        this.udpReplicatorBuilder = udpReplicatorBuilder;
        return this;
    }

    public BytesMarshallerFactory bytesMarshallerFactory() {
        return bytesMarshallerFactory == null ? bytesMarshallerFactory = new VanillaBytesMarshallerFactory() : bytesMarshallerFactory;
    }

    public SharedHashMapBuilder bytesMarshallerFactory(BytesMarshallerFactory bytesMarshallerFactory) {
        this.bytesMarshallerFactory = bytesMarshallerFactory;
        return this;
    }

    public ObjectSerializer objectSerializer() {
        return objectSerializer == null ? objectSerializer = BytesMarshallableSerializer.create(bytesMarshallerFactory(), JDKObjectSerializer.INSTANCE) : objectSerializer;
    }

    public SharedHashMapBuilder objectSerializer(ObjectSerializer objectSerializer) {
        this.objectSerializer = objectSerializer;
        return this;
    }

    public SharedHashMapBuilder externalReplicatorBuilder(@NotNull ExternalReplicatorBuilder externalReplicatorBuilder) {
        this.externalReplicatorBuilder = externalReplicatorBuilder;
        return this;
    }

    public <T extends ExternalReplicator> T externalReplicator() {
        return (T) externalReplicator;
    }

}

