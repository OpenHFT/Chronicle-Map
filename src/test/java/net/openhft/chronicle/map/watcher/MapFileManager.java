/*
 * Copyright (c) 2016-2019 Chronicle Software Ltd
 */

package net.openhft.chronicle.map.watcher;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.watcher.JMXFileManager;
import net.openhft.chronicle.hash.impl.PersistedChronicleHashResources;
import net.openhft.chronicle.hash.impl.SizePrefixedBlob;
import net.openhft.chronicle.map.VanillaChronicleMap;
import net.openhft.chronicle.wire.ReadAnyWire;
import net.openhft.chronicle.wire.Wires;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Type;
import java.nio.file.Paths;

public class MapFileManager extends JMXFileManager implements MapFileManagerMBean {
    private static final long TIME_OUT = 5_000;
    private String header;
    private Type keyClass;
    private Type valueClass;
    private long size;
    private String name;
    private long dataStoreSize;
    private int segments;

    public MapFileManager(String basePath, String relativePath) {
        super(basePath, relativePath);
    }

    @Override
    protected String type() {
        return "maps";
    }

    private long lastUpdate = 0;

    public String getHeader() {
        update();
        return header;
    }

    public String getKeyClass() {
        return ""+keyClass;
    }

    public String getValueClass() {
        return ""+valueClass;
    }

    public long getSize() {
        return size;
    }

    public String getName() {
        return name;
    }

    public long getDataStoreSize() {
        return dataStoreSize;
    }

    public int getSegments() {
        return segments;
    }

    public long getLastUpdate() {
        return lastUpdate;
    }

    private void update() {
        long now = System.currentTimeMillis();
        if (lastUpdate + TIME_OUT > now)
            return;

        File file = Paths.get(basePath, relativePath).toFile();

        MappedFile mf;
        try {
            mf = MappedFile.mappedFile(file, file.length());
        } catch (FileNotFoundException e) {
            Jvm.warn().on(getClass(), e);
            return;
        }
        try (MappedBytes bytes = MappedBytes.mappedBytes(mf)) {
            int length = bytes.readInt(SizePrefixedBlob.SIZE_WORD_OFFSET);
            if (!Wires.isReady(length)) {
                header = "not ready";
            } else {
                bytes.readPositionRemaining(SizePrefixedBlob.SIZE_WORD_OFFSET, Wires.lengthOf(length) + 4);
                header = Wires.fromSizePrefixedBlobs(bytes);

                bytes.readPositionRemaining(SizePrefixedBlob.SELF_BOOTSTRAPPING_HEADER_OFFSET, Wires.lengthOf(length));
                ReadAnyWire wire = new ReadAnyWire(bytes);
                VanillaChronicleMap map = wire.getValueIn()
                        .object(VanillaChronicleMap.class);
                int headerEnd = SizePrefixedBlob.SELF_BOOTSTRAPPING_HEADER_OFFSET + Wires.lengthOf(length);
                map.initBeforeMapping(file, mf.raf(), headerEnd, false);
                map.createMappedStoreAndSegments(
                        new PersistedChronicleHashResources(file));


                keyClass = map.keyType();
                valueClass = map.valueType();
                size = map.longSize();
                name = map.name();
                dataStoreSize = map.dataStoreSize();
                segments = map.segments();
            }
        } catch (Exception e) {
            Jvm.warn().on(getClass(), "Unable to update", e);
            header = e.toString();
        } finally {
            mf.release();
            assert mf.refCount() == 0;
        }
    }
}
