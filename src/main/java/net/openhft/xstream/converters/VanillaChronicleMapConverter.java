/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.xstream.converters;

import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import net.openhft.chronicle.map.ChronicleMap;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

/**
 * @author Rob Austin.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class VanillaChronicleMapConverter<K, V> extends AbstractChronicleMapConverter<K, V> {

    public VanillaChronicleMapConverter(@NotNull Map<K, V> map) {
        super(map);
    }

    @Override
    public void marshal(Object o, final HierarchicalStreamWriter writer, final MarshallingContext
            marshallingContext) {
        ((ChronicleMap<K, V>) o).forEachEntry(e -> {
            writer.startNode("entry");
            {
                final Object key = e.key().get();
                writer.startNode(key.getClass().getName());
                marshallingContext.convertAnother(key);
                writer.endNode();

                Object value = e.value().get();
                writer.startNode(value.getClass().getName());
                marshallingContext.convertAnother(value);
                writer.endNode();
            }
            writer.endNode();
        });
    }
}
