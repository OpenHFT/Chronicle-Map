/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.xstream.converters;

import com.thoughtworks.xstream.converters.ConversionException;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import net.openhft.chronicle.values.Values;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Map;

import static net.openhft.chronicle.values.ValueModel.$$HEAP;
import static net.openhft.chronicle.values.ValueModel.$$NATIVE;

/**
 * @author Rob Austin.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
class AbstractChronicleMapConverter<K, V> implements Converter {

    private final Map<K, V> map;
    private final Class<?> mapClazz;

    AbstractChronicleMapConverter(@NotNull Map<K, V> map) {
        this.map = map;
        this.mapClazz = map.getClass();
    }
    private static void debugReader(String label, HierarchicalStreamReader reader) { // TODO remove temporary CI diagnostics
        try { // TODO remove temporary CI diagnostics
            System.err.println("[CM-XSTREAM] " + label // TODO remove temporary CI diagnostics
                    + " readerClass=" + reader.getClass().getName() // TODO remove temporary CI diagnostics
                    + " node=" + reader.getNodeName() // TODO remove temporary CI diagnostics
                    + " hasMoreChildren=" + reader.hasMoreChildren() // TODO remove temporary CI diagnostics
                    + " value='" + reader.getValue() + "'"); // TODO remove temporary CI diagnostics
        } catch (Throwable t) { // TODO remove temporary CI diagnostics
            System.err.println("[CM-XSTREAM] " + label + " debug failed: " + t); // TODO remove temporary CI diagnostics
        } // TODO remove temporary CI diagnostics
    } // TODO remove temporary CI diagnostics

    private static <E> E deserialize(@NotNull UnmarshallingContext unmarshallingContext,
                                     @NotNull HierarchicalStreamReader reader) {

        switch (reader.getNodeName()) {

            case "java.util.Collections$EmptySet":
                return (E) Collections.emptySet();

            case "java.util.Collections$EmptyList":
                return (E) Collections.emptyList();

            case "java.util.Collections$EmptyMap":
            case "java.util.Collections.EmptyMap":
                return (E) Collections.emptyMap();

        }

        return (E) unmarshallingContext.convertAnother(null, forName(reader.getNodeName()));
    }

    private static Class<?> forName(String clazz) {

        try {
            return Class.forName(clazz);
        } catch (ClassNotFoundException e) {

            boolean isNative = clazz.endsWith($$NATIVE);
            boolean isHeap = clazz.endsWith($$HEAP);

            if (!isNative && !isHeap)
                throw new ConversionException("class=" + clazz, e);

            final String nativeInterface = isNative ?
                    clazz.substring(0, clazz.length() - $$NATIVE.length()) :
                    clazz.substring(0, clazz.length() - $$HEAP.length());
            try {
                Values.newNativeReference(Class.forName(clazz));
                return Class.forName(nativeInterface);
            } catch (Exception e1) {
                throw new ConversionException("class=" + clazz, e1);
            }
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean canConvert(Class aClass) {
        //noinspection unchecked
        return mapClazz.isAssignableFrom(aClass);
    }

    @Override
    public void marshal(Object o, HierarchicalStreamWriter writer, MarshallingContext
            marshallingContext) {

        for (Map.Entry e : (Iterable<Map.Entry>) ((Map) o).entrySet()) {

            writer.startNode("entry");
            {
                final Object key = e.getKey();
                writer.startNode(key.getClass().getName());
                marshallingContext.convertAnother(key);
                writer.endNode();

                Object value = e.getValue();
                writer.startNode(value.getClass().getName());
                marshallingContext.convertAnother(value);
                writer.endNode();
            }
            writer.endNode();
        }
    }

    @Override
    public Object unmarshal(HierarchicalStreamReader reader,
                            UnmarshallingContext context) {
        debugReader("unmarshal entry", reader); // TODO remove temporary CI diagnostics
        // empty map
        if ("[\"\"]".equals(reader.getValue()))
            return null;
        if (!"cmap".equals(reader.getNodeName()))
            throw new ConversionException("should be under 'cmap' node");
        while (reader.hasMoreChildren()) {
            debugReader("loop before entry moveDown", reader); // TODO remove temporary CI diagnostics
            reader.moveDown();
            debugReader("loop after entry moveDown", reader); // TODO remove temporary CI diagnostics

            final String nodeName0 = reader.getNodeName();

            if (!nodeName0.equals("entry"))
                throw new ConversionException("unable to convert node named=" + nodeName0);

            final K k;
            final V v;

            reader.moveDown();
            debugReader("key node", reader); // TODO remove temporary CI diagnostics
            k = deserialize(context, reader);
            reader.moveUp();
            debugReader("after key moveUp", reader); // TODO remove temporary CI diagnostics

            reader.moveDown();
            debugReader("value node", reader); // TODO remove temporary CI diagnostics
            v = deserialize(context, reader);
            reader.moveUp();
            debugReader("after value moveUp", reader); // TODO remove temporary CI diagnostics

            if (k != null)
                map.put(k, v);

            reader.moveUp();
            debugReader("after entry moveUp", reader); // TODO remove temporary CI diagnostics
        }
        debugReader("unmarshal exit", reader); // TODO remove temporary CI diagnostics
        return null;
    }
}
