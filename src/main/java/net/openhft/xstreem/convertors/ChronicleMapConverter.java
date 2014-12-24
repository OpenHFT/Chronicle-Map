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
package net.openhft.xstreem.convertors;

import com.thoughtworks.xstream.converters.ConversionException;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.model.constraints.NotNull;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;

/**
 * @author Rob Austin.
 */
public class ChronicleMapConverter<K, V> implements Converter {

    private final Class entrySetClass;
    private final Class writeThroughEntryClass;
    private final Map<K, V> map;

    public ChronicleMapConverter(@NotNull Map<K, V> map) {

        final String vanillaChronicleMap = "net.openhft.chronicle.map.VanillaChronicleMap";

        try {
            this.entrySetClass = Class.forName(vanillaChronicleMap + "$EntrySet");
            this.writeThroughEntryClass = Class.forName(vanillaChronicleMap + "$WriteThroughEntry");
        } catch (ClassNotFoundException e) {
            throw new ConversionException("", e);
        }

        this.map = map;
    }


    @Override
    public boolean canConvert(Class aClass) {

        //noinspection unchecked
        return entrySetClass.isAssignableFrom(aClass) ||
                writeThroughEntryClass.isAssignableFrom(aClass);

    }

    @Override
    public void marshal(Object o, HierarchicalStreamWriter writer, MarshallingContext
            marshallingContext) {

        if (entrySetClass.isAssignableFrom(o.getClass())) {

            for (Map.Entry e : (Iterable<Map.Entry>) o) {
                writer.startNode("entry");
                marshallingContext.convertAnother(e);
                writer.endNode();
            }
            return;
        }

        final AbstractMap.SimpleEntry e = (AbstractMap.SimpleEntry) o;

        final Object key = e.getKey();
        writer.startNode(key.getClass().getName());
        marshallingContext.convertAnother(key);
        writer.endNode();

        Object value = e.getValue();
        writer.startNode(value.getClass().getName());
        marshallingContext.convertAnother(value);
        writer.endNode();

    }

    @Override
    public Object unmarshal(HierarchicalStreamReader reader,
                            UnmarshallingContext context) {

        final String nodeName = reader.getNodeName();

        if (!"cmap".equals(nodeName))
            return null;

        while (reader.hasMoreChildren()) {
            reader.moveDown();

            final String nodeName0 = reader.getNodeName();

            if (!nodeName0.equals("entry"))
                throw new ConversionException("unable to convert node named=" + nodeName0);

            final K k;
            final V v;

            reader.moveDown();
            k = deserialize(context, reader);
            reader.moveUp();

            reader.moveDown();
            v = deserialize(context, reader);
            reader.moveUp();

            if (k != null)
                map.put(k, v);

            reader.moveUp();
        }

        return null;
    }

    static <E> E deserialize(@NotNull UnmarshallingContext unmarshallingContext,
                             @NotNull HierarchicalStreamReader reader) {

        switch (reader.getNodeName()) {

            case "java.util.Collections$EmptySet":
                return (E) Collections.EMPTY_SET;

            case "java.util.Collections$EmptyList":
                return (E) Collections.EMPTY_LIST;

            case "java.util.Collections$EmptyMap":
            case "java.util.Collections.EmptyMap":
                return (E) Collections.EMPTY_MAP;

        }

        return (E) unmarshallingContext.convertAnother(null, forName(reader.getNodeName()));
    }

    static Class forName(String clazz) {

        try {
            return Class.forName(clazz);
        } catch (ClassNotFoundException e) {

            boolean isNative = clazz.endsWith("$$Native");
            boolean isHeap = clazz.endsWith("$$Heap");

            if (!isNative && !isHeap)
                throw new ConversionException("class=" + clazz, e);

            final String nativeInterface = isNative ? clazz.substring(0, clazz.length() -
                    "$$Native".length()) : clazz.substring(0, clazz.length() -
                    "$$Heap".length());
            try {
                DataValueClasses.newDirectInstance(Class.forName(clazz));
                return Class.forName(nativeInterface);
            } catch (Exception e1) {
                throw new ConversionException("class=" + clazz, e1);
            }

        }
    }

}

