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
class AbstractChronicleMapConverter<K, V> implements Converter {

    private final Map<K, V> map;
    private final Class mapClazz;

    AbstractChronicleMapConverter(@NotNull Map<K, V> map) {
        this.map = map;
        this.mapClazz = map.getClass();
    }

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

    private static Class forName(String clazz) {

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
        // empty map
        if ("[\"\"]".equals(reader.getValue()))
            return null;
        if (!"cmap".equals(reader.getNodeName()))
            throw new ConversionException("should be under 'cmap' node");
        reader.moveDown();
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
        reader.moveUp();
        return null;
    }

}

