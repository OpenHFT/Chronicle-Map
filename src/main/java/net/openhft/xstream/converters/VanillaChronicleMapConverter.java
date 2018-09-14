/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

