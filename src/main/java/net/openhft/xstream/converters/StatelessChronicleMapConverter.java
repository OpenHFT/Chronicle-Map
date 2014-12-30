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
package net.openhft.xstream.converters;

import com.thoughtworks.xstream.converters.ConversionException;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import net.openhft.chronicle.map.MapEntryCallback;
import net.openhft.lang.model.constraints.NotNull;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * @author Rob Austin.
 */
public class StatelessChronicleMapConverter<K, V> extends AbstractChronicleMapConverter<K, V> {

    public StatelessChronicleMapConverter(@NotNull Map<K, V> map) {
        super(map);
    }

    @Override
    public void marshal(Object o, final HierarchicalStreamWriter writer, final MarshallingContext
            marshallingContext) {

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        try {

            final Method entrySize = o.getClass().getDeclaredMethod("entrySet", MapEntryCallback.class);
            entrySize.setAccessible(true);
            entrySize.invoke(o,

                    new MapEntryCallback() {

                        @Override
                        public void onEntry(Object key, Object value) {
                            writer.startNode("entry");
                            {
                                writer.startNode(key.getClass().getName());
                                marshallingContext.convertAnother(key);
                                writer.endNode();

                                writer.startNode(value.getClass().getName());
                                marshallingContext.convertAnother(value);
                                writer.endNode();
                            }
                            writer.endNode();
                        }

                        @Override
                        public void onFinished() {
                            countDownLatch.countDown();
                        }
                    });
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new ConversionException("", e);
        }


    }


}

