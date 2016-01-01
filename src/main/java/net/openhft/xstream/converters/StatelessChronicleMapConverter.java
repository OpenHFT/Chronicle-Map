/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

