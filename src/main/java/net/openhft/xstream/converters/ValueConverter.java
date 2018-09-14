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

import com.thoughtworks.xstream.converters.ConversionException;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import net.openhft.chronicle.values.ArrayFieldModel;
import net.openhft.chronicle.values.FieldModel;
import net.openhft.chronicle.values.ValueModel;
import net.openhft.chronicle.values.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author Rob Austin.
 */
public class ValueConverter implements Converter {

    private static final Logger LOG = LoggerFactory.getLogger(ValueConverter.class);

    @Override
    public void marshal(Object o, HierarchicalStreamWriter writer, MarshallingContext context) {

        ValueModel valueModel = ValueModel.acquire(o.getClass());

        valueModel.fields().forEach(fieldModel -> {

            if (fieldModel instanceof ArrayFieldModel) {
                try {

                    final Method indexedGet = fieldModel.getOrGetVolatile();
                    indexedGet.setAccessible(true);

                    writer.startNode(fieldModel.name());
                    for (int i = 0; i < ((ArrayFieldModel) fieldModel).array().length(); i++) {
                        writer.startNode(Integer.toString(i));
                        context.convertAnother(indexedGet.invoke(o, i));
                        writer.endNode();
                    }
                    writer.endNode();

                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new ConversionException("", e);
                }

                return;
            }

            try {

                final Method get = fieldModel.getOrGetVolatile();
                get.setAccessible(true);
                final Object value = get.invoke(o);

                writer.startNode(fieldModel.name());
                context.convertAnother(value);
                writer.endNode();

            } catch (Exception e) {
                LOG.error("class=" + fieldModel.name(), e);
            }
        });

    }

    @Override
    public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context) {
        try {
            ValueModel valueModel = ValueModel.acquire(context.getRequiredType());
            Object result = valueModel.heapClass().newInstance();
            fillInObject(reader, context, valueModel, result);
            return result;
        } catch (Exception e) {
            throw new ConversionException(
                    "class=" + context.getRequiredType().getCanonicalName(), e);
        }
    }

    private void fillInObject(HierarchicalStreamReader reader, UnmarshallingContext context,
                              ValueModel valueModel, Object using) throws ClassNotFoundException {

        while (reader.hasMoreChildren()) {
            reader.moveDown();

            final String name = reader.getNodeName();

            FieldModel fieldModel =
                    valueModel.fields().filter(f -> f.name().equals(name)).findAny().get();

            if (fieldModel instanceof ArrayFieldModel) {

                while (reader.hasMoreChildren()) {

                    reader.moveDown();
                    try {
                        String index = reader.getNodeName();
                        int i = Integer.parseInt(index);
                        Method indexedSet = fieldModel.setOrSetOrderedOrSetVolatile();
                        indexedSet.setAccessible(true);
                        Class<?>[] parameterTypes = indexedSet.getParameterTypes();
                        Object value = context.convertAnother(null, parameterTypes[1]);
                        indexedSet.invoke(using, i, value);
                    } catch (Exception e) {
                        throw new ConversionException("", e);
                    }

                    reader.moveUp();
                }
                reader.moveUp();
                continue;
            }

            Method set = fieldModel.setOrSetOrderedOrSetVolatile();
            set.setAccessible(true);
            final Class<?>[] parameterTypes = set.getParameterTypes();
            final Object value = context.convertAnother(null, parameterTypes[0]);

            try {
                set.invoke(using, value);
            } catch (Exception e) {
                throw new ConversionException("", e);
            }

            reader.moveUp();
        }
    }

    @Override
    public boolean canConvert(Class clazz) {
        return Values.isValueInterfaceOrImplClass(clazz);
    }
}
