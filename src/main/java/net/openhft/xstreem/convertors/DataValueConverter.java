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
import net.openhft.lang.values.StringValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.*;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;

import static java.beans.Introspector.getBeanInfo;


/**
 * @author Rob Austin.
 */
public class DataValueConverter implements Converter {

    private static final Logger LOG = LoggerFactory.getLogger(DataValueConverter.class);


    @Override
    public void marshal(Object o, HierarchicalStreamWriter writer, MarshallingContext context) {

        final String canonicalName = o.getClass().getCanonicalName();

        boolean isNative = canonicalName.endsWith("$$Native");
        boolean isHeap = canonicalName.endsWith("$$Heap");

        if (!isNative && !isHeap)
            return;

        if (canonicalName.startsWith("net.openhft.lang.values")) {

            Method[] methods = o.getClass().getMethods();

            for (Method method : methods) {

                if (!method.getName().equals("getValue") ||
                        method.getParameterTypes().length != 0) {
                    continue;
                }

                try {
                    context.convertAnother(method.invoke(o));
                    return;
                } catch (Exception e) {
                    throw new ConversionException("class=" + canonicalName, e);
                }
            }

            throw new ConversionException("class=" + canonicalName);
        }


        try {

            final BeanInfo info = getBeanInfo(o.getClass());  //  new BeanGenerator

            for (PropertyDescriptor p : info.getPropertyDescriptors()) {

                if (p instanceof IndexedPropertyDescriptor) {
                    try {

                        final IndexedPropertyDescriptor indexedPropertyDescriptor = (IndexedPropertyDescriptor) p;
                        final Method indexedReadMethod = indexedPropertyDescriptor.getIndexedReadMethod();

                        if (indexedReadMethod == null)
                            continue;

                        Class<?> returnType = indexedReadMethod.getReturnType();

                        if (returnType == null) {
                            continue;
                        }

                        final String simpleName = returnType.getSimpleName();
                        final String fieldName = "_" + toCamelCase(simpleName);

                        Field field = o.getClass().getDeclaredField(fieldName);
                        field.setAccessible(true);

                        final Object[] o1 = (Object[]) field.get(o);

                        if (o1 == null)
                            continue;

                        writer.startNode(p.getName());
                        int i = 0;
                        for (Object f : o1) {
                            if (f == null) {
                                continue;
                            }
                            writer.startNode(Integer.toString(i++));
                            context.convertAnother(f);
                            writer.endNode();
                        }
                        writer.endNode();

                    } catch (NoSuchFieldException | IllegalAccessException e1) {
                        LOG.error("", e1);
                    }
                }

                if (p.getName().equals("Class") ||
                        p.getReadMethod() == null ||
                        p.getWriteMethod() == null)
                    continue;

                try {

                    final Method readMethod = p.getReadMethod();
                    final Object value = readMethod.invoke(o);

                    if (value == null)
                        return;

                    writer.startNode(p.getDisplayName());
                    context.convertAnother(value);
                    writer.endNode();

                } catch (Exception e) {
                    LOG.error("class=" + p.getName(), e);
                }


            }

        } catch (IntrospectionException e) {
            throw new ConversionException("class=" + canonicalName, e);
        }
    }


    static String toCamelCase(String s) {

        final StringBuilder b = new StringBuilder();

        if (s.length() > 0)
            b.append(Character.toLowerCase(s.charAt(0)));

        if (s.length() > 0)
            b.append(s.substring(1));

        return b.toString();

    }


    @Override
    public Object unmarshal(HierarchicalStreamReader reader,
                            UnmarshallingContext context) {

        final String canonicalName = context.getRequiredType().getCanonicalName();

        boolean isNative = canonicalName.endsWith("$$Native");
        boolean isHeap = canonicalName.endsWith("$$Heap");

        if (!isNative && !isHeap)
            return null;

        if (context.getRequiredType().getName().startsWith
                ("net.openhft.lang.values"))
            return toNativeValueObjects(reader, context.getRequiredType(), context);

        final String nodeName = isNative ?

                canonicalName.substring(0, canonicalName.length() -
                        "$$Native".length()) :

                canonicalName.substring(0, canonicalName.length() -
                        "$$Heap".length());

        try {

            final Class<?> interfaceClass = Class.forName(nodeName);

            final Object result = (isNative) ?
                    DataValueClasses.newDirectInstance(interfaceClass) :
                    DataValueClasses.newInstance(interfaceClass);

            fillInObject(reader, context, result);

            return result;

        } catch (Exception e) {
            throw new ConversionException("class=" + canonicalName, e);
        }

    }

    private void fillInObject(HierarchicalStreamReader reader, UnmarshallingContext context, Object using) throws IntrospectionException {
        final BeanInfo beanInfo = getBeanInfo(using.getClass());

        while (reader.hasMoreChildren()) {
            reader.moveDown();

            final String name = reader.getNodeName();

            for (PropertyDescriptor descriptor : beanInfo.getPropertyDescriptors()) {

                if (!descriptor.getName().equals(name))
                    continue;

                if ((descriptor instanceof IndexedPropertyDescriptor)) {
                    IndexedPropertyDescriptor indexedPropertyDescriptor = (IndexedPropertyDescriptor) descriptor;
                    while (reader.hasMoreChildren()) {

                        reader.moveDown();
                        try {
                            String index = reader.getNodeName();
                            int i = Integer.parseInt(index);
                            Method writeMethod = indexedPropertyDescriptor.getIndexedWriteMethod();
                            Class<?>[] parameterTypes = writeMethod.getParameterTypes();

                            if (parameterTypes.length == 2) {
                                final Method indexedReadMethod = indexedPropertyDescriptor.getIndexedReadMethod();
                                final Object instance = indexedReadMethod.invoke(using, i);
                                fillInObject(reader, context, instance);
                            }
                        } catch (IllegalAccessException | InvocationTargetException e) {
                            LOG.error("", e);
                        }

                        reader.moveUp();
                    }
                    continue;
                }

                final Class<?>[] parameterTypes = descriptor.getWriteMethod().getParameterTypes();

                if (parameterTypes.length != 1)
                    continue;

                final Object object = context.convertAnother(null, parameterTypes[0]);

                try {
                    descriptor.getWriteMethod().invoke(using, object);
                } catch (Exception e) {
                    LOG.error("", e);
                }

                break;
            }

            reader.moveUp();
        }
    }


    void convertUsing(final UnmarshallingContext context, final Object using, HierarchicalStreamReader reader) throws IllegalAccessException, IntrospectionException, InvocationTargetException {
        UnmarshallingContext context1 = new UnmarshallingContext() {

            @Override
            public Object get(Object o) {
                return context.get(o);
            }

            @Override
            public void put(Object o, Object o1) {
                context.put(o, o1);
            }

            @Override
            public Iterator keys() {
                return context.keys();
            }

            @Override
            public Object convertAnother(Object o, Class aClass) {
                return context.convertAnother(o, aClass);
            }

            @Override
            public Object convertAnother(Object o, Class aClass, Converter converter) {
                return context.convertAnother(o, aClass, converter);
            }

            @Override
            public Object currentObject() {
                return using;
            }

            @Override
            public Class getRequiredType() {
                return using.getClass();
            }

            @Override
            public void addCompletionCallback(Runnable runnable, int i) {
                context.addCompletionCallback(runnable, i);
            }
        };

        fillInObject(reader, context1, using);

    }

    static Object toNativeValueObjects(HierarchicalStreamReader reader,
                                       final Class aClass,
                                       UnmarshallingContext context) {

        final Object o = DataValueClasses.newDirectInstance(aClass);

        try {

            final BeanInfo info = Introspector.getBeanInfo(o.getClass());  //  new BeanGenerator

            for (PropertyDescriptor p : info.getPropertyDescriptors()) {

                if (!p.getName().equals("value"))
                    continue;

                final String value = reader.getValue();

                if (StringValue.class.isAssignableFrom(o.getClass())) {
                    ((StringValue) o).setValue(value);
                    return o;
                }

                final Class<?> propertyType = p.getPropertyType();

                final Object o1 = context.convertAnother(value, propertyType);
                p.getWriteMethod().invoke(o, o1);

                return o;

            }

        } catch (Exception e) {
            //
        }

        throw new ConversionException("setValue(..) method not found in class=" +
                aClass.getCanonicalName());
    }


    @Override
    public boolean canConvert(Class clazz) {
        final String canonicalName = clazz.getCanonicalName();

        return canonicalName.startsWith("net.openhft.lang.values") ||
                canonicalName.endsWith("$$Native") ||
                canonicalName.endsWith("$$Heap");
    }
}
