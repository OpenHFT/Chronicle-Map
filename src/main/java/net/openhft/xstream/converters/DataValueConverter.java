/*
 *      Copyright (C) 2015  higherfrequencytrading.com
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
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.model.DataValueModel;
import net.openhft.lang.model.DataValueModels;
import net.openhft.lang.model.FieldModel;
import net.openhft.lang.values.StringValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;


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
            DataValueModel<?> dataValueModel = DataValueModels.acquireModel(interfaceClass(o.getClass()));

            for (Map.Entry<String, ? extends FieldModel> p : dataValueModel.fieldMap().entrySet()) {

                final FieldModel fileModel = p.getValue();

                if (fileModel.indexedGetter() != null) {
                    try {

                        final Method indexedReadMethod = fileModel.indexedGetter();

                        if (indexedReadMethod == null)
                            continue;

                        final Class<?> returnType = indexedReadMethod.getReturnType();

                        if (returnType == null)
                            continue;

                        final String fieldName = "_" + fileModel.name();

                        Field field;

                        try {
                            field = o.getClass().getDeclaredField(fieldName);
                        } catch (NoSuchFieldException e1) {

                            throw new ConversionException("JSON conversion of Classes containing " +
                                    "arrays is not " +
                                    "currently supported" +
                                    ", the following field could not be found=" + fileModel.name(), e1);
                        }

                        field.setAccessible(true);

                        final Object[] o1 = (Object[]) field.get(o);

                        if (o1 == null)
                            continue;
                        writer.startNode(fileModel.name());
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

                    } catch (IllegalAccessException e) {
                        throw new ConversionException("", e);
                    }

                    continue;
                }


                try {

                    final Method readMethod = fileModel.getter();

                    if (readMethod == null)
                        continue;

                    readMethod.setAccessible(true);

                    final Object value = readMethod.invoke(o);

                    if (value == null)
                        return;

                    writer.startNode(fileModel.name());
                    context.convertAnother(value);
                    writer.endNode();

                } catch (Exception e) {
                    LOG.error("class=" + fileModel.name(), e);
                }


            }

        } catch (ClassNotFoundException e) {
            throw new ConversionException("class=" + canonicalName, e);
        }
    }

    private Class interfaceClass(Class clazz) throws ClassNotFoundException {
        String className = clazz.getName();
        boolean isNative = className.endsWith("$$Native");
        boolean isHeap = className.endsWith("$$Heap");

        if (!isNative && !isHeap)
            throw new ClassNotFoundException();

        final String nodeName = isNative ?

                className.substring(0, className.length() -
                        "$$Native".length()) :

                className.substring(0, className.length() -
                        "$$Heap".length());

        return Class.forName(nodeName);
    }


    @Override
    public Object unmarshal(HierarchicalStreamReader reader,
                            UnmarshallingContext context) {

        final String canonicalName = context.getRequiredType().getName();

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

    private void fillInObject(HierarchicalStreamReader reader, UnmarshallingContext context, Object using) throws ClassNotFoundException {

        while (reader.hasMoreChildren()) {
            reader.moveDown();

            final String name = reader.getNodeName();

            DataValueModel<?> dataValueModel = DataValueModels.acquireModel(interfaceClass(using.getClass()));

            FieldModel fieldModel = dataValueModel.fieldMap().get(name);

            if (fieldModel.indexedGetter() != null) {

                while (reader.hasMoreChildren()) {

                    reader.moveDown();
                    try {
                        String index = reader.getNodeName();
                        int i = Integer.parseInt(index);
                        Method writeMethod = fieldModel.indexedSetter();
                        Class<?>[] parameterTypes = writeMethod.getParameterTypes();

                        if (parameterTypes.length == 2) {
                            final Method indexedReadMethod = fieldModel.indexedGetter();
                            indexedReadMethod.setAccessible(true);
                            final Object instance = indexedReadMethod.invoke(using, i);
                            fillInObject(reader, context, instance);
                        }
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        throw new ConversionException("", e);
                    }

                    reader.moveUp();
                }
                reader.moveUp();
                continue;
            }

            Method setter = fieldModel.setter();
            setter.setAccessible(true);

            final Class<?>[] parameterTypes = setter.getParameterTypes();

            if (parameterTypes.length != 1)
                continue;

            final Object object = context.convertAnother(null, parameterTypes[0]);

            try {
                setter.invoke(using, object);
            } catch (Exception e) {
                throw new ConversionException("", e);
            }

            reader.moveUp();
        }
    }


    private static Object toNativeValueObjects(HierarchicalStreamReader reader,
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
            throw new ConversionException("class=" + aClass.getName(), e);
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
