/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
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

package net.openhft.chronicle.map;

import com.thoughtworks.xstream.converters.ConversionException;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import net.openhft.lang.model.DataValueClasses;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;

/**
 * Created by Rob on 18/12/14.
 */
class XStreamHelper {
    static Object to$$Native2(HierarchicalStreamReader reader, final Class aClass2, final boolean isKey,
                              ChronicleMap map, UnmarshallingContext unmarshallingContext, String type) {


        if (type.endsWith("$$Native")) {

            Class<?> aClass = null;
            try {
                aClass = Class.forName(type);
            } catch (ClassNotFoundException e) {
                String clazz = type.substring(0, type.length() -
                        "$$Native".length());
                try {
                    DataValueClasses.newDirectInstance(Class.forName(clazz));
                } catch (ClassNotFoundException e1) {
                    throw new ConversionException(e1);
                }
                try {
                    aClass = Class.forName(type);
                } catch (ClassNotFoundException e1) {
                    throw new ConversionException(e1);
                }
            }


            return unmarshallingContext.convertAnother(null, aClass);
        }

        throw new ConversionException("setValue(..) method not found in class=" + type);
    }

    static Object to$$Native(HierarchicalStreamReader reader, final Class aClass, final boolean isKey, ChronicleMap map, UnmarshallingContext unmarshallingContext) {


        final Object o = isKey ? map.newKeyInstance() : map.newValueInstance();

        try {

            final BeanInfo info = Introspector.getBeanInfo(o.getClass());  //  new BeanGenerator

            for (PropertyDescriptor p : info.getPropertyDescriptors()) {

                if (p.getName().equals("value")) {

                    final String value = reader.getValue();
                    Class<?> parameterType = p.getPropertyType();

                    if (parameterType.isPrimitive()) {

                        // convert the primitive to their boxed type

                        if (parameterType == int.class)
                            parameterType = Integer.class;
                        else if (parameterType == char.class)
                            parameterType = Character.class;
                        else {
                            final String name = parameterType.getSimpleName();

                            final String properName = "java.lang." + Character.toString
                                    (name.charAt(0)).toUpperCase()
                                    + name.substring(1);
                            parameterType = Class.forName(properName);
                        }
                    }


                    final Method valueOf = parameterType.getMethod("valueOf", String.class);
                    final Object boxedValue = valueOf.invoke(null, value);

                    p.getWriteMethod().invoke(o, boxedValue);
                    return o;
                }
            }

        } catch (Exception e) {
            //
        }

        throw new ConversionException("setValue(..) method not found in class=" + aClass
                .getCanonicalName());
    }
}
