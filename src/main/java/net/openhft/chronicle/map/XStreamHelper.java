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
import com.thoughtworks.xstream.io.HierarchicalStreamReader;

import java.lang.reflect.Method;

/**
 * Created by Rob on 18/12/14.
 */
class XStreamHelper {

    static Object to$$Native(HierarchicalStreamReader reader, final Class aClass, final boolean isKey, ChronicleMap map) {


        final Object o = isKey ? map.newKeyInstance() : map.newValueInstance();
        try {
            for (Method $$nativeMethod : o.getClass().getMethods()) {
                if ($$nativeMethod.getName().equals("setValue") && $$nativeMethod.getParameterTypes()
                        .length == 1) {

                    Class<?> parameterType = $$nativeMethod.getParameterTypes()[0];
                    String value = reader.getValue();


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
                    final Object invoke = valueOf.invoke(null, value);
                    $$nativeMethod.invoke(o, invoke);
                    return o;

                }
            }
        } catch (Exception e) {
            throw new ConversionException("class=" + aClass.getCanonicalName(), e);
        }
        throw new ConversionException("setValue(..) method not found in class=" + aClass
                .getCanonicalName
                        ());
    }
}
