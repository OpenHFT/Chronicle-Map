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

package net.openhft.chronicle.hash.impl.util;

import net.openhft.chronicle.core.Jvm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class CleanerUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(CleanerUtils.class);

    private static final Method CREATE_METHOD;
    private static final Method CLEAN_METHOD;

    static {
        try {
            Class<?> cleanerClass = Class.forName(Jvm.isJava9Plus() ? "jdk.internal.ref.Cleaner" : "sun.misc.Cleaner");
            CREATE_METHOD = cleanerClass.getDeclaredMethod("create", Object.class, Runnable.class);
            Jvm.setAccessible(CREATE_METHOD);
            CLEAN_METHOD = cleanerClass.getDeclaredMethod("clean");
            Jvm.setAccessible(CLEAN_METHOD);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            LOGGER.error("Unable to initialise CleanerUtils", e);
            throw new RuntimeException(e);
        }
    }

    public static Cleaner createCleaner(Object ob, Runnable thunk) {
        try {
            Object cleanerInstance = CREATE_METHOD.invoke(null, ob, thunk);
            return () -> doClean(cleanerInstance);
        } catch (IllegalAccessException | InvocationTargetException e) {
            LOGGER.error("Unable to create cleaner", e);
            throw new RuntimeException(e);
        }
    }

    private static void doClean(Object cleanerInstance) {
        try {
            CLEAN_METHOD.invoke(cleanerInstance);
        } catch (IllegalAccessException | InvocationTargetException e) {
            LOGGER.warn("Failed to clean buffer", e);
        }
    }
}
