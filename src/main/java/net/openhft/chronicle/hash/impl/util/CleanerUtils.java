/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.util;

import net.openhft.chronicle.core.Jvm;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Reflection-based helpers for creating JDK cleaners without a hard dependency on internal APIs.
 */
public class CleanerUtils {

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
            Jvm.error().on(CleanerUtils.class, "Unable to initialise CleanerUtils", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a cleaner for the given object and cleanup action.
     *
     * @param ob    object to associate
     * @param thunk runnable invoked on clean
     * @return cleaner wrapper
     */
    public static Cleaner createCleaner(Object ob, Runnable thunk) {
        try {
            Object cleanerInstance = CREATE_METHOD.invoke(null, ob, thunk);
            return () -> doClean(cleanerInstance);
        } catch (IllegalAccessException | InvocationTargetException e) {
            Jvm.error().on(CleanerUtils.class, "Unable to create cleaner", e);
            throw new RuntimeException(e);
        }
    }

    private static void doClean(Object cleanerInstance) {
        try {
            CLEAN_METHOD.invoke(cleanerInstance);
        } catch (IllegalAccessException | InvocationTargetException e) {
            Jvm.warn().on(CleanerUtils.class, "Failed to clean buffer", e);
        }
    }
}
