/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.util;

import net.openhft.chronicle.core.Jvm;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Bridges Chronicle code to the JDK's cleaner mechanism.
 *
 * <p>This utility locates the appropriate cleaner implementation for the current Java
 * version and exposes it via the lightweight {@link Cleaner} interface. It is used to
 * arrange explicit cleanup of direct buffers and other off-heap state without relying
 * on finalisers.
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
