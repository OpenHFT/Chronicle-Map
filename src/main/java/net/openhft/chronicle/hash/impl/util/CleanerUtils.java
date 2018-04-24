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
            Class cleanerClass = Class.forName(Jvm.isJava9Plus() ? "jdk.internal.ref.Cleaner" : "sun.misc.Cleaner");
            CREATE_METHOD = cleanerClass.getDeclaredMethod("create", Object.class, Runnable.class);
            CLEAN_METHOD = cleanerClass.getDeclaredMethod("clean");
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
