/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.ChronicleHashCorruption;
import org.jetbrains.annotations.Nullable;
import org.slf4j.helpers.MessageFormatter;

import java.util.function.Supplier;

/**
 * Default {@link ChronicleHashCorruption} implementation used by Chronicle-Map.
 * <p>
 * Instances of this class capture information about suspected on-disk or
 * in-memory corruption, including the affected segment index, a lazily
 * evaluated message, and an optional cause. Helper methods route these
 * instances to a {@link ChronicleHashCorruption.Listener}.
 * <p>
 * Callers normally use the static {@code report} or {@code reportException}
 * methods rather than constructing this type directly.
 */
public class ChronicleHashCorruptionImpl implements ChronicleHashCorruption {

    private int segmentIndex;
    private Supplier<String> messageSupplier;
    private Throwable exception;
    private String message;

    public static void report(
            ChronicleHashCorruption.Listener corruptionListener,
            ChronicleHashCorruptionImpl corruption, int segmentIndex,
            Supplier<String> messageSupplier) {
        corruption.set(segmentIndex, messageSupplier, null);
        corruptionListener.onCorruption(corruption);
    }

    public static void reportException(
            ChronicleHashCorruption.Listener corruptionListener,
            ChronicleHashCorruptionImpl corruption, int segmentIndex,
            Supplier<String> messageSupplier, Throwable exception) {
        corruption.set(segmentIndex, messageSupplier, exception);
        corruptionListener.onCorruption(corruption);
    }

    public static String format(String message, Object... args) {
        return MessageFormatter.arrayFormat(message, args).getMessage();
    }

    private void set(int segmentIndex, Supplier<String> messageSupplier, Throwable exception) {
        this.segmentIndex = segmentIndex;
        this.messageSupplier = messageSupplier;
        this.exception = exception;
        this.message = null;
    }

    @Override
    public String message() {
        if (message == null) {
            message = messageSupplier.get();
        }
        return message;
    }

    @Nullable
    @Override
    public Throwable exception() {
        return exception;
    }

    @Override
    public int segmentIndex() {
        return segmentIndex;
    }
}
