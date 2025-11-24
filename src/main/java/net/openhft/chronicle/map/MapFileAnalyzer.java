/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.impl.InternalMapFileAnalyzer;

import java.io.IOException;

/**
 * Command line entry point for the internal map file analyser.
 * <p>
 * This class simply delegates to
 * {@link net.openhft.chronicle.hash.impl.InternalMapFileAnalyzer} so that
 * Chronicle-Map can offer a stable public main class while keeping the
 * implementation details in the Chronicle-Hash module.
 */
public final class MapFileAnalyzer {

    public static void main(String[] args) throws IOException {
        InternalMapFileAnalyzer.main(args);
    }
}
