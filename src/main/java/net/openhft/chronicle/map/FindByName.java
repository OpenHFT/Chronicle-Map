/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.ChronicleHash;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author Rob Austin.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
interface FindByName {

    /**
     * @param name the name of the map or set
     * @param <T>  the type returned
     * @return a chronicle map or set
     * @throws IllegalArgumentException if a map with this name can not be found
     */
    <T extends ChronicleHash> T from(String name) throws IllegalArgumentException;
}
