/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.set;

/**
 * Placeholder value used by Chronicle Set implementations that are backed by Chronicle Map.
 * A single shared enum instance keeps the map footprint minimal while still satisfying the
 * map API contract that every key has an associated value.
 */
enum DummyValue {
    DUMMY_VALUE
}
