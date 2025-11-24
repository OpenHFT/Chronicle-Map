/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
/**
 * Locking primitives built on top of Chronicle-Map.
 * <p>
 * Types in this package expose lock implementations backed by
 * Chronicle-Map entries, for example stamped locks that can be shared
 * across processes.
 * <p>
 * They are intended for advanced use-cases where cross-process locking
 * is required; typical applications should prefer in-JVM locking
 * mechanisms unless distributed semantics are needed.
 */
package net.openhft.chronicle.map.locks;
