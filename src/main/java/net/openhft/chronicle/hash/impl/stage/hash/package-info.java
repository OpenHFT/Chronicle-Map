/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
/**
 * Hash-table level stages for Chronicle-Hash.
 * <p>
 * The stages in this package manage bucket chains, key-to-bucket
 * mapping and consistency checks for the underlying off-heap hash
 * table. They are composed with entry and data stages to form the
 * complete Chronicle-Map and Chronicle-Set implementations.
 * <p>
 * These types are internal and may evolve as hashing strategies and
 * invariants change.
 */
package net.openhft.chronicle.hash.impl.stage.hash;
