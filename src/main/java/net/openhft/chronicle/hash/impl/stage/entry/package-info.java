/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
/**
 * Staged operations over individual Chronicle-Hash entries.
 * <p>
 * This package defines stages that locate, lock and mutate entries in
 * off-heap hash segments. Responsibilities include computing key
 * hashes, walking buckets, applying checksums and coordinating read,
 * write and update locks.
 * <p>
 * All classes are internal implementation details and are not part of
 * the public Chronicle-Map or Chronicle-Set APIs.
 */
package net.openhft.chronicle.hash.impl.stage.entry;

