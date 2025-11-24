/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
/**
 * Iteration stages for scanning Chronicle-Hash segments.
 * <p>
 * This package provides staged components that traverse segments and
 * entries to support map and set iteration. They encapsulate cursor
 * state, allocation strategy and recovery of partially written entries.
 * <p>
 * The classes are internal and tuned for performance; callers should
 * use the public iterator and query APIs rather than these stages.
 */
package net.openhft.chronicle.hash.impl.stage.iter;
