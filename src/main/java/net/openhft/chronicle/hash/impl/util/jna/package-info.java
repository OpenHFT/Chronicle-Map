/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
/**
 * JNA-based native integrations for Chronicle-Hash utilities.
 * <p>
 * This package currently provides access to POSIX-specific operations
 * such as {@code fallocate} used to pre-size map backing files. The
 * classes are isolated here so they can be substituted or disabled on
 * platforms where JNA or the underlying calls are unavailable.
 */
package net.openhft.chronicle.hash.impl.util.jna;

