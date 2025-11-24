/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.util.jna;

import net.openhft.chronicle.core.Jvm;
import net.openhft.posix.PosixAPI;

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;

/**
 * Thin wrapper around {@code posix_fallocate} exposed via {@link PosixAPI}.
 *
 * <p>Used to preallocate space for backing files on POSIX platforms so that Chronicle
 * data structures can rely on the file size and avoid fragmentation-related latency
 * spikes during growth.
 */
public final class PosixFallocate {

    private PosixFallocate() {
    }

    public static void fallocate(FileDescriptor descriptor, long offset, long length) throws IOException {
        int fd = getNativeFileDescriptor(descriptor);
        if (fd != -1) {
            int ret = PosixAPI.posix().fallocate(getNativeFileDescriptor(descriptor), 0, offset, length);
            if (ret != 0) {
                throw new IOException("posix_fallocate() returned " + ret);
            }
        }
    }

    private static int getNativeFileDescriptor(FileDescriptor descriptor) throws IOException {
        try {
            final Field field = descriptor.getClass().getDeclaredField("fd");
            Jvm.setAccessible(field);
            return (int) field.get(descriptor);
        } catch (final Exception e) {
            throw new IOException("unsupported FileDescriptor implementation", e);
        }
    }
}
