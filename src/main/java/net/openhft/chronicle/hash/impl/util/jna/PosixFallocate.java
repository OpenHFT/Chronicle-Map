package net.openhft.chronicle.hash.impl.util.jna;

import net.openhft.chronicle.core.Jvm;
import net.openhft.posix.PosixAPI;

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;

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
