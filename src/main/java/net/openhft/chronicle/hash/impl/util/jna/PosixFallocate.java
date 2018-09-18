package net.openhft.chronicle.hash.impl.util.jna;

import com.sun.jna.Native;
import com.sun.jna.Platform;
import net.openhft.chronicle.core.Jvm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileDescriptor;
import java.lang.reflect.Field;

public final class PosixFallocate {

    private static final Logger LOG =
            LoggerFactory.getLogger(PosixFallocate.class.getName());

    static {
        Native.register(Platform.C_LIBRARY_NAME);
    }

    private PosixFallocate() {
    }

    public static void fallocate(FileDescriptor descriptor, long offset, long length) {
        int fd = getNativeFileDescriptor(descriptor);
        if (fd != -1) {
            int ret = posix_fallocate(getNativeFileDescriptor(descriptor), offset, length);
            if (ret != 0) {
                LOG.warn("posix_fallocate() returned {}", ret);
            }
        }
    }

    private static native int posix_fallocate(int fd, long offset, long length);

    private static int getNativeFileDescriptor(FileDescriptor descriptor) {
        try {
            final Field field = descriptor.getClass().getDeclaredField("fd");
            Jvm.setAccessible(field);
            return (int) field.get(descriptor);
        } catch (final Exception e) {
            LOG.warn("unsupported FileDescriptor implementation: e={}", e.getLocalizedMessage());
            return -1;
        }
    }

}
