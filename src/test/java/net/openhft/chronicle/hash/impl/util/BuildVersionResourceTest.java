/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.util;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class BuildVersionResourceTest {
    @Test
    public void closesVersionResourceAndCachesResult() throws Exception {
        VersionLoader loader = new VersionLoader(" 2026.99\n", false);
        Class<?> type = loader.loadClass(BuildVersion.class.getName());
        assertEquals("2026.99", type.getMethod("version").invoke(null));
        assertTrue(loader.closed, "Version resource was not closed");
        assertEquals("2026.99", type.getMethod("version").invoke(null));
        assertEquals(1, loader.opens);
    }

    @Test
    public void closesEmptyVersionResource() throws Exception {
        VersionLoader loader = new VersionLoader("", false);
        loader.loadClass(BuildVersion.class.getName()).getMethod("version").invoke(null);
        assertTrue(loader.closed, "Empty version resource was not closed");
    }

    @Test
    public void closesVersionResourceAfterReadFailure() throws Exception {
        VersionLoader loader = new VersionLoader("unused", true);
        loader.loadClass(BuildVersion.class.getName()).getMethod("version").invoke(null);
        assertTrue(loader.closed, "Failed version resource was not closed");
    }

    private static final class VersionLoader extends ClassLoader {
        private final byte[] contents;
        private final boolean failRead;
        boolean closed;
        int opens;

        VersionLoader(String contents, boolean failRead) {
            super(BuildVersion.class.getClassLoader());
            this.contents = contents.getBytes(StandardCharsets.UTF_8);
            this.failRead = failRead;
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (!name.equals(BuildVersion.class.getName()))
                return super.loadClass(name, resolve);
            Class<?> type = findLoadedClass(name);
            if (type == null) {
                try (InputStream input = getParent().getResourceAsStream(name.replace('.', '/') + ".class")) {
                    ByteArrayOutputStream output = new ByteArrayOutputStream();
                    byte[] buffer = new byte[4096];
                    for (int n; (n = input.read(buffer)) != -1; )
                        output.write(buffer, 0, n);
                    byte[] bytes = output.toByteArray();
                    type = defineClass(name, bytes, 0, bytes.length);
                } catch (IOException e) {
                    throw new ClassNotFoundException(name, e);
                }
            }
            if (resolve)
                resolveClass(type);
            return type;
        }

        @Override
        public InputStream getResourceAsStream(String name) {
            if (!"map.version".equals(name))
                return super.getResourceAsStream(name);
            opens++;
            return new InputStream() {
                final InputStream input = new ByteArrayInputStream(contents);

                @Override
                public int read() throws IOException {
                    if (failRead)
                        throw new IOException("deliberate read failure");
                    return input.read();
                }

                @Override
                public void close() throws IOException {
                    closed = true;
                    input.close();
                }
            };
        }
    }
}
