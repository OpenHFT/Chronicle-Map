/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver;
import net.openhft.chronicle.core.Jvm;
import net.openhft.xstream.converters.*;

import java.io.*;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author Rob Austin.
 */
final class JsonSerializer {
    private JsonSerializer() {
    }

    static final String LOG_ERROR_SUGGEST_X_STREAM =
            "map.getAll(<file>) and map.putAll(<file>) methods require the JSON XStream serializer, " +
                    "we don't include these artifacts by default as some users don't require this functionality. " +
                    "Please add the following artifacts to your project\n" +
                    "<dependency>\n" +
                    " <groupId>com.thoughtworks.xstream</groupId>\n" +
                    " <artifactId>xstream</artifactId>\n" +
                    " <version>1.4.20</version>\n" +
                    "</dependency>\n" +
                    "<dependency>\n" +
                    " <groupId>org.codehaus.jettison</groupId>\n" +
                    " <artifactId>jettison</artifactId>\n" +
                    " <version>1.5.4</version>\n" +
                    "</dependency>\n";

    static synchronized <K, V> void getAll(final File toFile,
                                           final Map<K, V> map,
                                           final List<?> jsonConverters) throws IOException {
        final XStream xstream = xStream(map, jsonConverters);

        try (OutputStream outputStream = createOutputStream(toFile)) {
            xstream.toXML(map, outputStream);
        }
    }

    static synchronized <K, V> void putAll(final File fromFile,
                                           final Map<K, V> map,
                                           final List<?> jsonConverters) throws IOException {
        final XStream xstream = xStream(map, jsonConverters);

        try (InputStream inputStream = createInputStream(fromFile)) {
            xstream.fromXML(inputStream);
        }
    }

    private static InputStream createInputStream(final File toFile) throws IOException {
        if (toFile.getName().toLowerCase().endsWith(".gz"))
            return new GZIPInputStream(Files.newInputStream(toFile.toPath()));
        else
            return Files.newInputStream(toFile.toPath());
    }

    private static OutputStream createOutputStream(final File toFile) throws IOException {
        if (toFile.getName().toLowerCase().endsWith(".gz"))
            return new GZIPOutputStream(Files.newOutputStream(toFile.toPath()));
        else
            return Files.newOutputStream(toFile.toPath());
    }

    private static <K, V> XStream xStream(final Map<K, V> map, final List<?> jsonConverters) {
        try {
            final XStream xstream = new XStream(new JettisonMappedXmlDriver());
            xstream.setMode(XStream.NO_REFERENCES);
            xstream.alias("cmap", map.getClass());

            registerChronicleMapConverter(map, xstream);
            xstream.registerConverter(new ByteBufferConverter());
            xstream.registerConverter(new ValueConverter());
            xstream.registerConverter(new StringBuilderConverter());
            xstream.registerConverter(new CharSequenceConverter());

            for (Object c : jsonConverters) {
                if (c instanceof Converter) {
                    xstream.registerConverter((Converter) c);
                } else {
                    Jvm.warn().on(JsonSerializer.class,
                            "Skipping Converter of type class=" + c.getClass().getName() + " as " +
                                    " expecting an object of type com.thoughtworks.xstream.converters" +
                                    ".Converter");
                }
            }

            return xstream;
        } catch (NoClassDefFoundError e) {
            throw new RuntimeException(LOG_ERROR_SUGGEST_X_STREAM, e);
        }
    }

    private static <K, V> void registerChronicleMapConverter(final Map<K, V> map, final XStream xstream) {
        xstream.registerConverter(new VanillaChronicleMapConverter<>(map));
    }
}
