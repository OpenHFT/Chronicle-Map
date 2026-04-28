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
                    " <groupId>xstream</groupId>\n" +
                    " <artifactId>xstream</artifactId>\n" +
                    " <version>1.2.2</version>\n" +
                    "</dependency>\n" +
                    "<dependency>\n" +
                    " <groupId>org.codehaus.jettison</groupId>\n" +
                    " <artifactId>jettison</artifactId>\n" +
                    " <version>1.3.6</version>\n" +
                    "</dependency>\n";

    static synchronized <K, V> void getAll(final File toFile,
                                           final Map<K, V> map,
                                           final List<?> jsonConverters) throws IOException {
        final XStream xstream = xStream(map, jsonConverters);

        try (OutputStream outputStream = createOutputStream(toFile)) {
            xstream.toXML(map, outputStream);
        }
        debugFile("getAll output", toFile); // TODO remove temporary CI diagnostics
    }

    static synchronized <K, V> void putAll(final File fromFile,
                                           final Map<K, V> map,
                                           final List<?> jsonConverters) throws IOException {
        final XStream xstream = xStream(map, jsonConverters);
        debugFile("putAll input", fromFile); // TODO remove temporary CI diagnostics

        try (InputStream inputStream = createInputStream(fromFile)) {
            xstream.fromXML(inputStream);
        }
    }

    private static InputStream createInputStream(final File toFile) throws IOException {
        if (toFile.getName().toLowerCase().endsWith(".gz"))
            return new GZIPInputStream(new FileInputStream(toFile));
        else
            return new FileInputStream(toFile);
    }

    private static OutputStream createOutputStream(final File toFile) throws IOException {
        if (toFile.getName().toLowerCase().endsWith(".gz"))
            return new GZIPOutputStream(new FileOutputStream(toFile));
        else
            return new FileOutputStream(toFile);
    }

    private static <K, V> XStream xStream(final Map<K, V> map, final List<?> jsonConverters) {
        try {
            final XStream xstream = new XStream(new JettisonMappedXmlDriver());
            xstream.setMode(XStream.NO_REFERENCES);
            xstream.alias("cmap", map.getClass());
            System.err.println("[CM-XSTREAM] mapClass=" + map.getClass().getName()); // TODO remove temporary CI diagnostics
            System.err.println("[CM-XSTREAM] XStream version=" + // TODO remove temporary CI diagnostics
                    XStream.class.getPackage().getImplementationVersion()); // TODO remove temporary CI diagnostics
            System.err.println("[CM-XSTREAM] XStream jar=" + // TODO remove temporary CI diagnostics
                    XStream.class.getProtectionDomain().getCodeSource().getLocation()); // TODO remove temporary CI diagnostics
            System.err.println("[CM-XSTREAM] Jettison driver jar=" + // TODO remove temporary CI diagnostics
                    JettisonMappedXmlDriver.class.getProtectionDomain().getCodeSource().getLocation()); // TODO remove temporary CI diagnostics
            System.err.println("[CM-XSTREAM] java.version=" + System.getProperty("java.version")); // TODO remove temporary CI diagnostics

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
    private static void debugFile(String label, File file) throws IOException { // TODO remove temporary CI diagnostics
        final int maxPreview = 4000; // TODO remove temporary CI diagnostics
        ByteArrayOutputStream out = new ByteArrayOutputStream(); // TODO remove temporary CI diagnostics
        try (InputStream inputStream = createInputStream(file)) { // TODO remove temporary CI diagnostics
            byte[] bytes = new byte[1024]; // TODO remove temporary CI diagnostics
            int read; // TODO remove temporary CI diagnostics
            while (out.size() < maxPreview && // TODO remove temporary CI diagnostics
                    (read = inputStream.read(bytes, 0, Math.min(bytes.length, maxPreview - out.size()))) != -1) { // TODO remove temporary CI diagnostics
                out.write(bytes, 0, read); // TODO remove temporary CI diagnostics
            } // TODO remove temporary CI diagnostics
        } // TODO remove temporary CI diagnostics
        String content = out.toString("UTF-8"); // TODO remove temporary CI diagnostics
        System.err.println("[CM-XSTREAM] " + label + " file=" + file // TODO remove temporary CI diagnostics
                + " length=" + file.length() + " preview=" + content); // TODO remove temporary CI diagnostics
    } // TODO remove temporary CI diagnostics
}
