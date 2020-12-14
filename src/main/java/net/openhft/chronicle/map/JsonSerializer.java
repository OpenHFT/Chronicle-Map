/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.map;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver;
import net.openhft.xstream.converters.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOG = LoggerFactory.getLogger(JsonSerializer.class);

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

            registerChronicleMapConverter(map, xstream);
            xstream.registerConverter(new ByteBufferConverter());
            xstream.registerConverter(new ValueConverter());
            xstream.registerConverter(new StringBuilderConverter());
            xstream.registerConverter(new CharSequenceConverter());

            for (Object c : jsonConverters) {
                if (c instanceof Converter) {
                    xstream.registerConverter((Converter) c);
                } else {
                    LOG.warn("Skipping Converter of type class=" + c.getClass().getName() + " as " +
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