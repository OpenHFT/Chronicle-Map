/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import net.openhft.xstream.converters.ByteBufferConverter;
import net.openhft.xstream.converters.DataValueConverter;
import net.openhft.xstream.converters.StatelessChronicleMapConverter;
import net.openhft.xstream.converters.VanillaChronicleMapConverter;
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
class JsonSerializer {

    private static final Logger LOG = LoggerFactory.getLogger(StatelessChronicleMap.class);

    static synchronized <K, V> void getAll(File toFile, Map<K, V> map, List jsonConverters) throws IOException {
        final XStream xstream = xStream(map, jsonConverters);
        OutputStream outputStream = new FileOutputStream(toFile);
        if (toFile.getName().toLowerCase().endsWith(".gz"))
            outputStream = new GZIPOutputStream(outputStream);
        try (OutputStream out = outputStream) {
            xstream.toXML(map, out);
        }
    }

    static synchronized <K, V> void putAll(File fromFile, Map<K, V> map, List jsonConverters)
            throws IOException {
        final XStream xstream = xStream(map, jsonConverters);

        InputStream inputStream = new FileInputStream(fromFile);
        if (fromFile.getName().toLowerCase().endsWith(".gz"))
            inputStream = new GZIPInputStream(inputStream);
        try (InputStream out = inputStream) {
            xstream.fromXML(out);
        }
    }

    private static <K, V> XStream xStream(Map<K, V> map, List jsonConverters) {
        try {
            final XStream xstream = new XStream(new JettisonMappedXmlDriver());
            xstream.setMode(XStream.NO_REFERENCES);
            xstream.alias("cmap", map.getClass());

            registerChronicleMapConverter(map, xstream);
            xstream.registerConverter(new ByteBufferConverter());
            xstream.registerConverter(new DataValueConverter());

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
            logErrorSuggestXStreem(e);
            throw e;
        }
    }

    static private void logErrorSuggestXStreem(Error e) {
        LOG.error("map.getAll(<file>) and map.putAll(<file>) methods require the JSON XStream serializer, " +
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
                "</dependency>\n", e);
    }

    private static <K, V> void registerChronicleMapConverter(Map<K, V> map, XStream xstream) {
        Converter converter = map instanceof StatelessChronicleMap ?
                new StatelessChronicleMapConverter<K, V>(map) :
                new VanillaChronicleMapConverter<K, V>(map);

        xstream.registerConverter(converter);
    }
}

