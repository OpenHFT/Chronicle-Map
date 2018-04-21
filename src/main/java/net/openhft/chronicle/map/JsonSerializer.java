/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
class JsonSerializer {

    static final String logErrorSuggestXStreem =
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
            throw new RuntimeException(logErrorSuggestXStreem, e);
        }
    }

    private static <K, V> void registerChronicleMapConverter(Map<K, V> map, XStream xstream) {
        xstream.registerConverter(new VanillaChronicleMapConverter<>(map));
    }
}

