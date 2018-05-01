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

package net.openhft.chronicle.hash.impl.util;

import net.openhft.chronicle.map.ChronicleMapBuilder;
import shaded.org.apache.maven.model.Model;
import shaded.org.apache.maven.model.io.xpp3.MavenXpp3Reader;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * gets the version of the current build
 */
public final class BuildVersion {

    private static String version = null;

    private BuildVersion() {
    }

    public static void main(String[] args) {
        System.out.println(version());
    }

    /**
     * @return version of ChronicleMap being used, or NULL if its not known
     */
    public synchronized static String version() {

        if (version != null) {
            return version;
        }

        try {
            // the best way to get the version is to read the map.version file
            InputStream resource = BuildVersion.class.getClassLoader().getResourceAsStream("map" +
                    ".version");
            BufferedReader in = new BufferedReader(new InputStreamReader(resource, StandardCharsets.UTF_8));

            version = in.readLine().trim();
            if (!"${project.version}".equals(version()))
                return version;

            return version;
        } catch (Exception e) {
            // do nothing
        }

        // another way to get the version is to read it from the manifest
        final String versionFromManifest = getVersionFromManifest();

        if (versionFromManifest != null) {
            version = versionFromManifest;
            return version;
        }

        // as a fall back for development, we will read the version from the pom file
        version = getVersionFromPom();
        return version;
    }

    /**
     * This should be used by everyone that has install chronicle map as a JAR
     *
     * @return gets the version out of the manifest, or null if it can not be read
     */
    private static String getVersionFromManifest() {
        return ChronicleMapBuilder.class.getPackage().getImplementationVersion();
    }

    /**
     * reads the pom file to get this version, only to be used for development or within the IDE.
     *
     * @return gets the version from the pom.xml
     */
    private static String getVersionFromPom() {

        final String absolutePath = new File(BuildVersion.class.getResource(BuildVersion.class
                .getSimpleName() + ".class").getPath())
                .getParentFile().getParentFile().getParentFile().getParentFile().getParentFile()
                .getParentFile().getParentFile().getAbsolutePath();

        final File file = new File(absolutePath + "/pom.xml");

        try (InputStreamReader reader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)) {

            final MavenXpp3Reader xpp3Reader = new MavenXpp3Reader();
            Model model = xpp3Reader.read(reader);
            return model.getVersion();

        } catch (NoClassDefFoundError e) {
            // if you want to get the version possibly in development add in to your pom
            // pax-url-aether.jar
            return null;
        } catch (Exception e) {
            return null;
        }
    }
}