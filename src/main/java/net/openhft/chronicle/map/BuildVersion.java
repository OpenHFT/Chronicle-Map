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

import shaded.org.apache.maven.model.Model;
import shaded.org.apache.maven.model.io.xpp3.MavenXpp3Reader;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;

/**
 * gets the version of the current build
 */
class BuildVersion {


    /**
     * gets the version of NULL if it can not be read
     */
    public static String version() {

        final String version = getVersionFromManifest();

        if (version != null)
            return version;

        // as a fall back for development we will read the version from the pom file
        return getVersionFromPom();
    }

    /**
     * This should be used by everyone that has install chronicle map
     *
     * @return gets the version out of the manifest, or null if it can not be read
     */
    private static String getVersionFromManifest() {
        return ChronicleMapBuilder.class.getPackage().getImplementationVersion();
    }


    /**
     * reads the pom file to get this version, only used for development or within the ide.
     *
     * @return gets the version from the pom.xml
     */
    private static String getVersionFromPom() {
        final File file = new File(new File(BuildVersion.class.getResource("/").getPath())
                .getParentFile().getParentFile().getAbsolutePath() + "/pom.xml");

        try (Reader reader = new FileReader(file)) {

            final MavenXpp3Reader xpp3Reader = new MavenXpp3Reader();
            Model model = xpp3Reader.read(reader);
            return model.getVersion();


        } catch (Exception e) {
            return null;
        }
    }


}