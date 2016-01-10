/*
 *      Copyright (C) 2015  higherfrequencytrading.com
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

import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertArrayEquals;

public class Issue63Test {

    @Test
    public void issue63Test() throws Exception {
        Path path = Paths.get(System.getProperty("java.io.tmpdir") + "/test-vectors.dat");
        if (Files.exists(path)) Files.delete(path);
        File mapFile = path.toFile();
        mapFile.deleteOnExit();
        ChronicleMap<CharSequence, float[]> xVectors = ChronicleMap
                .of(CharSequence.class, float[].class)
                // use the actual UUID size
                .constantKeySizeBySample("2B6EC73CD63ACA5D93F4D5A710AD9CFE")
                .constantValueSizeBySample(new float[100])
                .putReturnsNull(true)
                .entries(10)
                .createPersistedTo(mapFile);


        float[] exp1 = new float[] {
                (float) -0.4200737, (float) -0.5428019, (float) 0.25542524, (float) -0.10631648,
                (float) 0.12206168, (float) 0.0411969, (float) 0.9899967, (float) 0.15887073,
                (float) -0.09775953, (float) 0.21812996, (float) -0.2724478, (float) 1.1872392,
                (float) -0.57449555, (float) 0.5036392, (float) 0.1658725, (float) 0.26468855,
                (float) -0.3454932, (float) 0.61344844, (float) -0.058357887, (float) 0.41589612,
                (float) -0.30034602, (float) -0.065557495, (float) -0.5450994, (float) 0.24787773,
                (float) -0.49933347, (float) -0.34362262, (float) -0.116148725, (float) 0.1267731,
                (float) -0.021314947, (float) 0.4289211, (float) 0.018796312, (float) 1.1027592,
                (float) 0.26406515, (float) -0.364442, (float) 0.032301463, (float) 0.7497238,
                (float) 0.022618806, (float) 0.44369924, (float) -0.3347779, (float) -0.21492186,
                (float) -0.16348012, (float) -0.07863602, (float) 0.22218524, (float) 0.13798094,
                (float) 0.9739758, (float) 0.18799895, (float) 0.16804655, (float) -0.94723654,
                (float) -0.09069447, (float) 1.0777866, (float) 0.45763463, (float) -0.99949086,
                (float) 0.1130747, (float) 1.1800445, (float) -0.7469727, (float) -1.480476,
                (float) 0.21458353, (float) 0.5420289, (float) 0.44423282, (float) -0.73524255,
                (float) -0.86806494, (float) 0.77911025, (float) 0.43587336, (float) 0.45608798,
                (float) -0.52584565, (float) 0.5979028, (float) 0.18747452, (float) -0.9211639,
                (float) 0.2969087, (float) -0.17334144, (float) -0.30227816, (float) 0.6624411,
                (float) -1.445531, (float) 0.068452656, (float) -0.54010916, (float) 0.7997881,
                (float) -1.1808084, (float) 1.0036258, (float) 0.23763403, (float) -0.95869386,
                (float) 0.2150584, (float) 0.16237195, (float) 0.35550624, (float) -0.59370506,
                (float) 0.977463, (float) -0.14227587, (float) -1.1346477, (float) -0.29077065,
                (float) -0.7924145, (float) -0.05505234, (float) -0.4519053, (float) 0.8662279,
                (float) 0.056166444, (float) -0.6824282, (float) -0.28487095, (float) -0.28058794,
                (float) -0.868858, (float) 0.4946002, (float) 0.61442167, (float) 0.70633507
        };

        float[] exp2 = new float[] {
                (float) -0.0043417793, (float) -0.004025369, (float) 1.8009785E-4,
                (float) 5.522854E-4, (float) -2.9725596E-4, (float) 0.0038219264,
                (float) 0.0057955547, (float) -0.0036915164, (float) 1.2905941E-5,
                (float) -0.0012608414, (float) 0.0075167217, (float) 1.2714228E-4,
                (float) 0.004510221, (float) -0.0030373763, (float) -0.0033150043,
                (float) -0.0027220408, (float) 0.0049406015, (float) 0.007475855,
                (float) -0.0039889063, (float) 5.387217E-4, (float) 3.014746E-4,
                (float) -0.0025138916, (float) -0.0014927724, (float) 0.0033432362,
                (float) 0.0027196375, (float) -0.001453709, (float) -0.004362245,
                (float) 0.0062709767, (float) 5.681349E-4, (float) 2.963205E-4, (float) 0.002127562,
                (float) -0.0025758513, (float) -0.0015946038, (float) 0.0020683268,
                (float) 0.004608029, (float) -0.006912731, (float) -0.003569094,
                (float) 0.0029314745, (float) -0.0044829296, (float) -0.004087928,
                (float) -3.7728698E-4, (float) -0.0040272907, (float) -0.006466153,
                (float) 2.1587547E-4, (float) -4.334211E-5, (float) 0.0013268286,
                (float) -1.1723964E-4, (float) 0.0017377065, (float) -0.009606785,
                (float) -0.0059685633, (float) 0.0061167465, (float) 0.00976628,
                (float) 0.0045020734, (float) 0.0072684726, (float) -0.002317661,
                (float) 0.0030898168, (float) 0.0013212592, (float) 0.0017718632,
                (float) 0.002785933, (float) 4.135881E-4, (float) -0.007407679,
                (float) -0.008016254, (float) -0.0015525677, (float) -5.22596E-4,
                (float) 0.003450544, (float) -1.4363142E-4, (float) -0.0055779675,
                (float) -0.002204401, (float) 3.5834382E-4, (float) -0.0043447977,
                (float) 0.0052861, (float) 0.0024472543, (float) 0.0019035664,
                (float) -0.0010579216, (float) 0.008568893, (float) -0.0025444124,
                (float) 0.0041700895, (float) 0.002440465, (float) -9.898118E-4,
                (float) -0.004972163, (float) 0.00445475, (float) 0.0028563882,
                (float) -6.568626E-4, (float) 0.0019806502, (float) 0.0021152704,
                (float) -8.9459366E-4, (float) -5.853446E-4, (float) 0.006775423,
                (float) -6.2033796E-5, (float) -0.0016326059, (float) 0.0028676696,
                (float) -0.0020935084, (float) 0.0012473571, (float) -0.00658647,
                (float) -2.9175522E-4, (float) -0.004172817, (float) -9.5688103E-4,
                (float) 0.0029572574, (float) 0.0013865299, (float) -0.001356384
        };

        String key1 = "A2E2CD3EEFF31AE7A2EC455D2D23F8B2";
        String key2 = "28C711B859926E05576CAF5084B4D66C";
        xVectors.put(key1, exp1);
        xVectors.put(key2, exp2);
        xVectors.close();

        ChronicleMap<CharSequence, float[]> xVectors2 = ChronicleMap
                .of(CharSequence.class, float[].class)
                // use the actual UUID size
                .constantKeySizeBySample("2B6EC73CD63ACA5D93F4D5A710AD9CFE")
                .constantValueSizeBySample(new float[100])
                .entries(10)
                .putReturnsNull(true)
                .recoverPersistedTo(mapFile, true);

        assertArrayEquals(exp1, xVectors2.get(key1), 0.0f);
        assertArrayEquals(exp2, xVectors2.get(key2), 0.0f);
    }

}
