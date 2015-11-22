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

import net.openhft.chronicle.hash.VanillaGlobalMutableState;
import net.openhft.chronicle.values.Values;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class GlobalMutableStateTest {

    private String dumpCode;

    @Before
    public void setDumpCode() {
        dumpCode = System.getProperty("dvg.dumpCode");
        System.setProperty("dvg.dumpCode", "true");
    }

    @After
    public void unsetDumpCode() {
        if (dumpCode != null)
            System.setProperty("dvg.dumpCode", dumpCode);
        else
            System.getProperties().remove("dvg.dumpCode");
    }

    @Test
    public void globalMutableStateTest() {
        Values.newNativeReference(ReplicatedGlobalMutableState.class);
        Values.newNativeReference(VanillaGlobalMutableState.class);
    }
}
