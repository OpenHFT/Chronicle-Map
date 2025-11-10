//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

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
        Values.newNativeReference(ReplicatedGlobalMutableStateV2.class);
        Values.newNativeReference(VanillaGlobalMutableState.class);
    }
}
