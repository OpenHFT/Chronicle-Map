/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.VanillaGlobalMutableState;
import net.openhft.chronicle.values.Values;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GlobalMutableStateTest {

    private String dumpCode;

    @BeforeEach
    void setDumpCode() {
        dumpCode = System.getProperty("dvg.dumpCode");
        System.setProperty("dvg.dumpCode", "true");
    }

    @AfterEach
    void unsetDumpCode() {
        if (dumpCode != null)
            System.setProperty("dvg.dumpCode", dumpCode);
        else
            System.getProperties().remove("dvg.dumpCode");
    }

    @Test
    void globalMutableStateTest() {
        Values.newNativeReference(ReplicatedGlobalMutableStateV2.class);
        Values.newNativeReference(VanillaGlobalMutableState.class);
    }
}
