/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.VanillaGlobalMutableState;
import net.openhft.chronicle.values.Values;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class GlobalMutableStateTest {

    private String dumpCode;

    @BeforeEach
    public void setDumpCode() {
        dumpCode = System.getProperty("dvg.dumpCode");
        System.setProperty("dvg.dumpCode", "true");
    }

    @AfterEach
    public void unsetDumpCode() {
        if (dumpCode != null)
            System.setProperty("dvg.dumpCode", dumpCode);
        else
            System.getProperties().remove("dvg.dumpCode");
    }

    @Test
    public void globalMutableStateTest() {
        ReplicatedGlobalMutableStateV2 replicated = Values.newNativeReference(ReplicatedGlobalMutableStateV2.class);
        VanillaGlobalMutableState vanilla = Values.newNativeReference(VanillaGlobalMutableState.class);
        assertNotNull(replicated, "ReplicatedGlobalMutableStateV2 reference");
        assertNotNull(vanilla, "VanillaGlobalMutableState reference");
    }
}
