//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.map.ipc;

/**
 *
 */
public enum StateMachineState {
    UNKNOWN(-1),
    STATE_0(0),
    STATE_0_WORKING(1),
    STATE_1(10),
    STATE_1_WORKING(11),
    STATE_2(20),
    STATE_2_WORKING(21),
    STATE_3(30),
    STATE_3_WORKING(31);

    private int state;

    /**
     * c-tor
     *
     */
    StateMachineState(int state) {
        this.state = state;
    }

    public static StateMachineState fromValue(int value) {
        for (StateMachineState sms : StateMachineState.values()) {
            if (sms.value() == value) {
                return sms;
            }
        }

        return StateMachineState.UNKNOWN;
    }

    /**
     */
    public int value() {
        return this.state;
    }
}
