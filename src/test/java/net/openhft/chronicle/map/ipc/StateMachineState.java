/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
     * @param state
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
     * @return
     */
    public int value() {
        return this.state;
    }
}
