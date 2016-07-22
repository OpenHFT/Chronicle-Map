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
