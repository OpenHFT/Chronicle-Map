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

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.BytesStore;

/**
 *
 */
public class StateMachineData implements Byteable {
    private BytesStore bs;
    private long offset;

    /**
     * c-tor
     */
    public StateMachineData() {
        this.bs = null;
        this.offset = -1;
    }

    // *************************************************************************
    //
    // *************************************************************************

    /**
     * @param states
     * @return
     */
    public boolean stateIn(StateMachineState... states) {
        StateMachineState currentState = getState();
        for (StateMachineState state : states) {
            if (state == currentState) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param from
     * @param to
     */
    public boolean setState(StateMachineState from, StateMachineState to) {
        if (this.bs == null) throw new NullPointerException("Byteable is not set to off heap");

        return this.bs.compareAndSwapInt(this.offset, from.value(), to.value());
    }

    /**
     * @return
     */
    public StateMachineState getState() {
        if (this.bs == null) throw new NullPointerException("Byteable is not set to off heap");

        int value = this.bs.readVolatileInt(this.offset);
        return StateMachineState.fromValue(value);
    }

    /**
     * @param state
     */
    public void setState(StateMachineState state) {
        if (this.bs == null) throw new NullPointerException("Byteable is not set to off heap");

        this.bs.writeInt(this.offset, state.value());
    }

    /**
     * Wait for a state and make a transition.
     * It spins initially (1000 iterations), then uses a Thread.yield() .
     *
     * @param from
     * @param to
     */
    public void waitForState(StateMachineState from, StateMachineState to) {
        if (this.bs == null) throw new NullPointerException("Byteable is not set to off heap");

        // spin
        for (int i = 0; !setState(from, to); i++) {
            if (i > 1000) {
                Thread.yield(); // back off a little.
            }
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    /**
     * @return
     */
    public int getStateData() {
        if (this.bs != null) {
            return this.bs.readVolatileInt(this.offset + 4);
        }

        return -1;
    }

    /**
     * @param data
     */
    public void setStateData(int data) {
        if (this.bs != null) {
            this.bs.writeInt(this.offset + 4, data);
        }
    }

    /**
     * @return
     */
    public int incStateData() {
        if (this.bs != null) {
            return this.bs.addAndGetInt(this.offset + 4, 1);
        }

        return -1;
    }

    /**
     * @return
     */
    public boolean done() {
        if (this.bs != null) {
            return getStateData() > 100;
        }

        return true;
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Override
    public void bytesStore(BytesStore bytes, long offset, long size) {
        if (size != 16)
            throw new IllegalArgumentException();
        this.bs = bytes;
        this.offset = offset;
    }

    @Override
    public BytesStore bytesStore() {
        return this.bs;
    }

    @Override
    public long offset() {
        return this.offset;
    }

    @Override
    public long maxSize() {
        return 16;
    }
}
