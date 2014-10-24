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

package net.openhft.chronicle.map.ipc;

import net.openhft.lang.io.Bytes;
import net.openhft.lang.model.Byteable;

/**
 *
 */
public class StateMachineData implements Byteable {
    private Bytes bytes;
    private long offset;

    /**
     * c-tor
     */
    public StateMachineData() {
        this.bytes = null;
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
        if (this.bytes == null) throw new NullPointerException("Byteable is not set to off heap");

        return this.bytes.compareAndSwapInt(this.offset, from.value(), to.value());
    }

    /**
     * @return
     */
    public StateMachineState getState() {
        if (this.bytes == null) throw new NullPointerException("Byteable is not set to off heap");

        int value = this.bytes.readVolatileInt(this.offset);
        return StateMachineState.fromValue(value);
    }

    /**
     * @param state
     */
    public void setState(StateMachineState state) {
        if (this.bytes == null) throw new NullPointerException("Byteable is not set to off heap");

        this.bytes.writeInt(this.offset, state.value());
    }

    /**
     * Wait for a state and make a transition.
     * It spins initially (1000 iterations), then uses a Thread.yield() .
     *
     * @param from
     * @param to
     */
    public void waitForState(StateMachineState from, StateMachineState to) {
        if (this.bytes == null) throw new NullPointerException("Byteable is not set to off heap");

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
        if (this.bytes != null) {
            return this.bytes.readVolatileInt(this.offset + 4);
        }

        return -1;
    }

    /**
     * @param data
     */
    public void setStateData(int data) {
        if (this.bytes != null) {
            this.bytes.writeInt(this.offset + 4, data);
        }
    }

    /**
     * @return
     */
    public int incStateData() {
        if (this.bytes != null) {
            return this.bytes.addInt(this.offset + 4, 1);
        }

        return -1;
    }

    /**
     * @return
     */
    public boolean done() {
        if (this.bytes != null) {
            return getStateData() > 100;
        }

        return true;
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Override
    public void bytes(Bytes bytes, long offset) {
        this.bytes = bytes;
        this.offset = offset;
    }

    @Override
    public Bytes bytes() {
        return this.bytes;
    }

    @Override
    public long offset() {
        return this.offset;
    }

    @Override
    public int maxSize() {
        return 16;
    }
}
