/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.ipc;

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.BytesStore;

import java.nio.channels.FileLock;

/**
 *
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class StateMachineData implements Byteable {
    private BytesStore<?, ?> bs;
    private long offset;

    /**
     * c-tor
     */
    public StateMachineData() {
        this.bs = null;
        this.offset = -1;
    }

    /**
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
     */
    public boolean setState(StateMachineState from, StateMachineState to) {
        if (this.bs == null) throw new NullPointerException("Byteable is not set to off heap");

        return this.bs.compareAndSwapInt(this.offset, from.value(), to.value());
    }

    /**
     */
    public StateMachineState getState() {
        if (this.bs == null) throw new NullPointerException("Byteable is not set to off heap");

        int value = this.bs.readVolatileInt(this.offset);
        return StateMachineState.fromValue(value);
    }

    /**
     */
    public void setState(StateMachineState state) {
        if (this.bs == null) throw new NullPointerException("Byteable is not set to off heap");

        this.bs.writeInt(this.offset, state.value());
    }

    /**
     * Wait for a state and make a transition.
     * It spins initially (1000 iterations), then uses a Thread.yield() .
     *
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

    /**
     */
    public int getStateData() {
        if (this.bs != null) {
            return this.bs.readVolatileInt(this.offset + 4);
        }

        return -1;
    }

    /**
     */
    public void setStateData(int data) {
        if (this.bs != null) {
            this.bs.writeInt(this.offset + 4, data);
        }
    }

    /**
     */
    public int incStateData() {
        if (this.bs != null) {
            return this.bs.addAndGetInt(this.offset + 4, 1);
        }

        return -1;
    }

    /**
     */
    public boolean done() {
        if (this.bs != null) {
            return getStateData() > 100;
        }

        return true;
    }

    @Override
    public void bytesStore(BytesStore bytes, long offset, long size) {
        if (size != 16)
            throw new IllegalArgumentException();
        this.bs = bytes;
        this.offset = offset;
    }

    @Override
    public BytesStore<?, ?> bytesStore() {
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

    @Override
    public FileLock lock(boolean shared) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileLock tryLock(boolean shared) {
        throw new UnsupportedOperationException();
    }
}
