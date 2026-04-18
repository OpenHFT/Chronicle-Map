/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import org.jetbrains.annotations.NotNull;

import java.nio.channels.SelectionKey;
import java.util.AbstractSet;
import java.util.Iterator;

final class SelectedSelectionKeySet extends AbstractSet<SelectionKey> {

    private SelectionKey[] keysA;
    private int keysASize;
    private SelectionKey[] keysB;
    private int keysBSize;
    private boolean isA = true;

    SelectedSelectionKeySet() {
        keysA = new SelectionKey[1024];
        keysB = keysA.clone();
    }

    @Override
    public boolean add(SelectionKey o) {
        if (o == null) {
            return false;
        }

        if (isA) {
            int size = keysASize;
            keysA[size++] = o;
            keysASize = size;
            if (size == keysA.length) {
                doubleCapacityA();
            }
        } else {
            int size = keysBSize;
            keysB[size++] = o;
            keysBSize = size;
            if (size == keysB.length) {
                doubleCapacityB();
            }
        }

        return true;
    }

    private void doubleCapacityA() {
        SelectionKey[] newKeysA = new SelectionKey[keysA.length << 1];
        System.arraycopy(keysA, 0, newKeysA, 0, keysASize);
        keysA = newKeysA;
    }

    private void doubleCapacityB() {
        SelectionKey[] newKeysB = new SelectionKey[keysB.length << 1];
        System.arraycopy(keysB, 0, newKeysB, 0, keysBSize);
        keysB = newKeysB;
    }

    SelectionKey[] flip() {
        if (isA) {
            isA = false;
            keysA[keysASize] = null;
            keysBSize = 0;
            return keysA;
        } else {
            isA = true;
            keysB[keysBSize] = null;
            keysASize = 0;
            return keysB;
        }
    }

    @Override
    public int size() {
        if (isA) {
            return keysASize;
        } else {
            return keysBSize;
        }
    }

    @Override
    public boolean remove(Object o) {
        return false;
    }

    @Override
    public boolean contains(Object o) {
        return false;
    }

    @NotNull
    @Override
    public Iterator<SelectionKey> iterator() {
        throw new UnsupportedOperationException();
    }
}
