/*
 *      Copyright (C) 2015  higherfrequencytrading.com
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

package net.openhft.chronicle.hash.impl.stage.data.bytes;

import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.impl.stage.hash.KeyBytesInterop;
import net.openhft.chronicle.hash.impl.stage.input.HashInputBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;


@Staged
public class InputKeyBytesData<K> extends AbstractData<K> {
    @StageRef HashInputBytes in;
    @StageRef KeyBytesInterop<K, ?, ?> ki;
    
    @Stage("CachedBytesInputKey") private K cachedBytesInputKey;
    @Stage("CachedBytesInputKey") private boolean cachedBytesInputKeyRead = false;
    
    private void initCachedBytesInputKey() {
        cachedBytesInputKey = getUsing(cachedBytesInputKey);
        cachedBytesInputKeyRead = true;
    }

    @Override
    public RandomDataInput bytes() {
        return in.inputStore;
    }

    @Override
    public long offset() {
        return in.inputKeyOffset;
    }

    @Override
    public long size() {
        return in.inputKeySize;
    }

    @Override
    public K get() {
        return cachedBytesInputKey;
    }

    @Override
    public K getUsing(K usingKey) {
        Bytes inputBytes = in.inputBytes;
        inputBytes.position(in.inputKeyOffset);
        return ki.keyReader.read(inputBytes, size(), usingKey);
    }
}
