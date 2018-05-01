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

package net.openhft.chronicle.map.impl.stage.data.instance;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class WrappedValueInstanceDataHolder<V> {

    public Data<V> wrappedData = null;
    @StageRef
    VanillaChronicleMapHolder<?, V, ?> mh;
    private final DataAccess<V> wrappedValueDataAccess = mh.m().valueDataAccess.copy();
    private WrappedValueInstanceDataHolder<V> next;
    private V value;

    boolean nextInit() {
        return true;
    }

    void closeNext() {
        // do nothing
    }

    @Stage("Next")
    public WrappedValueInstanceDataHolder<V> getUnusedWrappedValueHolder() {
        if (!valueInit())
            return this;
        if (next == null)
            next = new WrappedValueInstanceDataHolder<>();
        return next.getUnusedWrappedValueHolder();
    }

    public boolean valueInit() {
        return value != null;
    }

    public void initValue(V value) {
        mh.m().checkValue(value);
        this.value = value;
    }

    public void closeValue() {
        value = null;
        if (next != null)
            next.closeValue();
    }

    private void initWrappedData() {
        wrappedData = wrappedValueDataAccess.getData(value);
    }

    private void closeWrappedData() {
        wrappedData = null;
        wrappedValueDataAccess.uninit();
    }
}
