/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

import net.openhft.lang.io.Bytes;

final class ZeroOutValueBytes<K, V> implements PrepareValueBytes<K, V> {
    private static final long serialVersionUID = 0L;
    private final long valueSize;

    ZeroOutValueBytes(long valueSize) {
        this.valueSize = valueSize;
    }

    @Override
    public void prepare(Bytes bytes, K key) {
        long pos = bytes.position();
        bytes.zeroOut(pos, pos + valueSize);
        bytes.skip(valueSize);
    }
}
