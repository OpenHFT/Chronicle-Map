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

package net.openhft.chronicle.map;

import net.openhft.lang.io.NativeBytes;

import java.util.Map;

/**
 * Allow access to the Segments that user used to make up the shared map, these methods should be used with
 * exceptional caution and were initially introduce to facilitate remote map replication.
 */
interface SharedSegment<K, V> {

    void readLock();

    void writeLock();

    Map.Entry<K, V> getEntry(long pos);

    void readUnlock();

    void writeUnlock();

    NativeBytes entry(long pos);

    int getIndex();

    long offsetFromPos(long pos);


}
