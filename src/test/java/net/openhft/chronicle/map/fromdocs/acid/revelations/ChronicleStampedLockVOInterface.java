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

package net.openhft.chronicle.map.fromdocs.acid.revelations;

import net.openhft.chronicle.values.Array;
import net.openhft.chronicle.values.Group;
import net.openhft.chronicle.values.MaxUtf8Length;

interface ChronicleStampedLockVOInterface {

    @Group(0)
    long getEntryLockState();

    void setEntryLockState(long entryLockState);
//
//    @Group(1)
//    long getReaderCount();
//
//    void setReaderCount(long rc);  /* time in millis */
//
//    long addAtomicReaderCount(long toAdd);
//

}
