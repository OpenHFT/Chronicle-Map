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

package net.openhft.chronicle.hash.impl.stage.entry;

public enum NoChecksumStrategy implements ChecksumStrategy {
    INSTANCE;

    @Override
    public void computeAndStoreChecksum() {
        throw new UnsupportedOperationException("Checksum is not stored in this Chronicle Hash");
    }

    @Override
    public boolean innerCheckSum() {
        return true;
    }

    @Override
    public int computeChecksum() {
        return 0;
    }

    @Override
    public int storedChecksum() {
        return 0;
    }

    @Override
    public long extraEntryBytes() {
        return 0; // no extra bytes to store checksum
    }
}
