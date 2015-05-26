/*
 * Copyright 2015 Higher Frequency Trading
 *
 *  http://www.higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.openhft.chronicle.hash.impl;

import net.openhft.lang.io.MultiStoreBytes;

/**
 * This awful class is made for "conversion" from new ch.bytes.Bytes to old lang.io.Bytes
 *
 * TODO remove when Bytes system stabilize
 */
public class PublicMultiStoreBytes extends MultiStoreBytes {
    public void startAddr(long startAddr) { this.startAddr = startAddr; }
    public void positionAddr(long positionAddr) { this.positionAddr = positionAddr; }
    public void limitAddr(long limitAddr) { this.limitAddr = limitAddr; }
    public void capacityAddr(long capacityAddr) { this.capacityAddr = capacityAddr; }
}
