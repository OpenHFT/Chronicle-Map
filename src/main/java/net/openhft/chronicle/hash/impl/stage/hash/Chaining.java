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

package net.openhft.chronicle.hash.impl.stage.hash;

import net.openhft.sg.Staged;

import java.util.ArrayList;
import java.util.List;

@Staged
public class Chaining {

    public final List<Chaining> contextChain;
    public final int indexInContextChain;
    
    public Chaining() {
        contextChain = new ArrayList<>();
        contextChain.add(this);
        indexInContextChain = 0;
    }
    
    public Chaining(Chaining root) {
        contextChain = root.contextChain;
        indexInContextChain = contextChain.size();
        contextChain.add(this);
    }

    public <T> T contextAtIndexInChain(int index) {
        //noinspection unchecked
        return (T) contextChain.get(index);
    }

    
    boolean used;
    
    public boolean usedInit() {
        return used;
    }
    
    public void initUsed(boolean used) {
        this.used = used;
    }
    
    void closeUsed() {
        used = false;
    }

    public <T> T getContext() {
        for (Chaining context : contextChain) {
            if (!context.usedInit()) {
                //noinspection unchecked
                return (T) context;
            }
        }
        int maxNestedContexts = 1 << 16;
        if (contextChain.size() > maxNestedContexts) {
            throw new IllegalStateException("More than " + maxNestedContexts +
                    " nested ChronicleHash contexts are not supported. Very probable that " +
                    "you simply forgot to close context somewhere (recommended to use " +
                    "try-with-resources statement). " +
                    "Otherwise this is a bug, please report with this " +
                    "stack trace on https://github.com/OpenHFT/Chronicle-Map/issues");
        }
        //noinspection unchecked
        return (T) createChaining();
    }
    
    public Chaining createChaining() {
        return new Chaining(this);
    }
}
