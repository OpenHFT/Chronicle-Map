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
        int maxNestedContexts = 1 << 10;
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
