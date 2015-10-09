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
import java.util.function.Function;

@Staged
public class Chaining extends ChainingInterface {

    public final List<ChainingInterface> contextChain;
    public final int indexInContextChain;
    public final ChainingInterface rootContextInThisThread;

    public Chaining() {
        contextChain = new ArrayList<>();
        contextChain.add(this);
        indexInContextChain = 0;
        rootContextInThisThread = this;
    }
    
    public Chaining(ChainingInterface rootContextInThisThread) {
        contextChain = rootContextInThisThread.getContextChain();
        indexInContextChain = contextChain.size();
        contextChain.add(this);
        this.rootContextInThisThread = rootContextInThisThread;
    }

    @Override
    public List<ChainingInterface> getContextChain() {
        return contextChain;
    }

    public <T> T contextAtIndexInChain(int index) {
        //noinspection unchecked
        return (T) contextChain.get(index);
    }

    
    boolean used;

    @Override
    public boolean usedInit() {
        return used;
    }

    @Override
    public void initUsed(boolean used) {
        this.used = used;
    }
    
    void closeUsed() {
        used = false;
    }

    @Override
    public <T extends ChainingInterface> T getContext(
            Class<? extends T> contextClass, Function<ChainingInterface, T> createChaining) {
        for (ChainingInterface context : contextChain) {
            if (context.getClass() == contextClass && !context.usedInit()) {
                context.initUsed(true);
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
        T context = createChaining.apply(this);
        context.initUsed(true);
        return context;
    }
}
