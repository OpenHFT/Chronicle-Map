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

package net.openhft.chronicle.hash.impl.stage.hash;

import net.openhft.chronicle.map.VanillaChronicleMap;
import net.openhft.sg.Stage;
import net.openhft.sg.Staged;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

@Staged
public abstract class Chaining extends ChainingInterface {

    public final List<ChainingInterface> contextChain;
    public final int indexInContextChain;
    /**
     * First context, ever created in this thread. rootContextInThisThread === contextChain.get(0).
     */
    public final ChainingInterface rootContextInThisThread;
    @Stage("Used")
    public boolean used;
    @Stage("Used")
    private boolean firstContextLockedInThisThread;

    public Chaining(VanillaChronicleMap map) {
        contextChain = new ArrayList<>();
        contextChain.add(this);
        indexInContextChain = 0;
        rootContextInThisThread = this;
        initMap(map);
    }

    public Chaining(ChainingInterface rootContextInThisThread, VanillaChronicleMap map) {
        contextChain = rootContextInThisThread.getContextChain();
        indexInContextChain = contextChain.size();
        contextChain.add(this);
        this.rootContextInThisThread = rootContextInThisThread;
        initMap(map);
    }

    private static <T extends ChainingInterface> T initUsedAndReturn(
            VanillaChronicleMap map, ChainingInterface context) {
        try {
            context.initUsed(true, map);
            //noinspection unchecked
            return (T) context;
        } catch (Throwable throwable) {
            try {
                ((AutoCloseable) context).close();
            } catch (Throwable t) {
                throwable.addSuppressed(t);
            }
            throw throwable;
        }
    }

    @Override
    public List<ChainingInterface> getContextChain() {
        return contextChain;
    }

    public <T> T contextAtIndexInChain(int index) {
        //noinspection unchecked
        return (T) contextChain.get(index);
    }

    /**
     * This method stores a reference to the context's owner ChronicleMap into a field of the
     * context in the beginning of each usage of the context. Previously, this field was final and
     * set only once during context creation. It was preventing ChronicleMap objects from becoming
     * dead and collected by the GC, while any thread, from which the ChronicleMap was accessed
     * (hence a thread local context created), is alive.
     * <p>
     * <p>The chain of strong references:
     * 1) Thread ->
     * 2) ThreadLocalMap ->
     * 3) Entry with ThreadLocal {@link net.openhft.chronicle.map.VanillaChronicleMap#cxt} as weak
     * referent and a context (e. g. {@link net.openhft.chronicle.map.impl.CompiledMapQueryContext})
     * as value (a simple field, not a weak reference!) ->
     * 4) final reference to the owner {@link VanillaChronicleMap} ->
     * 5) ThreadLocal {@link net.openhft.chronicle.map.VanillaChronicleMap#cxt} (a strong reference
     * this time! note that this ThreadLocal is an instance field of VanillaChronicleMap)
     * <p>
     * <p>So in order to break this chain at step 4), contexts store references to their owner
     * ChronicleMaps only when contexts are used.
     */
    public abstract void initMap(VanillaChronicleMap map);

    @Override
    public boolean usedInit() {
        return used;
    }

    /**
     * Init method parameterized to avoid automatic initialization. {@code used} argument should
     * always be {@code true}.
     */
    @Override
    public void initUsed(boolean used, VanillaChronicleMap map) {
        assert used;
        firstContextLockedInThisThread = rootContextInThisThread.lockContextLocally(map);
        initMap(map);
        this.used = true;
    }

    @SuppressWarnings("unused")
    void closeUsed() {
        used = false;
        if (firstContextLockedInThisThread)
            rootContextInThisThread.unlockContextLocally();
    }

    @Override
    public <T extends ChainingInterface> T getContext(
            Class<? extends T> contextClass, BiFunction<ChainingInterface,
            VanillaChronicleMap, T> createChaining,
            VanillaChronicleMap map) {
        for (ChainingInterface context : contextChain) {
            if (context.getClass() == contextClass && !context.usedInit()) {
                return initUsedAndReturn(map, context);
            }
        }
        int maxNestedContexts = 1 << 10;
        if (contextChain.size() > maxNestedContexts) {
            throw new IllegalStateException(map.toIdentityString() +
                    ": More than " + maxNestedContexts + " nested ChronicleHash contexts\n" +
                    "are not supported. Very probable that you simply forgot to close context\n" +
                    "somewhere (recommended to use try-with-resources statement).\n" +
                    "Otherwise this is a bug, please report with this\n" +
                    "stack trace on https://github.com/OpenHFT/Chronicle-Map/issues");
        }
        //noinspection unchecked
        T context = createChaining.apply(this, map);
        return initUsedAndReturn(map, context);
    }
}
