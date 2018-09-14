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

import java.util.List;
import java.util.function.BiFunction;

public abstract class ChainingInterface extends ThreadLocalState {

    public abstract List<ChainingInterface> getContextChain();

    public abstract void initUsed(boolean used, VanillaChronicleMap map);

    public abstract boolean usedInit();

    public abstract <T extends ChainingInterface> T getContext(
            Class<? extends T> contextClass, BiFunction<ChainingInterface, VanillaChronicleMap, T> createChaining,
            VanillaChronicleMap map);
}
