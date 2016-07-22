/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
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

import java.util.List;
import java.util.function.Function;

public abstract class ChainingInterface extends ThreadLocalState {

    public abstract List<ChainingInterface> getContextChain();

    public abstract void initUsed(boolean used);

    public abstract boolean usedInit();

    public abstract <T extends ChainingInterface> T getContext(
            Class<? extends T> contextClass, Function<ChainingInterface, T> createChaining);
}
