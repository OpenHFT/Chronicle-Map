/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.ChronicleHash;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author Rob Austin.
 */
interface FindByName {

    /**
     * @param name the name of the map or set
     * @param <T>  the type returned
     * @return a chronicle map or set
     * @throws IllegalArgumentException if a map with this name can not be found
     * @throws IOException              if it not possible to create the map or set
     * @throws TimeoutException         if the call times out
     * @throws InterruptedException     if interrupted by another thread
     */
    <T extends ChronicleHash> T from(String name) throws IllegalArgumentException,
            IOException, TimeoutException, InterruptedException;
}
