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

package net.openhft.chronicle.hash;

import org.jetbrains.annotations.Nullable;

import java.io.File;

/**
 * Information about a corruption, encountered in a persisted Chronicle Map during <a
 * href="https://github.com/OpenHFT/Chronicle-Map#recovery">recovery</a>.
 * <p>
 * <p>Recovery procedure doesn't guarantee accuracy of the corruption events. Only two things are
 * guaranteed:
 * <ol>
 * <li>if {@link Listener} didn't receive any corruption events, the recovered Chronicle Map
 * was not corrupted;
 * </li>
 * <li>if {@link Listener} received some corruption events, the recovered Chronicle Map was
 * corrupted.</li>
 * </ol>
 * <p>
 * <p>{@code ChronicleHashCorruption} objects, passed to {@link Listener}, shouldn't be saved and
 * used outside of the {@link Listener#onCorruption(ChronicleHashCorruption)} method body, because
 * {@code ChronicleHashCorruption} objects could be reused during the recovery procedure.
 * <p>
 * <p>During a recovery procedure, <i>{@link Listener#onCorruption(ChronicleHashCorruption)} might
 * be called concurrently from multiple threads.</i> If the implementation of this method calls some
 * methods on some objects, that are not safe for concurrent use from multiple threads, the
 * implementation must care about synchronization itself.
 *
 * @see ChronicleHashBuilder#recoverPersistedTo(File, boolean, ChronicleHashCorruption.Listener)
 * @see ChronicleHashBuilder#createOrRecoverPersistedTo(File, boolean, ChronicleHashCorruption.Listener)
 */
@Beta
public interface ChronicleHashCorruption {

    /**
     * Returns the message, explaining this corruption.
     */
    String message();

    /**
     * Returns the exception, associated with this corruption, if any, or {@code null} if there is
     * no exception.
     */
    @Nullable
    Throwable exception();

    /**
     * Returns the index of the segment, with which this corruption is associated, or -1, if not
     * applicable. E. g. if this is a segment lock word corruption, returns the index of the
     * segment, guarded by the corrupted lock. If this is an entry data corruption, returns the
     * index of the segment, in which the corrupted entry is stored. If the corruption is not
     * associated with a particular segment, returns -1, e. g. if this is a ChronicleHash
     * header corruption.
     */
    int segmentIndex();

    /**
     * Listener of {@link ChronicleHashCorruption} events.
     */
    @Beta
    interface Listener {
        /**
         * Called when <a href="https://github.com/OpenHFT/Chronicle-Map#recovery">recovery</a>
         * procedure encounters a corruption of a persisted Chronicle Map.
         * <p>
         * <p>During a recovery procedure, <i>this method might be called concurrently from multiple
         * threads.</i> If the implementation of this method calls some methods on some objects,
         * that are not safe for concurrent use from multiple threads, the implementation must
         * care about synchronization itself.
         *
         * @param corruption the corruption object, must not be saved and used outside the body of
         *                   the {@code #onCorruption()} method, because during the recovery
         *                   procedure, the same corruption object could be reused.
         * @see ChronicleHashCorruption
         */
        void onCorruption(ChronicleHashCorruption corruption);
    }
}
