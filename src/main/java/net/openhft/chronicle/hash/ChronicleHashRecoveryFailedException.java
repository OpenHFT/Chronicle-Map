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

package net.openhft.chronicle.hash;

import java.io.File;

/**
 * This exception is thrown, when a Chronicle Hash recovery using {@link
 * ChronicleHashBuilder#recoverPersistedTo(File, boolean)} method is impossible, for example,
 * if the persistence file is corrupted too much.
 *
 * @see ChronicleHashBuilder#recoverPersistedTo(File, boolean)
 */
public final class ChronicleHashRecoveryFailedException extends RuntimeException {
    private static final long serialVersionUID = 0L;

    /**
     * Constructs a new {@code ChronicleHashRecoveryFailedException} with the specified cause.
     *
     * @param cause the cause (which is saved for later retrieval by the {@link #getCause()} method)
     */
    public ChronicleHashRecoveryFailedException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new {@code ChronicleHashRecoveryFailedException} with the specified detail
     * message.
     *
     * @param message the detail message. The detail message is saved for later retrieval by the
     *                {@link #getMessage()} method.
     */
    public ChronicleHashRecoveryFailedException(String message) {
        super(message);
    }
}
