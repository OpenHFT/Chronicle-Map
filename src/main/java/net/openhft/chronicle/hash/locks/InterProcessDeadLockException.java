/*
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

package net.openhft.chronicle.hash.locks;

/**
 * Thrown from {@link InterProcessLock#lock()} and {@link InterProcessLock#lockInterruptibly()} when
 * if fails to acquire the lock for some implementation-defined period of time.
 */
public final class InterProcessDeadLockException extends RuntimeException {
    private static final long serialVersionUID = 0L;

    /**
     * Constructs a new {@code InterProcessDeadLockException} with the specified detail message.
     *
     * @param message the detail message. The detail message is saved for later retrieval by the
     *                {@link #getMessage()} method.
     */
    public InterProcessDeadLockException(String message) {
        super(message);
    }

    /**
     * Constructs a new {@code InterProcessDeadLockException} with the specified detail message and
     * cause.
     *
     * @param message the detail message. The detail message is saved for later retrieval by the
     *                {@link #getMessage()} method.
     * @param cause   the cause (which is saved for later retrieval by the {@link #getCause()} method)
     */
    public InterProcessDeadLockException(String message, Throwable cause) {
        super(message, cause);
    }
}
