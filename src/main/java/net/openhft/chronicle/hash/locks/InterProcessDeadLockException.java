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
