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

/**
 * This exception is thrown, when a Chronicle Hash cannot acquire a required file lock.
 */
public final class ChronicleFileLockException extends RuntimeException {
    private static final long serialVersionUID = -2034623786298623984L;

    /**
     * Constructs a new {@code ChronicleHashLockException} with the specified cause.
     *
     * @param cause the cause (which is saved for later retrieval by the {@link #getCause()} method)
     */
    public ChronicleFileLockException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new {@code ChronicleHashLockException} with the specified detail
     * message.
     *
     * @param message the detail message. The detail message is saved for later retrieval by the
     *                {@link #getMessage()} method.
     */
    public ChronicleFileLockException(String message) {
        super(message);
    }

    public ChronicleFileLockException(String message, Throwable cause) {
        super(message, cause);
    }
}