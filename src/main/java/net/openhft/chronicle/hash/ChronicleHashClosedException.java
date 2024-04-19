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

import net.openhft.chronicle.core.io.ClosedIllegalStateException;

/**
 * Thrown when a {@link ChronicleHash} is accessed after {@link ChronicleHash#close()}.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class ChronicleHashClosedException extends ClosedIllegalStateException {
    private static final long serialVersionUID = 0L;

    public ChronicleHashClosedException(ChronicleHash hash) {
        this(hash.toIdentityString());
    }

    public ChronicleHashClosedException(String chronicleHashIdentityString) {
        super("Access to " + chronicleHashIdentityString + " after close()");
    }

    public ChronicleHashClosedException(String s, Throwable t) {
        super(s, t);
    }
}
