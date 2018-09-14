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

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.ChronicleHashCorruption;
import org.jetbrains.annotations.Nullable;
import org.slf4j.helpers.MessageFormatter;

import java.util.function.Supplier;

public class ChronicleHashCorruptionImpl implements ChronicleHashCorruption {

    private int segmentIndex;
    private Supplier<String> messageSupplier;
    private Throwable exception;
    private String message;

    public static void report(
            ChronicleHashCorruption.Listener corruptionListener,
            ChronicleHashCorruptionImpl corruption, int segmentIndex,
            Supplier<String> messageSupplier) {
        corruption.set(segmentIndex, messageSupplier, null);
        corruptionListener.onCorruption(corruption);
    }

    public static void reportException(
            ChronicleHashCorruption.Listener corruptionListener,
            ChronicleHashCorruptionImpl corruption, int segmentIndex,
            Supplier<String> messageSupplier, Throwable exception) {
        corruption.set(segmentIndex, messageSupplier, exception);
        corruptionListener.onCorruption(corruption);
    }

    public static String format(String message, Object... args) {
        return MessageFormatter.arrayFormat(message, args).getMessage();
    }

    private void set(int segmentIndex, Supplier<String> messageSupplier, Throwable exception) {
        this.segmentIndex = segmentIndex;
        this.messageSupplier = messageSupplier;
        this.exception = exception;
        this.message = null;
    }

    @Override
    public String message() {
        if (message == null) {
            message = messageSupplier.get();
        }
        return message;
    }

    @Nullable
    @Override
    public Throwable exception() {
        return exception;
    }

    @Override
    public int segmentIndex() {
        return segmentIndex;
    }
}
