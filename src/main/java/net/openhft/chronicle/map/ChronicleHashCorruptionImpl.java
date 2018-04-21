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
