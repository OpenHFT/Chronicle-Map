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

package net.openhft.chronicle.hash;

import org.slf4j.LoggerFactory;

public final class ChronicleHashErrorListeners {

    public static ChronicleHashErrorListener logging() {
        return LoggingErrorListener.INSTANCE;
    }

    public static ChronicleHashErrorListener error() {
        return ThrowingErrorListener.INSTANCE;
    }

    /**
     * Factories a more flexible than public static instances.
     * We can add some configuration-on-the-first-call in the future.
     */

    private enum LoggingErrorListener implements ChronicleHashErrorListener {
        INSTANCE;

        @Override
        public void onLockTimeout(long threadId) throws IllegalStateException {
            if (threadId > 1L << 32) {
                LoggerFactory.getLogger(getClass()).warn("Grabbing lock held by processId: {}, threadId: {}",
                        (threadId >>> 33), (threadId & 0xFFFFFFL));
            } else {
                LoggerFactory.getLogger(getClass()).warn("Grabbing lock held by threadId: {}", threadId);
            }
        }

        @Override
        public void errorOnUnlock(IllegalMonitorStateException e) {
            LoggerFactory.getLogger(getClass()).warn("Failed to unlock as expected", e);
        }
    }

    private enum ThrowingErrorListener implements ChronicleHashErrorListener {
        INSTANCE;

        @Override
        public void onLockTimeout(long threadId) throws IllegalStateException {
            throw new IllegalStateException("Unable to acquire lock held by threadId: " + threadId);
        }

        @Override
        public void errorOnUnlock(IllegalMonitorStateException e) {
            throw e;
        }
    }
}
