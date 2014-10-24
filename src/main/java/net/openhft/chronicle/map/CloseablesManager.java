/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

final class CloseablesManager implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(CloseablesManager.class.getName());

    private boolean isClosed = false;
    private final Set<Closeable> closeables = new LinkedHashSet<Closeable>();

    private void checkState() {
        if (isClosed)
            throw new IllegalStateException();
    }

    public synchronized void add(Closeable closeable) {
        checkState();
        if (closeable == null)
            throw new NullPointerException();
        closeables.add(closeable);
    }

    public synchronized void close(Closeable closeable) throws IOException {
        checkState();
        for (Iterator<Closeable> iterator = closeables.iterator(); iterator.hasNext(); ) {
            if (iterator.next() == closeable)
                iterator.remove();
        }
        closeable.close();
    }

    public synchronized void closeQuietly(Closeable closeable) {
        try {
            close(closeable);
        } catch (IllegalStateException e) {
            // this can occur if already closed ( for example closed is called from another thread )
        } catch (IOException e) {
            LOG.error("", e);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        IOException ex = null;
        for (Closeable closeable : closeables) {
            try {
                closeable.close();
            } catch (IOException e) {
                LOG.error("", e);
                ex = e;
            }
        }
        closeables.clear();
        isClosed = true;
        if (ex != null)
            throw ex;
    }

    public synchronized void closeQuietly() {
        try {
            close();
        } catch (IOException e) {
            // do nothing
        }
    }

    boolean isClosed() {
        return isClosed;
    }

    public boolean isEmpty() {
        return closeables.isEmpty();
    }
}
