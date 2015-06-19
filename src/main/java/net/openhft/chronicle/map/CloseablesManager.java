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

package net.openhft.chronicle.map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

final class CloseablesManager implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(CloseablesManager.class.getName());

    private final List<Closeable> closeables = new ArrayList<>();

    public synchronized void add(Closeable closeable) {
        if (closeable == null)
            throw new NullPointerException();
        if (!closeables.contains(closeable))
            closeables.add(closeable);
    }

    public synchronized void close(Closeable closeable) throws IOException {
        closeables.remove(closeable);
        closeable.close();
    }

    public synchronized void closeQuietly(Closeable closeable) {
        try {
            close(closeable);
        } catch (IOException e) {
            LOG.error("", e);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        IOException ex = null;
        for (int i = closeables.size() - 1; i >= 0; i--) {
            Closeable closeable = closeables.get(i);
            try {
                closeable.close();
            } catch (NullPointerException e) {
                // at java.nio.channels.spi.AbstractSelectableChannel.removeKey
                // (AbstractSelectableChannel.java:129) is throwing a NULL on close
                LOG.debug("", e);
            } catch (IOException e) {
                LOG.debug("", e);
                ex = e;
            }
        }
        closeables.clear();

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

    public synchronized boolean isEmpty() {
        return closeables.isEmpty();
    }
}
