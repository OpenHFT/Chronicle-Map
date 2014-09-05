/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
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

import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.System.arraycopy;
import static net.openhft.chronicle.map.Replica.EntryExternalizable;
import static net.openhft.chronicle.map.Replica.ModificationIterator;
import static net.openhft.chronicle.map.Replica.ModificationNotifier.NOP;

/**
 * This class replicates data from one ReplicatedShareHashMap to another using a queue it was originally
 * written to test the logic in the {@code ReplicatedChronicleMap}
 *
 * @author Rob Austin.
 */

public class QueueReplicator implements Closeable {

    public static final short MAX_NUMBER_OF_ENTRIES_PER_CHUNK = 10;
    private static final Logger LOG = LoggerFactory.getLogger(ReplicatedChronicleMap.class);
    private final AtomicBoolean isWritingEntry = new AtomicBoolean(true);
    private final AtomicBoolean isReadingEntry = new AtomicBoolean(true);
    private final ByteBufferBytes entryBuffer;
    private ByteBufferBytes buffer;

    public QueueReplicator(@NotNull final ModificationIterator modificationIterator,
                           @NotNull final BlockingQueue<byte[]> input,
                           @NotNull final BlockingQueue<byte[]> output,
                           final int entrySize,
                           @NotNull final EntryExternalizable externalizable) {

        //todo HCOLL-71 fix the 128 padding
        final int entrySize0 = entrySize + 128;
        entryBuffer = new ByteBufferBytes(ByteBuffer.allocateDirect(entrySize0));

        // in bound
        Executors.newSingleThreadExecutor(new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                final Thread thread = new Thread(r, "reader-map" + externalizable);
                thread.setDaemon(true);
                return thread;
            }

        }).execute(new Runnable() {

            @Override
            public void run() {

                // this is used in nextEntry() below, its what could be described as callback method

                try {

                    for (; ; ) {

                        byte[] item = null;
                        try {


                            for (; ; ) {
                                isReadingEntry.set(true);
                                item = input.poll();
                                if (item == null) {
                                    isReadingEntry.set(false);
                                    Thread.sleep(1);
                                } else {
                                    break;
                                }

                            }

                            final ByteBufferBytes bufferBytes = new ByteBufferBytes(ByteBuffer.wrap(item));

                            while (bufferBytes.remaining() > 0) {

                                final long entrySize = bufferBytes.readStopBit();

                                final long position = bufferBytes.position();
                                final long limit = bufferBytes.limit();

                                bufferBytes.limit(position + entrySize);
                                externalizable.readExternalEntry(bufferBytes);

                                bufferBytes.position(position);
                                bufferBytes.limit(limit);

                                // skip onto the next entry
                                bufferBytes.skip(entrySize);

                            }
                            isReadingEntry.set(false);

                        } catch (InterruptedException e1) {
                            LOG.warn("", e1);
                        }

                    }
                } catch (Exception e) {
                    LOG.warn("", e);
                }

            }

        });

        // out bound
        Executors.newSingleThreadExecutor().execute(new Runnable() {


            @Override
            public void run() {

                buffer = new ByteBufferBytes(ByteBuffer.allocate(entrySize0 * MAX_NUMBER_OF_ENTRIES_PER_CHUNK));

                // this is used in nextEntry() below, its what could be described as callback method
                final Replica.EntryCallback entryCallback =
                        new Replica.EntryCallback() {
                            @Override
                            public boolean onEntry(Bytes entry, final int chronicleId) {

                                entryBuffer.clear();
                                externalizable.writeExternalEntry(entry, entryBuffer, chronicleId);

                                if (entryBuffer.position() == 0)
                                    return false;

                                //  write the entry len
                                buffer.writeStopBit(entryBuffer.position());

                                //  write the entry
                                entryBuffer.flip();
                                buffer.write(entryBuffer);

                                return true;
                            }

                            @Override
                            public void onBeforeEntry() {
                                isWritingEntry.set(true);
                            }

                        };

                try {
                    for (; ; ) {

                        final boolean wasDataRead = modificationIterator.nextEntry(entryCallback, 0);

                        if (wasDataRead) {
                            isWritingEntry.set(false);
                        } else if (buffer.position() == 0) {
                            isWritingEntry.set(false);
                            continue;
                        }

                        if (buffer.remaining() <= entrySize0 && ((wasDataRead || buffer.position() == 0)))
                            continue;

                        // we are going to create an byte[] so that the buffer can be copied into this.
                        final byte[] source = buffer.buffer().array();
                        final int length = (int) buffer.position();
                        final byte[] dest = new byte[length];

                        arraycopy(source, 0, dest, 0, length);

                        try {
                            output.put(dest);
                        } catch (InterruptedException e1) {
                            LOG.warn("", e1);
                            break;
                        }

                        // clear the buffer for reuse, we can store a maximum of
                        // MAX_NUMBER_OF_ENTRIES_PER_CHUNK in this buffer
                        buffer.clear();

                    }
                } catch (Exception e) {
                    LOG.warn("", e);
                }
            }


        });


    }

    public static Replicator of(final byte localIdentifier,
                                final byte externalIdentifier,
                                @NotNull final BlockingQueue<byte[]> input,
                                @NotNull final BlockingQueue<byte[]> output) {
        return new Replicator() {
            boolean used = false;

            @Override
            public byte identifier() {
                return localIdentifier;
            }

            @Override
            protected Closeable applyTo(ChronicleMapBuilder builder,
                                        Replica map,
                                        EntryExternalizable entryExternalizable)
                    throws IOException {
                if (used)
                    throw new IllegalStateException();
                used = true;
                return new QueueReplicator(
                        map.acquireModificationIterator(externalIdentifier, NOP),
                        input, output, builder.entrySize(), entryExternalizable);
            }
        };
    }

    /**
     * @return true indicates that all the data has been processed ( it lock free so can not be relied upon )
     */
    public boolean isEmpty() {
        final boolean b = isWritingEntry.get() || isReadingEntry.get();
        return !b && buffer.position() == 0;
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}

