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

import com.sun.jdi.connect.spi.ClosedConnectionException;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.RemoteCallTimeoutException;
import net.openhft.chronicle.hash.impl.util.BuildVersion;
import net.openhft.chronicle.hash.impl.util.CloseablesManager;
import net.openhft.chronicle.map.MapWireHandler.EventId;
import net.openhft.chronicle.network2.event.EventGroup;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static net.openhft.chronicle.map.MapWireHandler.EventId.applicationVersion;
import static net.openhft.chronicle.map.MapWireHandler.SIZE_OF_SIZE;
import static net.openhft.chronicle.map.MapWireHandlerBuilder.Fields.*;

/**
 * Created by Rob Austin
 */
public class ClientWiredStatelessTcpConnectionHub {

    private static final Logger LOG = LoggerFactory.getLogger(StatelessChronicleMap.class);

    protected final String name;
    protected final InetSocketAddress remoteAddress;
    protected final long timeoutMs;
    protected final int tcpBufferSize;
    private final ReentrantLock inBytesLock = new ReentrantLock(true);
    private final ReentrantLock outBytesLock = new ReentrantLock();


    @NotNull
    private final AtomicLong transactionID = new AtomicLong(0);
    private final ClientWiredChronicleMapStatelessBuilder config;
    @Nullable
    protected CloseablesManager closeables;

    private final Wire outWire = new TextWire(Bytes.elasticByteBuffer());

    long largestChunkSoFar = 0;
    private final Wire intWire = new TextWire(Bytes.elasticByteBuffer());

    //  used by the enterprise version
    protected int localIdentifier;


    private SocketChannel clientChannel;
    // this is a transaction id and size that has been read by another thread.
    private volatile long parkedTransactionId;

    private volatile long parkedTransactionTimeStamp;
    private long limitOfLast = 0;

    // set up in the header
    private long startTime;
    private boolean doHandShaking;


    public ClientWiredStatelessTcpConnectionHub(
            ClientWiredChronicleMapStatelessBuilder config, byte localIdentifier,
            boolean doHandShaking) {
        this.localIdentifier = localIdentifier;
        this.doHandShaking = doHandShaking;
        this.remoteAddress = config.remoteAddress();
        this.tcpBufferSize = config.tcpBufferSize();
        this.config = config;
        this.name = config.name();
        this.timeoutMs = config.timeoutMs();
        attemptConnect(remoteAddress);
        checkVersion(config.channelID());
    }

    private synchronized void attemptConnect(final InetSocketAddress remoteAddress) {

        // ensures that the excising connection are closed
        closeExisting();

        try {
            SocketChannel socketChannel = AbstractChannelReplicator.openSocketChannel(closeables);

            if (socketChannel.connect(remoteAddress))
                clientChannel = socketChannel;


        } catch (IOException e) {
            LOG.error("", e);
            if (closeables != null) closeables.closeQuietly();
            clientChannel = null;
        }
    }

    ReentrantLock inBytesLock() {
        return inBytesLock;
    }

    ReentrantLock outBytesLock() {
        return outBytesLock;
    }

    protected void checkVersion(short channelID) {

        final String serverVersion = serverApplicationVersion(channelID);
        final String clientVersion = clientVersion();

        if (!serverVersion.equals(clientVersion)) {
            LOG.warn("DIFFERENT CHRONICLE-MAP VERSIONS: The Chronicle-Map-Server and " +
                    "Stateless-Client are on different " +
                    "versions, " +
                    " we suggest that you use the same version, server=" +
                    serverApplicationVersion(channelID) + ", " +
                    "client=" + clientVersion);
        }
    }

    public static boolean IS_DEBUG = java.lang.management.ManagementFactory.getRuntimeMXBean().
            getInputArguments().toString().indexOf("jdwp") >= 0;

    private void checkTimeout(long timeoutTime) {
        if (timeoutTime < System.currentTimeMillis() && !IS_DEBUG)
            throw new RemoteCallTimeoutException();
    }

    protected synchronized void lazyConnect(final long timeoutMs,
                                            final InetSocketAddress remoteAddress) {
        if (clientChannel != null)
            return;

        if (LOG.isDebugEnabled())
            LOG.debug("attempting to connect to " + remoteAddress + " ,name=" + name);

        SocketChannel result;

        long timeoutAt = System.currentTimeMillis() + timeoutMs;

        for (; ; ) {
            checkTimeout(timeoutAt);

            // ensures that the excising connection are closed
            closeExisting();

            try {
                result = AbstractChannelReplicator.openSocketChannel(closeables);
                if (!result.connect(remoteAddress)) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    continue;
                }

                result.socket().setTcpNoDelay(true);
                if (doHandShaking)

                break;
            } catch (IOException e) {
                if (closeables != null) closeables.closeQuietly();
            } catch (Exception e) {
                if (closeables != null) closeables.closeQuietly();
                throw e;
            }
        }
        clientChannel = result;

        checkVersion(config.channelID());

    }


    /**
     * closes the existing connections and establishes a new closeables
     */
    protected void closeExisting() {
        // ensure that any excising connection are first closed
        if (closeables != null)
            closeables.closeQuietly();

        closeables = new CloseablesManager();
    }



    public synchronized void close() {

        if (closeables != null)
            closeables.closeQuietly();

        closeables = null;
        clientChannel = null;

    }

    /**
     * the transaction id are generated as unique timestamps
     *
     * @param time in milliseconds
     * @return a unique transactionId
     */
    long nextUniqueTransaction(long time) {
        long id = time;
        for (; ; ) {
            long old = transactionID.get();
            if (old >= id) id = old + 1;
            if (transactionID.compareAndSet(old, id))
                break;
        }
        return id;
    }

    @NotNull
    public String serverApplicationVersion(short channelID) {
        TextWire wire = new TextWire(Bytes.elasticByteBuffer());
        String result = proxyReturnString(applicationVersion, channelID, wire);
        return (result == null) ? "" : result;
    }

    @SuppressWarnings("WeakerAccess")
    @NotNull
    String clientVersion() {
        return BuildVersion.version();
    }


    /**
     * sends data to the server via TCP/IP
     *
     * @param wire the {@code wire} containing the outbound data
     */
    void writeSocket(@NotNull final Wire wire) {

        assert outBytesLock().isHeldByCurrentThread();
        assert !inBytesLock().isHeldByCurrentThread();


        final long timeoutTime = startTime + this.timeoutMs;
        try {

            for (; ; ) {
                if (clientChannel == null)
                    lazyConnect(timeoutMs, remoteAddress);
                try {
                    writeLength(wire);

                    // send out all the bytes
                    writeSocket(wire, timeoutTime);
                    break;

                } catch (@NotNull java.nio.channels.ClosedChannelException |
                        ClosedConnectionException e) {
                    checkTimeout(timeoutTime);
                    lazyConnect(timeoutMs, remoteAddress);
                }
            }
        } catch (IOException e) {
            close();
            throw new IORuntimeException(e);
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    private void writeLength(Wire outWire) {
        assert outBytesLock().isHeldByCurrentThread();
        final Bytes<?> bytes = outWire.bytes();
        long position = bytes.position();
        if (position > Integer.MAX_VALUE || position < Integer.MIN_VALUE)
            throw new IllegalStateException("message too large");

        long pos = bytes.position();
        try {
            bytes.reset();
            int size = (int) (position - bytes.position());
            bytes.writeUnsignedShort(size - SIZE_OF_SIZE);
        } finally {
            bytes.position(pos);
        }
    }

    protected Wire proxyReply(long timeoutTime, final long tid) {

        assert inBytesLock().isHeldByCurrentThread();

        try {

            final Wire wire = proxyReplyThrowable(timeoutTime, tid);

            // handle an exception if the message contains the isException field
            if (wire.read(() -> "isException").bool()) {
                final String text = wire.read(() -> "exception").text();
                throw new RuntimeException(text);
            }
            return wire;
        } catch (IOException e) {
            close();
            throw new IORuntimeException(e);
        } catch (RuntimeException e) {
            close();
            throw e;
        } catch (Exception e) {
            close();
            throw new RuntimeException(e);
        } catch (AssertionError e) {
            LOG.error("name=" + name, e);
            throw e;
        }
    }


    private Wire proxyReplyThrowable(long timeoutTime, long tid) throws IOException {

        assert inBytesLock().isHeldByCurrentThread();

        for (; ; ) {

            // read the next item from the socket
            if (parkedTransactionId == 0) {

                assert parkedTransactionTimeStamp == 0;

                // if we have processed all the bytes that we have read in
                final Bytes<?> bytes = intWire.bytes();
                if (inWireByteBuffer().position() == bytes.position())
                    inWireClear();

                // todo change the size to include the meta data bit
                // reads just the size
                readSocket(SIZE_OF_SIZE, timeoutTime);

                final int messageSize =
                        bytes.readUnsignedShort(bytes.position());

                assert messageSize > 0 : "Invalid message size " + messageSize;
                assert messageSize < 16 << 20 : "Invalid message size " + messageSize;

                final int remainingBytes0 = messageSize;
                readSocket(remainingBytes0, timeoutTime);


                bytes.skip(SIZE_OF_SIZE);
                bytes.limit(bytes.position() + messageSize);

                System.out.println("\n--------------------------------\n" +
                        "client read:\n\n" + Bytes.toDebugString(bytes));

                int headerlen = bytes.readVolatileInt();

                assert !Wires.isData(headerlen);

                long tid0 = intWire.read(MapWireHandlerBuilder.Fields.tid).int64();


                // if the transaction id is for this thread process it
                if (tid0 == tid) {
                    clearParked();
                    return intWire;

                } else {

                    // if the transaction id is not for this thread, park it
                    // and allow another thread to pick it up
                    parkedTransactionTimeStamp = System.currentTimeMillis();
                    parkedTransactionId = tid0;
                    pause();
                    continue;
                }
            }

            // the transaction id was read by another thread, but is for this thread, process it
            if (parkedTransactionId == tid) {
                clearParked();
                return intWire;
            }

            // time out the old transaction id
            if (System.currentTimeMillis() - timeoutTime >
                    parkedTransactionTimeStamp) {

                LOG.error("name=" + name, new IllegalStateException("Skipped Message with " +
                        "transaction-id=" +
                        parkedTransactionTimeStamp +
                        ", this can occur when you have another thread which has called the " +
                        "stateless client and terminated abruptly before the message has been " +
                        "returned from the server and hence consumed by the other thread."));

                // read the the next message
                clearParked();
                pause();
            }

        }

    }

    /**
     * clears the wire and its underlying byte buffer
     */
    private void inWireClear() {
        inWireByteBuffer().clear();
        final Bytes<?> bytes = intWire.bytes();
        bytes.clear();
    }

    private void clearParked() {
        assert inBytesLock().isHeldByCurrentThread();
        parkedTransactionId = 0;
        parkedTransactionTimeStamp = 0;
    }

    private void pause() {

        assert !outBytesLock().isHeldByCurrentThread();
        assert inBytesLock().isHeldByCurrentThread();

        /// don't call inBytesLock.isHeldByCurrentThread() as it not atomic
        inBytesLock().unlock();

        // allows another thread to enter hear
        inBytesLock().lock();
    }

    /**
     * reads up to the number of byte in {@code requiredNumberOfBytes} from the socket
     *
     * @param requiredNumberOfBytes the number of bytes to read
     * @param timeoutTime           timeout in milliseconds
     * @return bytes read from the TCP/IP socket
     * @throws java.io.IOException socket failed to read data
     */
    @SuppressWarnings("UnusedReturnValue")
    private void readSocket(int requiredNumberOfBytes, long timeoutTime) throws IOException {

        assert inBytesLock().isHeldByCurrentThread();

        ByteBuffer buffer = inWireByteBuffer();
        int position = buffer.position();

        try {
            buffer.limit(position + requiredNumberOfBytes);
        } catch (IllegalArgumentException e) {
            buffer = inWireByteBuffer(position + requiredNumberOfBytes);
            buffer.limit(position + requiredNumberOfBytes);
            buffer.position(position);
        }

        long start = buffer.position();

        while (buffer.position() - start < requiredNumberOfBytes) {
            assert clientChannel != null;

            int len = clientChannel.read(buffer);

            if (len == -1)
                throw new IORuntimeException("Disconnection to server");

            checkTimeout(timeoutTime);
        }
    }

    private ByteBuffer inWireByteBuffer() {
        final Bytes<?> bytes = intWire.bytes();
        return (ByteBuffer) bytes.underlyingObject();
    }

    private ByteBuffer inWireByteBuffer(long requiredCapacity) {
        final Bytes<?> bytes = intWire.bytes();
        bytes.ensureCapacity(requiredCapacity);
        return (ByteBuffer) bytes.underlyingObject();
    }

    /**
     * writes the bytes to the socket
     *
     * @param outWire     the data that you wish to write
     * @param timeoutTime how long before a we timeout
     * @throws java.io.IOException
     */
    private void writeSocket(Wire outWire, long timeoutTime) throws IOException {

        assert outBytesLock().isHeldByCurrentThread();
        assert !inBytesLock().isHeldByCurrentThread();

        final Bytes<?> bytes = outWire.bytes();
        long outBytesPosition = bytes.position();


        // if we have other threads waiting to send and the buffer is not full,
        // let the other threads write to the buffer
        if (outBytesLock().hasQueuedThreads() &&
                outBytesPosition + largestChunkSoFar <= tcpBufferSize) {
            return;
        }

        final ByteBuffer outBuffer = (ByteBuffer) bytes.underlyingObject();
        outBuffer.limit((int) bytes.position());
        outBuffer.position(SIZE_OF_SIZE);

        if (EventGroup.IS_DEBUG ) {
            writeBytesToStandardOut(bytes, outBuffer);
        }

        outBuffer.position(0);

        upateLargestChunkSoFarSize(outBuffer);

        while (outBuffer.remaining() > 0) {

            int len = clientChannel.write(outBuffer);

            if (len == -1)
                throw new IORuntimeException("Disconnection to server");


            // if we have queued threads then we don't have to write all the bytes as the other
            // threads will write the remains bytes.
            if (outBuffer.remaining() > 0 && outBytesLock().hasQueuedThreads() &&
                    outBuffer.remaining() + largestChunkSoFar <= tcpBufferSize) {

                if (LOG.isDebugEnabled())
                    LOG.debug("continuing -  without all the data being written to the buffer as " +
                            "it will be written by the next thread");
                outBuffer.compact();
                bytes.limit(outBuffer.limit());
                bytes.position(outBuffer.position());
                return;
            }

            checkTimeout(timeoutTime);

        }

        outBuffer.clear();
        bytes.clear();

    }

    private void writeBytesToStandardOut(Bytes<?> bytes, ByteBuffer outBuffer) {
        final long position = bytes.position();
        final long limit = bytes.limit();
        try {

            bytes.limit(outBuffer.limit());
            bytes.position(outBuffer.position());
            System.out.println("\n--------------------------------------------\n" +
                    "client writes:\n\n" +
                    //        Bytes.toDebugString(outBuffer));
                    Wires.fromSizePrefixedBlobs(bytes));
        } finally {
            bytes.limit(limit);
            bytes.position(position);
        }
    }

    /**
     * calculates the size of each chunk
     *
     * @param outBuffer
     */
    private void upateLargestChunkSoFarSize(ByteBuffer outBuffer) {
        int sizeOfThisChunk = (int) (outBuffer.limit() - limitOfLast);
        if (largestChunkSoFar < sizeOfThisChunk)
            largestChunkSoFar = sizeOfThisChunk;

        limitOfLast = outBuffer.limit();
    }


    private long proxySend(@NotNull final EventId methodName,
                           final long startTime,
                           short channelID,
                           @NotNull final Wire wire) {

        assert outBytesLock().isHeldByCurrentThread();
        assert !inBytesLock().isHeldByCurrentThread();

        // send
        outBytesLock().lock();
        try {
            long tid = writeHeader(startTime, channelID, wire);
            wire.writeDocument(false, new Consumer<WireOut>() {
                @Override
                public void accept(WireOut wireOut) {
                    wireOut.writeEventName(methodName);
                }
            });

            writeSocket(wire);
            return tid;
        } finally {
            outBytesLock().unlock();
        }
    }

    @SuppressWarnings("SameParameterValue")
    @Nullable
    String proxyReturnString(@NotNull final EventId messageId, short channelID) {
        return proxyReturnString(messageId, channelID, outWire);
    }

    @SuppressWarnings("SameParameterValue")
    @Nullable
    String proxyReturnString(@NotNull final EventId messageId, short channelID, Wire outWire) {
        final long startTime = System.currentTimeMillis();
        long tid;

        outBytesLock().lock();
        try {
            tid = proxySend(messageId, startTime, channelID, outWire);
        } finally {
            outBytesLock().unlock();
        }

        long timeoutTime = startTime + this.timeoutMs;

        // receive
        inBytesLock().lock();
        try {
            return proxyReply(timeoutTime, tid).read(reply).text();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            inBytesLock().unlock();
        }
    }


    Wire outWire() {
        assert outBytesLock().isHeldByCurrentThread();
        return outWire;
    }

    long writeHeader(long startTime, short channelID, Wire wire) {

        assert outBytesLock().isHeldByCurrentThread();
        markSize(wire);
        startTime(startTime);

        long tid = nextUniqueTransaction(startTime);
        wire.writeDocument(true, new Consumer<WireOut>() {
            @Override
            public void accept(WireOut wireOut) {
                wireOut.write(csp).text("MAP");
                wireOut.write(MapWireHandlerBuilder.Fields.tid).int64(tid);
                wireOut.write(timeStamp).int64(startTime);
                wireOut.write(channelId).int16(channelID);
            }
        });

        final Bytes<?> bytes = wire.bytes();
        System.out.println(Bytes.toDebugString(wire.bytes(), 0, bytes.position()));

        return tid;
    }


    /**
     * mark the location of the outWire size
     *
     * @param outWire
     */
    void markSize(Wire outWire) {

        assert outBytesLock().isHeldByCurrentThread();

        // this is where the size will go
        final Bytes<?> bytes = outWire.bytes();
        bytes.mark();

        // skip the 2 bytes for the size
        bytes.skip(2);
    }

    void startTime(long startTime) {
        this.startTime = startTime;
    }
}
