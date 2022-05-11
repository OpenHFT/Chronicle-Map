package net.openhft.chronicle.hash.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.algo.XxHash;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.hash.serialization.impl.BytesMarshallableDataAccess;
import net.openhft.chronicle.hash.serialization.impl.BytesMarshallableReader;
import net.openhft.chronicle.hash.serialization.impl.StringSizedReader;
import net.openhft.chronicle.hash.serialization.impl.StringUtf8DataAccess;
import net.openhft.chronicle.map.VanillaChronicleMap;
import net.openhft.chronicle.wire.TextWire;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public final class InternalMapFileAnalyzer {

    public static void main(String[] args) throws IOException {

        // Todo: add more aliases
        ClassAliasPool.CLASS_ALIASES.addAlias(
                StringSizedReader.class,
                StringUtf8DataAccess.class,
                BytesMarshallableReader.class,
                BytesMarshallableDataAccess.class
        );

        if (args.length < 1) {
            System.out.println("Usage: MapFileAnalyzer filename");
            return;
        }

        final Path path = Paths.get(args[0]);

        System.out.println("Analyzing " + path.toAbsolutePath());
        System.out.println("Warning, this program is not capable of analyzing all map file types.");

        if (path.toFile().length() > 1L << 31) {
            System.out.println("This program can only handle files that are smaller than 2^31 bytes)");
            return;
        }

        try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(path, EnumSet.of(StandardOpenOption.READ))) {
            final MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            header("Self bootstrapping header");

            final long hashCode = buffer.getLong();
            final int len = buffer.getInt();
            final byte[] headerData = new byte[len];
            buffer.get(headerData);
            final String header = new String(headerData, StandardCharsets.UTF_8);

            final Bytes<Void> bytes = Bytes.allocateDirect(Integer.BYTES + (long) len);
            bytes.writeInt(len);
            bytes.write(headerData);
            final long expectedHashCode = new XxHash(0).applyAsLong(bytes.bytesStore());

            output(0, "hashcode (" + (hashCode == expectedHashCode ? "CORRECT" : "INCORRECT!") + ")", hashCode);
            output(8, "length", len);
            output(12, len, "header", header);

            final VanillaChronicleMap<?, ?, ?> map = (VanillaChronicleMap<?, ?, ?>) TextWire.from(header).objectInput().readObject();

            final Map<String, Object> props = parseProperties(map);

            align(buffer, 64);

            // Undocumented hole in the layout
            buffer.position(buffer.position() + 16);

            header("Global Mutable State");

            output64(buffer, "lock");
            output24u(buffer, "allocatedExtraTierBulks");
            output40u(buffer, "firstFreeTierIndex");
            output40u(buffer, "extraTiersInUse");
            int segmentHeadersOffset = Math.toIntExact(output32u(buffer, "segmentHeadersOffset"));
            output64(buffer, "dataStoreSize");
            output64(buffer, "currentCleanupSegmentIndex(r)");
            output8(buffer, "modificationIteratorsCount(r)");
            align(buffer, 2);
            output64(buffer, "modificationIteratorInitAt(r)0");
            output64(buffer, "modificationIteratorInitAt(r)1");

            header("Segment Headers Area");
            segmentHeadersOffset = 0x005c1000; // Why is this not picked up properly?
            buffer.position(segmentHeadersOffset);
            final int actualSegments = (int) props.get("actualSegments");
            System.out.println("actualSegments = " + actualSegments);
            final int segmentHeaderSize = (int) props.get("segmentHeaderSize");
            System.out.println("segmentHeaderSize = " + segmentHeaderSize);

            for (int segment = 0; segment < actualSegments; segment++) {
                buffer.position(segmentHeadersOffset + segment * segmentHeaderSize);
                if (segment != 0) {
                    System.out.println();
                }
                output64(buffer, "lock " + segment);
                output32u(buffer, "entries");
                output32u(buffer, "smallestIndexPossiblyFree");
                output64(buffer, "nextSegmentTier");
                output64(buffer, "deletedOffset");
            }

            header("Main Segments Area");
            align(buffer, 256); // According to doc, it is not aligned
            final int mainSegmentAreaStart = buffer.position();
            final long tierSize = (long) props.get("tierSize");
            System.out.println("tierSize = " + tierSize);
            final long tierHashLookupCapacity = (long) props.get("tierHashLookupCapacity");
            System.out.println("tierHashLookupCapacity = " + tierHashLookupCapacity);
            final int tierHashLookupSlotSize = (int) props.get("tierHashLookupSlotSize");
            System.out.println("tierHashLookupSlotSize = " + tierHashLookupSlotSize);
            final int tierHashLookupKeyBits = (int) props.get("tierHashLookupKeyBits");
            System.out.println("tierHashLookupKeyBits = " + tierHashLookupKeyBits);
            final long tierHashLookupOuterSize = (long) props.get("tierHashLookupOuterSize");
            System.out.println("tierHashLookupOuterSize = " + tierHashLookupOuterSize);
            final int actualChunksPerSegmentTier = Math.toIntExact((long) props.get("actualChunksPerSegmentTier"));
            System.out.println("actualChunksPerSegmentTier = " + actualChunksPerSegmentTier);
            final int tierFreeListOuterSize = Math.toIntExact((long) props.get("tierFreeListOuterSize"));
            System.out.println("tierFreeListOuterSize = " + tierFreeListOuterSize);
            final int tierEntrySpaceInnerOffset = (int) props.get("tierEntrySpaceInnerOffset");
            System.out.println("tierEntrySpaceInnerOffset = " + tierEntrySpaceInnerOffset);
            final int chunkSize = Math.toIntExact((long) props.get("chunkSize"));
            System.out.println("chunkSize = " + chunkSize);

            final List<Integer> cardinalities = new ArrayList<>();
            long oldPos = buffer.position();
            for (int segment = 0; segment < actualSegments; segment++) {

                header("Segment Tier Structure " + segment);
                buffer.position((int) (oldPos + segment * tierSize));
                header("Hash Lookup " + segment);

                final int entrySpacePos = (int) (oldPos + segment * tierSize + tierHashLookupOuterSize + 64 + tierFreeListOuterSize + tierEntrySpaceInnerOffset);

                int usedSlots = 0;
                for (int hl = 0; hl < tierHashLookupCapacity; hl++) {
                    final int pos = buffer.position();
                    final long hlv = unsignedValue(buffer, tierHashLookupSlotSize);
                    if (hlv != 0) {
                        usedSlots++;
                        final long hashLookupKey = hlv & ((1L << tierHashLookupKeyBits) - 1);
                        final long hashLookupValue = hlv >> tierHashLookupKeyBits;
                        final int chunkAddress = (int) (entrySpacePos + hashLookupValue * chunkSize);
                        final byte keySize = buffer.get(chunkAddress); // Assume stop-bit todo: Use the configured reader
                        final String keyText;
                        if (keySize > 0) {
                            keyText = read8Bit(buffer, chunkAddress + 1, keySize);
                        } else {
                            keyText = "";
                        }
                        outputFormat("%08X - %08X  %010X %010X %010X -> %08X %s %n",
                                pos, pos + tierHashLookupSlotSize - 1, hl,
                                hashLookupKey, hashLookupValue, chunkAddress, keyText);
                    } else {
                        outputFormat("%08X - %08X  %010X%n", pos, pos + tierHashLookupSlotSize - 1, hl);
                    }
                }
                cardinalities.add(usedSlots);
                outputFormat("%d cardinality (%.2f%% used)) %n", usedSlots, 100.00d * ((double) usedSlots) / actualChunksPerSegmentTier);

                header("Segment Tier Counters Area " + segment);
                buffer.position((int) (oldPos + segment * tierSize + tierHashLookupOuterSize));
                output64(buffer, "nextTierIndex(x)");
                output64(buffer, "previousTierIndex(x)");
                output64(buffer, "lowestPossiblyFreeChunk(x)");
                output32u(buffer, "segmentIndex(x)"); //
                output32u(buffer, "tier(x)"); //
                output32u(buffer, "entries(x)"); //
                output32u(buffer, "deleted(x)"); //

                header("Free List " + segment);
                final int freeListPos = (int) (oldPos + segment * tierSize + tierHashLookupOuterSize + 64);
                buffer.position(freeListPos);
                final byte[] bits = new byte[actualChunksPerSegmentTier / 8];
                buffer.get(bits);
                final int cardinality = cardinality(bits);
                outputFormat("%08X - %08X  BITMAP (%d bytes, %d bits, %d cardinality (%.2f%% used)) %n", freeListPos, freeListPos + bits.length - 1, bits.length, actualChunksPerSegmentTier, cardinality, 100.00d * ((double) cardinality) / actualChunksPerSegmentTier);

                header("Entry Space " + segment);
                buffer.position(entrySpacePos);
                final int chunksBytes = actualChunksPerSegmentTier * chunkSize;
                outputFormat("%08X - %08X  CHUNKS (%d bytes)%n", entrySpacePos, entrySpacePos + chunksBytes, chunksBytes);

                // break;
            }

            header("Allocation overview");
            for (int segment = 0; segment < cardinalities.size(); segment++) {
                final int cardinality = cardinalities.get(segment);
                outputFormat("%3d %10d (%2.2f%%)%n", segment, cardinality, 100.00d * ((double) cardinality) / actualChunksPerSegmentTier);
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    private static String read8Bit(ByteBuffer buffer, int address, int length) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            final byte b = buffer.get(address + i);
            if (b > 0x20 && b < 0x7E) {
                sb.append((char) b);
            } else {
                sb.append(".");
            }
        }
        return sb.toString();
    }

    private static Map<String, Object> parseProperties(Object map) {
        final Map<String, Object> props = new LinkedHashMap<>();
        for (Field field : fields(map.getClass())) {
            try {
                final Object value = field.get(map);
                if (value != null) {
                    props.put(field.getName(), field.get(map));
                }
            } catch (IllegalAccessException ignore) {
                // do nothing
            }
        }
        return props;
    }

    private static boolean showType(Class<?> type) {
        return Number.class.isAssignableFrom(type) ||
                Boolean.class.equals(type) ||
                type.isPrimitive();
    }

    private static Set<Field> fields(Class<?> type) {
        return fields0(new LinkedHashSet<>(), type);
    }

    private static Set<Field> fields0(Set<Field> fields, Class<?> type) {
        fields.addAll(Arrays.asList(type.getDeclaredFields()));
        if (!type.getSuperclass().equals(Object.class)) {
            fields0(fields, type.getSuperclass());
        }
        return fields;
    }

    static void header(final String tag) {
        outputFormat("%n*** %s ***%n", tag);
    }

    static int output8(ByteBuffer buffer, final String tag) {
        final long pos = buffer.position();
        final byte val = buffer.get();
        output(pos, tag, val);
        return (int) val;
    }

    static int output24u(ByteBuffer buffer, final String tag) {
        final long pos = buffer.position();
        final long val = unsignedValue(buffer, 3);
        output24u(pos, tag, val);
        return (int) val;
    }

    static long output32u(ByteBuffer buffer, final String tag) {
        final long pos = buffer.position();
        long val = unsignedValue(buffer, 4);
        output32u(pos, tag, val);
        return val;
    }

    static long output40u(ByteBuffer buffer, final String tag) {
        final long pos = buffer.position();
        final long val = unsignedValue(buffer, 5);
        output40u(pos, tag, val);
        return val;
    }

    static long output64(ByteBuffer buffer, final String tag) {
        final long pos = buffer.position();
        final long val = buffer.getLong();
        output(pos, tag, val);
        return val;
    }

    static void output(final long addr, final String tag, final long out) {
        outputFormat("%08X - %08X  %-30s: 0x%016X (%d)%n", addr, addr + Long.BYTES - 1, tag, out, out);
    }

    static void output24u(final long addr, final String tag, final long out) {
        outputFormat("%08X - %08X  %-30s: 0x%06X           (%d)%n", addr, addr + 3 - 1, tag, out, out);
    }

    static void output32u(final long addr, final String tag, final long out) {
        outputFormat("%08X - %08X  %-30s: 0x%08X         (%d)%n", addr, addr + 4 - 1, tag, out, out);
    }

    static void output40u(final long addr, final String tag, final long out) {
        outputFormat("%08X - %08X  %-30s: 0x%010X       (%d)%n", addr, addr + 5 - 1, tag, out, out);
    }

    static void output(final long addr, final String tag, final int out) {
        outputFormat("%08X - %08X  %-30s: 0x%08X         (%d)%n", addr, addr + Integer.BYTES - 1, tag, out, out);
    }

    static void output(final long addr, final String tag, final byte out) {
        outputFormat("%08X - %08X  %-30s: 0x%02X               (%d)%n", addr, addr, tag, out, out);
    }

    static void output(final long addr, final int length, final String tag, final Object out) {
        outputFormat("%08X - %08X  %-30s: %s%n", addr, addr + length - 1, tag, out.toString());
    }

    private static void outputFormat(String format, Object ... args) {
        System.out.format(format, args);
    }

    static void align(ByteBuffer buffer, int boundary) {
        if ((buffer.position() % boundary) == 0)
            return;
        final int alignmentLength = boundary - (buffer.position() % boundary);
        final byte[] alignment = new byte[alignmentLength];
        buffer.get(alignment);
        final String text = Arrays.toString(alignment) + " (" + alignmentLength + " bytes)";
        output(buffer.position() - (long) alignmentLength, alignmentLength, "[alignmentBytes]", text);
    }

    static long unsignedValue(ByteBuffer buffer, int bytes) {
        long val = 0;
        int shifts = 0;
        for (int i = 0; i < bytes; i++) {
            final long b = Byte.toUnsignedLong(buffer.get());
            final long term = b << shifts;
            val |= term;
            shifts += 8;
        }
        return val;
    }

    static int cardinality(byte[] bits) {
        int cnt = 0;
        for (int i = 0; i < bits.length; i++) {
            cnt += Integer.bitCount(Byte.toUnsignedInt(bits[i]));
        }
        return cnt;
    }

}