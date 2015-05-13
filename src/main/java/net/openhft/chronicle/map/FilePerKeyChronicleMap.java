package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.function.SerializableFunction;
import net.openhft.lang.Jvm;
import org.jetbrains.annotations.NotNull;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by daniel on 22/04/15.
 */
public class FilePerKeyChronicleMap implements ChronicleMap<String, String> {
    //    private final ChronicleMap<String, String> chMap;
    private final FilePerKeyMap fpMap;

    public FilePerKeyChronicleMap(String dir) throws IOException {
        fpMap = new FilePerKeyMap(dir);
        fpMap.putReturnsNull(true);
//        fpMap.valueMarshaller(SnappyInputStream::new, SnappyOutputStream::new);
//        fpMap.valueMarshaller(GZIPInputStream::new, GZIPOutputStream::new);
//        chMap = ChronicleMapBuilder.of(String.class, String.class)
//                .entries(500).averageValueSize(2_000_000).create();
    }

    public void registerForEvents(Consumer<FPMEvent> listener) {
        fpMap.registerForEvents(listener);
    }

    @Override
    public int size() {
        return fpMap.size();
    }

    @Override
    public boolean isEmpty() {
        return fpMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return fpMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return fpMap.containsValue(value);
    }

    @Override
    public String get(Object key) {
        return fpMap.get(key);
    }

    @Override
    public String put(String key, String value) {
        return fpMap.put(key, value);
//        return chMap.put(key,value);
    }

    @Override
    public String remove(Object key) {
//        chMap.remove(key);
        return fpMap.remove(key);
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        fpMap.putAll(m);
//        chMap.putAll(m);
    }

    @Override
    public void clear() {
        fpMap.clear();
    }

    @NotNull
    @Override
    public Set<String> keySet() {
        return fpMap.keySet();
    }

    @NotNull
    @Override
    public Collection<String> values() {
        return fpMap.values();
    }

    @NotNull
    @Override
    public Set<Entry<String, String>> entrySet() {
        return fpMap.entrySet();
    }

    @Override
    public String getUsing(String key, String usingValue) {
        throw new UnsupportedOperationException();
//        return chMap.getUsing(key, usingValue);
    }

    @Override
    public String acquireUsing(@NotNull String key, String usingValue) {
        throw new UnsupportedOperationException();
        //@todo ADD THE LOGIC FOR FPMAP
//        return chMap.acquireUsing(key, usingValue);
    }

    @NotNull
    @Override
    public MapKeyContext<String, String> acquireContext(@NotNull String key, @NotNull String usingValue) {
        throw new UnsupportedOperationException();
//        return chMap.acquireContext(key,usingValue);
    }

    @Override
    public <R> R getMapped(String key, @NotNull SerializableFunction<? super String, R> function) {
        throw new UnsupportedOperationException();
//        return chMap.getMapped(key, function);
    }

    @Override
    public String putMapped(String key, @NotNull UnaryOperator<String> unaryOperator) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getAll(File toFile) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(File fromFile) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String newValueInstance() {
        return new String();
    }

    @Override
    public String newKeyInstance() {
        return new String();
    }

    @Override
    public Class<String> valueClass() {
        return String.class;
    }

    @Override
    public File file() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long longSize() {
        return size();
    }

    @Override
    public MapKeyContext<String, String> context(String key) {
        throw new UnsupportedOperationException();
        // return chMap.context(key);
    }

    @Override
    public Class<String> keyClass() {
        return String.class;
    }

    @Override
    public boolean forEachEntryWhile(Predicate<? super MapKeyContext<String, String>> predicate) {
        throw new UnsupportedOperationException();

        // return chMap.forEachEntryWhile(predicate);
    }

    @Override
    public void forEachEntry(Consumer<? super MapKeyContext<String, String>> action) {
        throw new UnsupportedOperationException();
//        chMap.forEachEntry(action);
    }

    @Override
    public void close() {
        fpMap.close();
    }

    @Override
    public String putIfAbsent(String key, String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(String key, String oldValue, String newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String replace(String key, String value) {
        throw new UnsupportedOperationException();
    }
}
