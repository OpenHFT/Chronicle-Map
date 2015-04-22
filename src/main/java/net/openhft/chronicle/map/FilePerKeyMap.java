package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.function.SerializableFunction;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by daniel on 22/04/15.
 */
public class FilePerKeyMap<K, V> implements ChronicleMap<K, V> {
    private String dir;
    private Path dirPath;

    public FilePerKeyMap(String dir) {
        try {
            dirPath = Paths.get(dir);
            Files.createDirectories(dirPath);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        this.dir = dir;
    }

    @Override
    public int size() {
        return (int)getFiles(dirPath).count();
    }

    @Override
    public boolean isEmpty() {
        return size()==0 ? true: false;
    }

    @Override
    public boolean containsKey(Object key) {
        return getFiles(dirPath).anyMatch(p->p.getFileName().toString().equals(key));
    }

    @Override
    public boolean containsValue(Object value) {
        return false;
    }

    @Override
    public V get(Object key) {
        checkTypeIsString(key);

        Path path = Paths.get(dir,(String)key);
        return (V)getFileContents(path);
    }

    @Override
    public V put(K key, V value) {
        checkTypeIsString(key, value);

        Path path = Paths.get(dir,(String)key);
        String existingValue = getFileContents(path);
        writeToFile(path, (String) value);

        return (V) existingValue;
    }

    private Stream<Path> getFiles(Path path){
        try {
            return Files.walk(path).filter(p -> !Files.isDirectory(p));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private String getFileContents(Path path){
        String existingValue = null;
        if(Files.exists(path)) {
            try {
                existingValue = Files.readAllLines(path).stream().collect(Collectors.joining());
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
        return existingValue;
    }

    private void writeToFile(Path path, String value){
        try {
            Files.write(path, value.getBytes(), StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private void deleteFile(Path path){
        try {
            Files.delete(path);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public V remove(Object key) {
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {

    }

    @Override
    public void clear() {
        getFiles(dirPath).forEach(this::deleteFile);
    }

    @NotNull
    @Override
    public Set<K> keySet() {
        return null;
    }

    @NotNull
    @Override
    public Collection<V> values() {
        return null;
    }

    @NotNull
    @Override
    public Set<Entry<K, V>> entrySet() {
        return null;
    }

    @Override
    public V getUsing(K key, V usingValue) {
        return null;
    }

    @Override
    public V acquireUsing(@NotNull K key, V usingValue) {
        return null;
    }

    @NotNull
    @Override
    public MapKeyContext<K, V> acquireContext(@NotNull K key, @NotNull V usingValue) {
        return null;
    }

    @Override
    public <R> R getMapped(K key, @NotNull SerializableFunction<? super V, R> function) {
        return null;
    }

    @Override
    public V putMapped(K key, @NotNull UnaryOperator<V> unaryOperator) {
        return null;
    }

    @Override
    public void getAll(File toFile) throws IOException {

    }

    @Override
    public void putAll(File fromFile) throws IOException {

    }

    @Override
    public V newValueInstance() {
        return null;
    }

    @Override
    public K newKeyInstance() {
        return null;
    }

    @Override
    public Class<V> valueClass() {
        return null;
    }

    @Override
    public File file() {
        return null;
    }

    @Override
    public long longSize() {
        return 0;
    }

    @Override
    public MapKeyContext<K, V> context(K key) {
        return null;
    }

    @Override
    public Class<K> keyClass() {
        return null;
    }

    @Override
    public boolean forEachEntryWhile(Predicate<? super MapKeyContext<K, V>> predicate) {
        return false;
    }

    @Override
    public void forEachEntry(Consumer<? super MapKeyContext<K, V>> action) {

    }

    @Override
    public void close() {

    }

    @Override
    public V putIfAbsent(K key, V value) {
        return null;
    }

    @Override
    public boolean remove(Object key, Object value) {
        return false;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return false;
    }

    @Override
    public V replace(K key, V value) {
        return null;
    }

    private void checkTypeIsString(Object... objs) {
        for(Object o : objs){
            if(!(o instanceof String))
                throw new AssertionError("FilePerKeyMap only accepts Key and Values " +
                    "of type String. Unsupported type: '" + o + "'");
        }
    }
}
