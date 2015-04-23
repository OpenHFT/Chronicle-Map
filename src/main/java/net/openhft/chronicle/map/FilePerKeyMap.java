package net.openhft.chronicle.map;

import com.sun.nio.file.SensitivityWatchEventModifier;
import net.openhft.chronicle.hash.function.SerializableFunction;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
    private Map<File, Long> lastModifiedByProgram = new ConcurrentHashMap<>();

    public FilePerKeyMap(String dir) {
        try {
            this.dir = dir;
            dirPath = Paths.get(dir);
            Files.createDirectories(dirPath);

            new Thread("WatcherThread") {
                public void run() {
                    try {
                        WatchService watcher = FileSystems.getDefault().newWatchService();
                        dirPath.register(watcher, new WatchEvent.Kind[]{
                                    StandardWatchEventKinds.ENTRY_CREATE,
                                    StandardWatchEventKinds.ENTRY_DELETE,
                                    StandardWatchEventKinds.ENTRY_MODIFY},
                                    SensitivityWatchEventModifier.HIGH
                        );

                        while(true){
                            WatchKey key = null;
                            try {
                                key = watcher.take();
                                System.out.println("Event Taken");
                                for (WatchEvent<?> event : key.pollEvents()) {
                                    // get event type
                                    WatchEvent.Kind<?> kind = event.kind();

                                    // get file name
                                    @SuppressWarnings("unchecked")
                                    WatchEvent<Path> ev = (WatchEvent<Path>) event;
                                    Path fileName = ev.context();

                                    String mapKey = fileName.toString();


                                    if (kind == StandardWatchEventKinds.OVERFLOW) {
                                        System.out.println("OVERFLOW");
                                        continue;
                                    } else if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                                        Path p = dirPath.resolve(fileName);
                                        String mapVal = getFileContents(p);
                                        if(isProgrammaticUpdate(p.toFile())){
                                            System.out.println("Programmatic create to " + mapKey);// + " : " + mapVal);
                                        }else {
                                            System.out.println("created " + mapKey);// + " : " + mapVal);
                                        }
                                    } else if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
                                        Path p = dirPath.resolve(fileName);
                                        if(isProgrammaticUpdate(p.toFile())){
                                            System.out.println("Programmatic delete to " + mapKey);
                                            lastModifiedByProgram.remove(p.toFile());
                                        }else {
                                            System.out.println("User delete " + mapKey);
                                        }
                                    } else if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                                        Path p = dirPath.resolve(fileName);
                                        String mapVal = getFileContents(p);
                                        if(isProgrammaticUpdate(p.toFile())){
                                            System.out.println("Programmatic update to " + mapKey);// + " : " + mapVal);
                                        }else {
                                            System.out.println("User modified " + mapKey + " : ");// + mapVal);
                                        }
                                    }
                                }

                            } catch (InterruptedException e) {
                                System.out.println("Watcher service exiting");
                                return;
                            }finally{
                                key.reset();
                            }
                        }
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                }
            }.start();


        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private boolean isProgrammaticUpdate(File file){
        if(lastModifiedByProgram.containsKey(file)){
            return file.lastModified() == lastModifiedByProgram.get(file) ? true : false;
        }else{
            return false;
        }
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
        return getFiles(dirPath).anyMatch(p->getFileContents(p).equals(value));
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

    @Override
    public V remove(Object key) {
        String existing = (String)get(key);
        if(existing != null){
            deleteFile(Paths.get(dir, (String)key));
        }
        return (V)existing;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        m.entrySet().stream().forEach(e->put(e.getKey(), e.getValue()));
    }

    @Override
    public void clear() {
        getFiles(dirPath).forEach(this::deleteFile);
    }

    @NotNull
    @Override
    public Set<K> keySet() {
        return getFiles(dirPath).map(p -> (K) p.getFileName().toString())
                                .collect(Collectors.toSet());
    }

    @NotNull
    @Override
    public Collection<V> values() {
        return getFiles(dirPath).map(p -> (V) getFileContents(p))
                .collect(Collectors.toSet());
    }

    @NotNull
    @Override
    public Set<Entry<K, V>> entrySet() {
        return getFiles(dirPath).map(p ->
                (Entry<K, V>) new FPMEntry(p.getFileName().toString(), getFileContents(p)))
                .collect(Collectors.toSet());
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
            lastModifiedByProgram.put(path.toFile(), path.toFile().lastModified());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private void deleteFile(Path path){
        try {
            Files.delete(path);
            lastModifiedByProgram.put(path.toFile(), path.toFile().lastModified());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }



    private void checkTypeIsString(Object... objs) {
        for(Object o : objs){
            if(!(o instanceof String))
                throw new AssertionError("FilePerKeyMap only accepts Key and Values " +
                    "of type String. Unsupported type: '" + o + "'");
        }
    }

    private static class FPMEntry<K,V> implements Entry
    {
        private K key;
        private V value;

        public FPMEntry(K key, V value){
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public Object setValue(Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FPMEntry<?, ?> entry = (FPMEntry<?, ?>) o;

            if (key != null ? !key.equals(entry.key) : entry.key != null) return false;
            return !(value != null ? !value.equals(entry.value) : entry.value != null);

        }

        @Override
        public int hashCode() {
            int result = key != null ? key.hashCode() : 0;
            result = 31 * result + (value != null ? value.hashCode() : 0);
            return result;
        }
    }
}
