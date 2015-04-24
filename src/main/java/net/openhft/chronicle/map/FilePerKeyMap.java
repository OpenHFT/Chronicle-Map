package net.openhft.chronicle.map;

import com.sun.nio.file.SensitivityWatchEventModifier;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 */
public class FilePerKeyMap implements Map<String, String> {
    private final String dir;
    private final Path dirPath;
    private final Map<File, Long> lastModifiedByProgram = new ConcurrentHashMap<>();
    private final List<Consumer<FPMEvent>> listeners = new ArrayList<>();
    private final FPMWatcher fileFpmWatcher = new FPMWatcher();

    public FilePerKeyMap(String dir){
        this.dir = dir;
        this.dirPath = Paths.get(dir);
        try {
            Files.createDirectories(dirPath);
        }catch (IOException e){
            throw new RuntimeException(e);
        }
        fileFpmWatcher.start();
    }

    public void registerForEvents(Consumer<FPMEvent> listener){
        listeners.add(listener);
    }

    public void unregisterForEvents(Consumer<FPMEvent> listener){
        listeners.remove(listener);
    }

    private void fireEvent(FPMEvent event){
        for (Consumer<FPMEvent> listener : listeners) {
            listener.accept(event);
        }
    }

    @Override
    public int size() {
        return (int)getFiles().count();
    }

    @Override
    public boolean isEmpty() {
        return size()==0 ? true: false;
    }

    @Override
    public boolean containsKey(Object key) {

        return getFiles().anyMatch(p->p.getFileName().toString().equals(key));
    }

    @Override
    public boolean containsValue(Object value) {
        return getFiles().anyMatch(p->getFileContents(p).equals(value));
    }

    @Override
    public String get(Object key) {
        Path path = dirPath.resolve((String) key);
        return getFileContents(path);
    }

    @Override
    public String put(String key, String value) {
        Path path = dirPath.resolve(key);
        String existingValue = getFileContents(path);
        writeToFile(path, value);

        return existingValue;
    }

    @Override
    public String remove(Object key) {
        String existing = get(key);
        if(existing != null){
            deleteFile(Paths.get(dir, (String)key));
        }
        return existing;

    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        m.entrySet().stream().forEach(e->put(e.getKey(), e.getValue()));
    }

    @Override
    public void clear() {
        getFiles().forEach(this::deleteFile);
    }

    @NotNull
    @Override
    public Set<String> keySet() {
        return getFiles().map(p -> p.getFileName().toString())
                .collect(Collectors.toSet());
    }

    @NotNull
    @Override
    public Collection<String> values() {
        return getFiles().map(p -> getFileContents(p))
                .collect(Collectors.toSet());

    }

    @NotNull
    @Override
    public Set<Entry<String, String>> entrySet() {
        return getFiles().map(p ->
                (Entry<String, String>) new FPMEntry(p.getFileName().toString(), getFileContents(p)))
                .collect(Collectors.toSet());
    }



    private Stream<Path> getFiles(){
        try {
            return Files.walk(dirPath).filter(p -> !Files.isDirectory(p));
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

    public void close(){
        fileFpmWatcher.interrupt();
    }

    private static class FPMEntry<String> implements Entry<String, String>
    {
        private String key;
        private String value;

        public FPMEntry(String key, String value){
            this.key = key;
            this.value = value;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public String setValue(String value) {
            String lastValue = this.value;
            this.value = value;
            return lastValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FPMEntry<?> fpmEntry = (FPMEntry<?>) o;

            if (key != null ? !key.equals(fpmEntry.key) : fpmEntry.key != null) return false;
            return !(value != null ? !value.equals(fpmEntry.value) : fpmEntry.value != null);

        }

        @Override
        public int hashCode() {
            int result = key != null ? key.hashCode() : 0;
            result = 31 * result + (value != null ? value.hashCode() : 0);
            return result;
        }
    }

    private class FPMWatcher extends Thread{
        @Override
        public void run(){
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
                        for (WatchEvent<?> event : key.pollEvents()) {
                            // get event type
                            WatchEvent.Kind<?> kind = event.kind();

                            // get file name
                            WatchEvent<Path> ev = (WatchEvent<Path>) event;
                            Path fileName = ev.context();

                            String mapKey = fileName.toString();


                            if (kind == StandardWatchEventKinds.OVERFLOW) {
                                continue;
                            } else if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                                Path p = dirPath.resolve(fileName);
                                String mapVal = getFileContents(p);
                                if(isProgrammaticUpdate(p.toFile())){
                                    fireEvent(new FPMEvent(FPMEvent.EventType.NEW, true, mapKey,mapVal));
                                }else {
                                    fireEvent(new FPMEvent(FPMEvent.EventType.NEW, false, mapKey, mapVal));
                                }
                            } else if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
                                Path p = dirPath.resolve(fileName);
                                if(isProgrammaticUpdate(p.toFile())){
                                    fireEvent(new FPMEvent(FPMEvent.EventType.DELETE, true, mapKey, null));
                                    lastModifiedByProgram.remove(p.toFile());
                                }else {
                                    fireEvent(new FPMEvent(FPMEvent.EventType.DELETE, false, mapKey, null));
                                }
                            } else if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                                Path p = dirPath.resolve(fileName);
                                String mapVal = getFileContents(p);
                                if(isProgrammaticUpdate(p.toFile())){
                                    fireEvent(new FPMEvent(FPMEvent.EventType.UPDATE, true, mapKey,mapVal));
                                }else {
                                    fireEvent(new FPMEvent(FPMEvent.EventType.UPDATE, false, mapKey, mapVal));
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        return;
                    }finally{
                        if(key!=null)key.reset();
                    }
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
    }

    private boolean isProgrammaticUpdate(File file){
        if(lastModifiedByProgram.containsKey(file)){
            return file.lastModified() == lastModifiedByProgram.get(file) ? true : false;
        }else{
            return false;
        }
    }

}
