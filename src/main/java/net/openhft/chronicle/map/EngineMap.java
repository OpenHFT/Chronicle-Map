package net.openhft.chronicle.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.function.SerializableFunction;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Created by Rob Austin
 */
public class EngineMap<K, V> implements ChronicleMap<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(EngineMap.class);

    private final Class<K> kClass;
    private final Class<V> vClass;
    private final MapWireConnectionHub mapWireConnectionHub;
    private final Bytes<ByteBuffer> buffer = Bytes.elasticByteBuffer();
    private final Map<byte[], byte[]> map;
    private final Class<? extends Wire> wireType;

    public EngineMap(String name,
                     Class<K> kClass,
                     Class<V> vClass,
                     MapWireConnectionHub mapWireConnectionHub,
                     Class<? extends Wire> wireType) throws IOException {
        this.mapWireConnectionHub = mapWireConnectionHub;
        this.wireType = wireType;
        final BytesChronicleMap b = this.mapWireConnectionHub.acquireMap(name);
        this.map = (Map) b.delegate;
        this.vClass = vClass;
        this.kClass = kClass;
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }


    Wire toWire() {
        buffer.clear();
        if (TextWire.class.isAssignableFrom(wireType)) {
            return new TextWire(buffer);
        } else throw new UnsupportedOperationException("todo");
    }


    Wire toWire(byte[] bytes) {

        Bytes<byte[]> wrap = Bytes.wrap(bytes);

        if (TextWire.class.isAssignableFrom(wireType)) {
            return new TextWire(wrap);
        } else throw new UnsupportedOperationException("todo");
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(bytes(key));
    }

    private byte[] bytes(Object b) {
        if (b == null)
            return null;
        final Wire wire = toWire();
        AbstactStatelessClient.writeField(b, wire.getValueOut());
        wire.bytes().flip();

        return toWire().getValueIn().bytes();
    }


    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(bytes(value));
    }

    @Override
    public V get(@NotNull final Object key) {

        return toObject(vClass, () -> {

            final byte[] bytes = bytes(key);
            final byte[] bytes1 = map.get(bytes);
            return bytes1;
        });
    }

    @Override
    public V getUsing(K key, V usingValue) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public V acquireUsing(@NotNull K key, V usingValue) {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    @Override
    public MapKeyContext<K, V> acquireContext(@NotNull K key, @NotNull V usingValue) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <R> R getMapped(K key, @NotNull SerializableFunction<? super V, R> function) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public V putMapped(K key, @NotNull UnaryOperator<V> unaryOperator) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void getAll(File toFile) throws IOException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void putAll(File fromFile) throws IOException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public V newValueInstance() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public K newKeyInstance() {
        return null;
    }

    @Override
    public Class<V> valueClass() {
        return null;
    }

    private <E> E toObject(Class<E> eClass, Supplier<byte[]> b) {
        final byte[] bytes = b.get();
        if (bytes == null)
            return null;
        final Wire wire = toWire(bytes);
        buffer.flip();
        return AbstactStatelessClient.readObject(wire.getValueIn(), null, eClass);
    }

    @Override
    public V put(final K key, final V value) {
        nullCheck(key);
        //  nullCheck(value);
        return toObject(vClass, () -> map.put(bytes(key), bytes(value)));
    }

    @Override
    public V remove(Object key) {
        nullCheck(key);
        return toObject(vClass, () -> map.remove(bytes(key)));
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {

        for (final Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
            map.put(bytes(e.getKey()), bytes(e.getValue()));
        }

    }

    @Override
    public void clear() {
        map.clear();
    }


    @NotNull
    @Override
    public Set<K> keySet() {

        return new AbstractSet<K>() {

            public Iterator<K> iterator() {
                return new Iterator<K>() {
                    private Iterator<Entry<K, V>> i = entrySet().iterator();

                    public boolean hasNext() {
                        return i.hasNext();
                    }

                    public K next() {
                        return i.next().getKey();
                    }

                    public void remove() {
                        i.remove();
                    }
                };
            }

            public int size() {
                return EngineMap.this.size();
            }

            public boolean isEmpty() {
                return EngineMap.this.isEmpty();
            }

            public void clear() {
                EngineMap.this.clear();
            }

            public boolean contains(Object k) {
                return EngineMap.this.containsKey(k);
            }
        };

    }


    public Collection<V> values() {
        return new AbstractCollection<V>() {
            public Iterator<V> iterator() {
                return new Iterator<V>() {
                    private Iterator<Entry<K, V>> i = entrySet().iterator();

                    public boolean hasNext() {
                        return i.hasNext();
                    }

                    public V next() {
                        return i.next().getValue();
                    }

                    public void remove() {
                        i.remove();
                    }
                };
            }

            public int size() {
                return EngineMap.this.size();
            }

            public boolean isEmpty() {
                return EngineMap.this.isEmpty();
            }

            public void clear() {
                EngineMap.this.clear();
            }

            public boolean contains(Object v) {
                return EngineMap.this.containsValue(v);
            }
        };
    }


    @NotNull
    @Override
    public Set<Entry<K, V>> entrySet() {

        return new AbstractSet<Entry<K, V>>() {


            @NotNull
            @Override
            public Iterator<Entry<K, V>> iterator() {
                final Iterator<Entry<byte[], byte[]>> iterator = map.entrySet().iterator();
                return new Iterator<Entry<K, V>>() {

                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public Entry<K, V> next() {

                        final Entry<byte[], byte[]> next = iterator.next();

                        return new Entry<K, V>() {

                            @Override
                            public K getKey() {
                                return toObject(kClass, () -> next.getKey());
                            }

                            @Override
                            public V getValue() {
                                return toObject(vClass, () -> next.getValue());
                            }

                            @Override
                            public V setValue(V value) {
                                throw new UnsupportedOperationException("todo");
                            }
                        };


                    }
                };
            }


            public int size() {
                return EngineMap.this.size();
            }

            public boolean isEmpty() {
                return EngineMap.this.isEmpty();
            }

            public void clear() {
                EngineMap.this.clear();
            }

            public boolean contains(Object v) {
                return EngineMap.this.containsValue(v);
            }


            @Override
            public boolean remove(Object o) {
                return EngineMap.this.remove(o) != null;
            }
        };
    }


    @Override
    public V putIfAbsent(K key, V value) {
        nullCheck(key);
        //  nullCheck(value);
        return toObject(vClass, () -> map.putIfAbsent(bytes(key), bytes(value)));
    }

    private void nullCheck(Object o) {
        if (o == null)
            throw new NullPointerException();
    }

    @Override
    public boolean remove(Object key, Object value) {
        nullCheck(key);

        return map.remove(bytes(key), bytes(value));
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {

        nullCheck(key);
        nullCheck(oldValue);
        nullCheck(newValue);
        return map.replace(bytes(key), bytes(oldValue), bytes(newValue));
    }

    @Override
    public V replace(K key, V value) {
        nullCheck(key);
        //  nullCheck(value);
        return toObject(vClass, () -> map.replace(bytes(key), bytes(value)));
    }

    @Override
    public File file() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public long longSize() {
        return map.size();
    }

    @Override
    public MapKeyContext<K, V> context(K key) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Class<K> keyClass() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public boolean forEachEntryWhile(Predicate<? super MapKeyContext<K, V>> predicate) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void forEachEntry(Consumer<? super MapKeyContext<K, V>> action) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void close() {
        try {
            mapWireConnectionHub.close();
        } catch (IOException e) {
            LOG.error("", e);
        }
    }

    @Override
    public String toString() {
        if (isEmpty())
            return "{}";
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        forEach((k, v) -> sb
                .append(k != this ? k : "(this Map)")
                .append('=')
                .append(v != this ? v : "(this Map)")
                .append(',').append(' '));
        if (sb.length() > 2)
            sb.setLength(sb.length() - 2);
        sb.append('}');
        return sb.toString();
    }
}
