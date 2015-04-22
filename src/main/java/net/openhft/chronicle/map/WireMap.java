package net.openhft.chronicle.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Created by Rob Austin
 */
public class WireMap<K, V> implements Map<K, V> {

    private final Class<K> kClass;
    private final Class<V> vClass;
    private final MapWireConnectionHub mapWireConnectionHub;


    Bytes<ByteBuffer> buffer = Bytes.elasticByteBuffer();

    private final Map<byte[], byte[]> map;


    // todo
    private final Class<? extends Wire> wireType;

    public WireMap(String name,
                   Class<V> vClass,
                   Class<K> kClass,
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

    private byte[] bytes(Object key) {

        final Wire wire = toWire();
        AbstactStatelessClient.writeField(key, wire.getValueOut());
        wire.bytes().flip();

        return toWire().getValueIn().bytes();
    }


    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(bytes(value));
    }

    @Override
    public V get(@NotNull final Object key) {

        return toObject(vClass, new Supplier<byte[]>() {
            @Override
            public byte[] get() {

                final byte[] bytes = bytes(key);
                final byte[] bytes1 = map.get(bytes);
                return bytes1;
            }
        });
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
    public V put(@NotNull final K key, @NotNull final V value) {
        return toObject(vClass, () -> map.put(bytes(key), bytes(value)));
    }

    @Override
    public V remove(Object key) {
        return toObject(vClass, () -> map.remove(bytes(key)));
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void clear() {
        map.clear();
    }

    @NotNull
    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException("todo");
    }

}
