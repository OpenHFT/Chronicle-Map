package net.openhft.chronicle.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub.CoreFields;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.ValueOut;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Created by Rob Austin
 */
public class MapWireConnectionHubByName<K, V> implements Map<K, V> {

    private final Class<K> kClass;
    private final Class<V> vClass;
    private final MapWireConnectionHub mapWireConnectionHub;


    Bytes<ByteBuffer> buffer = Bytes.elasticByteBuffer();

    private final Map<byte[], byte[]> map;


    Class<? extends Wire> wireType = TextWire.class;

    public MapWireConnectionHubByName(String name,
                                      Class<V> vClass,
                                      Class<K> kClass,
                                      MapWireConnectionHub mapWireConnectionHub) throws IOException {
        this.mapWireConnectionHub = mapWireConnectionHub;
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
        ValueOut valueOut = toWire().getValueOut();
        AbstactStatelessClient.writeField(key, valueOut);
        buffer.flip();
        byte[] bytes = new byte[(int) buffer.limit()];
        buffer.read(bytes);
        return bytes;
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(bytes(value));

    }

    @Override
    public V get(Object key) {
        byte[] bytes = map.get(bytes(key));

        if (bytes == null)
            return null;

        final Wire wire = toWire(bytes);
        return AbstactStatelessClient.<V>readObject(CoreFields.reply, wire, null, vClass);
    }

    @Override
    public V put(K key, V value) {
        final Wire wire = toWire(map.put(bytes(key), bytes(value)));
        return AbstactStatelessClient.<V>readObject(CoreFields.reply, wire, null, vClass);
    }

    @Override
    public V remove(Object key) {
        final Wire wire = toWire(map.remove(bytes(key)));
        return AbstactStatelessClient.<V>readObject(CoreFields.reply, wire, null, vClass);
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
        return null;
    }

}
