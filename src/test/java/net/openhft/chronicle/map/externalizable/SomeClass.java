package net.openhft.chronicle.map.externalizable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

public class SomeClass implements Externalizable {

    final List<String> hits = new ArrayList<>();
    private long id;

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(id);
        out.writeInt(hits.size());
        for (int i = 0; i < hits.size(); i++) {
            out.writeObject(hits.get(i));
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = in.readLong();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            hits.add((String) in.readObject());
        }
    }
}