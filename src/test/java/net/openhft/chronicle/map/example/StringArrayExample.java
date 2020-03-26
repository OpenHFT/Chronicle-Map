package net.openhft.chronicle.map.example;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.values.Array;
import net.openhft.chronicle.values.MaxUtf8Length;
import net.openhft.chronicle.values.Values;
import org.junit.Test;

/**
 * an example of using a chronicle map with a string array like structure
 */
public class StringArrayExample {

    private final CharSequenceArray charSequenceArray = Values.newHeapInstance(CharSequenceArray.class);

    public interface CharSequenceArray {
        @Array(length = 8)
        void setCharSequenceWrapperAt(int index, CharSequenceWrapper value);

        CharSequenceWrapper getCharSequenceWrapperAt(int index);
    }

    public interface CharSequenceWrapper {
        void setCharSequence(@MaxUtf8Length(6) CharSequence charSequence);

        CharSequence getCharSequence();
    }

    @Test
    public void examplePutAndGet() {
        ChronicleMap<Integer, CharSequenceArray> map = ChronicleMapBuilder
                .of(Integer.class, CharSequenceArray.class)
                .entries(100)
                .create();
        {
            CharSequenceArray charSequenceArray = Values.newHeapInstance(CharSequenceArray.class);
            map.put(1, charSequenceArray);
        }
        {
            // compute - change the value in the array
            map.compute(1, this::setToHello);
        }

        {
            // get - read the value
            CharSequence charSequence = map.getUsing(1, charSequenceArray).getCharSequenceWrapperAt(1).getCharSequence();
            System.out.println(charSequence);
        }

        {
            // to string all the values
            System.out.println(map.getUsing(1, charSequenceArray).toString());
        }

    }

    private CharSequenceArray setToHello(final Integer integer, final CharSequenceArray charSequenceArray) {
        charSequenceArray.getCharSequenceWrapperAt(1).setCharSequence("hello");
        return charSequenceArray;
    }

}
