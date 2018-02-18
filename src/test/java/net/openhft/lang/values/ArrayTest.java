package net.openhft.lang.values;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

public class ArrayTest {
	@Test
    public void test() throws IOException {
		String TMP = System.getProperty("java.io.tmpdir");
        File file = new File(TMP + "/pf-PosistionsAndClose-" + System.nanoTime());
        ChronicleMap<Long, Double[]> writeMap = ChronicleMapBuilder
			    .of(Long.class, Double[].class)
			    .entries(1_000)
			    .averageValue(new Double[150])
			    .createPersistedTo(file); 
    	Double a[]={2D};
    	writeMap.put(1L,a);
    	
    	//read
        ChronicleMap<Long, Double[]> readMap =
        		ChronicleMapBuilder.of(Long.class, Double[].class)
        		.averageValue(new Double[150])
        		.createPersistedTo(file);
        Double b[] = readMap.get(1L);
	}
}
