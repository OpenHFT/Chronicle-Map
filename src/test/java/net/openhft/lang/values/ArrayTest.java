package net.openhft.lang.values;

import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import static org.junit.Assert.assertEquals;

public class ArrayTest {
	private static final File TMP = new File(System.getProperty("java.io.tmpdir"));

    @Test
    public void test() throws IOException {
		ClassAliasPool.CLASS_ALIASES.addAlias(Double[].class);
        File file = new File(TMP + "ArrayTestMap" + System.nanoTime());
        //write
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
        
        assertEquals(a[0],b[0]);
        file.delete(); 
    }
}
