package net.openhft.lang.values;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.junit.Test;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

public class ArrayTest {
	
	static String TMP = System.getProperty("java.io.tmpdir");
	
	@Test
	public void test0() throws IOException{
		
		File file = new File(TMP + "/pf-PosistionsAndClose-" + System.nanoTime());
		
		ChronicleMap<Long, MovingAverageCompact[]> mapWrite = ChronicleMap
	    .of(Long.class, MovingAverageCompact[].class)
	    .entries(100)
	    .averageValue(createSampleWithSize(6))
	    .createPersistedTo(file);
		mapWrite.put(1L, createSampleWithSize(6));
		mapWrite.close();


		ChronicleMap<UUID, MovingAverageCompact[]> mapRead = ChronicleMapBuilder
	    .of(UUID.class, MovingAverageCompact[].class)
	    .averageValue(createSampleWithSize(6))
	    .createPersistedTo(file);
		MovingAverageCompact[] m = mapRead.get(1L);
	}
	
	private MovingAverageCompact[] createSampleWithSize(int size){
		MovingAverageCompact[] sample = new MovingAverageCompact[size];
		for(int i=0;i<sample.length;i++){
			sample[i]=new MovingAverageCompact();
		}
		return sample;
	}
	
	
	@Test
    public void test() throws IOException {
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
