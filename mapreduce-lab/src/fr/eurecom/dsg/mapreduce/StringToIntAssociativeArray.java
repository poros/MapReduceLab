package fr.eurecom.dsg.mapreduce;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.Writable;

/*
 * Very simple (and scholastic) implementation of a Writable associative array for String to Int 
 *
 **/
public class StringToIntAssociativeArray implements Writable {

	// TODO: add an internal field that is the real associative array
	private HashMap<String, Integer> map = new HashMap<String, Integer>();

	@Override
	public void readFields(DataInput in) throws IOException {    
		// TODO: implement serialization
		map.clear(); //the framework tends to reuse the same object over time
		int size = in.readInt();
		for (int i = 0; i < size; i++) {
			map.put(in.readUTF(), in.readInt());
		}
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO: implement deserialization
		out.writeInt(map.size());
		for (Entry<String,Integer> pair : map.entrySet()) {
			out.writeUTF(pair.getKey());
			out.writeInt(pair.getValue());
		}
	}

	public Integer get(String key) {
		return map.get(key);
	}

	public void set(String key, Integer value) {
		map.put(key,value);
	}

	public String toString() {
		return map.toString();
	}
	
	public void clear() {
		map.clear();
	}
	
	public Set<Entry<String, Integer>> entrySet() {
		return map.entrySet();
	}
	
}
