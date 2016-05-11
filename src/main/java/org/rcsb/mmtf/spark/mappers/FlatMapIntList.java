package org.rcsb.mmtf.spark.mappers;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

/**
 * Converts an Integer into a list of integers.
 * @author Anthony Bradley
 *
 */
public class FlatMapIntList implements FlatMapFunction<Integer,Integer> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6649798711352371580L;

	@Override
	public Iterable<Integer> call(Integer t) throws Exception {
		List<Integer> outList = new ArrayList<>();
		for(int i=0; i<t.intValue(); i++) {
			outList.add(i);
		}
		return outList; 
	}


}
