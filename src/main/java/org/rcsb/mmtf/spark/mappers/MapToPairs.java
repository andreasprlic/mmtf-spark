package org.rcsb.mmtf.spark.mappers;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

/**
 * Map the individual numbers to pairs of integers - to do half matrix comparisons.
 * @author Anthony Bradley
 *
 */
public class MapToPairs implements PairFlatMapFunction<Integer,Integer,Integer> {

	/**
	 * The serial id for this version of the class
	 */
	private static final long serialVersionUID = -7282152297877946517L;
	private int totalNum;
	
	/**
	 * Constructor takes the to 
	 * @param totalNum the size of the matrix to calculate
	 */
	public MapToPairs(int totalNum) {
		this.totalNum = totalNum;
	}
	
	@Override
	public Iterable<Tuple2<Integer, Integer>> call(Integer t) throws Exception {
		List<Tuple2<Integer,Integer>> outList = new ArrayList<>();
		for(int j=t.intValue()+1; j<totalNum; j++) {
			outList.add(new Tuple2<Integer, Integer>(t.intValue(), j));
		}
		return outList;
	}



}
