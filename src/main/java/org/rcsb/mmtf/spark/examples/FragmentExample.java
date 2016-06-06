package org.rcsb.mmtf.spark.examples;

import org.rcsb.mmtf.spark.data.SegmentClusters;
import org.rcsb.mmtf.spark.data.StructureDataRDD;

/**
 * An example generating fragments from the whole PDB and then clustering them.
 * @author Anthony Bradley
 *
 */
public class FragmentExample {

	
	/**
	 * Function to fragment and group those fragments based on sequence identity.
	 * @param args the arguments for the function
	 */
	public static void main(String[] args) {		
		new StructureDataRDD().getCalpha();
		SegmentClusters fragClusters = new StructureDataRDD().getFragments(8).groupBySequence();
		System.out.println(fragClusters.size());
	}
	
}
