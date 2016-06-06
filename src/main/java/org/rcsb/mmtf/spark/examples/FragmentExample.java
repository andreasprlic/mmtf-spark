package org.rcsb.mmtf.spark.examples;

import org.rcsb.mmtf.spark.data.SegmentClusters;
import org.rcsb.mmtf.spark.data.SegmentDataRDD;
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
		StructureDataRDD structureDataRDD = new StructureDataRDD("/path/to/hadoopfolder");
		SegmentDataRDD framgents = structureDataRDD.getFragments(8);
		SegmentClusters fragClusters =  framgents.groupBySequence();
        System.out.println("Number of different sequences of length 8: "+fragClusters.size());
	}
	
}
