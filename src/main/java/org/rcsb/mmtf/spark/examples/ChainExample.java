	package org.rcsb.mmtf.spark.examples;

import java.io.IOException;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.rcsb.mmtf.spark.data.SegmentDataRDD;
import org.rcsb.mmtf.spark.data.StructureDataRDD;

/**
 * An example of taking a list of PDB IDs, pulling them from the MMTF server and 
 * returning a {@link SegmentDataRDD} of their calpha chains. These can then be operated
 * upon.
 * @author Anthony Bradley
 *
 */
public class ChainExample {

	/**
	 * The main function to take the input IDs.
	 * @param args the arguments for the function
	 * @throws IOException due to an error reading from the URL
	 */
	public static void main(String[] args) throws IOException {
		StructureDataRDD structureDataRDD = new StructureDataRDD("/path/to/file");
		SegmentDataRDD calphaChains = structureDataRDD.getCalpha().filterLength(10, 300);
		JavaDoubleRDD lengthDist = calphaChains.getLengthDist().cache();
		System.out.println(lengthDist.mean());
		System.out.println(lengthDist.min());
		System.out.println(lengthDist.max());
		System.out.println(lengthDist.count());	
	}

}
