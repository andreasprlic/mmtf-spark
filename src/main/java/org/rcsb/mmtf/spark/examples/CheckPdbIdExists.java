package org.rcsb.mmtf.spark.examples;

import org.rcsb.mmtf.spark.data.StructureDataRDD;

/**
 * Check a given PDB exists in a hadoop sequence file
 * @author Anthony Bradley
 *
 */
public class CheckPdbIdExists {

	/**
	 * Main function to check if a given file is in the PDB file and to 
	 * check if all can be parsed.
	 * @param args no input args
	 */
	public static void main(String[] args) {
		StructureDataRDD structureDataRDD = new StructureDataRDD();
		long count = structureDataRDD.getJavaRdd()
				.filter(t -> t._2.getStructureId().equals("5E5Q"))
				.count();
		System.out.println(count);
		
	}
	
	
}
