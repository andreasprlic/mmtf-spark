package org.rcsb.mmtf.spark.examples;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.rcsb.mmtf.spark.data.StructureDataRDD;

/** 
 * Simple example of how to download the PDB.
 * @author Anthony Bradley
 *
 */
public class DownloadPdb {

	/**
	 * Simple example of how to download the PDB.
	 * @param args the arguments for the function
	 * @throws IOException due to an error reading from the URL
	 * @throws FileNotFoundException an error transferring files
	 */
	public static void main(String[] args) throws FileNotFoundException, IOException {
		new StructureDataRDD(true);
	}

}
