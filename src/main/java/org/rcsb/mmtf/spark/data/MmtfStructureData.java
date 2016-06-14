package org.rcsb.mmtf.spark.data;

import org.apache.spark.api.java.JavaPairRDD;
import org.rcsb.mmtf.dataholders.MmtfStructure;
import org.rcsb.mmtf.spark.utils.SparkUtils;

/**
 * Class to hold the undecoded {@link MmtfStructure} data. Useful for basic analysis.
 * @author Anthony Bradley
 *
 */
public class MmtfStructureData {

	
	private JavaPairRDD<String, MmtfStructure> javaPairRdd;

	/**
	 * Constructor from a file. 
	 * @param inputPath the input path of the Hadoop sequence file to read
	 */
	public MmtfStructureData(String inputPath) {
		javaPairRdd = SparkUtils.getMmtfStructureRdd(inputPath);
	}

	
	/**
	 * Constructor from am RDD
	 * @param inputPath the input path of the Hadoop sequence file to read
	 */
	public MmtfStructureData(JavaPairRDD<String, MmtfStructure> inputRdd) {
		javaPairRdd = inputRdd;
	}
	/**
	 * @return the javaPairRdd
	 */
	public JavaPairRDD<String, MmtfStructure> getJavaPairRdd() {
		return javaPairRdd;
	}

}
