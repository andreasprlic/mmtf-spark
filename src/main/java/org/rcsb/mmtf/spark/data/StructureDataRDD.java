package org.rcsb.mmtf.spark.data;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import javax.vecmath.Point3d;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.partial.BoundedDouble;
import org.apache.spark.partial.PartialResult;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.encoder.GenericEncoder;
import org.rcsb.mmtf.encoder.ReducedEncoder;
import org.rcsb.mmtf.serialization.MessagePackSerialization;
import org.rcsb.mmtf.spark.mappers.GenerateSegments;
import org.rcsb.mmtf.spark.utils.SparkUtils;

import scala.Tuple2;

/**
 * A class to provide functions on a series of 
 * {@link StructureDataInterface} objects.
 * @author Anthony Bradley
 *
 */
public class StructureDataRDD {

	/** The RDD of the {@link StructureDataInterface} data. */
	private JavaPairRDD<String, StructureDataInterface> javaPairRdd;


	/**
	 * Empty constructor reads the sample data if {@link SparkUtils} has not been set 
	 * with a path.
	 */
	public StructureDataRDD() {
		setupRdd();
	}
	
	/**
	 * A constructor to download the PDB on construction.
	 * @param download whether to update or not
	 * @throws IOException due to retrieving data from the URL
	 * @throws FileNotFoundException whilst transferring data
	 */
	public StructureDataRDD(boolean download) throws FileNotFoundException, IOException {
		SparkUtils.downloadPdb();
		setupRdd();
	}
	
	/**
	 * Function to setup an RDD of data.
	 */
	private void setupRdd() {
		String filePath = SparkUtils.getFilePath();
		if(filePath==null){
			// First try the full
			if(SparkUtils.getFullPdbFile()!=null && new File(SparkUtils.getFullPdbFile()).exists()) {
				System.out.println("Using full PDB data.");
				javaPairRdd = SparkUtils.getStructureDataRdd(SparkUtils.getFullPdbFile());
			}
			else{
				URL inputPath = SparkUtils.class.getClassLoader().getResource("hadoop/subset");
				// Set the config for the spark context
				javaPairRdd = SparkUtils.getStructureDataRdd(inputPath.toString());
				System.out.println("Full data not available");
				System.out.println("Using small 1% subset");
			}
		}
		else{
			javaPairRdd = SparkUtils.getStructureDataRdd(SparkUtils.getFilePath());
		}		
	}

	/**
	 * Constructor from a file. 
	 * @param inputPath the input path of the Hadoop sequence file to read
	 */
	public StructureDataRDD(String inputPath) {
		// Set the config for the spark context
		javaPairRdd = SparkUtils.getStructureDataRdd(inputPath);
	}

	/**
	 * Constructor from a {@link JavaPairRDD} of {@link String} and {@link StructureDataInterface}.
	 * @param javaPairRDD the input {@link JavaPairRDD} of 
	 * {@link String} {@link StructureDataInterface}
	 */
	public StructureDataRDD(JavaPairRDD<String, StructureDataInterface> javaPairRDD) {
		// Set the config for the spark context
		this.javaPairRdd = javaPairRDD;
	}

	/**
	 * Constructor from a list of PDB ids.
	 * @param pdbIdList the input list of PDB ids
	 * @throws IOException due to reading from the URL
	 */
	public StructureDataRDD(List<String> pdbIdList) throws IOException {
		this.javaPairRdd = SparkUtils.getFromList(pdbIdList);
	}
	
	/**
	 * Construtor from an array of PDB ids.
	 * @param pdbIdArray the input array of PDB ids
	 * @throws IOException due to reading from the URL
	 */
	public StructureDataRDD(String[] pdbIdArray) throws IOException {
		this.javaPairRdd = SparkUtils.getFromList(Arrays.asList(pdbIdArray));
	}

	/**
	 * Get the {@link JavaPairRDD} of {@link String} {@link StructureDataInterface}
	 * for this instance
	 * @return the {@link JavaPairRDD} of {@link String} {@link StructureDataInterface}
	 */
	public JavaPairRDD<String, StructureDataInterface> getJavaRdd() {
		return javaPairRdd;
	}

	/**
	 * Filter the {@link StructureDataRDD} based on R-free.
	 * @param maxRFree the maximum allowed R-free
	 * @return the filtered {@link StructureDataRDD}
	 */
	public StructureDataRDD filterRfree(double maxRFree) {
		return new StructureDataRDD(javaPairRdd.filter(t -> t._2.getRfree()<maxRFree));
	}
	/**
	 * Filter the {@link StructureDataRDD} based on resolution.
	 * @param maxRes the maximum allowed resolution (in Angstrom)
	 * @return the filtered {@link StructureDataRDD}
	 */
	public StructureDataRDD filterResolution(double maxRes) {
		return new StructureDataRDD(javaPairRdd.filter(t -> t._2.getResolution()<maxRes));
	}

	/**
	 * Get the {@link Point3d} of the C-alpha co-ordinate data
	 * as lightweight point 3d objects.
	 * @return the segment  data
	 * array
	 */
	public SegmentDataRDD getCalpha() {	
		return new SegmentDataRDD(javaPairRdd
				.flatMapToPair(new GenerateSegments(null)));
	}
	
	/**
	 * Covert the {@link StructureDataRDD} to the reduced format.
	 * @return the {@link StructureDataRDD} of the reduced format
	 */
	public StructureDataRDD getReduced() {
		JavaPairRDD<String, StructureDataInterface> reduced = javaPairRdd.mapToPair(t -> new Tuple2<String, StructureDataInterface>(t._1, ReducedEncoder.getReduced(t._2)));
		return new StructureDataRDD(reduced);
	}
	
	/**
	 * Get the {@link Point3d} of the C-alpha co-ordinate data
	 * as lightweight point 3d objects. Fragmented based on size. The fragments
	 * are continuous and overlapping.
	 * @param fragSize the size of every fragment
	 * @return the fragments as an RDD
	 */
	public SegmentDataRDD getFragments(int fragSize) {	
		return new SegmentDataRDD(javaPairRdd
				.flatMapToPair(new GenerateSegments(fragSize)));
	}


	/**
	 * Get the number of entries in the RDD.
	 * @return the {@link Long} number of entries
	 */
	public Long size() {
		return javaPairRdd
				.count();
	}

	/**
	 * Save the data as a Hadoop sequence file.
	 * @param filePath the path to save the data to
	 */
	public void saveToFile(String filePath) {
		javaPairRdd
		.mapToPair( t -> {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			new  MessagePackSerialization().serialize(new GenericEncoder(t._2).getMmtfEncodedStructure(), bos);
			return new Tuple2<String, byte[]>(t._1, SparkUtils.gzipCompress(
					bos.toByteArray()));
		})
		.mapToPair(t -> new Tuple2<Text, BytesWritable>(new Text(t._1), new BytesWritable(t._2)))
		.saveAsHadoopFile(filePath, Text.class, BytesWritable.class, SequenceFileOutputFormat.class);
	}


	/**
	 * Allow the user to sample the data.
	 * @param fraction the fraction of data 
	 * to be used (e.g. 0.1 retains 10%)
	 * @return the {@link SegmentDataRDD} updated
	 */
	public StructureDataRDD sample(double fraction) {
		return new StructureDataRDD(javaPairRdd.sample(false, fraction));
	}

	/**
	 * Cache the underlying RDD.
	 * @return the cached {@link StructureDataRDD}
	 */
	public StructureDataRDD cache() {
		javaPairRdd.cache();
		return this;
	}


}
